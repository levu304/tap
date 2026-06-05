//! Raw PostgreSQL wire-protocol replication client.
//!
//! Implements the `COPY_BOTH` protocol needed for logical replication
//! streaming.  This module bypasses `tokio-postgres` 0.7 (which does not
//! support `copy_both`) and talks the wire protocol directly over TCP/TLS.
//!
//! # Flow
//!
//! 1. Open TCP connection (optionally wrapped in TLS).
//! 2. Send [`StartupMessage`] with `replication=database`.
//! 3. Authenticate (cleartext password, MD5, or SCRAM-SHA-256 SASL).
//! 4. Send `START_REPLICATION` as a `Query` message.
//! 5. Read `CopyBothResponse`, then spawn a background reader that feeds
//!    a channel-backed [`ReplicationStream`].
//! 6. The reader parses `XLogData` frames (strips 25-byte header) and
//!    auto-responds to `Keepalive` messages with `StandbyStatusUpdate`.

use std::pin::Pin;
use std::task::{Context, Poll};

use base64::Engine;
use futures::Stream;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::config::{validate_identifier, SourceConfig, SslMode};
use crate::error::TapError;
use crate::postgres::Lsn;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// PostgreSQL protocol version 3.0 (196608 = 0x00030000).
const PG_PROTOCOL_VERSION: i32 = 196608;

/// Maximum wire-message payload we are willing to buffer (64 MiB).
const MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

/// Channel depth for WAL data between the background reader and the Stream.
const CHANNEL_CAPACITY: usize = 1024;

/// Interval (seconds) to send a timed standby status update even when the
/// server hasn't requested one.
const HEARTBEAT_INTERVAL_SECS: u64 = 10;

/// SSLRequest code for the pre-TLS negotiation message.
/// Sent as: Int32(8) | Int32(80877103)
const SSL_REQUEST_CODE: i32 = 80877103;

// ---------------------------------------------------------------------------
// MaybeTls — unified AsyncRead + AsyncWrite for plain / TLS streams
// ---------------------------------------------------------------------------

/// A pooled stream type that erases whether the connection is plain TCP or
/// TLS-wrapped.
enum MaybeTls {
    Plain(tokio::net::TcpStream),
    Tls(tokio_native_tls::TlsStream<tokio::net::TcpStream>),
}

impl AsyncRead for MaybeTls {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        match &mut *self {
            MaybeTls::Plain(s) => Pin::new(s).poll_read(cx, buf),
            MaybeTls::Tls(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for MaybeTls {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        match &mut *self {
            MaybeTls::Plain(s) => Pin::new(s).poll_write(cx, buf),
            MaybeTls::Tls(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        match &mut *self {
            MaybeTls::Plain(s) => Pin::new(s).poll_flush(cx),
            MaybeTls::Tls(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        match &mut *self {
            MaybeTls::Plain(s) => Pin::new(s).poll_shutdown(cx),
            MaybeTls::Tls(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

// ---------------------------------------------------------------------------
// ReplicationStream
// ---------------------------------------------------------------------------

/// A stream of raw WAL payload bytes from a Postgres logical replication
/// connection.
///
/// Backed by an mpsc channel that is fed by a background task reading from
/// a raw TCP/TLS connection.  The 25-byte `XLogData` header (byte `'w'`,
/// start LSN, end LSN, timestamp) is stripped — only the WAL data bytes
/// are yielded.
pub struct ReplicationStream {
    rx: mpsc::Receiver<Result<Vec<u8>, TapError>>,
}

impl ReplicationStream {
    /// Create a stream from an mpsc receiver (used internally and for tests).
    pub fn from_receiver(rx: mpsc::Receiver<Result<Vec<u8>, TapError>>) -> Self {
        Self { rx }
    }
}

impl Stream for ReplicationStream {
    type Item = Result<Vec<u8>, TapError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

// ---------------------------------------------------------------------------
// Public entry-point
// ---------------------------------------------------------------------------

/// Connect to Postgres over TCP/TLS, authenticate, issue `START_REPLICATION`,
/// and return a [`ReplicationStream`] yielding WAL payload bytes.
///
/// The connection is independent of the tokio-postgres `Client` used by
/// [`PgConnection`](crate::postgres::PgConnection) — only this raw stream
/// carries the COPY_BOTH protocol.
///
/// # Errors
///
/// Returns [`TapError::Io`] for network issues,
/// [`TapError::PostgresConnectionRedacted`] for auth or protocol errors,
/// and [`TapError::Config`] for configuration problems.
pub async fn start(
    config: &SourceConfig,
    slot_name: &str,
    publication: &str,
    start_lsn: Lsn,
    plugin: &str,
) -> Result<ReplicationStream, TapError> {
    info!(
        "starting replication stream (slot={slot_name}, publication={publication}, \
         lsn={start_lsn}, plugin={plugin})"
    );

    // Validate replication identifiers before any network I/O
    validate_identifier(slot_name, "slot_name")?;
    validate_identifier(publication, "publication")?;
    validate_identifier(plugin, "plugin")?;

    // 1. TCP connect
    let addr = format!("{}:{}", config.host, config.port);
    info!("connecting to {addr}");
    let mut tcp = tokio::net::TcpStream::connect(&addr)
        .await
        .map_err(|e| TapError::PostgresConnectionRedacted(format!("TCP connect failed: {e}")))?;

    // 2. TLS wrapper (if configured)
    //
    // Before wrapping, the Postgres postmaster requires an explicit SSLRequest
    // handshake.  We send 8 bytes (length=8, code=80877103) and the server
    // responds with a single byte: 'S' (proceed with TLS) or 'N' (refuse).
    // Without this pre-exchange, the postmaster sees ClientHello bytes as a
    // malformed startup message and closes the connection.
    let mut stream: MaybeTls = if config.ssl_mode == SslMode::Disable {
        MaybeTls::Plain(tcp)
    } else {
        info!("sending SSLRequest handshake");
        let ssl_request = [
            (8i32).to_be_bytes(),
            SSL_REQUEST_CODE.to_be_bytes(),
        ]
        .concat();
        tokio::io::AsyncWriteExt::write_all(&mut tcp, &ssl_request)
            .await
            .map_err(|e| {
                TapError::PostgresConnectionRedacted(format!("SSLRequest write failed: {e}"))
            })?;
        tokio::io::AsyncWriteExt::flush(&mut tcp)
            .await
            .map_err(|e| {
                TapError::PostgresConnectionRedacted(format!("SSLRequest flush failed: {e}"))
            })?;

        let mut response = [0u8; 1];
        tokio::io::AsyncReadExt::read_exact(&mut tcp, &mut response)
            .await
            .map_err(|e| {
                TapError::PostgresConnectionRedacted(format!("SSLRequest read failed: {e}"))
            })?;

        if response[0] != b'S' {
            return Err(TapError::PostgresConnectionRedacted(format!(
                "server rejected TLS connection (response byte: 0x{:02x})",
                response[0]
            )));
        }

        info!("wrapping connection with TLS");
        let native_connector = native_tls::TlsConnector::builder().build().map_err(|e| {
            TapError::PostgresConnectionRedacted(format!("failed to build TLS connector: {e}"))
        })?;
        let connector = tokio_native_tls::TlsConnector::from(native_connector);
        let tls = connector.connect(&config.host, tcp).await.map_err(|e| {
            TapError::PostgresConnectionRedacted(format!("TLS handshake failed: {e}"))
        })?;
        MaybeTls::Tls(tls)
    };

    // 3. Send startup message
    send_startup(&mut stream, config).await?;

    // 4. Authenticate
    authenticate(&mut stream, config).await?;

    // 5. Consume ReadyForQuery
    read_ready_for_query(&mut stream).await?;

    // 6. Send START_REPLICATION
    // pgoutput accepts: publication_names, binary, proto_version, messages,
    // streaming, two_phase.  The plugin is bound at slot-creation time, not
    // passed here.  For future plugin compatibility the caller provides a
    // `plugin` parameter, but pgoutput uses `publication_names`.
    let lsn_str = start_lsn.to_string();
    let query = format!(
        "START_REPLICATION SLOT \"{slot_name}\" LOGICAL {lsn_str} \
         (publication_names '{publication}')"
    );
    send_query(&mut stream, &query).await?;

    // 7. Read CopyBothResponse
    read_copy_both_response(&mut stream).await?;

    // 8. Spawn background reader task
    let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
    tokio::spawn(reader_task(stream, tx));

    info!("replication stream established");
    Ok(ReplicationStream::from_receiver(rx))
}

// ---------------------------------------------------------------------------
// Wire-protocol helpers
// ---------------------------------------------------------------------------

/// Send a PostgreSQL startup message.
///
/// The startup message uses a different framing from regular messages:
/// Int32 length | Int32 protocol_version | key\0value\0...\0
async fn send_startup(stream: &mut MaybeTls, config: &SourceConfig) -> Result<(), TapError> {
    let user_param = format!("user\0{}\0", config.user);
    let db_param = format!("database\0{}\0", config.dbname);
    let repl_param = "replication\0database\0".to_string();

    let mut payload = Vec::with_capacity(128);
    payload.extend_from_slice(&PG_PROTOCOL_VERSION.to_be_bytes());
    payload.extend_from_slice(user_param.as_bytes());
    payload.extend_from_slice(db_param.as_bytes());
    payload.extend_from_slice(repl_param.as_bytes());
    payload.push(0); // terminator

    let len = (payload.len() + 4) as i32; // +4 for the length field itself
    let mut buf = Vec::with_capacity(payload.len() + 4);
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(&payload);

    stream.write_all(&buf).await.map_err(wrap_io_err)?;
    stream.flush().await.map_err(wrap_io_err)?;
    Ok(())
}

/// Complete the authentication exchange.
///
/// Reads the server's `Authentication*` message and sends the appropriate
/// response.  Handles:
///
/// | Type | Variant                      |
/// |------|------------------------------|
/// | 0    | AuthenticationOk             |
/// | 3    | CleartextPassword            |
/// | 5    | MD5Password                  |
/// | 10   | SASL (SCRAM-SHA-256 / SCRAM-SHA-256-PLUS) |
async fn authenticate(stream: &mut MaybeTls, config: &SourceConfig) -> Result<(), TapError> {
    loop {
        let msg_type = read_u8(stream).await?;
        if msg_type != b'R' {
            return Err(proto_err(format!(
                "expected Authentication message ('R'), got 0x{msg_type:02x}"
            )));
        }

        let _len = read_i32(stream).await?; // includes self + type
        let auth_type = read_i32(stream).await?;

        match auth_type {
            0 => {
                // AuthenticationOk
                debug!("authentication ok");
                return Ok(());
            }
            3 => {
                // AuthenticationCleartextPassword
                debug!("auth: cleartext password requested");
                send_password_message(stream, config.password.as_bytes()).await?;
            }
            5 => {
                // AuthenticationMD5Password
                debug!("auth: MD5 password requested");
                let mut salt = [0u8; 4];
                stream.read_exact(&mut salt).await.map_err(wrap_io_err)?;
                let inner_digest =
                    md5_digest(&[config.password.as_bytes(), config.user.as_bytes()].concat());
                let mut combined = inner_digest.as_bytes().to_vec();
                combined.extend_from_slice(&salt);
                let hash = md5_digest(&combined);
                let response = format!("md5{hash}");
                send_password_message(stream, response.as_bytes()).await?;
            }
            10 => {
                // AuthenticationSASL
                debug!("auth: SASL requested");
                let mechanisms = read_sasl_mechanisms(stream).await?;
                let chosen = mechanisms
                    .iter()
                    .find(|m| *m == "SCRAM-SHA-256")
                    .or_else(|| mechanisms.iter().find(|m| *m == "SCRAM-SHA-256-PLUS"))
                    .ok_or_else(|| {
                        TapError::PostgresConnectionRedacted(
                            "no supported SASL mechanism found (need SCRAM-SHA-256)".into(),
                        )
                    })?;

                if chosen == "SCRAM-SHA-256-PLUS" {
                    // We don't support channel binding, but we can still
                    // fall through with regular SCRAM-SHA-256.
                    warn!("SCRAM-SHA-256-PLUS requested but not implemented; server may reject");
                }

                let password = config.password.as_bytes();
                let client_nonce = generate_nonce();
                let client_first_bare =
                    format!("n={},r={}", scram_client_first_bare(config), client_nonce);
                let client_first = String::from("n,,") + &client_first_bare;
                let client_first_bytes = client_first.as_bytes();
                let client_first_len = client_first_bytes.len() as i32;

                // SASLInitialResponse
                let mechanism = b"SCRAM-SHA-256";
                let mut sasl_resp = Vec::new();
                sasl_resp.extend_from_slice(mechanism);
                sasl_resp.push(0); // null-terminated mechanism
                sasl_resp.extend_from_slice(&client_first_len.to_be_bytes());
                sasl_resp.extend_from_slice(client_first_bytes);

                let mut pw_msg = Vec::new();
                pw_msg.push(b'p');
                pw_msg.extend_from_slice(&(sasl_resp.len() as i32 + 4).to_be_bytes());
                pw_msg.extend_from_slice(&sasl_resp);
                stream.write_all(&pw_msg).await.map_err(wrap_io_err)?;
                stream.flush().await.map_err(wrap_io_err)?;

                // Read AuthenticationSASLContinue (type 11)
                let msg_type = read_u8(stream).await?;
                if msg_type != b'R' {
                    return Err(proto_err(format!(
                        "expected SASLContinue ('R'), got 0x{msg_type:02x}"
                    )));
                }
                let _len = read_i32(stream).await?;
                let sasl_type = read_i32(stream).await?;
                if sasl_type != 11 {
                    return Err(proto_err(format!(
                        "expected SASLContinue (type 11), got {sasl_type}"
                    )));
                }

                let server_first = read_string_to_nul(stream).await?;
                debug!("SCRAM server-first: {server_first}");

                // Parse server-first
                let (salt_b64, iterations, server_nonce) =
                    parse_scram_server_first(&server_first, &client_nonce)?;

                // Compute client-final
                let client_final_without_proof = format!("c=biws,r={server_nonce}");
                let auth_message =
                    format!("{client_first_bare},{server_first},{client_final_without_proof}");

                let salted_password = hi(
                    password,
                    &base64::engine::general_purpose::STANDARD
                        .decode(&salt_b64)
                        .map_err(|e| TapError::Decode(format!("invalid SCRAM salt base64: {e}")))?,
                    iterations,
                );
                let client_key = hmac_sha256(&salted_password, b"Client Key");
                let stored_key = sha256(&client_key);
                let client_signature = hmac_sha256(&stored_key, auth_message.as_bytes());
                let client_proof = xor_bytes(&client_key, &client_signature);
                let client_proof_b64 =
                    base64::engine::general_purpose::STANDARD.encode(&client_proof);

                let client_final = format!("{client_final_without_proof},p={client_proof_b64}");
                let client_final_bytes = client_final.as_bytes();

                // Send SASLResponse (type 'p')
                // Format: 'p' | Int32 len | ByteN client-final-message
                // Per the PostgreSQL protocol, SASLResponse is a plain
                // PasswordMessage containing just the client-final bytes —
                // no extra inner length prefix (unlike SASLInitialResponse).
                let resp = build_sasl_response(client_final_bytes);

                stream.write_all(&resp).await.map_err(wrap_io_err)?;
                stream.flush().await.map_err(wrap_io_err)?;

                // Read AuthenticationSASLFinal (type 12) or AuthenticationOk
                let msg_type = read_u8(stream).await?;
                if msg_type != b'R' {
                    return Err(proto_err(format!(
                        "expected SASLFinal/Ok ('R'), got 0x{msg_type:02x}"
                    )));
                }
                let _len = read_i32(stream).await?;
                let sasl_type = read_i32(stream).await?;
                match sasl_type {
                    0 => {
                        debug!("SASL authentication ok (after final)");
                        return Ok(());
                    }
                    12 => {
                        let _server_final = read_string_to_nul(stream).await?;
                        debug!("SASL server-final received");
                        // Verify server signature (optional but recommended)
                        return Ok(());
                    }
                    other => {
                        return Err(proto_err(format!(
                            "expected SASLFinal (12) or Ok (0), got {other}"
                        )));
                    }
                }
            }
            other => {
                return Err(TapError::PostgresConnectionRedacted(format!(
                    "unsupported authentication type: {other}"
                )));
            }
        }
    }
}

/// Send a PasswordMessage ('p').
async fn send_password_message(stream: &mut MaybeTls, password: &[u8]) -> Result<(), TapError> {
    let mut msg = Vec::new();
    msg.push(b'p');
    // Include null terminator in the password field
    let len = (password.len() + 1 + 4) as i32; // +4 for the length field itself
    msg.extend_from_slice(&len.to_be_bytes());
    msg.extend_from_slice(password);
    msg.push(0); // null terminator for the password string
    stream.write_all(&msg).await.map_err(wrap_io_err)?;
    stream.flush().await.map_err(wrap_io_err)?;
    Ok(())
}

/// Read a Query response until we see ReadyForQuery ('Z').
///
/// Some auth responses are followed by ReadyForQuery, and the
/// START_REPLICATION command also expects a specific response.
async fn read_ready_for_query(stream: &mut MaybeTls) -> Result<(), TapError> {
    loop {
        let msg_type = read_u8(stream).await?;
        match msg_type {
            b'Z' => {
                let _len = read_i32(stream).await?;
                let _status = read_u8(stream).await?; // 'I', 'T', or 'E'
                return Ok(());
            }
            b'E' => {
                let error_msg = read_error_response(stream).await?;
                return Err(TapError::PostgresConnectionRedacted(error_msg));
            }
            b'N' => {
                // NoticeResponse — skip
                let len = read_i32(stream).await?;
                skip_bytes(stream, (len - 4) as usize).await?;
            }
            b'K' => {
                // BackendKeyData — skip
                let len = read_i32(stream).await?;
                skip_bytes(stream, (len - 4) as usize).await?;
            }
            other => {
                debug!(
                    "skipping unexpected message type 0x{other:02x} (waiting for ReadyForQuery)"
                );
                let len = read_i32(stream).await?;
                skip_bytes(stream, (len - 4) as usize).await?;
            }
        }
    }
}

/// Send a Query ('Q') message.
async fn send_query(stream: &mut MaybeTls, query: &str) -> Result<(), TapError> {
    let mut msg = Vec::new();
    msg.push(b'Q');
    let len = (query.len() + 1 + 4) as i32; // +4 for length field, +1 for \0
    msg.extend_from_slice(&len.to_be_bytes());
    msg.extend_from_slice(query.as_bytes());
    msg.push(0);
    stream.write_all(&msg).await.map_err(wrap_io_err)?;
    stream.flush().await.map_err(wrap_io_err)?;
    Ok(())
}

/// Read a CopyBothResponse ('W') message.
async fn read_copy_both_response(stream: &mut MaybeTls) -> Result<(), TapError> {
    let msg_type = read_u8(stream).await?;
    match msg_type {
        b'W' => {
            let len = read_i32(stream).await?;
            // CopyBothResponse: Int8 overall_format, Int16 num_cols,
            // Int16 format_codes[num_cols]
            skip_bytes(stream, (len - 4) as usize).await?;
            debug!("received CopyBothResponse");
            Ok(())
        }
        b'E' => {
            let error_msg = read_error_response(stream).await?;
            Err(TapError::PostgresConnectionRedacted(format!(
                "START_REPLICATION rejected: {error_msg}"
            )))
        }
        other => Err(proto_err(format!(
            "expected CopyBothResponse ('W') or ErrorResponse ('E'), got 0x{other:02x}"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Background reader task
// ---------------------------------------------------------------------------

/// Background task that reads CopyData messages from the wire, parses
/// XLogData frames, auto-replies to Keepalive, and sends WAL payloads
/// through the channel.
async fn reader_task(mut stream: MaybeTls, tx: mpsc::Sender<Result<Vec<u8>, TapError>>) {
    let mut last_received_lsn: i64 = 0;
    let mut last_flushed_lsn: i64 = 0;
    let mut last_keepalive_time: tokio::time::Instant = tokio::time::Instant::now();

    loop {
        // Read message type byte
        let msg_type = match read_u8(&mut stream).await {
            Ok(t) => t,
            Err(e) => {
                let _ = tx.send(Err(e)).await;
                return;
            }
        };

        match msg_type {
            b'd' => {
                // CopyData
                let len = match read_i32(&mut stream).await {
                    Ok(l) => l as usize,
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                };
                let payload_len = len.saturating_sub(4);
                if payload_len > MAX_MESSAGE_SIZE {
                    let _ = tx
                        .send(Err(TapError::Decode(format!(
                            "CopyData payload too large: {payload_len} bytes"
                        ))))
                        .await;
                    return;
                }

                let mut payload = vec![0u8; payload_len];
                if let Err(e) = stream.read_exact(&mut payload).await {
                    let _ = tx.send(Err(wrap_io_err(e))).await;
                    return;
                }

                if payload.is_empty() {
                    debug!("empty CopyData payload (possible keepalive)");
                    // Send empty slot to keep the stream alive
                    let _ = tx.send(Ok(Vec::new())).await;
                    continue;
                }

                let sub_type = payload[0];
                match sub_type {
                    b'w' => {
                        // XLogData: Byte1 'w' | Int64 start_lsn | Int64 end_lsn
                        //           | Int64 timestamp | Byte[n] data
                        if payload.len() < 25 {
                            let _ = tx
                                .send(Err(TapError::Decode(format!(
                                    "truncated XLogData: {} bytes",
                                    payload.len()
                                ))))
                                .await;
                            return;
                        }
                        let _start_lsn = i64::from_be_bytes(payload[1..9].try_into().unwrap());
                        let end_lsn = i64::from_be_bytes(payload[9..17].try_into().unwrap());
                        let _timestamp = i64::from_be_bytes(payload[17..25].try_into().unwrap());

                        last_received_lsn = end_lsn;

                        // Yield the actual WAL data (after the 25-byte header)
                        let wal_data = payload[25..].to_vec();
                        if !wal_data.is_empty() && tx.send(Ok(wal_data)).await.is_err() {
                            // Receiver dropped
                            return;
                        }
                        // Even if empty, the stream is alive
                    }
                    b'k' => {
                        // Keepalive: Byte1 'k' | Int64 end_lsn | Int64 ts
                        //            | Byte1 reply_required
                        if payload.len() < 18 {
                            debug!("truncated Keepalive message, skipping");
                            continue;
                        }
                        let wal_end = i64::from_be_bytes(payload[1..9].try_into().unwrap());
                        let _ts = i64::from_be_bytes(payload[9..17].try_into().unwrap());
                        let reply_required = payload.len() >= 18 && payload[17] != 0;

                        last_flushed_lsn = wal_end;
                        last_received_lsn = wal_end;

                        if reply_required {
                            debug!("sending standby status update (keepalive requested)");
                            if let Err(e) = send_standby_status_update(
                                &mut stream,
                                last_received_lsn,
                                last_flushed_lsn,
                                false,
                            )
                            .await
                            {
                                let _ = tx.send(Err(e)).await;
                                return;
                            }
                            last_keepalive_time = tokio::time::Instant::now();
                        }

                        // Yield an empty Vec to allow the stream consumer
                        // to observe progress
                        let _ = tx.send(Ok(Vec::new())).await;
                    }
                    b'r' => {
                        // StandbyStatusUpdate (echoed back by server) — skip
                        debug!("received standby status update echo");
                    }
                    other => {
                        debug!(
                            "unknown CopyData sub-type 0x{other:02x}, skipping {} bytes",
                            payload_len
                        );
                    }
                }
            }
            b'c' => {
                // CopyDone — server is done sending
                info!("server sent CopyDone, replication stream ending");
                return;
            }
            b'E' => {
                // ErrorResponse
                let error_msg = match read_error_response_raw(&mut stream).await {
                    Ok(m) => m,
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                };
                let _ = tx
                    .send(Err(TapError::PostgresConnectionRedacted(error_msg)))
                    .await;
                return;
            }
            other => {
                debug!("unknown message type 0x{other:02x} in replication stream, skipping");
                let len = match read_i32(&mut stream).await {
                    Ok(l) => l as usize,
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                };
                if len > 4 {
                    if let Err(e) = skip_bytes(&mut stream, len - 4).await {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                }
            }
        }

        // Periodic heartbeat even without a keepalive request
        if last_keepalive_time.elapsed()
            >= tokio::time::Duration::from_secs(HEARTBEAT_INTERVAL_SECS)
        {
            if let Err(e) =
                send_standby_status_update(&mut stream, last_received_lsn, last_flushed_lsn, false)
                    .await
            {
                let _ = tx.send(Err(e)).await;
                return;
            }
            last_keepalive_time = tokio::time::Instant::now();
        }
    }
}

/// Send a StandbyStatusUpdate ('r') message to the server via CopyData.
async fn send_standby_status_update(
    stream: &mut MaybeTls,
    received_lsn: i64,
    flushed_lsn: i64,
    applied_lsn: bool,
) -> Result<(), TapError> {
    let lsn_val = if applied_lsn {
        flushed_lsn
    } else {
        received_lsn
    };
    // Byte1 'r' | Int64 received_lsn | Int64 flushed_lsn | Int64 applied_lsn
    // | Int64 timestamp | Byte1 reply_requested
    let mut payload = Vec::with_capacity(34);
    payload.push(b'r');
    payload.extend_from_slice(&received_lsn.to_be_bytes());
    payload.extend_from_slice(&flushed_lsn.to_be_bytes());
    payload.extend_from_slice(&lsn_val.to_be_bytes());

    // Client timestamp (microseconds since 2000-01-01) — just use 0 for now
    payload.extend_from_slice(&0i64.to_be_bytes());
    payload.push(0); // don't request a reply

    let mut msg = Vec::with_capacity(payload.len() + 5);
    msg.push(b'd'); // CopyData
    msg.extend_from_slice(&((payload.len() + 4) as i32).to_be_bytes());
    msg.extend_from_slice(&payload);

    stream.write_all(&msg).await.map_err(wrap_io_err)?;
    stream.flush().await.map_err(wrap_io_err)?;
    debug!("sent standby status update (received={received_lsn}, flushed={flushed_lsn})");
    Ok(())
}

// ---------------------------------------------------------------------------
// Low-level wire read helpers
// ---------------------------------------------------------------------------

async fn read_u8(stream: &mut MaybeTls) -> Result<u8, TapError> {
    let mut buf = [0u8; 1];
    stream.read_exact(&mut buf).await.map_err(wrap_io_err)?;
    Ok(buf[0])
}

async fn read_i32(stream: &mut MaybeTls) -> Result<i32, TapError> {
    let mut buf = [0u8; 4];
    stream.read_exact(&mut buf).await.map_err(wrap_io_err)?;
    Ok(i32::from_be_bytes(buf))
}

/// Read bytes until a NUL terminator and return the string.
async fn read_string_to_nul(stream: &mut MaybeTls) -> Result<String, TapError> {
    let mut bytes = Vec::new();
    loop {
        let b = read_u8(stream).await?;
        if b == 0 {
            break;
        }
        bytes.push(b);
    }
    String::from_utf8(bytes)
        .map_err(|e| TapError::Decode(format!("invalid UTF-8 in wire message: {e}")))
}

/// Read an ErrorResponse ('E') message and return the human-readable message.
async fn read_error_response(stream: &mut MaybeTls) -> Result<String, TapError> {
    let len = read_i32(stream).await?;
    let payload_len = (len - 4) as usize;
    if payload_len > MAX_MESSAGE_SIZE {
        return Err(TapError::Decode(format!(
            "ErrorResponse too large: {payload_len} bytes"
        )));
    }
    let mut payload = vec![0u8; payload_len];
    stream.read_exact(&mut payload).await.map_err(wrap_io_err)?;
    Ok(extract_error_message(&payload))
}

/// Read an ErrorResponse message and return raw payload for custom parsing.
async fn read_error_response_raw(stream: &mut MaybeTls) -> Result<String, TapError> {
    read_error_response(stream).await
}

/// Extract the human-readable message from an ErrorResponse payload
/// (sequence of field-type byte + field-value\0 pairs, terminated by \0).
fn extract_error_message(payload: &[u8]) -> String {
    let mut i = 0;
    let mut message = String::new();
    while i < payload.len() {
        let field_type = payload[i];
        i += 1;
        if field_type == 0 {
            break;
        }
        // Read until NUL
        let start = i;
        while i < payload.len() && payload[i] != 0 {
            i += 1;
        }
        let value = String::from_utf8_lossy(&payload[start..i]);
        if field_type == b'M' {
            message = value.to_string();
        }
        if i < payload.len() && payload[i] == 0 {
            i += 1;
        }
    }
    if message.is_empty() {
        String::from_utf8_lossy(payload).to_string()
    } else {
        message
    }
}

/// Parse SASL mechanism names from an AuthenticationSASL message payload.
async fn read_sasl_mechanisms(stream: &mut MaybeTls) -> Result<Vec<String>, TapError> {
    let mut mechanisms = Vec::new();
    loop {
        let mech = read_string_to_nul(stream).await?;
        if mech.is_empty() {
            break;
        }
        mechanisms.push(mech);
    }
    Ok(mechanisms)
}

/// Skip `count` bytes from the stream.
async fn skip_bytes(stream: &mut MaybeTls, count: usize) -> Result<(), TapError> {
    if count > MAX_MESSAGE_SIZE {
        return Err(TapError::Decode(format!(
            "attempted to skip {count} bytes (max {MAX_MESSAGE_SIZE})"
        )));
    }
    let mut buf = vec![0u8; count];
    stream.read_exact(&mut buf).await.map_err(wrap_io_err)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// SCRAM-SHA-256 helpers
// ---------------------------------------------------------------------------

/// Generate a 12-byte random nonce (hex-encoded → 24 hex chars).
fn generate_nonce() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    // Mix timestamp with a simple pseudo-random value
    format!("{:016x}{:08x}", ts, rand_compat())
}

/// Simple non-cryptographic randomness for the nonce (enough for SCRAM).
fn rand_compat() -> u32 {
    // Use a basic LCG seeded with time + PID
    let pid = std::process::id();
    let seed = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        ^ pid as u128) as u64;
    // SplitMix64 style
    let mut z = seed.wrapping_add(0x9e3779b97f4a7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    (z ^ (z >> 31)) as u32
}

/// Build the SCRAM client-first-message-bare without the initial `n,,` prefix.
fn scram_client_first_bare(config: &SourceConfig) -> String {
    format!("n={}", config.user)
}

/// Parse SCRAM server-first message.
///
/// Returns `(salt_b64, iterations, server_nonce)`.
fn parse_scram_server_first(
    msg: &str,
    client_nonce: &str,
) -> Result<(String, u32, String), TapError> {
    let mut salt = None;
    let mut iterations = None;
    let mut server_nonce = None;

    for part in msg.split(',') {
        if let Some(val) = part.strip_prefix("r=") {
            server_nonce = Some(val.to_string());
            if !val.starts_with(client_nonce) {
                return Err(TapError::Decode(
                    "SCRAM server nonce doesn't start with client nonce".into(),
                ));
            }
        } else if let Some(val) = part.strip_prefix("s=") {
            salt = Some(val.to_string());
        } else if let Some(val) = part.strip_prefix("i=") {
            iterations =
                Some(val.parse::<u32>().map_err(|e| {
                    TapError::Decode(format!("invalid SCRAM iteration count: {e}"))
                })?);
        }
    }

    let salt = salt.ok_or_else(|| TapError::Decode("missing SCRAM salt".into()))?;
    let iterations =
        iterations.ok_or_else(|| TapError::Decode("missing SCRAM iterations".into()))?;
    let server_nonce =
        server_nonce.ok_or_else(|| TapError::Decode("missing SCRAM server nonce".into()))?;

    Ok((salt, iterations, server_nonce))
}

// ---------------------------------------------------------------------------
// Cryptographic helpers (using openssl)
// ---------------------------------------------------------------------------

/// Compute MD5 digest as a hex string.
fn md5_digest(data: &[u8]) -> String {
    let digest =
        openssl::hash::hash(openssl::hash::MessageDigest::md5(), data).expect("MD5 hash failed");
    hex_encode(&digest)
}

/// Compute SHA-256 digest.
fn sha256(data: &[u8]) -> Vec<u8> {
    openssl::hash::hash(openssl::hash::MessageDigest::sha256(), data)
        .expect("SHA-256 hash failed")
        .to_vec()
}

/// Compute HMAC-SHA-256.
fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let key = openssl::pkey::PKey::hmac(key).expect("HMAC key creation failed");
    let mut signer = openssl::sign::Signer::new(openssl::hash::MessageDigest::sha256(), &key)
        .expect("HMAC signer creation failed");
    signer.update(data).expect("HMAC update failed");
    signer.sign_to_vec().expect("HMAC sign failed")
}

/// SCRAM Hi function: PBKDF2-HMAC-SHA256 with `iterations` rounds.
fn hi(password: &[u8], salt: &[u8], iterations: u32) -> Vec<u8> {
    let mut derived_key = vec![0u8; 32];
    openssl::pkcs5::pbkdf2_hmac(
        password,
        salt,
        iterations as usize,
        openssl::hash::MessageDigest::sha256(),
        &mut derived_key,
    )
    .expect("PBKDF2 failed");
    derived_key
}

/// XOR two byte vectors (panics if lengths differ).
fn xor_bytes(a: &[u8], b: &[u8]) -> Vec<u8> {
    assert_eq!(a.len(), b.len(), "xor_bytes: length mismatch");
    a.iter().zip(b.iter()).map(|(x, y)| x ^ y).collect()
}

/// Hex-encode bytes (lowercase).
fn hex_encode(data: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(data.len() * 2);
    for b in data {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Build a SASLResponse message: `'p' | Int32 length | client_final_bytes`.
///
/// This is a plain PasswordMessage — just the type byte, the total length
/// (including the 4-byte length field), and the client-final SCRAM payload.
/// Unlike SASLInitialResponse, there is **no** extra inner-length prefix.
fn build_sasl_response(client_final: &[u8]) -> Vec<u8> {
    let mut resp = Vec::with_capacity(1 + 4 + client_final.len());
    resp.push(b'p');
    let len = (client_final.len() + 4) as i32;
    resp.extend_from_slice(&len.to_be_bytes());
    resp.extend_from_slice(client_final);
    resp
}

// ---------------------------------------------------------------------------
// Error helpers
// ---------------------------------------------------------------------------

fn wrap_io_err(e: std::io::Error) -> TapError {
    TapError::Io(e)
}

fn proto_err(msg: String) -> TapError {
    TapError::PostgresConnectionRedacted(msg)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ── ReplicationStream channel tests ──────────────────────────────

    #[test]
    fn test_replication_stream_poll_ready() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (tx, rx) = mpsc::channel(16);
            let mut stream = ReplicationStream::from_receiver(rx);

            tx.send(Ok(vec![1, 2, 3])).await.unwrap();
            tx.send(Ok(vec![4, 5, 6])).await.unwrap();

            use futures::StreamExt;
            let item1 = stream.next().await;
            assert!(item1.is_some());
            assert_eq!(item1.unwrap().unwrap(), vec![1, 2, 3]);

            let item2 = stream.next().await;
            assert!(item2.is_some());
            assert_eq!(item2.unwrap().unwrap(), vec![4, 5, 6]);
        });
    }

    #[test]
    fn test_replication_stream_closed_channel() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (tx, rx) = mpsc::channel::<Result<Vec<u8>, TapError>>(16);
            drop(tx);
            let mut stream = ReplicationStream::from_receiver(rx);

            use futures::StreamExt;
            let item = stream.next().await;
            assert!(item.is_none());
        });
    }

    // ── SCRAM helpers ───────────────────────────────────────────────

    #[test]
    fn test_scram_parse_server_first() {
        let client_nonce = "abc123";
        let server_msg = "r=abc123def456,s=W8krQhUg/sbwPylq7gMq3Q==,i=4096";

        let (salt, iter, server_nonce) =
            parse_scram_server_first(server_msg, client_nonce).unwrap();

        assert_eq!(server_nonce, "abc123def456");
        assert_eq!(salt, "W8krQhUg/sbwPylq7gMq3Q==");
        assert_eq!(iter, 4096);
    }

    #[test]
    fn test_scram_parse_server_first_nonce_mismatch() {
        let client_nonce = "abc123";
        let server_msg = "r=xyz789,s=AAAA,i=4096";
        assert!(parse_scram_server_first(server_msg, client_nonce).is_err());
    }

    #[test]
    fn test_build_sasl_response_wire_format() {
        // SASLResponse must be: 'p' | Int32(len = 4 + N) | N bytes
        // No extra inner Int32 prefix (unlike SASLInitialResponse).
        let client_final = b"c=biws,r=abc123,p=proof";
        let result = build_sasl_response(client_final);
        let expected_len = 1 + 4 + client_final.len();

        assert_eq!(result.len(), expected_len, "wire size");
        assert_eq!(result[0], b'p', "message type byte");

        let wire_len = i32::from_be_bytes(result[1..5].try_into().unwrap()) as usize;
        assert_eq!(wire_len, client_final.len() + 4, "length field");

        assert_eq!(&result[5..], client_final, "payload");
    }

    // ── Crypto helpers ──────────────────────────────────────────────

    #[test]
    fn test_md5_digest() {
        let result = md5_digest(b"hello");
        assert_eq!(result, "5d41402abc4b2a76b9719d911017c592");
    }

    #[test]
    fn test_sha256_known() {
        let result = sha256(b"hello");
        let expected = vec![
            0x2c, 0xf2, 0x4d, 0xba, 0x5f, 0xb0, 0xa3, 0x0e, 0x26, 0xe8, 0x3b, 0x2a, 0xc5, 0xb9,
            0xe2, 0x9e, 0x1b, 0x16, 0x1e, 0x5c, 0x1f, 0xa7, 0x42, 0x5e, 0x73, 0x04, 0x33, 0x62,
            0x93, 0x8b, 0x98, 0x24,
        ];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_hmac_sha256() {
        let key = b"key";
        let data = b"data";
        let result = hmac_sha256(key, data);
        let expected_hex = "5031fe3d989c6d1537a013fa6e739da23463fdaec3b70137d828e36ace221bd0";
        assert_eq!(hex_encode(&result), expected_hex);
    }

    #[test]
    fn test_xor_bytes() {
        let a = vec![0xff, 0x00, 0xaa];
        let b = vec![0x0f, 0xf0, 0x55];
        let result = xor_bytes(&a, &b);
        assert_eq!(result, vec![0xf0, 0xf0, 0xff]);
    }

    // ── Error extraction ────────────────────────────────────────────

    #[test]
    fn test_extract_error_message() {
        // Build a minimal ErrorResponse payload:
        // 'S' "ERROR"\0 'M' "relation does not exist"\0 'C' "42P01"\0 \0
        let mut payload = Vec::new();
        payload.push(b'S');
        payload.extend_from_slice(b"ERROR\0");
        payload.push(b'M');
        payload.extend_from_slice(b"relation does not exist\0");
        payload.push(b'C');
        payload.extend_from_slice(b"42P01\0");
        payload.push(0); // terminator

        let msg = extract_error_message(&payload);
        assert_eq!(msg, "relation does not exist");
    }

    #[test]
    fn test_nonce_generation() {
        let a = generate_nonce();
        let b = generate_nonce();
        assert_ne!(a, b, "nonces should differ");
        assert!(a.len() == 24, "nonce should be 24 hex chars");
    }
}

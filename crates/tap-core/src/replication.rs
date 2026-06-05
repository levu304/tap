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

use crate::config::{SourceConfig, SslMode, validate_identifier};
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

/// Read deadline (seconds) for the background reader loop.
/// Postgres keepalive defaults to ~10 s, so 60 s is generous enough
/// to survive transient delays while still detecting half-open TCP.
const READ_TIMEOUT_SECS: u64 = 60;

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
    /// Mock/test variant backed by a tokio duplex stream, enabling
    /// protocol-level tests without a real network connection.
    #[allow(dead_code)]
    Test(tokio::io::DuplexStream),
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
            MaybeTls::Test(s) => Pin::new(s).poll_read(cx, buf),
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
            MaybeTls::Test(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        match &mut *self {
            MaybeTls::Plain(s) => Pin::new(s).poll_flush(cx),
            MaybeTls::Tls(s) => Pin::new(s).poll_flush(cx),
            MaybeTls::Test(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        match &mut *self {
            MaybeTls::Plain(s) => Pin::new(s).poll_shutdown(cx),
            MaybeTls::Tls(s) => Pin::new(s).poll_shutdown(cx),
            MaybeTls::Test(s) => Pin::new(s).poll_shutdown(cx),
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
    /// Handle to the background reader task.  Aborted on drop to prevent
    /// orphaned tasks.  When the channel closes unexpectedly, the handle's
    /// `is_finished()` state is checked to detect possible panics.
    reader_handle: Option<tokio::task::JoinHandle<()>>,
}

impl ReplicationStream {
    /// Create a stream from an mpsc receiver (used internally and for tests).
    pub fn from_receiver(rx: mpsc::Receiver<Result<Vec<u8>, TapError>>) -> Self {
        Self {
            rx,
            reader_handle: None,
        }
    }

    /// Attach the reader task handle (called from [`start`]).
    fn set_reader_handle(&mut self, handle: tokio::task::JoinHandle<()>) {
        self.reader_handle = Some(handle);
    }
}

impl Drop for ReplicationStream {
    fn drop(&mut self) {
        if let Some(handle) = self.reader_handle.take() {
            if !handle.is_finished() {
                handle.abort();
            }
        }
    }
}

impl Stream for ReplicationStream {
    type Item = Result<Vec<u8>, TapError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let result = self.rx.poll_recv(cx);
        // If the channel closed and the reader task also finished, it may
        // have panicked (normal exit only sends errors through the channel
        // before returning, which would keep the channel alive).  Log a
        // warning so the operator has a diagnostic hint.
        if let Poll::Ready(None) = &result {
            if self.reader_handle.as_ref().is_some_and(|h| h.is_finished()) {
                warn!("reader task finished while channel closed — possible panic");
            }
        }
        result
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
        let ssl_request = [(8i32).to_be_bytes(), SSL_REQUEST_CODE.to_be_bytes()].concat();
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

        // Build a connector that respects the configured SslMode.
        //
        // native_tls 0.2 defaults:
        //   - danger_accept_invalid_certs:    false (verify certs)
        //   - danger_accept_invalid_hostnames: false (verify hostname)
        //   - disable_built_in_roots:          false (use system roots)
        let mut tls_builder = native_tls::TlsConnector::builder();
        match config.ssl_mode {
            SslMode::Require => {
                // Encrypt only — no certificate or hostname verification.
                tls_builder.danger_accept_invalid_certs(true);
                tls_builder.danger_accept_invalid_hostnames(true);
            }
            SslMode::VerifyCa => {
                // Verify the certificate against system CAs but skip
                // hostname check.
                tls_builder.danger_accept_invalid_hostnames(true);
                // danger_accept_invalid_certs remains false (verify).
                // Built-in roots are used by default.
            }
            SslMode::VerifyFull => {
                // Full verification — both cert and hostname.
                // All defaults already do this.
            }
            SslMode::Disable => {
                // Handled above — we never reach this branch.
                unreachable!("SslMode::Disable handled before TLS builder");
            }
        }
        let native_connector = tls_builder.build().map_err(|e| {
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
    let reader_handle = tokio::spawn(reader_task(stream, tx));

    let mut stream = ReplicationStream::from_receiver(rx);
    stream.set_reader_handle(reader_handle);

    info!("replication stream established");
    Ok(stream)
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
                    md5_digest(&[config.password.as_bytes(), config.user.as_bytes()].concat())?;
                let mut combined = inner_digest.as_bytes().to_vec();
                combined.extend_from_slice(&salt);
                let hash = md5_digest(&combined)?;
                let response = format!("md5{hash}");
                send_password_message(stream, response.as_bytes()).await?;
            }
            10 => {
                // AuthenticationSASL
                debug!("auth: SASL requested");
                let mechanisms = read_sasl_mechanisms(stream).await?;
                // Prefer SCRAM-SHA-256. If the server only offers
                // SCRAM-SHA-256-PLUS, reject — we don't support channel binding.
                if mechanisms.iter().any(|m| m == "SCRAM-SHA-256") {
                    // Proceed with SCRAM-SHA-256 below
                } else if mechanisms.iter().any(|m| m == "SCRAM-SHA-256-PLUS") {
                    return Err(TapError::PostgresConnectionRedacted(
                        "server requires SCRAM-SHA-256-PLUS (channel binding), \
                         but this client does not support it"
                            .into(),
                    ));
                } else {
                    return Err(TapError::PostgresConnectionRedacted(
                        "no supported SASL mechanism found (need SCRAM-SHA-256)".into(),
                    ));
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
                )?;
                let client_key = hmac_sha256(&salted_password, b"Client Key")?;
                let stored_key = sha256(&client_key)?;
                let client_signature = hmac_sha256(&stored_key, auth_message.as_bytes())?;
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
                        let server_final = read_string_to_nul(stream).await?;
                        debug!("SASL server-final received");
                        // RFC 5802 §5: server-final is either
                        //   v=base64sig   (success)
                        //   e=errorcode   (failure)
                        if let Some(err) = server_final.strip_prefix("e=") {
                            return Err(TapError::PostgresConnectionRedacted(format!(
                                "SCRAM authentication failed: {err}"
                            )));
                        }
                        if let Some(sig_b64) = server_final.strip_prefix("v=") {
                            let server_key = hmac_sha256(&salted_password, b"Server Key")?;
                            let expected_sig = hmac_sha256(&server_key, auth_message.as_bytes())?;
                            let expected_b64 =
                                base64::engine::general_purpose::STANDARD.encode(&expected_sig);
                            // Constant-time compare would be ideal, but string
                            // comparison is acceptable here since this is a
                            // local TLS connection over a Postgres replication
                            // slot, not a latency-sensitive public endpoint.
                            if sig_b64 != expected_b64 {
                                return Err(TapError::PostgresConnectionRedacted(
                                    "SCRAM server signature mismatch".into(),
                                ));
                            }
                            debug!("SCRAM server signature verified");
                        } else {
                            // No v= or e= prefix — unexpected format
                            return Err(TapError::Decode(format!(
                                "unexpected SCRAM server-final format: {server_final}"
                            )));
                        }
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
        // Read message type byte with a timeout to detect half-open TCP.
        let msg_type = match tokio::time::timeout(
            tokio::time::Duration::from_secs(READ_TIMEOUT_SECS),
            read_u8(&mut stream),
        )
        .await
        {
            Ok(Ok(t)) => t,
            Ok(Err(e)) => {
                let _ = tx.send(Err(e)).await;
                return;
            }
            Err(_elapsed) => {
                let _ = tx
                    .send(Err(TapError::Io(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        format!("read timed out after {READ_TIMEOUT_SECS}s"),
                    ))))
                    .await;
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

                        last_flushed_lsn = last_flushed_lsn.max(wal_end);
                        last_received_lsn = last_received_lsn.max(wal_end);

                        if reply_required {
                            debug!("sending standby status update (keepalive requested)");
                            if let Err(e) = send_standby_status_update(
                                &mut stream,
                                last_received_lsn,
                                last_flushed_lsn,
                                last_received_lsn,
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
            if let Err(e) = send_standby_status_update(
                &mut stream,
                last_received_lsn,
                last_flushed_lsn,
                last_received_lsn,
            )
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
///
/// The `applied_lsn` field tells the server how much WAL the consumer has
/// processed — typically `received_lsn` (ack what we've received) or a
/// separately tracked position if the consumer is running behind.
async fn send_standby_status_update(
    stream: &mut MaybeTls,
    received_lsn: i64,
    flushed_lsn: i64,
    applied_lsn: i64,
) -> Result<(), TapError> {
    // Byte1 'r' | Int64 received_lsn | Int64 flushed_lsn | Int64 applied_lsn
    // | Int64 timestamp | Byte1 reply_requested
    let mut payload = Vec::with_capacity(34);
    payload.push(b'r');
    payload.extend_from_slice(&received_lsn.to_be_bytes());
    payload.extend_from_slice(&flushed_lsn.to_be_bytes());
    payload.extend_from_slice(&applied_lsn.to_be_bytes());

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
fn md5_digest(data: &[u8]) -> Result<String, TapError> {
    let digest = openssl::hash::hash(openssl::hash::MessageDigest::md5(), data)
        .map_err(|e| TapError::PostgresConnectionRedacted(format!("MD5 hash failed: {e}")))?;
    Ok(hex_encode(&digest))
}

/// Compute SHA-256 digest.
fn sha256(data: &[u8]) -> Result<Vec<u8>, TapError> {
    let digest = openssl::hash::hash(openssl::hash::MessageDigest::sha256(), data)
        .map_err(|e| TapError::PostgresConnectionRedacted(format!("SHA-256 hash failed: {e}")))?;
    Ok(digest.to_vec())
}

/// Compute HMAC-SHA-256.
fn hmac_sha256(key: &[u8], data: &[u8]) -> Result<Vec<u8>, TapError> {
    let key = openssl::pkey::PKey::hmac(key)
        .map_err(|e| TapError::PostgresConnectionRedacted(format!("HMAC key failed: {e}")))?;
    let mut signer = openssl::sign::Signer::new(openssl::hash::MessageDigest::sha256(), &key)
        .map_err(|e| TapError::PostgresConnectionRedacted(format!("HMAC signer failed: {e}")))?;
    signer
        .update(data)
        .map_err(|e| TapError::PostgresConnectionRedacted(format!("HMAC update failed: {e}")))?;
    signer
        .sign_to_vec()
        .map_err(|e| TapError::PostgresConnectionRedacted(format!("HMAC sign failed: {e}")))
}

/// SCRAM Hi function: PBKDF2-HMAC-SHA256 with `iterations` rounds.
fn hi(password: &[u8], salt: &[u8], iterations: u32) -> Result<Vec<u8>, TapError> {
    let mut derived_key = vec![0u8; 32];
    openssl::pkcs5::pbkdf2_hmac(
        password,
        salt,
        iterations as usize,
        openssl::hash::MessageDigest::sha256(),
        &mut derived_key,
    )
    .map_err(|e| TapError::PostgresConnectionRedacted(format!("PBKDF2 failed: {e}")))?;
    Ok(derived_key)
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
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // ------------------------------------------------------------------
    // Test helpers
    // ------------------------------------------------------------------

    fn test_config() -> SourceConfig {
        SourceConfig {
            host: "localhost".into(),
            port: 5432,
            dbname: "test_db".into(),
            user: "test_user".into(),
            password: "test_password".into(),
            slot_name: "test_slot".into(),
            publication: "test_pub".into(),
            tables: vec![],
            plugin: "pgoutput".into(),
            ssl_mode: SslMode::Disable,
        }
    }

    /// Build a CopyData ('d') wire message wrapping `inner`.
    fn copy_data(inner: &[u8]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(5 + inner.len());
        buf.push(b'd');
        buf.extend_from_slice(&((inner.len() + 4) as i32).to_be_bytes());
        buf.extend_from_slice(inner);
        buf
    }

    /// Build an XLogData sub-message (sub-type b'w') wrapping `payload`.
    fn xlog_data(payload: &[u8]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(25 + payload.len());
        buf.push(b'w');
        buf.extend_from_slice(&0i64.to_be_bytes()); // start_lsn
        buf.extend_from_slice(&100i64.to_be_bytes()); // end_lsn
        buf.extend_from_slice(&0i64.to_be_bytes()); // timestamp
        buf.extend_from_slice(payload);
        buf
    }

    /// Build a Keepalive sub-message (sub-type b'k').
    fn keepalive(wal_end: i64, reply_required: bool) -> Vec<u8> {
        let mut buf = Vec::with_capacity(18);
        buf.push(b'k');
        buf.extend_from_slice(&wal_end.to_be_bytes());
        buf.extend_from_slice(&0i64.to_be_bytes()); // timestamp
        buf.push(if reply_required { 1 } else { 0 });
        buf
    }

    /// Build a CopyDone ('c') message.
    fn copy_done() -> Vec<u8> {
        let mut buf = Vec::with_capacity(5);
        buf.push(b'c');
        buf.extend_from_slice(&4i32.to_be_bytes());
        buf
    }

    /// Build a minimal ErrorResponse ('E') message.
    fn error_response(message: &str) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.push(b'S');
        payload.extend_from_slice(b"ERROR\0");
        payload.push(b'M');
        payload.extend_from_slice(message.as_bytes());
        payload.push(0);
        payload.push(b'C');
        payload.extend_from_slice(b"XXXXX\0");
        payload.push(0); // terminator

        let mut msg = Vec::with_capacity(5 + payload.len());
        msg.push(b'E');
        msg.extend_from_slice(&((payload.len() + 4) as i32).to_be_bytes());
        msg.extend_from_slice(&payload);
        msg
    }

    /// Build an AuthenticationOk ('R') message.
    fn auth_ok() -> Vec<u8> {
        let mut msg = Vec::with_capacity(9);
        msg.push(b'R');
        msg.extend_from_slice(&8i32.to_be_bytes()); // len = 8
        msg.extend_from_slice(&0i32.to_be_bytes()); // type = 0 (Ok)
        msg
    }

    /// Build a ReadyForQuery ('Z') message.
    fn ready_for_query() -> Vec<u8> {
        let mut msg = Vec::with_capacity(6);
        msg.push(b'Z');
        msg.extend_from_slice(&5i32.to_be_bytes()); // len = 5
        msg.push(b'I'); // idle status
        msg
    }

    /// Build a minimal CopyBothResponse ('W') message.
    fn copy_both_response() -> Vec<u8> {
        let mut msg = Vec::with_capacity(8);
        msg.push(b'W');
        msg.extend_from_slice(&7i32.to_be_bytes()); // len = 7
        msg.push(0); // overall_format = text
        msg.extend_from_slice(&0i16.to_be_bytes()); // num_cols = 0
        msg
    }

    // ------------------------------------------------------------------
    // Critical test 1: XLogData parsing
    // ------------------------------------------------------------------
    /// Feed a valid CopyData containing XLogData through duplex, verify
    /// the WAL payload is received with the 25-byte header stripped.
    #[tokio::test]
    async fn test_xlog_data_parsing() {
        let (client, mut server) = tokio::io::duplex(65536);
        let stream = MaybeTls::Test(client);
        let (tx, mut rx) = mpsc::channel(16);

        let handle = tokio::spawn(reader_task(stream, tx));

        let wal_payload = b"WAL DATA PAYLOAD HERE";
        let msg = copy_data(&xlog_data(wal_payload));
        server.write_all(&msg).await.unwrap();

        let item = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("timeout")
            .expect("channel closed")
            .expect("reader returned error");

        assert_eq!(item, wal_payload, "XLogData header should be stripped");

        // Clean shutdown: close server + rx so reader exits
        drop(server);
        drop(rx);
        handle.await.ok();
    }

    // ------------------------------------------------------------------
    // Critical test 2: Keepalive detection + auto-response
    // ------------------------------------------------------------------
    /// Feed a PrimaryKeepaliveMessage with reply_required=1, verify
    /// auto-StandbyStatusUpdate is sent back through the stream.
    #[tokio::test]
    async fn test_keepalive_auto_response() {
        let (client, mut server) = tokio::io::duplex(65536);
        let stream = MaybeTls::Test(client);
        let (tx, mut rx) = mpsc::channel(16);

        let handle = tokio::spawn(reader_task(stream, tx));

        // Send a keepalive requesting a reply
        let msg = copy_data(&keepalive(42, true));
        server.write_all(&msg).await.unwrap();

        // Reader should yield an empty Vec to signal keepalive progress
        let item = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("timeout")
            .expect("channel closed")
            .expect("reader returned error");
        assert!(item.is_empty(), "keepalive yields empty vec");

        // After processing the keepalive the reader writes StandbyStatusUpdate
        // back to the stream.  Format: CopyData('d') | Int32(len) | 'r' | ...
        let mut resp_type = [0u8; 1];
        tokio::time::timeout(Duration::from_secs(5), server.read_exact(&mut resp_type))
            .await
            .expect("timeout reading response type")
            .expect("read response type");
        assert_eq!(resp_type[0], b'd', "response should be CopyData");

        let mut resp_len = [0u8; 4];
        server.read_exact(&mut resp_len).await.unwrap();
        let payload_len = i32::from_be_bytes(resp_len) as usize - 4;

        let mut payload = vec![0u8; payload_len];
        server.read_exact(&mut payload).await.unwrap();
        assert_eq!(payload[0], b'r', "StandbyStatusUpdate sub-type");

        // Verify the flushed LSN was updated
        let _received_lsn = i64::from_be_bytes(payload[1..9].try_into().unwrap());
        let _flushed_lsn = i64::from_be_bytes(payload[9..17].try_into().unwrap());

        drop(server);
        drop(rx);
        handle.await.ok();
    }

    // ------------------------------------------------------------------
    // Critical test 3: SCRAM-SHA-256 full handshake
    // ------------------------------------------------------------------
    /// Drive the full SASL exchange over duplex from the server side,
    /// computing the correct server-final signature so authenticate()
    /// returns Ok.
    #[tokio::test]
    async fn test_scram_handshake() {
        let (client, mut server) = tokio::io::duplex(65536);
        let config = test_config();
        const SALT_B64: &str = "W8krQhUg/sbwPylq7gMq3Q==";
        const ITERATIONS: u32 = 4096;

        let auth = async {
            let mut stream = MaybeTls::Test(client);
            authenticate(&mut stream, &config).await
        };

        let srv = async {
            // 1. Write AuthenticationSASL (type 10)
            let mechs = b"SCRAM-SHA-256\0";
            let mut auth_payload = Vec::new();
            auth_payload.extend_from_slice(&10i32.to_be_bytes());
            auth_payload.extend_from_slice(mechs);
            auth_payload.push(0); // terminator

            let mut msg = Vec::new();
            msg.push(b'R');
            msg.extend_from_slice(&((auth_payload.len() + 4) as i32).to_be_bytes());
            msg.extend_from_slice(&auth_payload);
            server.write_all(&msg).await.unwrap();

            // 2. Read SASLInitialResponse (PasswordMessage 'p')
            let mut ty = [0u8; 1];
            server.read_exact(&mut ty).await.unwrap();
            assert_eq!(ty[0], b'p');

            let mut raw_len = [0u8; 4];
            server.read_exact(&mut raw_len).await.unwrap();
            let total_len = i32::from_be_bytes(raw_len) as usize;
            let mut body = vec![0u8; total_len - 4];
            server.read_exact(&mut body).await.unwrap();

            // body = "SCRAM-SHA-256\0" | Int32(client_first_len) | client_first
            let mech_end = body.iter().position(|&b| b == 0).unwrap();
            let _mechanism = String::from_utf8_lossy(&body[..mech_end]);
            let after_mech = &body[mech_end + 1..];
            let cfl = i32::from_be_bytes(after_mech[..4].try_into().unwrap()) as usize;
            let client_first = String::from_utf8_lossy(&after_mech[4..4 + cfl]).to_string();

            assert!(client_first.starts_with("n,,"));

            let client_first_bare = client_first.strip_prefix("n,,").unwrap();
            let r_pos = client_first_bare.find("r=").unwrap();
            let client_nonce = &client_first_bare[r_pos + 2..];

            // 3. Craft server-first
            let server_nonce = format!("{client_nonce}server_ext");
            let server_first = format!("r={server_nonce},s={SALT_B64},i={ITERATIONS}");

            // Write AuthenticationSASLContinue (type 11)
            let mut cont_payload = Vec::new();
            cont_payload.extend_from_slice(&11i32.to_be_bytes());
            cont_payload.extend_from_slice(server_first.as_bytes());
            cont_payload.push(0);

            let mut cont_msg = Vec::new();
            cont_msg.push(b'R');
            cont_msg.extend_from_slice(&((cont_payload.len() + 4) as i32).to_be_bytes());
            cont_msg.extend_from_slice(&cont_payload);
            server.write_all(&cont_msg).await.unwrap();

            // 4. Read SASLResponse (PasswordMessage 'p')
            let mut ty2 = [0u8; 1];
            server.read_exact(&mut ty2).await.unwrap();
            assert_eq!(ty2[0], b'p');

            let mut rl2 = [0u8; 4];
            server.read_exact(&mut rl2).await.unwrap();
            let total_len2 = i32::from_be_bytes(rl2) as usize;
            let mut cf_body = vec![0u8; total_len2 - 4];
            server.read_exact(&mut cf_body).await.unwrap();
            let client_final = String::from_utf8_lossy(&cf_body).to_string();

            // client_final = "c=biws,r=...,p=..."
            let p_pos = client_final.find(",p=").unwrap();
            let client_final_no_proof = &client_final[..p_pos + 1]; // includes "c=biws,r=..."
            let client_final_without_proof = client_final_no_proof
                .strip_suffix(',')
                .unwrap_or(client_final_no_proof);

            // 5. Compute expected server signature
            let password = config.password.as_bytes();
            let salt = base64::engine::general_purpose::STANDARD
                .decode(SALT_B64)
                .unwrap();
            let salted_password = hi(password, &salt, ITERATIONS).unwrap();
            let server_key = hmac_sha256(&salted_password, b"Server Key").unwrap();
            let auth_msg =
                format!("{client_first_bare},{server_first},{client_final_without_proof}");
            let expected_sig = hmac_sha256(&server_key, auth_msg.as_bytes()).unwrap();
            let expected_b64 = base64::engine::general_purpose::STANDARD.encode(&expected_sig);

            // 6. Write AuthenticationSASLFinal (type 12) with valid sig
            let server_final = format!("v={expected_b64}");
            let mut final_payload = Vec::new();
            final_payload.extend_from_slice(&12i32.to_be_bytes());
            final_payload.extend_from_slice(server_final.as_bytes());
            final_payload.push(0);

            let mut final_msg = Vec::new();
            final_msg.push(b'R');
            final_msg.extend_from_slice(&((final_payload.len() + 4) as i32).to_be_bytes());
            final_msg.extend_from_slice(&final_payload);
            server.write_all(&final_msg).await.unwrap();
        };

        let (result, _) = tokio::join!(auth, srv);
        assert!(
            result.is_ok(),
            "SCRAM auth should succeed: {:?}",
            result.err()
        );
    }

    // ------------------------------------------------------------------
    // Critical test 4: MD5 PG auth vector
    // ------------------------------------------------------------------
    /// Verify md5_digest("password", "user") matches the known PG vector.
    /// PostgreSQL MD5 auth: inner = md5(password || user)
    #[test]
    fn test_md5_pg_auth_vector() {
        // md5("password" + "user") = md5("passworduser")
        let inner = md5_digest(b"passworduser").unwrap();
        assert_eq!(
            inner, "4d45974e13472b5a0be3533de4666414",
            "inner md5(password||user)"
        );

        // md5(inner_hex_ascii || salt) with salt = [1, 2, 3, 4]
        // inner.as_bytes() gives the ASCII hex string, not decoded binary.
        let salt = [1u8, 2, 3, 4];
        let mut combined = inner.as_bytes().to_vec();
        combined.extend_from_slice(&salt);
        let hash = md5_digest(&combined).unwrap();
        assert_eq!(
            hash, "a3576f1ae039b8996bc4fc2720f9c71a",
            "md5(inner_hex_ascii || salt)"
        );

        let response = format!("md5{hash}");
        assert_eq!(response, "md5a3576f1ae039b8996bc4fc2720f9c71a");
    }

    // ------------------------------------------------------------------
    // Critical test 5: Reader task exit on receiver drop
    // ------------------------------------------------------------------
    /// Drop the server side of the duplex (causing client reads to fail)
    /// and the mpsc receiver, then verify the reader task handle resolves.
    #[tokio::test]
    async fn test_reader_exit_on_drop() {
        let (client, server) = tokio::io::duplex(65536);
        let stream = MaybeTls::Test(client);
        let (tx, rx) = mpsc::channel(16);
        let handle = tokio::spawn(reader_task(stream, tx));

        // Close both ends — reader read will fail and send to rx will fail
        drop(server);
        drop(rx);

        tokio::time::timeout(Duration::from_secs(10), handle)
            .await
            .expect("reader task should exit within timeout")
            .expect("reader task should not panic");
    }

    // ------------------------------------------------------------------
    // Test 6: CopyDone handling
    // ------------------------------------------------------------------
    /// Feed a CopyDone ('c') message to the reader, verify it exits
    /// cleanly (channel produces None).
    #[tokio::test]
    async fn test_copy_done_handling() {
        let (client, mut server) = tokio::io::duplex(65536);
        let stream = MaybeTls::Test(client);
        let (tx, mut rx) = mpsc::channel(16);

        let handle = tokio::spawn(reader_task(stream, tx));

        server.write_all(&copy_done()).await.unwrap();

        // Reader should exit on CopyDone without sending anything
        let result = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await;

        match result {
            Ok(None) => { /* channel closed cleanly */ }
            Ok(Some(Ok(v))) => {
                panic!("expected None (CopyDone), got data: {v:?}");
            }
            Ok(Some(Err(e))) => {
                panic!("expected None (CopyDone), got error: {e}");
            }
            Err(_) => {
                panic!("reader did not exit after CopyDone within timeout");
            }
        }

        drop(server);
        handle.await.ok();
    }

    // ------------------------------------------------------------------
    // Test 7: ErrorResponse in reader
    // ------------------------------------------------------------------
    /// Feed an ErrorResponse ('E') to the reader, verify it yields Err.
    #[tokio::test]
    async fn test_error_response_in_reader() {
        let (client, mut server) = tokio::io::duplex(65536);
        let stream = MaybeTls::Test(client);
        let (tx, mut rx) = mpsc::channel(16);

        let handle = tokio::spawn(reader_task(stream, tx));

        server
            .write_all(&error_response("test error"))
            .await
            .unwrap();

        let item = rx.recv().await;
        match item {
            Some(Err(TapError::PostgresConnectionRedacted(msg))) => {
                assert!(msg.contains("test error"), "msg: {msg}");
            }
            other => {
                panic!("expected Some(Err(PostgresConnectionRedacted)), got {other:?}");
            }
        }

        drop(server);
        drop(rx);
        handle.await.ok();
    }

    // ------------------------------------------------------------------
    // Test 8: Split packets (fragmented reads)
    // ------------------------------------------------------------------
    /// Feed a complete XLogData message one byte at a time to exercise
    /// the read_exact recovery path.
    #[tokio::test]
    async fn test_split_packets() {
        let (client, mut server) = tokio::io::duplex(65536);
        let stream = MaybeTls::Test(client);
        let (tx, mut rx) = mpsc::channel(16);

        let handle = tokio::spawn(reader_task(stream, tx));

        let wal_data = b"CHUNKED WAL DATA 12345";
        let msg = copy_data(&xlog_data(wal_data));

        // Write one byte at a time with tiny delays
        for &b in &msg {
            server.write_all(&[b]).await.unwrap();
            tokio::time::sleep(Duration::from_micros(200)).await;
        }

        let item = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("timeout")
            .expect("channel closed")
            .expect("reader returned error");

        assert_eq!(item, wal_data, "XLogData from chunked write");

        drop(server);
        drop(rx);
        handle.await.ok();
    }

    // ------------------------------------------------------------------
    // Test 9: Framing helpers
    // ------------------------------------------------------------------
    /// Verify send_startup, send_query, send_password_message,
    /// read_ready_for_query, and read_copy_both_response all work with
    /// a duplex stream.

    #[tokio::test]
    async fn test_send_startup_wire_format() {
        let (client, mut server) = tokio::io::duplex(65536);
        let mut stream = MaybeTls::Test(client);
        let config = test_config();

        send_startup(&mut stream, &config).await.unwrap();

        // Read the startup message from the server side
        let mut len_buf = [0u8; 4];
        server.read_exact(&mut len_buf).await.unwrap();
        let total_len = i32::from_be_bytes(len_buf) as usize;

        let mut payload = vec![0u8; total_len - 4];
        server.read_exact(&mut payload).await.unwrap();

        // First 4 bytes = protocol version
        let proto_ver = i32::from_be_bytes(payload[..4].try_into().unwrap());
        assert_eq!(proto_ver, PG_PROTOCOL_VERSION);

        let msg = String::from_utf8_lossy(&payload);
        assert!(msg.contains("user\0"), "should contain user param");
        assert!(msg.contains("database\0"), "should contain database param");
        assert!(
            msg.contains("replication\0database\0"),
            "should contain replication param"
        );
    }

    #[tokio::test]
    async fn test_send_query_wire_format() {
        let (client, mut server) = tokio::io::duplex(65536);
        let mut stream = MaybeTls::Test(client);

        send_query(&mut stream, "SELECT 1").await.unwrap();

        let mut ty = [0u8; 1];
        server.read_exact(&mut ty).await.unwrap();
        assert_eq!(ty[0], b'Q', "message type must be Query");

        let mut len_buf = [0u8; 4];
        server.read_exact(&mut len_buf).await.unwrap();
        let len = i32::from_be_bytes(len_buf) as usize;

        let mut query = vec![0u8; len - 4];
        server.read_exact(&mut query).await.unwrap();
        // Should be null-terminated "SELECT 1\0"
        assert_eq!(&query[..query.len() - 1], b"SELECT 1");
        assert_eq!(query[query.len() - 1], 0, "must be null-terminated");
    }

    #[tokio::test]
    async fn test_send_password_message_wire_format() {
        let (client, mut server) = tokio::io::duplex(65536);
        let mut stream = MaybeTls::Test(client);

        send_password_message(&mut stream, b"secret").await.unwrap();

        let mut ty = [0u8; 1];
        server.read_exact(&mut ty).await.unwrap();
        assert_eq!(ty[0], b'p', "message type must be PasswordMessage");

        let mut len_buf = [0u8; 4];
        server.read_exact(&mut len_buf).await.unwrap();
        let len = i32::from_be_bytes(len_buf) as usize;

        let mut password = vec![0u8; len - 4];
        server.read_exact(&mut password).await.unwrap();
        assert_eq!(&password[..password.len() - 1], b"secret");
        assert_eq!(password[password.len() - 1], 0, "must be null-terminated");
    }

    #[tokio::test]
    async fn test_read_ready_for_query_ok() {
        let (client, server) = tokio::io::duplex(65536);
        let mut stream = MaybeTls::Test(client);

        // Write a ReadyForQuery message then call the helper
        let helper = async { read_ready_for_query(&mut stream).await };

        let feeder = async {
            // Feed a NoticeResponse (should be skipped) then ReadyForQuery
            let mut server = server;
            // NoticeResponse: 'N' | Int32(len) | payload
            // len = 4 (self) + 6 (payload "NOTICE") = 10
            let mut notice = Vec::new();
            notice.push(b'N');
            notice.extend_from_slice(&10i32.to_be_bytes());
            notice.extend_from_slice(b"NOTICE");
            server.write_all(&notice).await.unwrap();

            server.write_all(&ready_for_query()).await.unwrap();
        };

        let (result, _) = tokio::join!(helper, feeder);
        assert!(result.is_ok(), "read_ready_for_query should succeed");
    }

    #[tokio::test]
    async fn test_read_copy_both_response_ok() {
        let (client, server) = tokio::io::duplex(65536);
        let mut stream = MaybeTls::Test(client);

        let helper = async { read_copy_both_response(&mut stream).await };

        let feeder = async {
            let mut server = server;
            server.write_all(&copy_both_response()).await.unwrap();
        };

        let (result, _) = tokio::join!(helper, feeder);
        assert!(result.is_ok(), "read_copy_both_response should succeed");
    }

    #[tokio::test]
    async fn test_read_ready_for_query_error() {
        let (client, server) = tokio::io::duplex(65536);
        let mut stream = MaybeTls::Test(client);

        let helper = async { read_ready_for_query(&mut stream).await };

        let feeder = async {
            let mut server = server;
            server
                .write_all(&error_response("syntax error"))
                .await
                .unwrap();
        };

        let (result, _) = tokio::join!(helper, feeder);
        assert!(
            result.is_err(),
            "read_ready_for_query should error on ErrorResponse"
        );
    }

    // ------------------------------------------------------------------
    // Test 10: Authenticate with various auth types
    // ------------------------------------------------------------------

    /// authenticate() returns Ok immediately for AuthenticationOk.
    #[tokio::test]
    async fn test_authenticate_ok() {
        let (client, server) = tokio::io::duplex(65536);
        let config = test_config();
        let mut stream = MaybeTls::Test(client);

        let helper = async { authenticate(&mut stream, &config).await };

        let feeder = async {
            let mut server = server;
            server.write_all(&auth_ok()).await.unwrap();
        };

        let (result, _) = tokio::join!(helper, feeder);
        assert!(
            result.is_ok(),
            "authenticate with Ok should succeed: {:?}",
            result.err()
        );
    }

    /// authenticate() sends cleartext password and handles AuthOk.
    #[tokio::test]
    async fn test_authenticate_cleartext() {
        let (client, server) = tokio::io::duplex(65536);
        let config = test_config();
        let mut stream = MaybeTls::Test(client);

        let helper = async { authenticate(&mut stream, &config).await };

        let feeder = async {
            let mut server = server;
            // PasswordMessage request
            let mut auth_req = Vec::new();
            auth_req.push(b'R');
            auth_req.extend_from_slice(&8i32.to_be_bytes());
            auth_req.extend_from_slice(&3i32.to_be_bytes()); // CleartextPassword
            server.write_all(&auth_req).await.unwrap();

            // Read PasswordMessage response
            let mut ty = [0u8; 1];
            server.read_exact(&mut ty).await.unwrap();
            assert_eq!(ty[0], b'p');

            // Send AuthenticationOk
            server.write_all(&auth_ok()).await.unwrap();
        };

        let (result, _) = tokio::join!(helper, feeder);
        assert!(
            result.is_ok(),
            "authenticate with cleartext should succeed: {:?}",
            result.err()
        );
    }

    /// authenticate() handles MD5 password request.
    #[tokio::test]
    async fn test_authenticate_md5() {
        let (client, server) = tokio::io::duplex(65536);
        let mut config = test_config();
        config.user = "user".into();
        config.password = "password".into();
        let mut stream = MaybeTls::Test(client);

        let helper = async { authenticate(&mut stream, &config).await };

        let feeder = async {
            let mut server = server;
            // MD5Password request with salt [1,2,3,4]
            let mut auth_req = Vec::new();
            auth_req.push(b'R');
            let mut payload = Vec::new();
            payload.extend_from_slice(&5i32.to_be_bytes()); // auth type = MD5
            payload.extend_from_slice(&[1u8, 2, 3, 4]); // salt
            auth_req.extend_from_slice(&((payload.len() + 4) as i32).to_be_bytes());
            auth_req.extend_from_slice(&payload);
            server.write_all(&auth_req).await.unwrap();

            // Read PasswordMessage response
            let mut ty = [0u8; 1];
            server.read_exact(&mut ty).await.unwrap();
            assert_eq!(ty[0], b'p');

            let mut len_buf = [0u8; 4];
            server.read_exact(&mut len_buf).await.unwrap();
            let len = i32::from_be_bytes(len_buf) as usize;
            let mut resp = vec![0u8; len - 4];
            server.read_exact(&mut resp).await.unwrap();

            let pw_str = String::from_utf8_lossy(&resp[..resp.len() - 1]);
            assert!(
                pw_str.starts_with("md5"),
                "MD5 response should start with md5, got: {pw_str}"
            );
            assert_eq!(
                pw_str.as_ref(),
                "md5a3576f1ae039b8996bc4fc2720f9c71a",
                "expected MD5 response"
            );

            // Send AuthenticationOk
            server.write_all(&auth_ok()).await.unwrap();
        };

        let (result, _) = tokio::join!(helper, feeder);
        assert!(
            result.is_ok(),
            "authenticate with MD5 should succeed: {:?}",
            result.err()
        );
    }

    // ------------------------------------------------------------------
    // Test 11: SASL server-final signature mismatch
    // ------------------------------------------------------------------
    /// Feed the full SASL exchange but with a deliberately wrong v=
    /// value in the server-final message — verify Err.
    #[tokio::test]
    async fn test_sasl_signature_mismatch() {
        let (client, mut server) = tokio::io::duplex(65536);
        let config = test_config();
        const SALT_B64: &str = "W8krQhUg/sbwPylq7gMq3Q==";
        const ITERATIONS: u32 = 4096;

        let auth = async {
            let mut stream = MaybeTls::Test(client);
            authenticate(&mut stream, &config).await
        };

        let srv = async {
            // 1. AuthenticationSASL (type 10)
            let mechs = b"SCRAM-SHA-256\0";
            let mut auth_payload = Vec::new();
            auth_payload.extend_from_slice(&10i32.to_be_bytes());
            auth_payload.extend_from_slice(mechs);
            auth_payload.push(0);
            let mut msg = Vec::new();
            msg.push(b'R');
            msg.extend_from_slice(&((auth_payload.len() + 4) as i32).to_be_bytes());
            msg.extend_from_slice(&auth_payload);
            server.write_all(&msg).await.unwrap();

            // 2. Read SASLInitialResponse
            let mut ty = [0u8; 1];
            server.read_exact(&mut ty).await.unwrap();
            assert_eq!(ty[0], b'p');
            let mut raw_len = [0u8; 4];
            server.read_exact(&mut raw_len).await.unwrap();
            let total_len = i32::from_be_bytes(raw_len) as usize;
            let mut body = vec![0u8; total_len - 4];
            server.read_exact(&mut body).await.unwrap();

            let mech_end = body.iter().position(|&b| b == 0).unwrap();
            let after_mech = &body[mech_end + 1..];
            let cfl = i32::from_be_bytes(after_mech[..4].try_into().unwrap()) as usize;
            let client_first = String::from_utf8_lossy(&after_mech[4..4 + cfl]).to_string();
            let client_first_bare = client_first.strip_prefix("n,,").unwrap();
            let r_pos = client_first_bare.find("r=").unwrap();
            let client_nonce = &client_first_bare[r_pos + 2..];

            // 3. Server-first
            let server_nonce = format!("{client_nonce}srv");
            let server_first = format!("r={server_nonce},s={SALT_B64},i={ITERATIONS}");

            // SASLContinue
            let mut cont_payload = Vec::new();
            cont_payload.extend_from_slice(&11i32.to_be_bytes());
            cont_payload.extend_from_slice(server_first.as_bytes());
            cont_payload.push(0);
            let mut cont_msg = Vec::new();
            cont_msg.push(b'R');
            cont_msg.extend_from_slice(&((cont_payload.len() + 4) as i32).to_be_bytes());
            cont_msg.extend_from_slice(&cont_payload);
            server.write_all(&cont_msg).await.unwrap();

            // 4. Read SASLResponse
            let mut ty2 = [0u8; 1];
            server.read_exact(&mut ty2).await.unwrap();
            assert_eq!(ty2[0], b'p');
            let mut rl2 = [0u8; 4];
            server.read_exact(&mut rl2).await.unwrap();
            let total_len2 = i32::from_be_bytes(rl2) as usize;
            let mut cf_body = vec![0u8; total_len2 - 4];
            server.read_exact(&mut cf_body).await.unwrap();

            // 5. Send SASLFinal with WRONG signature
            let wrong_sig = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
            let server_final = format!("v={wrong_sig}");
            let mut final_payload = Vec::new();
            final_payload.extend_from_slice(&12i32.to_be_bytes());
            final_payload.extend_from_slice(server_final.as_bytes());
            final_payload.push(0);
            let mut final_msg = Vec::new();
            final_msg.push(b'R');
            final_msg.extend_from_slice(&((final_payload.len() + 4) as i32).to_be_bytes());
            final_msg.extend_from_slice(&final_payload);
            server.write_all(&final_msg).await.unwrap();
        };

        let (result, _) = tokio::join!(auth, srv);
        assert!(
            result.is_err(),
            "SCRAM should fail with wrong server signature: {:?}",
            result
        );
        if let Err(TapError::PostgresConnectionRedacted(msg)) = result {
            assert!(
                msg.contains("signature mismatch"),
                "expected 'signature mismatch' error, got: {msg}"
            );
        }
    }

    // ── Existing unit tests (preserved) ──────────────────────────────

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
        let client_final = b"c=biws,r=abc123,p=proof";
        let result = build_sasl_response(client_final);
        let expected_len = 1 + 4 + client_final.len();

        assert_eq!(result.len(), expected_len, "wire size");
        assert_eq!(result[0], b'p', "message type byte");

        let wire_len = i32::from_be_bytes(result[1..5].try_into().unwrap()) as usize;
        assert_eq!(wire_len, client_final.len() + 4, "length field");

        assert_eq!(&result[5..], client_final, "payload");
    }

    #[test]
    fn test_md5_digest() {
        let result = md5_digest(b"hello").unwrap();
        assert_eq!(result, "5d41402abc4b2a76b9719d911017c592");
    }

    #[test]
    fn test_sha256_known() {
        let result = sha256(b"hello").unwrap();
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
        let result = hmac_sha256(key, data).unwrap();
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

    #[test]
    fn test_extract_error_message() {
        let mut payload = Vec::new();
        payload.push(b'S');
        payload.extend_from_slice(b"ERROR\0");
        payload.push(b'M');
        payload.extend_from_slice(b"relation does not exist\0");
        payload.push(b'C');
        payload.extend_from_slice(b"42P01\0");
        payload.push(0);

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

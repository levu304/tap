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
//!
//! # TLS modes
//!
//! The [`SslMode`](crate::config::SslMode) variants affect TLS behaviour as
//! follows:
//!
//! | Mode | Description |
//! |---|---|
//! | `Disable` | Connect without TLS. |
//! | `Require` | TLS required. Accepts any certificate (self-signed
//!   included). |
//! | `VerifyCa` | TLS required. Verifies the server certificate is
//!   signed by a trusted CA but does **not** check the hostname. |
//! | `VerifyFull` | TLS required. Full verification: trusted CA +
//!   matching hostname. |

use tokio::io::AsyncReadExt;
use tracing::debug;

use crate::config::SourceConfig;
use crate::error::TapError;

mod tls;
pub(crate) use tls::*;
mod protocol;
pub(crate) use protocol::*;
mod scram;
pub(crate) use scram::*;
mod stream;
pub use stream::ReplicationStream;
#[allow(unused_imports)]
pub(crate) use stream::{ReplicationOptions, reader_task, start};

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

// ── CopyData sub-types (first byte of payload inside CopyData) ──────────

/// XLogData sub-type (w): WAL data from the server.
const SUBTYPE_XLOG_DATA: u8 = b'w';
/// Keepalive sub-type (k): primary keepalive message.
const SUBTYPE_KEEPALIVE: u8 = b'k';
/// StandbyStatusUpdate sub-type (r): client → server or echo.
const SUBTYPE_STANDBY_STATUS_UPDATE: u8 = b'r';

// ── Wire-protocol message types ────────────────────────────────────────

const TYPE_COPY_DATA: u8 = b'd';
const TYPE_COPY_DONE: u8 = b'c';
const TYPE_ERROR_RESPONSE: u8 = b'E';
const TYPE_AUTHENTICATION: u8 = b'R';
const TYPE_READY_FOR_QUERY: u8 = b'Z';
const TYPE_NOTICE_RESPONSE: u8 = b'N';
const TYPE_BACKEND_KEY_DATA: u8 = b'K';
const TYPE_QUERY: u8 = b'Q';
const TYPE_PASSWORD_MESSAGE: u8 = b'p';
const TYPE_COPY_BOTH_RESPONSE: u8 = b'W';
const TYPE_SSL_ACCEPTED: u8 = b'S';

// ── Wire-header sizes ──────────────────────────────────────────────────

/// Size of the XLogData frame header:
///   Byte1 'w' | Int64 start_lsn | Int64 end_lsn | Int64 timestamp
/// = 1 + 8 + 8 + 8 = 25 bytes.
const XLOG_DATA_HEADER_SIZE: usize = 25;

/// Minimum size of a Keepalive frame header:
///   Byte1 'k' | Int64 end_lsn | Int64 timestamp | Byte1 reply_required
/// = 1 + 8 + 8 + 1 = 18 bytes.
const KEEPALIVE_HEADER_SIZE: usize = 18;

// ---------------------------------------------------------------------------
// Wire-protocol helpers
// ---------------------------------------------------------------------------

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
pub(crate) async fn authenticate(
    stream: &mut MaybeTls,
    config: &SourceConfig,
) -> Result<(), TapError> {
    loop {
        let msg_type = read_u8(stream).await?;
        if msg_type != TYPE_AUTHENTICATION {
            return Err(proto_err(format!(
                "expected Authentication message ('R'), got 0x{msg_type:02x}"
            )));
        }

        let _len = read_i32(stream).await?; // includes self + type
        if _len < 8 {
            return Err(proto_err(format!(
                "Authentication message too short: {_len} bytes (need at least 8)"
            )));
        }
        let auth_type = read_i32(stream).await?;

        match auth_type {
            0 => {
                debug!("authentication ok");
                return Ok(());
            }
            3 => {
                debug!("auth: cleartext password requested");
                send_password_message(stream, config.password.as_bytes()).await?;
            }
            5 => {
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
                debug!("auth: SASL requested");
                return perform_scram_auth(stream, config).await;
            }
            other => {
                return Err(TapError::PostgresConnectionRedacted(format!(
                    "unsupported authentication type: {other}"
                )));
            }
        }
    }
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
    use crate::config::SslMode;
    use base64::Engine;
    use std::sync::Arc;
    use std::sync::atomic::AtomicI64;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::mpsc;

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

        let handle = tokio::spawn(reader_task(stream, tx, Arc::new(AtomicI64::new(-1))));

        let wal_payload = b"WAL DATA PAYLOAD HERE";
        let msg = copy_data(&xlog_data(wal_payload));
        server.write_all(&msg).await.unwrap();

        let item = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("timeout")
            .expect("channel closed")
            .expect("reader returned error");

        assert_eq!(item, wal_payload, "XLogData header should be stripped");

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

        let handle = tokio::spawn(reader_task(stream, tx, Arc::new(AtomicI64::new(-1))));

        let msg = copy_data(&keepalive(42, true));
        server.write_all(&msg).await.unwrap();

        let item = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("timeout")
            .expect("channel closed")
            .expect("reader returned error");
        assert!(item.is_empty(), "keepalive yields empty vec");

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

        let _received_lsn = i64::from_be_bytes(payload[1..9].try_into().unwrap());
        let _flushed_lsn = i64::from_be_bytes(payload[9..17].try_into().unwrap());

        drop(server);
        drop(rx);
        handle.await.ok();
    }

    // ------------------------------------------------------------------
    // Critical test 3: SCRAM-SHA-256 full handshake
    // ------------------------------------------------------------------
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

            let mut ty = [0u8; 1];
            server.read_exact(&mut ty).await.unwrap();
            assert_eq!(ty[0], b'p');

            let mut raw_len = [0u8; 4];
            server.read_exact(&mut raw_len).await.unwrap();
            let total_len = i32::from_be_bytes(raw_len) as usize;
            let mut body = vec![0u8; total_len - 4];
            server.read_exact(&mut body).await.unwrap();

            let mech_end = body.iter().position(|&b| b == 0).unwrap();
            let _mechanism = String::from_utf8_lossy(&body[..mech_end]);
            let after_mech = &body[mech_end + 1..];
            let cfl = i32::from_be_bytes(after_mech[..4].try_into().unwrap()) as usize;
            let client_first = String::from_utf8_lossy(&after_mech[4..4 + cfl]).to_string();

            assert!(client_first.starts_with("n,,"));

            let client_first_bare = client_first.strip_prefix("n,,").unwrap();
            let r_pos = client_first_bare.find("r=").unwrap();
            let client_nonce = &client_first_bare[r_pos + 2..];

            let server_nonce = format!("{client_nonce}server_ext");
            let server_first = format!("r={server_nonce},s={SALT_B64},i={ITERATIONS}");

            let mut cont_payload = Vec::new();
            cont_payload.extend_from_slice(&11i32.to_be_bytes());
            cont_payload.extend_from_slice(server_first.as_bytes());
            cont_payload.push(0);

            let mut cont_msg = Vec::new();
            cont_msg.push(b'R');
            cont_msg.extend_from_slice(&((cont_payload.len() + 4) as i32).to_be_bytes());
            cont_msg.extend_from_slice(&cont_payload);
            server.write_all(&cont_msg).await.unwrap();

            let mut ty2 = [0u8; 1];
            server.read_exact(&mut ty2).await.unwrap();
            assert_eq!(ty2[0], b'p');

            let mut rl2 = [0u8; 4];
            server.read_exact(&mut rl2).await.unwrap();
            let total_len2 = i32::from_be_bytes(rl2) as usize;
            let mut cf_body = vec![0u8; total_len2 - 4];
            server.read_exact(&mut cf_body).await.unwrap();
            let client_final = String::from_utf8_lossy(&cf_body).to_string();

            let p_pos = client_final.find(",p=").unwrap();
            let client_final_no_proof = &client_final[..p_pos + 1];
            let client_final_without_proof = client_final_no_proof
                .strip_suffix(',')
                .unwrap_or(client_final_no_proof);

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
    #[test]
    fn test_md5_pg_auth_vector() {
        let inner = md5_digest(b"passworduser").unwrap();
        assert_eq!(
            inner, "4d45974e13472b5a0be3533de4666414",
            "inner md5(password||user)"
        );

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
    #[tokio::test]
    async fn test_reader_exit_on_drop() {
        let (client, server) = tokio::io::duplex(65536);
        let stream = MaybeTls::Test(client);
        let (tx, rx) = mpsc::channel(16);
        let handle = tokio::spawn(reader_task(stream, tx, Arc::new(AtomicI64::new(-1))));

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
    #[tokio::test]
    async fn test_copy_done_handling() {
        let (client, mut server) = tokio::io::duplex(65536);
        let stream = MaybeTls::Test(client);
        let (tx, mut rx) = mpsc::channel(16);

        let handle = tokio::spawn(reader_task(stream, tx, Arc::new(AtomicI64::new(-1))));

        server.write_all(&copy_done()).await.unwrap();

        let result = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await;

        match result {
            Ok(None) => {}
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
    #[tokio::test]
    async fn test_error_response_in_reader() {
        let (client, mut server) = tokio::io::duplex(65536);
        let stream = MaybeTls::Test(client);
        let (tx, mut rx) = mpsc::channel(16);

        let handle = tokio::spawn(reader_task(stream, tx, Arc::new(AtomicI64::new(-1))));

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
    #[tokio::test]
    async fn test_split_packets() {
        let (client, mut server) = tokio::io::duplex(65536);
        let stream = MaybeTls::Test(client);
        let (tx, mut rx) = mpsc::channel(16);

        let handle = tokio::spawn(reader_task(stream, tx, Arc::new(AtomicI64::new(-1))));

        let wal_data = b"CHUNKED WAL DATA 12345";
        let msg = copy_data(&xlog_data(wal_data));

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
    #[tokio::test]
    async fn test_send_startup_wire_format() {
        let (client, mut server) = tokio::io::duplex(65536);
        let mut stream = MaybeTls::Test(client);
        let config = test_config();

        send_startup(&mut stream, &config).await.unwrap();

        let mut len_buf = [0u8; 4];
        server.read_exact(&mut len_buf).await.unwrap();
        let total_len = i32::from_be_bytes(len_buf) as usize;

        let mut payload = vec![0u8; total_len - 4];
        server.read_exact(&mut payload).await.unwrap();

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

        let helper = async { read_ready_for_query(&mut stream).await };

        let feeder = async {
            let mut server = server;
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

    #[tokio::test]
    async fn test_authenticate_cleartext() {
        let (client, server) = tokio::io::duplex(65536);
        let config = test_config();
        let mut stream = MaybeTls::Test(client);

        let helper = async { authenticate(&mut stream, &config).await };

        let feeder = async {
            let mut server = server;
            let mut auth_req = Vec::new();
            auth_req.push(b'R');
            auth_req.extend_from_slice(&8i32.to_be_bytes());
            auth_req.extend_from_slice(&3i32.to_be_bytes());
            server.write_all(&auth_req).await.unwrap();

            let mut ty = [0u8; 1];
            server.read_exact(&mut ty).await.unwrap();
            assert_eq!(ty[0], b'p');

            server.write_all(&auth_ok()).await.unwrap();
        };

        let (result, _) = tokio::join!(helper, feeder);
        assert!(
            result.is_ok(),
            "authenticate with cleartext should succeed: {:?}",
            result.err()
        );
    }

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
            let mut auth_req = Vec::new();
            auth_req.push(b'R');
            let mut payload = Vec::new();
            payload.extend_from_slice(&5i32.to_be_bytes());
            payload.extend_from_slice(&[1u8, 2, 3, 4]);
            auth_req.extend_from_slice(&((payload.len() + 4) as i32).to_be_bytes());
            auth_req.extend_from_slice(&payload);
            server.write_all(&auth_req).await.unwrap();

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

            let server_nonce = format!("{client_nonce}srv");
            let server_first = format!("r={server_nonce},s={SALT_B64},i={ITERATIONS}");

            let mut cont_payload = Vec::new();
            cont_payload.extend_from_slice(&11i32.to_be_bytes());
            cont_payload.extend_from_slice(server_first.as_bytes());
            cont_payload.push(0);
            let mut cont_msg = Vec::new();
            cont_msg.push(b'R');
            cont_msg.extend_from_slice(&((cont_payload.len() + 4) as i32).to_be_bytes());
            cont_msg.extend_from_slice(&cont_payload);
            server.write_all(&cont_msg).await.unwrap();

            let mut ty2 = [0u8; 1];
            server.read_exact(&mut ty2).await.unwrap();
            assert_eq!(ty2[0], b'p');
            let mut rl2 = [0u8; 4];
            server.read_exact(&mut rl2).await.unwrap();
            let total_len2 = i32::from_be_bytes(rl2) as usize;
            let mut cf_body = vec![0u8; total_len2 - 4];
            server.read_exact(&mut cf_body).await.unwrap();

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
        let result = xor_bytes(&a, &b).unwrap();
        assert_eq!(result, vec![0xf0, 0xf0, 0xff]);
    }

    #[test]
    fn test_parse_error_response() {
        let mut payload = Vec::new();
        payload.push(b'S');
        payload.extend_from_slice(b"ERROR\0");
        payload.push(b'M');
        payload.extend_from_slice(b"relation does not exist\0");
        payload.push(b'C');
        payload.extend_from_slice(b"42P01\0");
        payload.push(0);

        let info = parse_error_response(&payload);
        assert_eq!(info.severity, "ERROR");
        assert_eq!(info.message, "relation does not exist");
        assert_eq!(info.code, "42P01");
    }

    #[test]
    fn test_saslprep_user() {
        assert_eq!(saslprep_user("alice"), "alice");
        assert_eq!(saslprep_user(""), "");
        assert_eq!(saslprep_user("a,b"), "a=2Cb");
        assert_eq!(saslprep_user("a=b"), "a=3Db");
        assert_eq!(saslprep_user("a=b,c"), "a=3Db=2Cc");
        assert_eq!(saslprep_user("a=3Db"), "a=3D3Db");
    }

    #[test]
    fn test_scram_client_first_bare_saslprep() {
        assert_eq!(
            scram_client_first_bare(&SourceConfig {
                user: "alice".into(),
                ..SourceConfig::default()
            }),
            "n=alice"
        );
        assert_eq!(
            scram_client_first_bare(&SourceConfig {
                user: "a=b,c".into(),
                ..SourceConfig::default()
            }),
            "n=a=3Db=2Cc"
        );
        assert_eq!(
            scram_client_first_bare(&SourceConfig {
                user: "test,user=1".into(),
                ..SourceConfig::default()
            }),
            "n=test=2Cuser=3D1"
        );
    }

    #[test]
    fn test_nonce_generation() {
        let a = generate_nonce();
        let b = generate_nonce();
        assert_ne!(a, b, "nonces should differ");
        assert!(a.len() == 24, "nonce should be 24 hex chars");
    }
}

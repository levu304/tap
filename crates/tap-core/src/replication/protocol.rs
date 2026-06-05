use super::{
    MAX_MESSAGE_SIZE, MaybeTls, PG_PROTOCOL_VERSION, SUBTYPE_STANDBY_STATUS_UPDATE, SourceConfig,
    TYPE_BACKEND_KEY_DATA, TYPE_COPY_BOTH_RESPONSE, TYPE_COPY_DATA, TYPE_ERROR_RESPONSE,
    TYPE_NOTICE_RESPONSE, TYPE_PASSWORD_MESSAGE, TYPE_QUERY, TYPE_READY_FOR_QUERY, TapError,
    proto_err, wrap_io_err,
};
use tokio::io::AsyncWriteExt;
use tracing::debug;

/// Send a PostgreSQL startup message.
///
/// The startup message uses a different framing from regular messages:
/// Int32 length | Int32 protocol_version | key\0value\0...\0
pub(crate) async fn send_startup(
    stream: &mut MaybeTls,
    config: &SourceConfig,
) -> Result<(), TapError> {
    let user_param = format!("user\0{}\0", config.user);
    let db_param = format!("database\0{}\0", config.dbname);
    let repl_param = "replication\0database\0".to_string();

    let mut payload = Vec::with_capacity(128);
    payload.extend_from_slice(&PG_PROTOCOL_VERSION.to_be_bytes());
    payload.extend_from_slice(user_param.as_bytes());
    payload.extend_from_slice(db_param.as_bytes());
    payload.extend_from_slice(repl_param.as_bytes());
    payload.push(0);

    let len = (payload.len() + 4) as i32;
    let mut buf = Vec::with_capacity(payload.len() + 4);
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(&payload);

    stream.write_all(&buf).await.map_err(wrap_io_err)?;
    stream.flush().await.map_err(wrap_io_err)?;
    Ok(())
}

/// Send a PasswordMessage ('p').
///
/// The password is sent with a trailing NUL terminator as required by the
/// PostgreSQL wire protocol.
pub(crate) async fn send_password_message(
    stream: &mut MaybeTls,
    password: &[u8],
) -> Result<(), TapError> {
    let mut payload = password.to_vec();
    payload.push(0);
    let msg = build_message(TYPE_PASSWORD_MESSAGE, &payload);
    stream.write_all(&msg).await.map_err(wrap_io_err)?;
    stream.flush().await.map_err(wrap_io_err)?;
    Ok(())
}

/// Read a Query response until we see ReadyForQuery ('Z').
pub(crate) async fn read_ready_for_query(stream: &mut MaybeTls) -> Result<(), TapError> {
    loop {
        let msg_type = read_u8(stream).await?;
        match msg_type {
            TYPE_READY_FOR_QUERY => {
                let _len = read_i32(stream).await?;
                if _len < 5 {
                    return Err(proto_err(format!(
                        "ReadyForQuery message too short: {_len} bytes (need at least 5)"
                    )));
                }
                let _status = read_u8(stream).await?;
                return Ok(());
            }
            TYPE_ERROR_RESPONSE => {
                let error_info = read_error_response(stream).await?;
                return Err(TapError::PostgresConnectionRedacted(error_info.message));
            }
            TYPE_NOTICE_RESPONSE => {
                let len = read_i32(stream).await?;
                skip_bytes(stream, (len - 4) as usize).await?;
            }
            TYPE_BACKEND_KEY_DATA => {
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
///
/// The query string is sent with a trailing NUL terminator as required by
/// the PostgreSQL wire protocol.
pub(crate) async fn send_query(stream: &mut MaybeTls, query: &str) -> Result<(), TapError> {
    let mut payload = query.as_bytes().to_vec();
    payload.push(0);
    let msg = build_message(TYPE_QUERY, &payload);
    stream.write_all(&msg).await.map_err(wrap_io_err)?;
    stream.flush().await.map_err(wrap_io_err)?;
    Ok(())
}

/// Read a CopyBothResponse ('W') message.
pub(crate) async fn read_copy_both_response(stream: &mut MaybeTls) -> Result<(), TapError> {
    let msg_type = read_u8(stream).await?;
    match msg_type {
        TYPE_COPY_BOTH_RESPONSE => {
            let len = read_i32(stream).await?;
            skip_bytes(stream, (len - 4) as usize).await?;
            debug!("received CopyBothResponse");
            Ok(())
        }
        TYPE_ERROR_RESPONSE => {
            let error_info = read_error_response(stream).await?;
            Err(TapError::PostgresConnectionRedacted(format!(
                "START_REPLICATION rejected: {error_info}"
            )))
        }
        other => Err(proto_err(format!(
            "expected CopyBothResponse ('W') or ErrorResponse ('E'), got 0x{other:02x}"
        ))),
    }
}

/// Send a StandbyStatusUpdate ('r') message to the server via CopyData.
pub(crate) async fn send_standby_status_update(
    stream: &mut MaybeTls,
    received_lsn: i64,
    flushed_lsn: i64,
    applied_lsn: i64,
) -> Result<(), TapError> {
    let mut payload = Vec::with_capacity(34);
    payload.push(SUBTYPE_STANDBY_STATUS_UPDATE);
    payload.extend_from_slice(&received_lsn.to_be_bytes());
    payload.extend_from_slice(&flushed_lsn.to_be_bytes());
    payload.extend_from_slice(&applied_lsn.to_be_bytes());
    payload.extend_from_slice(&0i64.to_be_bytes());
    payload.push(0);

    let msg = build_message(TYPE_COPY_DATA, &payload);
    stream.write_all(&msg).await.map_err(wrap_io_err)?;
    stream.flush().await.map_err(wrap_io_err)?;
    debug!("sent standby status update (received={received_lsn}, flushed={flushed_lsn})");
    Ok(())
}

pub(crate) async fn read_u8(stream: &mut MaybeTls) -> Result<u8, TapError> {
    let mut buf = [0u8; 1];
    tokio::io::AsyncReadExt::read_exact(stream, &mut buf)
        .await
        .map_err(wrap_io_err)?;
    Ok(buf[0])
}

pub(crate) async fn read_i32(stream: &mut MaybeTls) -> Result<i32, TapError> {
    let mut buf = [0u8; 4];
    tokio::io::AsyncReadExt::read_exact(stream, &mut buf)
        .await
        .map_err(wrap_io_err)?;
    Ok(i32::from_be_bytes(buf))
}

/// Read bytes until a NUL terminator and return the string.
pub(crate) async fn read_string_to_nul(stream: &mut MaybeTls) -> Result<String, TapError> {
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

/// Read a NUL-terminated string from the stream, consuming at most
/// `max_bytes` bytes.  Prevents unbounded reads and protocol desync
/// when the peer sends an empty or minimal payload.
///
/// If the NUL byte is found within `max_bytes`, returns the string
/// before the NUL.  If no NUL is found within `max_bytes`, returns
/// an error — this prevents consuming bytes from the next wire message.
pub(crate) async fn read_string_to_nul_bounded(
    stream: &mut MaybeTls,
    max_bytes: usize,
) -> Result<String, TapError> {
    if max_bytes == 0 {
        return Ok(String::new());
    }
    let mut buf = vec![0u8; max_bytes];
    tokio::io::AsyncReadExt::read_exact(stream, &mut buf)
        .await
        .map_err(wrap_io_err)?;

    let nul_pos = buf.iter().position(|&b| b == 0).unwrap_or(max_bytes);
    let bytes = &buf[..nul_pos];
    String::from_utf8(bytes.to_vec())
        .map_err(|e| TapError::Decode(format!("invalid UTF-8 in wire message: {e}")))
}

/// Read an ErrorResponse ('E') message and return structured error information.
pub(crate) async fn read_error_response(stream: &mut MaybeTls) -> Result<ErrorInfo, TapError> {
    let len = read_i32(stream).await?;
    let payload_len = (len - 4) as usize;
    if payload_len > MAX_MESSAGE_SIZE {
        return Err(TapError::Decode(format!(
            "ErrorResponse too large: {payload_len} bytes"
        )));
    }
    let mut payload = vec![0u8; payload_len];
    tokio::io::AsyncReadExt::read_exact(stream, &mut payload)
        .await
        .map_err(wrap_io_err)?;
    Ok(parse_error_response(&payload))
}

/// Structured error information from a PostgreSQL ErrorResponse.
///
/// The PostgreSQL wire protocol ErrorResponse ('E') contains fields keyed
/// by a single byte. Common fields include:
/// * `S` — severity (e.g. "ERROR", "FATAL")
/// * `M` — human-readable message
/// * `C` — SQLSTATE code (e.g. "42710")
/// * `H` — hint (optional)
/// * `P` — position in the original query (optional)
#[derive(Debug, Clone, Default)]
pub(crate) struct ErrorInfo {
    pub severity: String,
    pub code: String,
    pub message: String,
    pub hint: String,
    pub position: String,
}

impl std::fmt::Display for ErrorInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

/// Parse an ErrorResponse payload into a structured [`ErrorInfo`].
///
/// If the payload cannot be parsed as field-type-delimited frames (e.g. it
/// contains only raw text), the entire payload is stored as the message.
pub(crate) fn parse_error_response(payload: &[u8]) -> ErrorInfo {
    let mut info = ErrorInfo::default();
    let mut i = 0;
    while i < payload.len() {
        let field_type = payload[i];
        i += 1;
        if field_type == 0 {
            break;
        }
        let start = i;
        while i < payload.len() && payload[i] != 0 {
            i += 1;
        }
        let value = String::from_utf8_lossy(&payload[start..i]).to_string();
        match field_type {
            b'S' => info.severity = value,
            b'M' => info.message = value,
            b'C' => info.code = value,
            b'H' => info.hint = value,
            b'P' => info.position = value,
            _ => {}
        }
        if i < payload.len() && payload[i] == 0 {
            i += 1;
        }
    }
    if info.message.is_empty() {
        info.message = String::from_utf8_lossy(payload).to_string();
    }
    info
}

/// Parse SASL mechanism names from an AuthenticationSASL message payload.
pub(crate) async fn read_sasl_mechanisms(stream: &mut MaybeTls) -> Result<Vec<String>, TapError> {
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
pub(crate) async fn skip_bytes(stream: &mut MaybeTls, count: usize) -> Result<(), TapError> {
    if count > MAX_MESSAGE_SIZE {
        return Err(TapError::Decode(format!(
            "attempted to skip {count} bytes (max {MAX_MESSAGE_SIZE})"
        )));
    }
    let mut buf = vec![0u8; count];
    tokio::io::AsyncReadExt::read_exact(stream, &mut buf)
        .await
        .map_err(wrap_io_err)?;
    Ok(())
}

/// Build a wire message frame: `msg_type | Int32(total_len) | payload`.
///
/// `total_len` = payload.len() + 4 (for the length field itself).
/// This is the standard PostgreSQL message framing used by CopyData,
/// PasswordMessage, Query, and most other protocol messages.
pub(crate) fn build_message(msg_type: u8, payload: &[u8]) -> Vec<u8> {
    let mut msg = Vec::with_capacity(1 + 4 + payload.len());
    msg.push(msg_type);
    msg.extend_from_slice(&((payload.len() + 4) as i32).to_be_bytes());
    msg.extend_from_slice(payload);
    msg
}

/// Build a SASLResponse message: `'p' | Int32 length | client_final_bytes`.
///
/// Unlike `SASLInitialResponse` (which uses a nested `Int32` for the
/// mechanism name), the `SASLResponse` message is a plain password-message
/// frame with no inner length prefix — the payload is just the client-final
/// data. See the PostgreSQL wire-protocol docs for `AuthenticationSASLFinal`.
pub(crate) fn build_sasl_response(client_final: &[u8]) -> Vec<u8> {
    build_message(TYPE_PASSWORD_MESSAGE, client_final)
}

use super::{
    MaybeTls, TapError, SourceConfig, PG_PROTOCOL_VERSION, MAX_MESSAGE_SIZE,
    TYPE_COPY_DATA, TYPE_COPY_BOTH_RESPONSE, TYPE_ERROR_RESPONSE,
    TYPE_PASSWORD_MESSAGE, TYPE_QUERY, TYPE_READY_FOR_QUERY, TYPE_NOTICE_RESPONSE,
    TYPE_BACKEND_KEY_DATA, SUBTYPE_STANDBY_STATUS_UPDATE, wrap_io_err, proto_err,
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
pub(crate) async fn send_password_message(
    stream: &mut MaybeTls,
    password: &[u8],
) -> Result<(), TapError> {
    let mut msg = Vec::new();
    msg.push(TYPE_PASSWORD_MESSAGE);
    let len = (password.len() + 1 + 4) as i32;
    msg.extend_from_slice(&len.to_be_bytes());
    msg.extend_from_slice(password);
    msg.push(0);
    stream.write_all(&msg).await.map_err(wrap_io_err)?;
    stream.flush().await.map_err(wrap_io_err)?;
    Ok(())
}

/// Read a Query response until we see ReadyForQuery ('Z').
pub(crate) async fn read_ready_for_query(
    stream: &mut MaybeTls,
) -> Result<(), TapError> {
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
                let error_msg = read_error_response(stream).await?;
                return Err(TapError::PostgresConnectionRedacted(error_msg));
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
pub(crate) async fn send_query(
    stream: &mut MaybeTls,
    query: &str,
) -> Result<(), TapError> {
    let mut msg = Vec::new();
    msg.push(TYPE_QUERY);
    let len = (query.len() + 1 + 4) as i32;
    msg.extend_from_slice(&len.to_be_bytes());
    msg.extend_from_slice(query.as_bytes());
    msg.push(0);
    stream.write_all(&msg).await.map_err(wrap_io_err)?;
    stream.flush().await.map_err(wrap_io_err)?;
    Ok(())
}

/// Read a CopyBothResponse ('W') message.
pub(crate) async fn read_copy_both_response(
    stream: &mut MaybeTls,
) -> Result<(), TapError> {
    let msg_type = read_u8(stream).await?;
    match msg_type {
        TYPE_COPY_BOTH_RESPONSE => {
            let len = read_i32(stream).await?;
            skip_bytes(stream, (len - 4) as usize).await?;
            debug!("received CopyBothResponse");
            Ok(())
        }
        TYPE_ERROR_RESPONSE => {
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

    let mut msg = Vec::with_capacity(payload.len() + 5);
    msg.push(TYPE_COPY_DATA);
    msg.extend_from_slice(&((payload.len() + 4) as i32).to_be_bytes());
    msg.extend_from_slice(&payload);

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
pub(crate) async fn read_string_to_nul(
    stream: &mut MaybeTls,
) -> Result<String, TapError> {
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
pub(crate) async fn read_error_response(
    stream: &mut MaybeTls,
) -> Result<String, TapError> {
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
    Ok(extract_error_message(&payload))
}

/// Extract the human-readable message from an ErrorResponse payload.
pub(crate) fn extract_error_message(payload: &[u8]) -> String {
    let mut i = 0;
    let mut message = String::new();
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
pub(crate) async fn read_sasl_mechanisms(
    stream: &mut MaybeTls,
) -> Result<Vec<String>, TapError> {
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
pub(crate) async fn skip_bytes(
    stream: &mut MaybeTls,
    count: usize,
) -> Result<(), TapError> {
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

/// Build a SASLResponse message: `'p' | Int32 length | client_final_bytes`.
pub(crate) fn build_sasl_response(client_final: &[u8]) -> Vec<u8> {
    let mut resp = Vec::with_capacity(1 + 4 + client_final.len());
    resp.push(TYPE_PASSWORD_MESSAGE);
    let len = (client_final.len() + 4) as i32;
    resp.extend_from_slice(&len.to_be_bytes());
    resp.extend_from_slice(client_final);
    resp
}

use super::{
    MaybeTls, SourceConfig, TYPE_AUTHENTICATION, TapError, build_sasl_response, proto_err,
    read_i32, read_sasl_mechanisms, read_string_to_nul, read_u8, wrap_io_err,
};
use base64::Engine;
use tokio::io::AsyncWriteExt;
use tracing::debug;

// ---------------------------------------------------------------------------
// SCRAM-SHA-256 helpers
// ---------------------------------------------------------------------------

/// Generate a 12-byte random nonce (hex-encoded 24 hex chars).
pub(crate) fn generate_nonce() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("SystemTime before UNIX_EPOCH")
        .as_nanos();
    let pid = std::process::id();
    let seed = (ts ^ pid as u128) as u64;
    format!("{:016x}{:08x}", ts, splitmix32(seed))
}

/// SplitMix32 pseudo-random number generator (enough for SCRAM nonces).
pub(crate) fn splitmix32(seed: u64) -> u32 {
    let mut z = seed.wrapping_add(0x9e3779b97f4a7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    (z ^ (z >> 31)) as u32
}

/// SASL-prep the username per RFC 5802 7 and RFC 4013.
pub(crate) fn saslprep_user(user: &str) -> String {
    user.replace('=', "=3D").replace(',', "=2C")
}

/// Build the SCRAM client-first-message-bare without the initial `n,,` prefix.
pub(crate) fn scram_client_first_bare(config: &SourceConfig) -> String {
    format!("n={}", saslprep_user(&config.user))
}

/// Parse SCRAM server-first message.
pub(crate) fn parse_scram_server_first(
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
pub(crate) fn md5_digest(data: &[u8]) -> Result<String, TapError> {
    let digest = openssl::hash::hash(openssl::hash::MessageDigest::md5(), data)
        .map_err(|e| TapError::PostgresConnectionRedacted(format!("MD5 hash failed: {e}")))?;
    Ok(hex_encode(&digest))
}

/// Compute SHA-256 digest.
pub(crate) fn sha256(data: &[u8]) -> Result<Vec<u8>, TapError> {
    let digest = openssl::hash::hash(openssl::hash::MessageDigest::sha256(), data)
        .map_err(|e| TapError::PostgresConnectionRedacted(format!("SHA-256 hash failed: {e}")))?;
    Ok(digest.to_vec())
}

/// Compute HMAC-SHA-256.
pub(crate) fn hmac_sha256(key: &[u8], data: &[u8]) -> Result<Vec<u8>, TapError> {
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
pub(crate) fn hi(password: &[u8], salt: &[u8], iterations: u32) -> Result<Vec<u8>, TapError> {
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

/// XOR two byte vectors.
pub(crate) fn xor_bytes(a: &[u8], b: &[u8]) -> Result<Vec<u8>, TapError> {
    if a.len() != b.len() {
        return Err(TapError::Decode(format!(
            "xor_bytes length mismatch ({} vs {})",
            a.len(),
            b.len()
        )));
    }
    Ok(a.iter().zip(b.iter()).map(|(x, y)| x ^ y).collect())
}

/// Hex-encode bytes (lowercase).
pub(crate) fn hex_encode(data: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(data.len() * 2);
    for b in data {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Perform the SCRAM-SHA-256 authentication exchange.
pub(crate) async fn perform_scram_auth(
    stream: &mut MaybeTls,
    config: &SourceConfig,
) -> Result<(), TapError> {
    let mechanisms = read_sasl_mechanisms(stream).await?;
    if mechanisms.iter().any(|m| m == "SCRAM-SHA-256") {
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
    let client_first_bare = format!("n={},r={}", scram_client_first_bare(config), client_nonce);
    let client_first = String::from("n,,") + &client_first_bare;
    let client_first_bytes = client_first.as_bytes();
    let client_first_len = client_first_bytes.len() as i32;

    // SASLInitialResponse
    let mechanism = b"SCRAM-SHA-256";
    let mut sasl_resp = Vec::new();
    sasl_resp.extend_from_slice(mechanism);
    sasl_resp.push(0);
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
    if msg_type != TYPE_AUTHENTICATION {
        return Err(proto_err(format!(
            "expected SASLContinue ('R'), got 0x{msg_type:02x}"
        )));
    }
    let _len = read_i32(stream).await?;
    if _len < 8 {
        return Err(proto_err(format!(
            "SASLContinue message too short: {_len} bytes (need at least 8)"
        )));
    }
    let sasl_type = read_i32(stream).await?;
    if sasl_type != 11 {
        return Err(proto_err(format!(
            "expected SASLContinue (type 11), got {sasl_type}"
        )));
    }

    let server_first = read_string_to_nul(stream).await?;
    debug!("SCRAM server-first: {server_first}");

    let (salt_b64, iterations, server_nonce) =
        parse_scram_server_first(&server_first, &client_nonce)?;

    let client_final_without_proof = format!("c=biws,r={server_nonce}");
    let auth_message = format!("{client_first_bare},{server_first},{client_final_without_proof}");

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
    let client_proof = xor_bytes(&client_key, &client_signature)?;
    let client_proof_b64 = base64::engine::general_purpose::STANDARD.encode(&client_proof);

    let client_final = format!("{client_final_without_proof},p={client_proof_b64}");
    let client_final_bytes = client_final.as_bytes();

    let resp = build_sasl_response(client_final_bytes);

    stream.write_all(&resp).await.map_err(wrap_io_err)?;
    stream.flush().await.map_err(wrap_io_err)?;

    // Read AuthenticationSASLFinal (type 12) or AuthenticationOk
    let msg_type = read_u8(stream).await?;
    if msg_type != TYPE_AUTHENTICATION {
        return Err(proto_err(format!(
            "expected SASLFinal/Ok ('R'), got 0x{msg_type:02x}"
        )));
    }
    let _len = read_i32(stream).await?;
    if _len < 8 {
        return Err(proto_err(format!(
            "SASLFinal message too short: {_len} bytes (need at least 8)"
        )));
    }
    let sasl_type = read_i32(stream).await?;
    match sasl_type {
        0 => {
            debug!("SASL authentication ok (after final)");
            return Ok(());
        }
        12 => {
            let server_final = read_string_to_nul(stream).await?;
            debug!("SASL server-final received");
            if let Some(err) = server_final.strip_prefix("e=") {
                return Err(TapError::PostgresConnectionRedacted(format!(
                    "SCRAM authentication failed: {err}"
                )));
            }
            if let Some(sig_b64) = server_final.strip_prefix("v=") {
                let server_key = hmac_sha256(&salted_password, b"Server Key")?;
                let expected_sig = hmac_sha256(&server_key, auth_message.as_bytes())?;
                let expected_b64 = base64::engine::general_purpose::STANDARD.encode(&expected_sig);
                if sig_b64 != expected_b64 {
                    return Err(TapError::PostgresConnectionRedacted(
                        "SCRAM server signature mismatch".into(),
                    ));
                }
                debug!("SCRAM server signature verified");
            } else {
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

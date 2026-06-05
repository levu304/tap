use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::task::{Context, Poll};

use futures::Stream;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::config::{SourceConfig, SslMode, validate_identifier};
use crate::error::TapError;
use crate::postgres::Lsn;

use super::{
    CHANNEL_CAPACITY, HEARTBEAT_INTERVAL_SECS, KEEPALIVE_HEADER_SIZE, MaybeTls, READ_TIMEOUT_SECS,
    SSL_REQUEST_CODE, SUBTYPE_KEEPALIVE, SUBTYPE_STANDBY_STATUS_UPDATE, SUBTYPE_XLOG_DATA,
    TYPE_COPY_DATA, TYPE_COPY_DONE, TYPE_ERROR_RESPONSE, TYPE_SSL_ACCEPTED, XLOG_DATA_HEADER_SIZE,
    authenticate, read_copy_both_response, read_error_response, read_i32, read_ready_for_query,
    read_u8, send_query, send_standby_status_update, send_startup, skip_bytes, wrap_io_err,
};

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
    /// Last received WAL end position from XLogData frames, shared with the
    /// background reader task.  Updated atomically so the consumer can call
    /// [`current_lsn()`](Self::current_lsn) without message-passing delay.
    /// Initialised to `-1` (no position received yet).
    current_lsn: Arc<AtomicI64>,
}

impl ReplicationStream {
    /// Create a stream from an mpsc receiver (used internally and for tests).
    pub fn from_receiver(rx: mpsc::Receiver<Result<Vec<u8>, TapError>>) -> Self {
        Self {
            rx,
            reader_handle: None,
            current_lsn: Arc::new(AtomicI64::new(-1)),
        }
    }

    /// Return the most recent WAL end-position received from the server, if
    /// any XLogData frames have been processed.
    ///
    /// This is the `end_lsn` field from the most recent XLogData message.
    /// Returns `None` before the first XLogData frame is received.
    pub fn current_lsn(&self) -> Option<Lsn> {
        let val = self.current_lsn.load(Ordering::Acquire);
        if val >= 0 {
            Some(Lsn::from_u64(val as u64))
        } else {
            None
        }
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
        if let Poll::Ready(None) = &result {
            if self.reader_handle.as_ref().is_some_and(|h| h.is_finished()) {
                warn!("reader task finished while channel closed — possible panic");
            }
        }
        result
    }
}

/// Parameters for starting a PostgreSQL replication stream.
///
/// Bundles the replication-specific parameters. The [`SourceConfig`] (host,
/// port, credentials, TLS mode) is passed separately to [`start()`] since it
/// is shared with the rest of the application.
pub(crate) struct ReplicationOptions<'a> {
    pub slot_name: &'a str,
    pub publication: &'a str,
    /// The LSN from which to begin consuming WAL.
    ///
    /// Use [`Lsn::ZERO`](crate::postgres::Lsn) to start from the slot's
    /// last flushed position (PostgreSQL resumes where it left off).
    pub start_lsn: Lsn,
    pub plugin: &'a str,
}

/// Connect to Postgres over TCP/TLS, authenticate, issue `START_REPLICATION`,
/// and return a [`ReplicationStream`] yielding WAL payload bytes.
///
/// **Important:** The function returns `Ok` *before* the background reader has
/// processed the first message. Connection errors after `CopyBothResponse`
/// (e.g. auth failures during `START_REPLICATION`) surface on the first call
/// to [`ReplicationStream::poll_next`], not during `start()` itself.
///
/// The connection is independent of the tokio-postgres `Client` used by
/// [`PgConnection`](crate::postgres::PgConnection) — only this raw stream
/// carries the COPY_BOTH protocol.
pub async fn start(
    config: &SourceConfig,
    opts: &ReplicationOptions<'_>,
) -> Result<ReplicationStream, TapError> {
    info!(
        "starting replication stream (slot={}, publication={}, \
         lsn={}, plugin={})",
        opts.slot_name, opts.publication, opts.start_lsn, opts.plugin
    );

    validate_identifier(opts.slot_name, "slot_name")?;
    validate_identifier(opts.publication, "publication")?;
    validate_identifier(opts.plugin, "plugin")?;

    let addr = format!("{}:{}", config.host, config.port);
    info!("connecting to {addr}");
    let mut tcp = tokio::net::TcpStream::connect(&addr)
        .await
        .map_err(|e| TapError::PostgresConnectionRedacted(format!("TCP connect failed: {e}")))?;

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

        if response[0] != TYPE_SSL_ACCEPTED {
            return Err(TapError::PostgresConnectionRedacted(format!(
                "server rejected TLS connection (response byte: 0x{:02x})",
                response[0]
            )));
        }

        info!("wrapping connection with TLS");

        // A new TlsConnector is built on every `start()` call because
        // native_tls connectors are cheap to construct — caching them
        // across calls adds complexity without measurable benefit.
        let mut tls_builder = native_tls::TlsConnector::builder();
        match config.ssl_mode {
            SslMode::Require => {
                // Accept any server certificate (including self-signed) but
                // still verify the hostname matches.  Users who need to
                // disable hostname verification should use VerifyCa.
                tls_builder.danger_accept_invalid_certs(true);
            }
            SslMode::VerifyCa => {
                // ⚠️ SECURITY: CA-issued certs are accepted, but the hostname
                // is NOT checked (matching PostgreSQL's sslmode=verify-ca).
                // This means a valid cert issued to *any* hostname will be
                // accepted.  Prefer SslMode::VerifyFull in production.
                warn!("SslMode::VerifyCa: server certificate hostname will NOT be verified");
                tls_builder.danger_accept_invalid_hostnames(true);
            }
            SslMode::VerifyFull => {}
            SslMode::Disable => {
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

    send_startup(&mut stream, config).await?;
    authenticate(&mut stream, config).await?;
    read_ready_for_query(&mut stream).await?;

    let lsn_str = opts.start_lsn.to_string();
    let query = format!(
        "START_REPLICATION SLOT \"{}\" LOGICAL {lsn_str} \
         (proto_version '1', publication_names '{}')",
        opts.slot_name, opts.publication
    );
    send_query(&mut stream, &query).await?;
    read_copy_both_response(&mut stream).await?;

    let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
    let current_lsn = Arc::new(AtomicI64::new(-1));
    let reader_lsn = Arc::clone(&current_lsn);
    let reader_handle = tokio::spawn(reader_task(stream, tx, reader_lsn));

    let stream = ReplicationStream {
        rx,
        reader_handle: Some(reader_handle),
        current_lsn,
    };

    info!("replication stream established");
    Ok(stream)
}

/// Process a single CopyData payload from the replication stream.
pub(crate) async fn parse_copy_data_payload(
    stream: &mut MaybeTls,
    tx: &mpsc::Sender<Result<Vec<u8>, TapError>>,
    current_lsn: &Arc<AtomicI64>,
    last_received_lsn: &mut i64,
    last_flushed_lsn: &mut i64,
    last_keepalive_time: &mut tokio::time::Instant,
) -> bool {
    let raw_len = match read_i32(stream).await {
        Ok(l) => {
            if l < 4 {
                let _ = tx
                    .send(Err(TapError::Decode(format!(
                        "negative CopyData length: {l}"
                    ))))
                    .await;
                return false;
            }
            l as usize
        }
        Err(e) => {
            let _ = tx.send(Err(e)).await;
            return false;
        }
    };
    let payload_len = raw_len - 4;
    if payload_len > super::MAX_MESSAGE_SIZE {
        let _ = tx
            .send(Err(TapError::Decode(format!(
                "CopyData payload too large: {payload_len} bytes"
            ))))
            .await;
        return false;
    }

    let mut payload = vec![0u8; payload_len];
    if let Err(e) = stream.read_exact(&mut payload).await {
        let _ = tx.send(Err(wrap_io_err(e))).await;
        return false;
    }

    if payload.is_empty() {
        debug!("empty CopyData payload (possible keepalive)");
        let _ = tx.send(Ok(Vec::new())).await;
        return true;
    }

    let sub_type = payload[0];
    match sub_type {
        SUBTYPE_XLOG_DATA => {
            if payload.len() < XLOG_DATA_HEADER_SIZE {
                let _ = tx
                    .send(Err(TapError::Decode(format!(
                        "truncated XLogData: {} bytes",
                        payload.len()
                    ))))
                    .await;
                return false;
            }
            let start_lsn_arr: [u8; 8] = payload[1..9].try_into().expect("XLogData size validated");
            let end_lsn_arr: [u8; 8] = payload[9..17].try_into().expect("XLogData size validated");
            let ts_arr: [u8; 8] = payload[17..XLOG_DATA_HEADER_SIZE]
                .try_into()
                .expect("XLogData size validated");
            let _start_lsn = i64::from_be_bytes(start_lsn_arr);
            let end_lsn = i64::from_be_bytes(end_lsn_arr);
            let _timestamp = i64::from_be_bytes(ts_arr);

            *last_received_lsn = end_lsn;
            current_lsn.store(end_lsn, Ordering::Release);

            let wal_data = payload[XLOG_DATA_HEADER_SIZE..].to_vec();
            if !wal_data.is_empty() && tx.send(Ok(wal_data)).await.is_err() {
                return false;
            }
        }
        SUBTYPE_KEEPALIVE => {
            if payload.len() < KEEPALIVE_HEADER_SIZE {
                debug!("truncated Keepalive message, skipping");
                return true;
            }
            let wal_end_arr: [u8; 8] = payload[1..9].try_into().expect("Keepalive size validated");
            let _ts_arr: [u8; 8] = payload[9..17].try_into().expect("Keepalive size validated");
            let wal_end = i64::from_be_bytes(wal_end_arr);
            let _ts = i64::from_be_bytes(_ts_arr);
            let reply_required = payload.len() >= KEEPALIVE_HEADER_SIZE && payload[17] != 0;

            *last_flushed_lsn = (*last_flushed_lsn).max(wal_end);
            *last_received_lsn = (*last_received_lsn).max(wal_end);
            current_lsn.store(*last_received_lsn, Ordering::Release);

            if reply_required {
                debug!("sending standby status update (keepalive requested)");
                if let Err(e) = send_standby_status_update(
                    stream,
                    *last_received_lsn,
                    *last_flushed_lsn,
                    *last_received_lsn,
                )
                .await
                {
                    let _ = tx.send(Err(e)).await;
                    return false;
                }
                *last_keepalive_time = tokio::time::Instant::now();
            }

            let _ = tx.send(Ok(Vec::new())).await;
        }
        SUBTYPE_STANDBY_STATUS_UPDATE => {
            debug!("received standby status update echo");
        }
        other => {
            debug!(
                "unknown CopyData sub-type 0x{other:02x}, skipping {} bytes",
                payload_len
            );
        }
    }
    true
}

/// Background task that reads CopyData messages from the wire, parses
/// XLogData frames, auto-replies to Keepalive, and sends WAL payloads
/// through the channel.
pub(crate) async fn reader_task(
    mut stream: MaybeTls,
    tx: mpsc::Sender<Result<Vec<u8>, TapError>>,
    current_lsn: Arc<AtomicI64>,
) {
    let mut last_received_lsn: i64 = 0;
    let mut last_flushed_lsn: i64 = 0;
    // Initialized to now() so the first heartbeat check doesn't fire
    // immediately — it waits HEARTBEAT_INTERVAL_SECS before sending the
    // first StandbyStatusUpdate, avoiding a redundant update on connect.
    let mut last_keepalive_time: tokio::time::Instant = tokio::time::Instant::now();

    loop {
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
            TYPE_COPY_DATA => {
                if !parse_copy_data_payload(
                    &mut stream,
                    &tx,
                    &current_lsn,
                    &mut last_received_lsn,
                    &mut last_flushed_lsn,
                    &mut last_keepalive_time,
                )
                .await
                {
                    return;
                }
            }
            TYPE_COPY_DONE => {
                info!("server sent CopyDone, replication stream ending");
                return;
            }
            TYPE_ERROR_RESPONSE => {
                let error_info = match read_error_response(&mut stream).await {
                    Ok(info) => info,
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                };
                let _ = tx
                    .send(Err(TapError::PostgresConnectionRedacted(
                        error_info.message,
                    )))
                    .await;
                return;
            }
            other => {
                debug!("unknown message type 0x{other:02x} in replication stream, skipping");
                let len = match read_i32(&mut stream).await {
                    Ok(l) => {
                        if l < 4 {
                            let _ = tx
                                .send(Err(TapError::Decode(format!(
                                    "negative message length: {l}"
                                ))))
                                .await;
                            return;
                        }
                        l as usize
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                };
                let _ = skip_bytes(&mut stream, len.saturating_sub(4)).await;
            }
        }

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

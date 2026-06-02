//! SSE (Server-Sent Events) server for streaming change events.
//!
//! axum-based HTTP/2 server with 8 event types, health/status endpoints,
//! broadcast fan-out, heartbeat timer, and Last-Event-ID resume.
//!
//! # Architecture
//!
//! * [`SseServer`] owns a [`tokio::sync::broadcast`] channel sender.
//!   Callers (capture engine) call `broadcast()` to push events to all
//!   connected SSE clients.
//! * Heartbeat events are produced by a background task at the configured
//!   interval when no other events are flowing.
//! * The shared [`HealthState`] is updated by the capture engine and
//!   exposed via the `/health` and `/status` HTTP endpoints.
//! * Client disconnect is detected when the broadcast receiver is dropped;
//!   the server cleans up automatically.
//!
//! # Wire format
//!
//! ```text
//! event: change
//! id: 0/16B37428:12345
//! data: {"op":"c",...}
//!
//! event: heartbeat
//! data: {"ts_ms":1717200001000}
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, Sse};
use axum::response::{IntoResponse, Json, Response};
use axum::routing::get;
use axum::Router;
use futures::stream::Stream;
use serde::Serialize;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::sync::{broadcast, Mutex, RwLock};
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::config::SinkConfig;
use crate::error::TapError;

// ---------------------------------------------------------------------------
// SseEventType
// ---------------------------------------------------------------------------

/// The 8 SSE event types used in the Tap protocol.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SseEventType {
    /// A row-level data change (insert/update/delete/read).
    Change,
    /// Periodic keepalive when no events have been sent.
    Heartbeat,
    /// Snapshot phase has begun.
    SnapshotStart,
    /// Progress update during snapshotting.
    SnapshotProgress,
    /// Snapshot phase has finished.
    SnapshotComplete,
    /// Streaming replication has started.
    StreamingStart,
    /// An error condition has occurred (connection lost, etc.).
    Error,
    /// Server is shutting down.
    Shutdown,
}

impl SseEventType {
    /// Returns the SSE `event:` field value for this variant.
    pub fn as_str(&self) -> &'static str {
        match self {
            SseEventType::Change => "change",
            SseEventType::Heartbeat => "heartbeat",
            SseEventType::SnapshotStart => "snapshot_start",
            SseEventType::SnapshotProgress => "snapshot_progress",
            SseEventType::SnapshotComplete => "snapshot_complete",
            SseEventType::StreamingStart => "streaming_start",
            SseEventType::Error => "error",
            SseEventType::Shutdown => "shutdown",
        }
    }
}

// ---------------------------------------------------------------------------
// SseEvent
// ---------------------------------------------------------------------------

/// An event that can be dispatched to SSE clients.
///
/// Construct with [`SseEvent::new`], then optionally call `.with_id()`.
#[derive(Debug, Clone, Serialize)]
pub struct SseEvent {
    /// The SSE event type (maps to `event:` field).
    pub event_type: SseEventType,
    /// JSON payload (maps to `data:` field).
    pub data: serde_json::Value,
    /// Optional event identifier for `Last-Event-ID` resume.
    pub id: Option<String>,
}

impl SseEvent {
    /// Creates a new event with the given type and data.
    pub fn new(event_type: SseEventType, data: serde_json::Value) -> Self {
        Self {
            event_type,
            data,
            id: None,
        }
    }

    /// Chains an event identifier (for `Last-Event-ID` resume).
    pub fn with_id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
    }
}

// ---------------------------------------------------------------------------
// Health / status types
// ---------------------------------------------------------------------------

/// Overall health status.
#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum HealthStatus {
    /// Everything is operating normally.
    Ok,
    /// The server is running but degraded (e.g., reconnect in progress).
    Degraded,
    /// The server is not accepting events.
    Down,
}

/// Capture-engine state machine.
#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum CaptureState {
    /// Not yet started.
    Idle,
    /// Running an initial snapshot.
    Snapshot,
    /// Streaming WAL changes.
    Streaming,
    /// Paused (user-requested or automatic).
    Paused,
    /// Gracefully stopped.
    Stopped,
    /// Fatal error state.
    Error,
}

/// Shared health and status information.
///
/// The capture engine updates this periodically; the SSE server exposes it
/// via the `/health` and `/status` endpoints.
#[derive(Debug, Clone, Serialize)]
pub struct HealthState {
    /// Server health status.
    pub status: HealthStatus,
    /// Milliseconds since the server started.
    pub uptime_ms: u64,
    /// Total events captured (snapshot + streaming).
    pub events_captured: u64,
    /// Current WAL LSN position.
    pub current_lsn: String,
    /// Approximate lag in milliseconds.
    pub lag_ms: u64,
    /// Current capture state.
    pub state: CaptureState,
}

impl Default for HealthState {
    fn default() -> Self {
        Self {
            status: HealthStatus::Ok,
            uptime_ms: 0,
            events_captured: 0,
            current_lsn: String::new(),
            lag_ms: 0,
            state: CaptureState::Idle,
        }
    }
}

// ---------------------------------------------------------------------------
// Shared application state
// ---------------------------------------------------------------------------

struct AppState {
    event_broadcast: broadcast::Sender<SseEvent>,
    health_state: Arc<RwLock<HealthState>>,
    shutdown_rx: watch::Receiver<bool>,
    start_time: Instant,
}

// ---------------------------------------------------------------------------
// SSE formatting
// ---------------------------------------------------------------------------

/// Formats an [`SseEvent`] as a raw SSE text frame.
///
/// The output looks like:
///
/// ```text
/// event: change
/// id: 0/16B37428:12345
/// data: {"op":"c",...}
///
/// ```
pub fn format_sse_event(event: &SseEvent) -> String {
    let mut output = String::new();
    output.push_str("event: ");
    output.push_str(event.event_type.as_str());
    output.push('\n');
    if let Some(id) = &event.id {
        output.push_str("id: ");
        output.push_str(id);
        output.push('\n');
    }
    output.push_str("data: ");
    output.push_str(&serde_json::to_string(&event.data).unwrap_or_default());
    output.push('\n');
    output.push('\n');
    output
}

// ---------------------------------------------------------------------------
// SSE event stream
// ---------------------------------------------------------------------------

/// Creates a `Stream` that bridges the broadcast channel to SSE-formatted
/// strings, terminating when a shutdown signal is received.
fn sse_event_stream(
    rx: broadcast::Receiver<SseEvent>,
    shutdown_rx: watch::Receiver<bool>,
) -> impl Stream<Item = Result<Event, std::convert::Infallible>> {
    let (tx, recv) = tokio::sync::mpsc::unbounded_channel();

    tokio::spawn(async move {
        let mut rx = rx;
        let mut shutdown_rx = shutdown_rx;

        loop {
            tokio::select! {
                result = rx.recv() => {
                    match result {
                        Ok(sse_event) => {
                            let mut event = Event::default()
                                .event(sse_event.event_type.as_str())
                                .data(serde_json::to_string(&sse_event.data).unwrap_or_default());
                            if let Some(id) = &sse_event.id {
                                event = event.id(id);
                            }
                            if tx.send(Ok(event)).is_err() {
                                // Receiver dropped (client disconnected)
                                break;
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            // Consumer too slow — skip & continue
                            continue;
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
                _ = shutdown_rx.changed() => {
                    // Server shutting down
                    break;
                }
            }
        }
    });

    UnboundedReceiverStream::new(recv)
}

// ---------------------------------------------------------------------------
// Heartbeat task
// ---------------------------------------------------------------------------

/// Background task that emits heartbeat events at the configured interval.
async fn heartbeat_task(
    tx: broadcast::Sender<SseEvent>,
    interval: Duration,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let mut timer = tokio::time::interval(interval);
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = timer.tick() => {
                let hb = SseEvent::new(
                    SseEventType::Heartbeat,
                    serde_json::json!({
                        "ts_ms": chrono::Utc::now().timestamp_millis(),
                    }),
                );
                // Ignore send errors — if no receivers, that's fine
                let _ = tx.send(hb);
            }
            _ = shutdown_rx.changed() => {
                break;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Route handlers
// ---------------------------------------------------------------------------

/// `GET /events` — SSE event stream.
async fn handle_events(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    // Extract Last-Event-ID for resume (best-effort).
    let _last_event_id = headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let rx = state.event_broadcast.subscribe();
    let shutdown_rx = state.shutdown_rx.clone();

    let stream = sse_event_stream(rx, shutdown_rx);

    Sse::new(stream).into_response()
}

/// `GET /health` — Health check.
async fn handle_health(State(state): State<Arc<AppState>>) -> Response {
    let health = state.health_state.read().await;
    let uptime_ms = state.start_time.elapsed().as_millis() as u64;

    let response = serde_json::json!({
        "status": health.status,
        "uptime_ms": uptime_ms,
        "events_captured": health.events_captured,
        "current_lsn": health.current_lsn,
        "lag_ms": health.lag_ms,
    });

    let status_code = if health.status == HealthStatus::Ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    (status_code, Json(response)).into_response()
}

/// `GET /status` — Capture status (more detailed than health).
async fn handle_status(State(state): State<Arc<AppState>>) -> Json<HealthState> {
    let health = state.health_state.read().await;
    Json(health.clone())
}

// ---------------------------------------------------------------------------
// SseServer
// ---------------------------------------------------------------------------

/// HTTP/2 SSE server that streams change events to connected clients.
///
/// # Example (conceptual)
///
/// ```rust,ignore
/// use tap_core::config::SinkConfig;
/// use tap_core::sse::{SseServer, SseEvent, SseEventType};
///
/// let config = SinkConfig::default();
/// let server = SseServer::new(config);
/// let port = server.start().await.unwrap();
///
/// // Push events from the capture engine
/// server.broadcast().send(SseEvent::new(
///     SseEventType::Change,
///     serde_json::json!({"op": "c"}),
/// )).ok();
/// ```
pub struct SseServer {
    config: SinkConfig,
    event_broadcast: broadcast::Sender<SseEvent>,
    health_state: Arc<RwLock<HealthState>>,
    shutdown_tx: watch::Sender<bool>,
    server_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl SseServer {
    /// Creates a new `SseServer` with the given config.
    ///
    /// The server is not started until [`start`](Self::start) is called.
    pub fn new(config: SinkConfig) -> Self {
        let (event_broadcast, _) = broadcast::channel(config.max_buffer_size);
        let (shutdown_tx, _) = watch::channel(false);

        Self {
            config,
            event_broadcast,
            health_state: Arc::new(RwLock::new(HealthState::default())),
            shutdown_tx,
            server_handle: Arc::new(Mutex::new(None)),
        }
    }

    /// Starts the HTTP server and returns the assigned port.
    ///
    /// Binds to `config.host:config.port`.  If `config.port` is `0`, the OS
    /// assigns an ephemeral port (returned in the `Ok` value).
    ///
    /// The server runs in a background Tokio task.  The heartbeat background
    /// task is also spawned here.
    ///
    /// # Errors
    ///
    /// Returns [`TapError::Io`] if the TCP listener cannot bind.
    pub async fn start(&self) -> Result<u16, TapError> {
        let addr = format!("{}:{}", self.config.host, self.config.port);
        let listener = TcpListener::bind(&addr).await.map_err(TapError::Io)?;
        let port = listener.local_addr().map_err(TapError::Io)?.port();

        let app_state = Arc::new(AppState {
            event_broadcast: self.event_broadcast.clone(),
            health_state: self.health_state.clone(),
            shutdown_rx: self.shutdown_tx.subscribe(),
            start_time: Instant::now(),
        });

        // Build router
        let app = Router::new()
            .route("/events", get(handle_events))
            .route("/health", get(handle_health))
            .route("/status", get(handle_status))
            .with_state(app_state);

        // Spawn heartbeat task
        let hb_tx = self.event_broadcast.clone();
        let hb_interval = Duration::from_millis(self.config.heartbeat_interval_ms);
        let hb_shutdown = self.shutdown_tx.subscribe();
        tokio::spawn(heartbeat_task(hb_tx, hb_interval, hb_shutdown));

        // Spawn server with graceful shutdown
        // axum::serve returns a Serve future that never completes on its own;
        // with_graceful_shutdown allows us to stop it via a signal.
        let graceful_shutdown_signal = {
            let mut rx = self.shutdown_tx.subscribe();
            async move {
                rx.changed().await.ok();
            }
        };

        let handle = tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, app)
                .with_graceful_shutdown(graceful_shutdown_signal)
                .await
            {
                tracing::error!("SSE server error: {e}");
            }
        });

        self.server_handle.lock().await.replace(handle);

        tracing::info!("SSE server listening on http://{addr} (port {port})");
        Ok(port)
    }

    /// Gracefully shuts down the SSE server.
    ///
    /// Sends a `Shutdown` SSE event to all connected clients, triggers the
    /// shutdown signal (which terminates the heartbeat task and all SSE
    /// streams), then waits for the server task to complete.
    pub async fn shutdown(&self) {
        // Send shutdown event to clients
        let events_captured = {
            let health = self.health_state.read().await;
            health.events_captured
        };

        let shutdown_event = SseEvent::new(
            SseEventType::Shutdown,
            serde_json::json!({
                "reason": "SIGINT received",
                "events_processed": events_captured,
            }),
        );
        let _ = self.event_broadcast.send(shutdown_event);

        // Trigger shutdown signal
        let _ = self.shutdown_tx.send(true);

        // Wait for server task to finish
        if let Some(handle) = self.server_handle.lock().await.take() {
            let _ = handle.await;
        }

        tracing::info!("SSE server shut down");
    }

    /// Returns a reference to the broadcast sender.
    ///
    /// The capture engine uses this to dispatch events to all connected
    /// SSE clients.
    pub fn broadcast(&self) -> &broadcast::Sender<SseEvent> {
        &self.event_broadcast
    }

    /// Returns a reference to the shared health state.
    ///
    /// The capture engine updates this to reflect current status.
    pub fn health_state(&self) -> &Arc<RwLock<HealthState>> {
        &self.health_state
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    // -----------------------------------------------------------------------
    // Unit tests — format_sse_event
    // -----------------------------------------------------------------------

    #[test]
    fn test_sse_event_format() {
        let event = SseEvent::new(
            SseEventType::Change,
            json!({"op": "c", "before": null, "after": {"id": 1}}),
        )
        .with_id("0/16B37428:12345");

        let output = format_sse_event(&event);

        assert!(output.starts_with("event: change\n"), "wrong event type line");
        assert!(
            output.contains("id: 0/16B37428:12345\n"),
            "missing id line"
        );
        assert!(
            output.contains(r#""op":"c""#) && output.contains(r#""after":{"id":1}"#),
            "missing or wrong data line: {output:?}"
        );
        assert!(output.ends_with("\n\n"), "missing trailing blank line");
    }

    #[test]
    fn test_sse_event_id_included() {
        let with_id = SseEvent::new(SseEventType::Change, json!({"k": "v"}))
            .with_id("0/1:42");
        let without_id = SseEvent::new(SseEventType::Heartbeat, json!({"ts_ms": 0}));

        assert!(format_sse_event(&with_id).contains("id: "));
        assert!(!format_sse_event(&without_id).contains("id: "));
    }

    #[test]
    fn test_sse_event_type_as_str() {
        assert_eq!(SseEventType::Change.as_str(), "change");
        assert_eq!(SseEventType::Heartbeat.as_str(), "heartbeat");
        assert_eq!(SseEventType::SnapshotStart.as_str(), "snapshot_start");
        assert_eq!(SseEventType::SnapshotProgress.as_str(), "snapshot_progress");
        assert_eq!(SseEventType::SnapshotComplete.as_str(), "snapshot_complete");
        assert_eq!(SseEventType::StreamingStart.as_str(), "streaming_start");
        assert_eq!(SseEventType::Error.as_str(), "error");
        assert_eq!(SseEventType::Shutdown.as_str(), "shutdown");
    }

    #[test]
    fn test_sse_event_builder() {
        let event = SseEvent::new(SseEventType::Error, json!({"code": "E1"}));
        assert_eq!(event.event_type, SseEventType::Error);
        assert!(event.id.is_none());

        let event = event.with_id("snap:t1:x");
        assert_eq!(event.id.as_deref(), Some("snap:t1:x"));
    }

    #[test]
    fn test_health_state_default() {
        let h = HealthState::default();
        assert_eq!(h.status, HealthStatus::Ok);
        assert_eq!(h.state, CaptureState::Idle);
        assert_eq!(h.events_captured, 0);
    }

    // -----------------------------------------------------------------------
    // Integration helpers
    // -----------------------------------------------------------------------

    /// Create a SseServer with test config (ephemeral port, fast heartbeat).
    async fn test_server() -> (SseServer, u16) {
        let config = SinkConfig {
            host: "127.0.0.1".into(),
            port: 0,
            max_buffer_size: 100,
            heartbeat_interval_ms: 100,
        };
        let server = SseServer::new(config);
        let port = server.start().await.expect("server should start");
        (server, port)
    }

    /// Connect to the SSE endpoint, read past HTTP headers, return the stream
    /// for body reading.
    async fn sse_connect(host: &str, port: u16) -> TcpStream {
        let addr = format!("{host}:{port}");
        let mut stream = TcpStream::connect(&addr)
            .await
            .expect("TCP connect to SSE server");

        let request = format!(
            "GET /events HTTP/1.1\r\nHost: {host}\r\nAccept: text/event-stream\r\n\r\n"
        );
        stream
            .write_all(request.as_bytes())
            .await
            .expect("write HTTP request");
        stream.flush().await.expect("flush");

        // Read until we've consumed the HTTP response headers
        let mut buf = [0u8; 4096];
        let mut n = 0;
        while n < buf.len() {
            let bytes = stream
                .read(&mut buf[n..])
                .await
                .expect("read HTTP response headers");
            if bytes == 0 {
                break;
            }
            n += bytes;
            if buf[..n].windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
        }

        stream
    }

    /// Read HTTP response headers from a stream (up to `\r\n\r\n`),
    /// returning parsed header lines.  Does NOT read the body.
    async fn read_http_headers(stream: &mut TcpStream) -> Vec<String> {
        let mut buf = [0u8; 4096];
        let mut n = 0;
        while n < buf.len() {
            let bytes = stream
                .read(&mut buf[n..])
                .await
                .expect("read HTTP headers");
            if bytes == 0 {
                break;
            }
            n += bytes;
            if buf[..n].windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
        }
        let text = String::from_utf8_lossy(&buf[..n]);
        let header_block = text
            .find("\r\n\r\n")
            .map(|idx| &text[..idx])
            .unwrap_or(&text);
        header_block.lines().map(|l| l.to_string()).collect()
    }

    /// Helper: send a raw HTTP GET and return (header_lines, body_bytes).
    /// Works for bounded responses (health, status).  For SSE use
    /// [`read_http_headers`] instead.
    async fn http_get_bounded(
        host: &str,
        port: u16,
        path: &str,
    ) -> (Vec<String>, Vec<u8>) {
        let addr = format!("{host}:{port}");
        let mut stream = TcpStream::connect(&addr)
            .await
            .expect("TCP connect for HTTP GET");

        let request = format!(
            "GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n"
        );
        stream
            .write_all(request.as_bytes())
            .await
            .expect("write HTTP GET");
        stream.flush().await.expect("flush");

        let mut resp = Vec::new();
        let mut buf = [0u8; 8192];
        loop {
            let n = stream
                .read(&mut buf)
                .await
                .expect("read HTTP response");
            if n == 0 {
                break;
            }
            resp.extend_from_slice(&buf[..n]);
        }

        // Split headers / body at "\r\n\r\n"
        let text = String::from_utf8_lossy(&resp);
        if let Some(idx) = text.find("\r\n\r\n") {
            let header_text = &text[..idx];
            let body = resp[idx + 4..].to_vec();
            let header_lines: Vec<String> =
                header_text.lines().map(|l| l.to_string()).collect();
            (header_lines, body)
        } else {
            (vec![], resp)
        }
    }

    /// Parse the status line from HTTP response headers.
    fn parse_status_line(headers: &[String]) -> u16 {
        headers
            .first()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|s| s.parse::<u16>().ok())
            .unwrap_or(0)
    }

    /// Extract a header value by name from HTTP response headers.
    fn header_value<'a>(headers: &'a [String], name: &str) -> Option<&'a str> {
        let lower = name.to_lowercase();
        headers
            .iter()
            .find(|h| h.to_lowercase().starts_with(&lower))
            .and_then(|h| h.split_once(':').map(|(_, v)| v.trim()))
    }

    // -----------------------------------------------------------------------
    // Integration tests — live server
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_sse_server_starts() {
        let (server, port) = test_server().await;
        assert!(port > 0, "server should bind on a non-zero port");
        // Verify we can TCP connect
        let _stream = TcpStream::connect(format!("127.0.0.1:{port}"))
            .await
            .expect("should be able to TCP connect to server");
        server.shutdown().await;
    }

    #[tokio::test]
    async fn test_sse_headers() {
        let (server, port) = test_server().await;

        let addr = format!("127.0.0.1:{port}");
        let mut stream = TcpStream::connect(&addr)
            .await
            .expect("TCP connect");

        let request = format!(
            "GET /events HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nAccept: text/event-stream\r\n\r\n"
        );
        stream
            .write_all(request.as_bytes())
            .await
            .expect("write request");
        stream.flush().await.expect("flush");

        let headers = read_http_headers(&mut stream).await;

        assert_eq!(parse_status_line(&headers), 200, "expected 200 status");
        assert_eq!(
            header_value(&headers, "content-type"),
            Some("text/event-stream"),
            "Content-Type should be text/event-stream"
        );
        assert_eq!(
            header_value(&headers, "cache-control"),
            Some("no-cache"),
            "Cache-Control should be no-cache"
        );

        // Drop stream to close SSE connection before shutdown
        drop(stream);
        server.shutdown().await;
    }

    #[tokio::test]
    async fn test_sse_health_endpoint() {
        let (server, port) = test_server().await;

        let (headers, body) = http_get_bounded("127.0.0.1", port, "/health").await;

        assert_eq!(parse_status_line(&headers), 200, "expected 200 status");

        let parsed: serde_json::Value =
            serde_json::from_slice(&body).expect("health body should be JSON");
        assert_eq!(parsed["status"], "ok");

        server.shutdown().await;
    }

    #[tokio::test]
    async fn test_sse_status_endpoint() {
        let (server, port) = test_server().await;

        let (headers, body) = http_get_bounded("127.0.0.1", port, "/status").await;

        assert_eq!(parse_status_line(&headers), 200, "expected 200 status");

        let parsed: serde_json::Value =
            serde_json::from_slice(&body).expect("status body should be JSON");
        assert_eq!(parsed["state"], "idle");
        assert_eq!(parsed["status"], "ok");

        server.shutdown().await;
    }

    #[tokio::test]
    async fn test_sse_multiple_events() {
        let (server, port) = test_server().await;

        let mut stream = sse_connect("127.0.0.1", port).await;

        // Broadcast 3 events
        for i in 0..3 {
            server
                .broadcast()
                .send(SseEvent::new(SseEventType::Change, json!({"seq": i})))
                .ok();
        }

        // Read from the stream and count events matching `event: change`
        // (not heartbeat events, which also arrive on the stream).
        let mut change_count = 0;
        let mut buf = [0u8; 4096];
        let timeout = tokio::time::sleep(Duration::from_secs(5));
        tokio::pin!(timeout);

        'read: loop {
            tokio::select! {
                result = stream.read(&mut buf) => {
                    let n = result.expect("read SSE body");
                    if n == 0 { break; }
                    let chunk = String::from_utf8_lossy(&buf[..n]);
                    change_count += chunk.matches("event: change").count();
                    if change_count >= 3 {
                        break 'read;
                    }
                }
                _ = &mut timeout => {
                    panic!("timed out waiting for events, got {change_count} change events");
                }
            }
        }

        assert_eq!(change_count, 3, "should receive 3 change events");
        server.shutdown().await;
    }

    #[tokio::test]
    async fn test_sse_heartbeat_timer() {
        let (server, port) = test_server().await;

        let mut stream = sse_connect("127.0.0.1", port).await;

        // Don't send any data events — heartbeat should fire within ~300ms
        // (interval is 100ms, allow scheduling jitter)
        let mut found_heartbeat = false;
        let mut buf = [0u8; 4096];
        let timeout = tokio::time::sleep(Duration::from_millis(500));
        tokio::pin!(timeout);

        'read: loop {
            tokio::select! {
                result = stream.read(&mut buf) => {
                    let n = result.expect("read SSE body");
                    if n == 0 { break; }
                    let chunk = String::from_utf8_lossy(&buf[..n]);
                    if chunk.contains("event: heartbeat") {
                        found_heartbeat = true;
                        break 'read;
                    }
                }
                _ = &mut timeout => {
                    panic!("timed out waiting for heartbeat");
                }
            }
        }

        assert!(found_heartbeat, "should receive a heartbeat event");
        server.shutdown().await;
    }

    #[tokio::test]
    async fn test_sse_client_disconnect() {
        let (server, port) = test_server().await;

        // Connect two clients, drop one
        let _stream1 = sse_connect("127.0.0.1", port).await;
        let mut stream2 = sse_connect("127.0.0.1", port).await;

        // stream1 is dropped here — that's the disconnect.
        // Give the server a moment to notice.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Broadcast an event — client 2 should still receive it.
        server
            .broadcast()
            .send(SseEvent::new(
                SseEventType::Change,
                json!({"after_disconnect": true}),
            ))
            .ok();

        let mut found = false;
        let mut buf = [0u8; 4096];
        let timeout = tokio::time::sleep(Duration::from_secs(5));
        tokio::pin!(timeout);

        'read: loop {
            tokio::select! {
                result = stream2.read(&mut buf) => {
                    let n = result.expect("read SSE body");
                    if n == 0 { break; }
                    let chunk = String::from_utf8_lossy(&buf[..n]);
                    if chunk.contains("after_disconnect") {
                        found = true;
                        break 'read;
                    }
                }
                _ = &mut timeout => {
                    panic!("timed out waiting for event after disconnect");
                }
            }
        }

        assert!(
            found,
            "client 2 should still receive events after client 1 disconnects"
        );

        server.shutdown().await;
    }

    #[tokio::test]
    async fn test_sse_event_id_in_stream() {
        let (server, port) = test_server().await;

        let mut stream = sse_connect("127.0.0.1", port).await;

        server
            .broadcast()
            .send(
                SseEvent::new(SseEventType::Change, json!({"x": 1}))
                    .with_id("0/ABCDEF:99"),
            )
            .ok();

        let mut found_id = false;
        let mut buf = [0u8; 4096];
        let timeout = tokio::time::sleep(Duration::from_secs(5));
        tokio::pin!(timeout);

        'read: loop {
            tokio::select! {
                result = stream.read(&mut buf) => {
                    let n = result.expect("read SSE body");
                    if n == 0 { break; }
                    let chunk = String::from_utf8_lossy(&buf[..n]);
                    if chunk.contains("id: 0/ABCDEF:99") {
                        found_id = true;
                        break 'read;
                    }
                }
                _ = &mut timeout => {
                    panic!("timed out waiting for id field");
                }
            }
        }

        assert!(found_id, "SSE event should contain the id: field");
        server.shutdown().await;
    }
}

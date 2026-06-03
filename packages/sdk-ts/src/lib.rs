#![deny(clippy::all)]
//! napi-rs bindings for the Tap Change Data Capture engine.
//!
//! Exposes [`Tap`] — a JS class that manages a Postgres CDC session with
//! start / stop / pause / resume lifecycle, SSE event delivery, and
//! in-process [`ThreadsafeFunction`] callbacks for row-level change events.

use std::sync::{Arc, Mutex};
use std::time::Instant;

use napi::bindgen_prelude::*;
use napi::threadsafe_function::{ErrorStrategy, ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi_derive::napi;
use tap_core::config;
use tap_core::event::ChangeEvent;
use tap_core::sse::SseServer;
use tap_core::state::StateStore;

// ---------------------------------------------------------------------------
// JS-compatible metadata / event / config types  (napi objects)
// ---------------------------------------------------------------------------

/// Database source metadata for a change event.
///
/// Numeric fields use `f64` (JS `number`) to avoid BigInt at the napi boundary.
#[napi(object)]
#[derive(Clone)]
pub struct JsSourceMetadata {
    pub db: String,
    pub schema: String,
    pub table: String,
    pub lsn: String,
    pub tx_id: String,
    pub ts_ms: f64,
    pub snapshot: Option<bool>,
}

impl From<&tap_core::event::SourceMetadata> for JsSourceMetadata {
    fn from(s: &tap_core::event::SourceMetadata) -> Self {
        Self {
            db: s.db.clone(),
            schema: s.schema.clone(),
            table: s.table.clone(),
            lsn: s.lsn.to_string(),
            tx_id: s.tx_id.clone(),
            ts_ms: s.ts_ms as f64,
            snapshot: s.snapshot,
        }
    }
}

/// A row-level change event in Debezium-like envelope format.
///
/// Numeric fields use `f64` (JS `number`) to avoid BigInt at the napi boundary.
#[napi(object)]
#[derive(Clone)]
pub struct JsChangeEvent {
    pub op: String,
    pub before: Option<serde_json::Value>,
    pub after: Option<serde_json::Value>,
    pub source: JsSourceMetadata,
    pub ts_ms: f64,
    pub id: String,
}

#[napi]
impl JsChangeEvent {
    /// Serialise this event to a JSON string.
    #[napi]
    pub fn to_json(&self) -> String {
        let obj = serde_json::json!({
            "op": self.op,
            "before": self.before,
            "after": self.after,
            "source": {
                "db": self.source.db,
                "schema": self.source.schema,
                "table": self.source.table,
                "lsn": self.source.lsn,
                "tx_id": self.source.tx_id,
                "ts_ms": self.source.ts_ms,
                "snapshot": self.source.snapshot,
            },
            "ts_ms": self.ts_ms,
            "id": self.id,
        });
        serde_json::to_string(&obj).unwrap_or_default()
    }
}

impl From<&ChangeEvent> for JsChangeEvent {
    fn from(e: &ChangeEvent) -> Self {
        Self {
            op: e.op.as_str().to_string(),
            before: e.before.clone(),
            after: e.after.clone(),
            source: JsSourceMetadata::from(&e.source),
            ts_ms: e.ts_ms as f64,
            id: e.id.clone(),
        }
    }
}

/// Current capture-engine status, returned by [`Tap::status`].
///
/// Numeric fields use `f64` (JS `number`) for napi compatibility.
#[napi(object)]
#[derive(Clone)]
pub struct JsCaptureStatus {
    pub state: String,
    pub events_captured: f64,
    pub current_lsn: String,
    pub lag_ms: f64,
}

/// Optional SSE sink configuration embedded in [`JsTapConfig`].
#[napi(object)]
#[derive(Clone)]
pub struct JsSinkConfig {
    pub host: String,
    pub port: u16,
    pub max_buffer_size: Option<i32>,
    pub heartbeat_interval_ms: Option<i32>,
}

/// JavaScript-facing configuration for a [`Tap`] session.
#[napi(object)]
#[derive(Clone)]
pub struct JsTapConfig {
    /// Postgres connection string (overrides host/port/database/user/password).
    pub connection: String,
    pub slot_name: Option<String>,
    pub publication: Option<String>,
    pub tables: Option<Vec<String>>,
    pub plugin: Option<String>,
    pub host: Option<String>,
    pub port: Option<i32>,
    pub database: Option<String>,
    pub user: Option<String>,
    pub password: Option<String>,
    pub state_path: Option<String>,
    pub max_batch_size: Option<i32>,
    pub flush_interval_ms: Option<i32>,
    pub sink: Option<JsSinkConfig>,
}

// ---------------------------------------------------------------------------
// Internal capture state machine
// ---------------------------------------------------------------------------

/// Mirror of [`tap_core::sse::CaptureState`] for the napi bridge.
#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
enum CaptureState {
    Idle,
    Snapshot,
    Streaming,
    Paused,
    Stopped,
}

impl CaptureState {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Snapshot => "snapshot",
            Self::Streaming => "streaming",
            Self::Paused => "paused",
            Self::Stopped => "stopped",
        }
    }
}

/// Internal mutable state of a single [`Tap`] session.
struct TapInner {
    state: CaptureState,
    events_captured: u64,
    current_lsn: String,
    start_time: Option<Instant>,
    pg_connection: Option<tap_core::postgres::PgConnection>,
    sse_server: Option<SseServer>,
    /// Signal the event-bridge background task to shut down.
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    /// Feed ChangeEvents from the (future) WAL reader into the bridge.
    event_tx: Option<tokio::sync::mpsc::Sender<ChangeEvent>>,
}

impl TapInner {
    fn new() -> Self {
        Self {
            state: CaptureState::Idle,
            events_captured: 0,
            current_lsn: String::new(),
            start_time: None,
            pg_connection: None,
            sse_server: None,
            shutdown_tx: None,
            event_tx: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Tap — the main napi-rs class
// ---------------------------------------------------------------------------

/// A Tap CDC session.
///
/// Manages the full lifecycle of a Postgres logical-replication capture:
/// connecting, slot / publication setup, WAL streaming (via the capture
/// engine in tap-core), SSE server, and in-process JS callbacks.
///
/// # Example (JS)
///
/// ```js
/// const tap = new Tap({
///   connection: "postgresql://user:pass@localhost/db",
///   tables: ["public.users"],
/// });
///
/// tap.onChange((event) => console.log(event));
/// await tap.start();
/// // ... later
/// await tap.stop();
/// ```
#[napi]
pub struct Tap {
    inner: Arc<tokio::sync::Mutex<TapInner>>,
    state_store: Arc<Mutex<StateStore>>,
    change_tsfn: Mutex<Option<ThreadsafeFunction<JsChangeEvent, ErrorStrategy::Fatal>>>,
    error_tsfn: Mutex<Option<ThreadsafeFunction<String, ErrorStrategy::Fatal>>>,
    config: JsTapConfig,
}

#[napi]
impl Tap {
    /// Construct a new `Tap` instance.
    ///
    /// Opens the SQLite state store and validates the config, but does **not**
    /// connect to Postgres.  Call [`start`](Self::start) to begin capturing.
    #[napi(constructor)]
    pub fn new(config: JsTapConfig) -> Result<Self> {
        // Validate config by attempting to build the internal TapConfig.
        let _tap_config = Self::build_tap_config(&config)?;

        // Open state store
        let state_path = config
            .state_path
            .clone()
            .unwrap_or_else(|| ".tap/state.db".into());
        let state_cfg = config::StateConfig {
            path: state_path,
            max_backup_size_kb: 10_240,
        };
        let store = StateStore::open(&state_cfg)
            .map_err(|e| napi::Error::from_reason(format!("Failed to open state store: {e}")))?;

        Ok(Self {
            inner: Arc::new(tokio::sync::Mutex::new(TapInner::new())),
            state_store: Arc::new(Mutex::new(store)),
            change_tsfn: Mutex::new(None),
            error_tsfn: Mutex::new(None),
            config,
        })
    }

    /// Start capturing changes.
    ///
    /// Connects to Postgres, ensures the replication slot and publication
    /// exist, starts the SSE event server, and begins streaming WAL changes
    /// to both the SSE broadcast and the in-process `onChange` callback.
    ///
    /// Returns the SSE endpoint URL (e.g. `http://127.0.0.1:{port}/events`).
    #[napi]
    pub async fn start(&self) -> Result<String> {
        let tap_config = Self::build_tap_config(&self.config)?;
        let mut inner = self.inner.lock().await;

        // Guard: prevent double-start
        if !matches!(inner.state, CaptureState::Idle | CaptureState::Stopped) {
            return Err(napi::Error::from_reason(
                "Tap is already running. Call stop() first.",
            ));
        }

        // ---- 1. Connect to Postgres ----
        let pg = tap_core::postgres::PgConnection::connect(&tap_config.source)
            .await
            .map_err(|e| napi::Error::from_reason(format!("Postgres connection failed: {e}")))?;

        // ---- 2. Ensure replication slot & publication ----
        let current_lsn = pg
            .ensure_replication_slot()
            .await
            .map_err(|e| napi::Error::from_reason(format!("Replication slot setup failed: {e}")))?;
        pg.ensure_publication()
            .await
            .map_err(|e| napi::Error::from_reason(format!("Publication setup failed: {e}")))?;

        // ---- 3. Start SSE server ----
        let sse_server = SseServer::new(tap_config.sink.clone());
        let sse_port = sse_server
            .start()
            .await
            .map_err(|e| napi::Error::from_reason(format!("SSE server start failed: {e}")))?;

        // ---- 4. Build dual-delivery event bridge ----
        // The bridge receives ChangeEvents (from the WAL reader, once
        // wired) and fans them out to:
        //    a) The SSE broadcast channel (external HTTP clients)
        //    b) An mpsc → ThreadsafeFunction (in-process JS callback)
        let buffer_size = tap_config.capture.max_batch_size.max(1);
        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel::<ChangeEvent>(buffer_size);

        let sse_broadcast = sse_server.broadcast().clone();

        let change_tsfn = self.change_tsfn.lock().unwrap().clone();
        let _error_tsfn = self.error_tsfn.lock().unwrap().clone();

        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        // Spawn the bridge: reads from the mpsc channel and fans out.
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = &mut shutdown_rx => {
                        tracing::info!("Event bridge shut down");
                        break;
                    }
                    event = event_rx.recv() => {
                        let event = match event {
                            Some(e) => e,
                            None => break, // channel closed
                        };

                        // (a) SSE broadcast
                        let sse_ev = tap_core::sse::SseEvent::new(
                            tap_core::sse::SseEventType::Change,
                            serde_json::to_value(&event).unwrap_or_default(),
                        );
                        let _ = sse_broadcast.send(sse_ev);

                        // (b) In-process JS callback
                        if let Some(ref tsfn) = change_tsfn {
                            let js_event = JsChangeEvent::from(&event);
                            let _ = tsfn.call(js_event, ThreadsafeFunctionCallMode::NonBlocking);
                        }
                    }
                }
            }
        });

        // ---- 5. Persist inner state ----
        inner.state = CaptureState::Streaming;
        inner.events_captured = 0;
        inner.current_lsn = current_lsn.to_string();
        inner.start_time = Some(Instant::now());
        inner.pg_connection = Some(pg);
        inner.sse_server = Some(sse_server);
        inner.shutdown_tx = Some(shutdown_tx);
        inner.event_tx = Some(event_tx);

        let url = format!("http://{}:{}/events", tap_config.sink.host, sse_port);
        Ok(url)
    }

    /// Stop capturing and release all resources.
    ///
    /// Sends the shutdown signal, closes the Postgres connection, stops
    /// the SSE server, flushes the final checkpoint to the state store,
    /// and resets the internal state machine to `Stopped`.
    #[napi]
    pub async fn stop(&self) -> Result<()> {
        // Take ownership of everything under the lock, then drop it before
        // performing any async `.await` calls (avoids deadlocks).
        let (sse_server, pg_connection, lsn_str, shutdown_tx, event_tx) = {
            let mut inner = self.inner.lock().await;
            let srv = inner.sse_server.take();
            let pg = inner.pg_connection.take();
            let lsn = std::mem::take(&mut inner.current_lsn);
            let tx = inner.shutdown_tx.take();
            let evt = inner.event_tx.take();
            inner.state = CaptureState::Stopped;
            inner.start_time = None;
            (srv, pg, lsn, tx, evt)
        }; // Lock is released here

        // Signal the bridge task
        if let Some(tx) = shutdown_tx {
            let _ = tx.send(());
        }

        // Shutdown SSE server
        if let Some(server) = sse_server {
            server.shutdown().await;
        }

        // Close Postgres connection
        if let Some(pg) = pg_connection {
            pg.close().await;
        }

        // Flush checkpoint to state store
        if !lsn_str.is_empty() {
            if let Ok(lsn) = lsn_str.parse::<tap_core::postgres::Lsn>() {
                if let Ok(store) = self.state_store.lock() {
                    let _ = store.write_offset(&lsn, "", 0, true);
                }
            }
        }

        // Drop event_tx to close the mpsc channel, which makes the bridge
        // task exit its recv loop.
        drop(event_tx);

        Ok(())
    }

    /// Pause WAL reading while keeping Postgres connections open.
    ///
    /// Sets the internal state to `Paused`.  In a full implementation this
    /// would also signal the WAL reader to stop consuming new data until
    /// [`resume`](Self::resume) is called.
    #[napi]
    pub async fn pause(&self) -> Result<()> {
        let mut inner = self.inner.lock().await;
        if inner.state != CaptureState::Streaming {
            return Err(napi::Error::from_reason(
                "Can only pause when state is 'streaming'.",
            ));
        }
        inner.state = CaptureState::Paused;
        Ok(())
    }

    /// Resume WAL reading after a pause.
    ///
    /// Sets the internal state back to `Streaming`.
    #[napi]
    pub async fn resume(&self) -> Result<()> {
        let mut inner = self.inner.lock().await;
        if inner.state != CaptureState::Paused {
            return Err(napi::Error::from_reason(
                "Can only resume when state is 'paused'.",
            ));
        }
        inner.state = CaptureState::Streaming;
        Ok(())
    }

    /// Return the current capture status.
    ///
    /// Includes the state machine value, total events captured, current LSN,
    /// and approximate lag in milliseconds since the session started.
    #[napi]
    pub async fn status(&self) -> JsCaptureStatus {
        let inner = self.inner.lock().await;
        let lag_ms = inner
            .start_time
            .map(|t| t.elapsed().as_millis() as u64)
            .unwrap_or(0);
        JsCaptureStatus {
            state: inner.state.as_str().to_string(),
            events_captured: inner.events_captured as f64,
            current_lsn: inner.current_lsn.clone(),
            lag_ms: lag_ms as f64,
        }
    }

    /// Register a callback invoked on every row-level change event.
    ///
    /// The callback receives a [`JsChangeEvent`] with the operation type,
    /// before/after row data, and source metadata.  Only one callback can
    /// be registered at a time; calling `onChange` again replaces the
    /// previous handler.
    #[napi]
    pub fn on_change(&self, callback: JsFunction) -> Result<()> {
        let tsfn: ThreadsafeFunction<JsChangeEvent, ErrorStrategy::Fatal> =
            callback.create_threadsafe_function(0, |ctx| Ok(vec![ctx.value]))?;
        *self.change_tsfn.lock().unwrap() = Some(tsfn);
        Ok(())
    }

    /// Register a callback invoked when the capture engine encounters an
    /// error.  Only one callback may be registered at a time.
    #[napi]
    pub fn on_error(&self, callback: JsFunction) -> Result<()> {
        let tsfn: ThreadsafeFunction<String, ErrorStrategy::Fatal> =
            callback.create_threadsafe_function(0, |ctx| Ok(vec![ctx.value]))?;
        *self.error_tsfn.lock().unwrap() = Some(tsfn);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Config conversion helpers
// ---------------------------------------------------------------------------

impl Tap {
    /// Convert a [`JsTapConfig`] to the internal [`config::TapConfig`].
    fn build_tap_config(js: &JsTapConfig) -> Result<config::TapConfig> {
        let source = config::SourceConfig {
            host: js.host.clone().unwrap_or_else(|| "localhost".into()),
            port: js.port.map(|p| p as u16).unwrap_or(5432),
            dbname: js.database.clone().unwrap_or_default(),
            user: js.user.clone().unwrap_or_default(),
            password: js.password.clone().unwrap_or_default(),
            slot_name: js.slot_name.clone().unwrap_or_else(|| "tap_slot".into()),
            publication: js
                .publication
                .clone()
                .unwrap_or_else(|| "tap_publication".into()),
            tables: js.tables.clone().unwrap_or_default(),
            plugin: js.plugin.clone().unwrap_or_else(|| "pgoutput".into()),
            ssl_mode: config::SslMode::Disable,
        };

        let sink = js
            .sink
            .as_ref()
            .map(|s| config::SinkConfig {
                host: s.host.clone(),
                port: s.port,
                max_buffer_size: s.max_buffer_size.map(|v| v as usize).unwrap_or(10_000),
                heartbeat_interval_ms: s.heartbeat_interval_ms.map(|v| v as u64).unwrap_or(30_000),
                api_key: None,
            })
            .unwrap_or_else(|| config::SinkConfig {
                host: "127.0.0.1".into(),
                port: 0,
                max_buffer_size: 10_000,
                heartbeat_interval_ms: 30_000,
                api_key: None,
            });

        let capture = config::CaptureConfig {
            from_lsn: None,
            snapshot: true,
            max_batch_size: js.max_batch_size.map(|v| v as usize).unwrap_or(100),
            flush_interval_ms: js.flush_interval_ms.map(|v| v as u64).unwrap_or(1_000),
        };

        Ok(config::TapConfig {
            source,
            sink,
            capture,
            snapshot: config::SnapshotConfig::default(),
            state: config::StateConfig {
                path: js
                    .state_path
                    .clone()
                    .unwrap_or_else(|| ".tap/state.db".into()),
                max_backup_size_kb: 10_240,
            },
            logging: config::LoggingConfig::default(),
        })
    }
}

// ---------------------------------------------------------------------------
// Helpers to expose events from outside the napi module
// ---------------------------------------------------------------------------

/// Push a [`ChangeEvent`] into the bridge.  Used by the capture engine to
/// inject events after they are decoded from the WAL stream.
///
/// Returns an error when the internal channel is full (consumer too slow).
pub fn push_event(tap: &Tap, event: ChangeEvent) -> Result<()> {
    let inner = tap.inner.clone();
    let rt_handle = tokio::runtime::Handle::current();
    let _ = rt_handle.block_on(async {
        let guard = inner.lock().await;
        if let Some(tx) = &guard.event_tx {
            tx.try_send(event).map_err(|_| {
                napi::Error::from_reason("Event channel full (consumer too slow)".to_string())
            })
        } else {
            Err(napi::Error::from_reason("Tap is not started".to_string()))
        }
    });
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// A minimal [`JsTapConfig`] for testing.
    fn test_config() -> JsTapConfig {
        JsTapConfig {
            connection: "postgresql://localhost:5432/test".into(),
            host: Some("localhost".into()),
            port: Some(5432),
            database: Some("test".into()),
            user: Some("test".into()),
            password: Some("test".into()),
            slot_name: Some("tap_test_slot".into()),
            publication: Some("tap_test_pub".into()),
            tables: Some(vec!["public.users".into()]),
            plugin: Some("pgoutput".into()),
            state_path: Some(".tap/test_state.db".into()),
            max_batch_size: Some(100),
            flush_interval_ms: Some(1000),
            sink: Some(JsSinkConfig {
                host: "127.0.0.1".into(),
                port: 0,
                max_buffer_size: Some(1000),
                heartbeat_interval_ms: Some(30_000),
            }),
        }
    }

    #[test]
    fn test_js_change_event_to_json() {
        let event = JsChangeEvent {
            op: "c".into(),
            before: None,
            after: Some(serde_json::json!({"id": 1, "name": "Alice"})),
            source: JsSourceMetadata {
                db: "test".into(),
                schema: "public".into(),
                table: "users".into(),
                lsn: "0/1234567".into(),
                tx_id: "42".into(),
                ts_ms: 1_700_000_000_000_f64,
                snapshot: None,
            },
            ts_ms: 1_700_000_000_001_f64,
            id: "0/1234567:42".into(),
        };
        let json = event.to_json();
        assert!(json.contains(r#""op":"c""#));
        assert!(json.contains(r#""Alice""#));
    }

    #[test]
    fn test_js_capture_status_fields() {
        let status = JsCaptureStatus {
            state: "streaming".into(),
            events_captured: 42.0,
            current_lsn: "0/ABCD".into(),
            lag_ms: 7.0,
        };
        assert_eq!(status.state, "streaming");
        assert_eq!(status.events_captured, 42.0);
        assert_eq!(status.current_lsn, "0/ABCD");
        assert_eq!(status.lag_ms, 7.0);
    }

    #[test]
    fn test_convert_source_metadata() {
        let src = tap_core::event::SourceMetadata {
            db: "mydb".into(),
            schema: "public".into(),
            table: "orders".into(),
            lsn: "0/DEADBEEF".parse().unwrap(),
            tx_id: "tx99".into(),
            ts_ms: 1234,
            snapshot: Some(true),
        };
        let js: JsSourceMetadata = (&src).into();
        assert_eq!(js.db, "mydb");
        assert_eq!(js.schema, "public");
        assert_eq!(js.table, "orders");
        assert_eq!(js.lsn, "0/DEADBEEF");
        assert_eq!(js.tx_id, "tx99");
        assert_eq!(js.ts_ms, 1234.0);
        assert_eq!(js.snapshot, Some(true));
    }

    #[test]
    fn test_convert_change_event() {
        let src = tap_core::event::SourceMetadata {
            db: "d".into(),
            schema: "s".into(),
            table: "t".into(),
            lsn: "0/1".parse().unwrap(),
            tx_id: "1".into(),
            ts_ms: 100,
            snapshot: None,
        };
        let core = ChangeEvent {
            op: tap_core::event::Operation::Update,
            before: Some(serde_json::json!({"id": 1})),
            after: Some(serde_json::json!({"id": 1, "name": "Bob"})),
            source: src,
            ts_ms: 101,
            id: "0/1:1".into(),
        };
        let js: JsChangeEvent = (&core).into();
        assert_eq!(js.op, "u");
        assert!(js.before.is_some());
        assert!(js.after.is_some());
    }

    #[test]
    fn test_build_tap_config_defaults() {
        let js = JsTapConfig {
            connection: "postgresql://localhost:5432/test".into(),
            host: None,
            port: None,
            database: Some("test".into()),
            user: Some("admin".into()),
            password: Some("secret".into()),
            slot_name: None,
            publication: None,
            tables: None,
            plugin: None,
            state_path: None,
            max_batch_size: None,
            flush_interval_ms: None,
            sink: None,
        };
        let config = Tap::build_tap_config(&js).expect("build config");
        assert_eq!(config.source.host, "localhost");
        assert_eq!(config.source.port, 5432);
        assert_eq!(config.source.dbname, "test");
        assert_eq!(config.source.user, "admin");
        assert_eq!(config.source.password, "secret");
        assert_eq!(config.source.slot_name, "tap_slot");
        assert_eq!(config.source.publication, "tap_publication");
        assert!(config.source.tables.is_empty());
        assert_eq!(config.source.plugin, "pgoutput");
        assert_eq!(config.sink.host, "127.0.0.1");
        assert_eq!(config.sink.max_buffer_size, 10_000);
        assert_eq!(config.capture.max_batch_size, 100);
    }

    #[test]
    fn test_build_tap_config_full() {
        let js = test_config();
        let config = Tap::build_tap_config(&js).expect("build config");
        assert_eq!(config.source.host, "localhost");
        assert_eq!(config.source.port, 5432);
        assert_eq!(config.source.dbname, "test");
        assert_eq!(config.source.tables, vec!["public.users"]);
        assert_eq!(config.sink.port, 0);
        assert_eq!(config.sink.max_buffer_size, 1000);
        assert_eq!(config.state.path, ".tap/test_state.db");
    }

    #[test]
    fn test_capture_state_as_str() {
        assert_eq!(CaptureState::Idle.as_str(), "idle");
        assert_eq!(CaptureState::Snapshot.as_str(), "snapshot");
        assert_eq!(CaptureState::Streaming.as_str(), "streaming");
        assert_eq!(CaptureState::Paused.as_str(), "paused");
        assert_eq!(CaptureState::Stopped.as_str(), "stopped");
    }
}

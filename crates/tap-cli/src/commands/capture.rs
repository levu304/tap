//! `tap capture` — main capture session orchestrator.
//!
//! Loads config, connects to Postgres, opens the state store, starts
//! the SSE event server, runs an initial snapshot if requested (or
//! resumes from a checkpoint), streams WAL changes, and handles
//! graceful shutdown on SIGINT/SIGTERM.

use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Args;
use futures::StreamExt;
use tap_core::error::TapError;
use tap_core::event::ChangeEvent;
use tap_core::postgres::{Lsn, PgConnection, connect_plain};
use tap_core::postgres::create_decoder;
use tap_core::snapshot::SnapshotRunner;
use tap_core::sse::{SseEvent, SseEventType, SseServer};
use tap_core::state::StateStore;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::Mutex;
use tracing::{info, warn};

/// Arguments for the `tap capture` command.
#[derive(Args, Debug, Clone)]
pub struct CaptureArgs {
    /// Path to the TOML configuration file.
    #[arg(
        short = 'c',
        long = "config",
        default_value_t = String::from(crate::config::DEFAULT_CONFIG_PATH)
    )]
    pub config: String,

    /// Start replication from a specific LSN (overrides saved checkpoint).
    #[arg(long = "from-lsn", value_parser = validate_lsn_string)]
    pub from_lsn: Option<String>,

    /// Force a full snapshot before starting streaming.
    #[arg(short = 's', long = "snapshot")]
    pub snapshot: bool,

    /// Tables to capture (overrides config file tables for this session).
    #[arg(short = 't', long = "table")]
    pub tables: Vec<String>,
}

/// Status line update interval.
const STATUS_INTERVAL: Duration = Duration::from_secs(1);

/// Validate an LSN string at the CLI argument parsing boundary.
fn validate_lsn_string(s: &str) -> Result<String, String> {
    s.parse::<Lsn>()
        .map(|_| s.to_string())
        .map_err(|e| format!("invalid --from-lsn '{s}': {e}"))
}

/// Run `tap capture`.
pub async fn run(args: CaptureArgs) -> Result<(), TapError> {
    // ── 1. Load config ───────────────────────────────────────────────
    let mut config = crate::config::load_config(&args.config)?;

    // Apply CLI overrides
    if args.snapshot {
        config.capture.snapshot = true;
    }
    if !args.tables.is_empty() {
        config.source.tables.clone_from(&args.tables);
    }
    if let Some(ref lsn) = args.from_lsn {
        config.capture.from_lsn = Some(lsn.clone());
    }

    info!(
        "Starting capture session (db={}, tables={:?})",
        config.source.dbname, config.source.tables,
    );

    // ── 2. Connect to Postgres (replication) ─────────────────────────
    let pg = PgConnection::connect(&config.source).await?;
    info!("Connected to Postgres (replication mode)");

    // Validate tables exist
    if !config.source.tables.is_empty() {
        pg.validate_tables().await?;
    }

    // Ensure replication slot and publication exist (idempotent)
    pg.ensure_replication_slot().await?;
    pg.ensure_publication().await?;

    // ── 3. Open state store ──────────────────────────────────────────
    let state = Arc::new(Mutex::new(StateStore::open(&config.state)?));

    // ── 4. Determine start LSN ───────────────────────────────────────
    let mut start_lsn: Option<Lsn> = if let Some(ref lsn_str) = config.capture.from_lsn {
        info!("Using --from-lsn: {lsn_str}");
        Some(lsn_str.parse::<Lsn>()?)
    } else {
        let saved = {
            let store = state.lock().await;
            store.read_last_offset()?
        };
        if let Some(offset) = saved {
            info!(
                "Resuming from saved checkpoint LSN: {}",
                offset.committed_lsn
            );
            Some(offset.committed_lsn.parse::<Lsn>()?)
        } else {
            info!("No saved checkpoint found — will start from current position");
            None
        }
    };

    // ── 5. Create SSE server ─────────────────────────────────────────
    let sse = SseServer::new(config.sink.clone());
    let port = sse.start().await?;
    info!("SSE server listening on port {port}");

    // ── 6. Channel: bridge ChangeEvent → SseEvent ────────────────────
    let (change_tx, mut change_rx) = tokio::sync::mpsc::unbounded_channel::<ChangeEvent>();

    // Spawn bridge task: forward ChangeEvents to SSE broadcast
    let sse_broadcast = sse.broadcast().clone();
    tokio::spawn(async move {
        while let Some(ce) = change_rx.recv().await {
            let event_data = match serde_json::to_value(&ce) {
                Ok(v) => v,
                Err(e) => {
                    warn!("Failed to serialize ChangeEvent: {e}");
                    continue;
                }
            };
            let sse_event = SseEvent::new(SseEventType::Change, event_data).with_id(&ce.id);
            if sse_broadcast.send(sse_event).is_err() {
                // No active SSE listeners — this is fine
            }
        }
    });

    // ── 7. Update health state ───────────────────────────────────────
    {
        let mut health = sse.health_state().write().await;
        health.state = tap_core::sse::CaptureState::Idle;
        health.current_lsn = start_lsn.map(|l| l.to_string()).unwrap_or_default();
    }

    // ── 8. Snapshot or stream ────────────────────────────────────────
    if args.snapshot || (config.capture.snapshot && start_lsn.is_none()) {
        if args.snapshot && start_lsn.is_some() {
            warn!("--snapshot overrides existing checkpoint — re-snapshotting from scratch");
        }
        // Run snapshot
        info!("Starting initial snapshot...");
        {
            let mut health = sse.health_state().write().await;
            health.state = tap_core::sse::CaptureState::Snapshot;
        }

        // Send SnapshotStart SSE event
        sse.broadcast()
            .send(SseEvent::new(
                SseEventType::SnapshotStart,
                serde_json::json!({
                    "tables": config.source.tables,
                    "batch_size": config.snapshot.batch_size,
                }),
            ))
            .ok();

        // Create plain connections for snapshot
        let (keeper, keeper_handle) = connect_plain(&config.source).await?;
        let (worker, worker_handle) = connect_plain(&config.source).await?;

        let mut snapshot_runner = SnapshotRunner::new(
            keeper,
            worker,
            state.clone(),
            config.snapshot.clone(),
            config.source.dbname.clone(),
            change_tx.clone(),
        );

        let snapshot_result = snapshot_runner.run().await;
        // Drop runner to release Clients before awaiting handles, preventing
        // a tokio self-deadlock (handles resolve only after Client is dropped).
        drop(snapshot_runner);
        let _ = keeper_handle.await;
        let _ = worker_handle.await;
        let snapshot_result = snapshot_result?;

        // Update start_lsn so replication resumes from snapshot position
        start_lsn = Some(snapshot_result.lsn);

        info!(
            "Snapshot complete: {} rows in {} tables, LSN={}",
            snapshot_result.total_rows,
            snapshot_result.tables_snapshotted.len(),
            snapshot_result.lsn,
        );

        // Send SnapshotComplete SSE event
        sse.broadcast()
            .send(SseEvent::new(
                SseEventType::SnapshotComplete,
                serde_json::json!({
                    "tables": snapshot_result.tables_snapshotted,
                    "rows": snapshot_result.total_rows,
                    "lsn": snapshot_result.lsn.to_string(),
                }),
            ))
            .ok();

        // Update health
        {
            let mut health = sse.health_state().write().await;
            health.events_captured = snapshot_result.total_rows;
            health.current_lsn = snapshot_result.lsn.to_string();
            health.state = tap_core::sse::CaptureState::Streaming;
        }
    } else {
        // No snapshot — send streaming start
        sse.broadcast()
            .send(SseEvent::new(
                SseEventType::StreamingStart,
                serde_json::json!({
                    "lsn": start_lsn.map(|l| l.to_string()).unwrap_or_default(),
                }),
            ))
            .ok();

        {
            let mut health = sse.health_state().write().await;
            health.state = tap_core::sse::CaptureState::Streaming;
        }
    }

    // ── 9. Start replication stream ──────────────────────────────────
    debug_assert!(
        start_lsn.is_some() || !config.capture.snapshot,
        "snapshot ran but start_lsn is still None"
    );
    let replication_start = start_lsn.unwrap_or_else(|| {
        warn!("No start LSN available — starting replication from ZERO (will replay all WAL)");
        Lsn::ZERO
    });
    info!("Starting replication from LSN {replication_start}");
    let mut replication_stream = pg
        .start_replication(
            &config.source.slot_name,
            &config.source.publication,
            replication_start,
            &config.source.plugin,
        )
        .await?;

    info!(
        "Replication stream active (slot={})",
        config.source.slot_name
    );

    // Create WAL decoder based on configured plugin
    let mut decoder = create_decoder(&config.source.plugin, &config.source.dbname)
        .map_err(|e| TapError::Decode(format!("Failed to create decoder: {e}")))?;

    // ── 10. Main event loop ──────────────────────────────────────────
    info!("Capture running — waiting for events (SSE on port {port})");

    // Status line updater
    let start_time = Instant::now();
    let mut last_event_count: u64 = 0;
    let mut status_interval = tokio::time::interval(STATUS_INTERVAL);
    status_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // Shutdown signal via watch channel (SIGINT + SIGTERM)
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        let mut sigterm =
            signal(SignalKind::terminate()).expect("Failed to register SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("SIGINT received — initiating graceful shutdown...");
            }
            _ = sigterm.recv() => {
                info!("SIGTERM received — initiating graceful shutdown...");
            }
        }
        let _ = shutdown_tx.send(true);
    });

    loop {
        tokio::select! {
            // Shutdown signal
            _ = shutdown_rx.changed() => {
                info!("Shutdown signal received");
                break;
            }

            // Periodic status update
            _ = status_interval.tick() => {
                let elapsed = start_time.elapsed();
                let health = sse.health_state().read().await;
                let event_delta = health.events_captured - last_event_count;
                last_event_count = health.events_captured;

                print!(
                    "\r\x1b[K[{:?}] state={:?} events={} ({}/s) lsn={}",
                    elapsed.as_secs(),
                    health.state,
                    health.events_captured,
                    event_delta,
                    health.current_lsn,
                );
                // Flush stdout manually (no newline)
                let _ = std::io::stdout().flush();
            }

            // Replication stream: consume WAL, decode, forward, checkpoint
            result = replication_stream.next() => {
                let wal_bytes = match result {
                    Some(Ok(b)) => b,
                    Some(Err(e)) => {
                        warn!("Replication stream error: {e}");
                        break;
                    }
                    None => {
                        warn!("Replication stream ended unexpectedly");
                        break;
                    }
                };

                match decoder.decode(&wal_bytes) {
                    Ok(events) => {
                        if events.is_empty() {
                            // Decoder accumulates data across calls;
                            // only emits events on transaction commit
                            continue;
                        }

                        let count = events.len() as u64;

                        // Update health state
                        {
                            let mut health = sse.health_state().write().await;
                            health.events_captured += count;
                            if let Some(first) = events.first() {
                                health.current_lsn = first.source.lsn.to_string();
                            }
                        }

                        // Extract checkpoint metadata from the first event
                        let checkpoint_lsn: Option<Lsn> = events.first()
                            .and_then(|e| e.source.lsn.0.parse::<Lsn>().ok());
                        let checkpoint_tx = events.first()
                            .map(|e| e.source.tx_id.clone());
                        let checkpoint_ts = events.first()
                            .map(|e| e.source.ts_ms)
                            .unwrap_or(0);

                        // Forward decoded events to the SSE bridge task
                        for event in events {
                            let _ = change_tx.send(event);
                        }

                        // Persist offset checkpoint
                        if let (Some(lsn), Some(tx_id)) = (checkpoint_lsn, checkpoint_tx) {
                            let store = state.lock().await;
                            if let Err(e) = store.write_offset(&lsn, &tx_id, checkpoint_ts, false) {
                                warn!("Failed to persist offset checkpoint: {e}");
                            }
                        }
                    }
                    Err(e) => {
                        warn!("WAL decode error: {e}");
                    }
                }
            }
        }
    }

    println!(); // newline after status line

    // ── 11. Graceful shutdown ────────────────────────────────────────
    info!("Shutting down...");

    // Stop SSE server (sends Shutdown event)
    sse.shutdown().await;

    // Close Postgres connection
    pg.close().await;

    info!("Capture session ended");
    Ok(())
}

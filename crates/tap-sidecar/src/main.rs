use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{any, get, post},
};
use clap::Parser;
use serde::Serialize;
use tokio::sync::Mutex;
use tracing::info;

mod sse;
mod ws;

/// Tap CDC sidecar — HTTP/WebSocket/SSE event stream server.
#[derive(Parser, Debug)]
#[command(
    name = "tap-sidecar",
    version = "0.2.0",
    about = "Tap CDC sidecar server"
)]
struct Args {
    /// Listen address
    #[arg(long, default_value = "127.0.0.1:9911")]
    bind: String,
}

#[derive(Clone, Serialize)]
struct HealthResponse {
    status: String,
    version: String,
    uptime_seconds: u64,
}

struct AppState {
    started_at: tokio::time::Instant,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    info!(
        "tap-sidecar v{} starting on {}",
        env!("CARGO_PKG_VERSION"),
        args.bind
    );

    let state = Arc::new(Mutex::new(AppState {
        started_at: tokio::time::Instant::now(),
    }));

    let app = Router::new()
        // Observability
        .route("/health", get(health_handler))
        .route("/metrics", get(metrics_handler))
        // SSE event stream
        .route("/events", get(sse::events_handler))
        // WebSocket event stream
        .route("/ws/events", any(ws::ws_handler))
        // Capture control (stubs for now)
        .route("/capture/start", post(capture_start_handler))
        .route("/capture/stop", post(capture_stop_handler))
        .route("/capture/pause", post(capture_pause_handler))
        .route("/capture/resume", post(capture_resume_handler))
        .with_state(state);

    let addr: SocketAddr = args.bind.parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn health_handler(State(state): State<Arc<Mutex<AppState>>>) -> impl IntoResponse {
    let state = state.lock().await;
    Json(HealthResponse {
        status: "ok".into(),
        version: env!("CARGO_PKG_VERSION").into(),
        uptime_seconds: state.started_at.elapsed().as_secs(),
    })
}

async fn metrics_handler() -> impl IntoResponse {
    // Stub: return minimal Prometheus-format metrics
    (StatusCode::OK, "# tap-sidecar metrics stub\n").into_response()
}

async fn capture_start_handler() -> impl IntoResponse {
    StatusCode::ACCEPTED
}

async fn capture_stop_handler() -> impl IntoResponse {
    StatusCode::ACCEPTED
}

async fn capture_pause_handler() -> impl IntoResponse {
    StatusCode::OK
}

async fn capture_resume_handler() -> impl IntoResponse {
    StatusCode::OK
}

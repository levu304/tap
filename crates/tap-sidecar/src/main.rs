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
use tower_http::validate_request::ValidateRequestHeaderLayer;
use tracing::info;

mod sse;
mod ws;

/// Tap CDC sidecar — HTTP/WebSocket/SSE event stream server.
#[derive(Parser, Debug)]
#[command(
    name = "tap-sidecar",
    version = env!("CARGO_PKG_VERSION"),
    about = "Tap CDC sidecar server"
)]
struct Args {
    /// Listen address
    #[arg(long, default_value = "127.0.0.1:9911")]
    bind: String,

    /// API key for capture control endpoints (default: TAP_SIDECAR_API_KEY env or "dev-key")
    #[arg(long, env = "TAP_SIDECAR_API_KEY", default_value = "dev-key")]
    api_key: String,
}

#[derive(Clone, Serialize)]
struct HealthResponse {
    status: String,
    uptime_seconds: u64,
}

struct AppState {
    started_at: tokio::time::Instant,
}

#[allow(clippy::result_large_err)]
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

    // Capture control endpoints — protected by bearer auth
    let capture_routes = Router::new()
        .route("/capture/start", post(capture_start_handler))
        .route("/capture/stop", post(capture_stop_handler))
        .route("/capture/pause", post(capture_pause_handler))
        .route("/capture/resume", post(capture_resume_handler))
        .layer(ValidateRequestHeaderLayer::custom({
            let expected = format!("Bearer {}", args.api_key);
            move |req: &mut http::Request<axum::body::Body>| {
                if req
                    .headers()
                    .get("Authorization")
                    .and_then(|v| v.to_str().ok())
                    .is_some_and(|v| v == expected)
                {
                    Ok(())
                } else {
                    Err(http::Response::builder()
                        .status(StatusCode::UNAUTHORIZED)
                        .body(axum::body::Body::empty())
                        .unwrap())
                }
            }
        }));

    let app = Router::new()
        // Observability
        .route("/health", get(health_handler))
        .route("/metrics", get(metrics_handler))
        // SSE event stream
        .route("/events", get(sse::events_handler))
        // WebSocket event stream
        .route("/ws/events", any(ws::ws_handler))
        // Capture control (protected by bearer auth)
        .merge(capture_routes)
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
    StatusCode::ACCEPTED
}

async fn capture_resume_handler() -> impl IntoResponse {
    StatusCode::ACCEPTED
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::util::ServiceExt;

    #[tokio::test]
    async fn test_health_returns_ok() {
        let state = Arc::new(Mutex::new(AppState {
            started_at: tokio::time::Instant::now(),
        }));

        let app = Router::new()
            .route("/health", get(health_handler))
            .with_state(state);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_health_body_fields() {
        let state = Arc::new(Mutex::new(AppState {
            started_at: tokio::time::Instant::now(),
        }));

        let app = Router::new()
            .route("/health", get(health_handler))
            .with_state(state);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let body: serde_json::Value = serde_json::from_slice(
            &axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();

        assert_eq!(body["status"], "ok");
        assert!(
            body.get("version").is_none(),
            "version must not leak on unauthenticated endpoint"
        );
        assert!(body["uptime_seconds"].as_u64().is_some());
    }
}

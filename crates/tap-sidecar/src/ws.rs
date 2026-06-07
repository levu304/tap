use axum::extract::ws::{WebSocket, WebSocketUpgrade};
use axum::response::IntoResponse;
use tracing::info;

pub async fn ws_handler(ws: WebSocketUpgrade) -> impl IntoResponse {
    ws.on_upgrade(handle_socket)
}

async fn handle_socket(mut socket: WebSocket) {
    info!("WebSocket client connected");
    // Stub: keep connection alive, send ping frames
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
        if socket
            .send(axum::extract::ws::Message::Ping(vec![].into()))
            .await
            .is_err()
        {
            break;
        }
    }
}

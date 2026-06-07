use axum::response::sse::{Event, Sse};
use futures::stream;
use std::convert::Infallible;

pub async fn events_handler() -> Sse<impl stream::Stream<Item = Result<Event, Infallible>>> {
    // Stub: returns a "connected" event then keeps connection open
    Sse::new(stream::once(async {
        Ok(Event::default()
            .event("connected")
            .data("{\"status\":\"ok\"}"))
    }))
    .keep_alive(axum::response::sse::KeepAlive::new().interval(std::time::Duration::from_secs(30)))
}

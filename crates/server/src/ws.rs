//! WebSocket upgrade handler placeholder. Task 9 fills in the real protocol
//! (server_info push on connect, command/event framing over the socket).

use axum::extract::{State, WebSocketUpgrade};
use axum::response::Response;

use crate::http::AppState;

pub async fn ws_upgrade(ws: WebSocketUpgrade, State(_state): State<AppState>) -> Response {
    ws.on_upgrade(|_socket| async {})
}

//! WebSocket connection actor.
//!
//! Protocol (matches python-matter-server / node-server clients):
//! 1. On connect, push the bare `ServerInfoMessage` JSON (no envelope).
//! 2. For each inbound text frame, parse a `CommandMessage` and dispatch it
//!    to the `Controller`, replying with a `SuccessResult` or `ErrorResult`.
//! 3. Malformed JSON (or a message missing required fields) yields an
//!    `ErrorResult` with `message_id: ""` and `error_code: 8`
//!    (`ServerErrorCode::InvalidArguments`) — provisional; plan 3 verifies
//!    this against Node fixtures.
//! 4. `start_listening` additionally flips the connection into listening
//!    mode; Task 10 uses that flag to start forwarding events.

use axum::extract::ws::{Message, WebSocket};
use axum::extract::{State, WebSocketUpgrade};
use axum::response::Response;

use matter_rs_wire::envelope::{CommandMessage, ErrorResult, SuccessResult};
use matter_rs_wire::error::ServerErrorCode;

use crate::http::AppState;

pub async fn ws_upgrade(ws: WebSocketUpgrade, State(state): State<AppState>) -> Response {
    ws.on_upgrade(move |socket| handle_connection(socket, state))
}

async fn handle_connection(mut socket: WebSocket, state: AppState) {
    // 1. Unsolicited, bare server_info (matterjs-server behavior).
    let info = state.controller.server_info();
    if socket
        .send(Message::Text(serde_json::to_string(&info).unwrap().into()))
        .await
        .is_err()
    {
        return;
    }

    let mut listening = false;

    while let Some(Ok(msg)) = socket.recv().await {
        let Message::Text(text) = msg else { continue };

        let cmd: CommandMessage = match serde_json::from_str(&text) {
            Ok(c) => c,
            Err(e) => {
                let err = ErrorResult::new(String::new(), ServerErrorCode::InvalidArguments, e.to_string());
                if socket.send(Message::Text(serde_json::to_string(&err).unwrap().into())).await.is_err() {
                    return;
                }
                continue;
            }
        };

        let is_start_listening = cmd.command == "start_listening";
        let frame = match state.controller.handle_command(&cmd).await {
            Ok(result) => {
                if is_start_listening {
                    listening = true; // Task 10 forwards events based on this
                }
                serde_json::to_string(&SuccessResult { message_id: cmd.message_id, result }).unwrap()
            }
            Err(e) => serde_json::to_string(&ErrorResult::new(cmd.message_id, e.code, e.details)).unwrap(),
        };
        if socket.send(Message::Text(frame.into())).await.is_err() {
            return;
        }
    }
    let _ = listening; // consumed for real in Task 10
}

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
//!    mode; controller events are forwarded as `{"event","data"}` frames
//!    only while listening, dropped otherwise. Because the events receiver
//!    is subscribed at connect time, any events published before
//!    `start_listening` succeeds are drained (discarded) at that point, so
//!    the subscription effectively begins at `start_listening`.
//! 5. A `true` on the shared shutdown watch (or the sender being dropped)
//!    sends a `{"event":"server_shutdown","data":null}` frame and closes
//!    the connection, regardless of listening state.

use std::sync::atomic::{AtomicU64, Ordering};

use axum::extract::ws::{Message, WebSocket};
use axum::extract::{State, WebSocketUpgrade};
use axum::response::Response;
use futures_util::SinkExt;

use matter_rs_wire::envelope::{CommandMessage, ErrorResult, SuccessResult};
use matter_rs_wire::error::ServerErrorCode;

use crate::http::AppState;

static NEXT_CONN_ID: AtomicU64 = AtomicU64::new(1);

pub async fn ws_upgrade(ws: WebSocketUpgrade, State(state): State<AppState>) -> Response {
    ws.on_upgrade(move |socket| handle_connection(socket, state))
}

async fn handle_connection(mut socket: WebSocket, state: AppState) {
    let conn = matter_rs_controller::api::ConnId(NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed));

    // Guard: connection_closed fires on every exit path, including panics.
    struct CloseGuard {
        controller: std::sync::Arc<dyn matter_rs_controller::api::Controller>,
        conn: matter_rs_controller::api::ConnId,
    }
    impl Drop for CloseGuard {
        fn drop(&mut self) {
            self.controller.connection_closed(self.conn);
        }
    }
    let _close_guard = CloseGuard { controller: state.controller.clone(), conn };

    // 1. Unsolicited, bare server_info (matterjs-server behavior).
    let info = state.controller.server_info();
    if socket
        .send(Message::Text(serde_json::to_string(&info).unwrap().into()))
        .await
        .is_err()
    {
        return;
    }

    let mut events = state.controller.subscribe_events();
    let mut shutdown = state.shutdown.clone();
    let mut listening = false;

    loop {
        tokio::select! {
            msg = socket.recv() => {
                let Some(Ok(msg)) = msg else { return };
                let Message::Text(text) = msg else { continue };
                let (frame, started_listening) = handle_text_frame(&state, conn, &text, &mut listening).await;
                if started_listening {
                    // The events receiver subscribed at connect time; drain
                    // anything queued before this start_listening so events
                    // only flow from this point forward.
                    while events.try_recv().is_ok() {}
                }
                if socket.send(Message::Text(frame.into())).await.is_err() { return; }
            }
            ev = events.recv() => {
                match ev {
                    Ok(ev) if listening => {
                        let frame = serde_json::to_string(&ev).unwrap();
                        if socket.send(Message::Text(frame.into())).await.is_err() { return; }
                    }
                    Ok(_) => {}                       // not listening: drop
                    Err(_) => {}                      // lagged/closed: keep serving commands
                }
            }
            res = shutdown.changed() => {
                // A dropped sender (Err) is treated as shutdown too — otherwise
                // changed() returns Err instantly forever and this arm busy-loops.
                if res.is_err() || *shutdown.borrow() {
                    let bye = serde_json::json!({"event": "server_shutdown", "data": null});
                    let _ = socket.send(Message::Text(bye.to_string().into())).await;
                    let _ = socket.close().await;
                    return;
                }
            }
        }
    }
}

/// Handles one inbound text frame, returning the reply frame and whether
/// this call just flipped `listening` from false to true (i.e. a
/// successful `start_listening`), so the caller can drain any events
/// queued on the broadcast receiver before this point.
async fn handle_text_frame(
    state: &AppState,
    conn: matter_rs_controller::api::ConnId,
    text: &str,
    listening: &mut bool,
) -> (String, bool) {
    let cmd: CommandMessage = match serde_json::from_str(text) {
        Ok(c) => c,
        Err(e) => {
            return (serde_json::to_string(&ErrorResult::new(
                String::new(), ServerErrorCode::InvalidArguments, e.to_string(),
            )).unwrap(), false);
        }
    };
    let is_start_listening = cmd.command == "start_listening";
    match state.controller.handle_command(conn, &cmd).await {
        Ok(result) => {
            let started_listening = is_start_listening && !*listening;
            if is_start_listening { *listening = true; }
            (serde_json::to_string(&SuccessResult { message_id: cmd.message_id, result }).unwrap(), started_listening)
        }
        Err(e) => (serde_json::to_string(&ErrorResult::new(cmd.message_id, e.code, e.details)).unwrap(), false),
    }
}

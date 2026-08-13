use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use matter_rs_controller::stub::StubController;
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::Message;

mod common;
use common::{spawn_server, test_server_info};

async fn connect(addr: std::net::SocketAddr)
-> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    let (ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws")).await.unwrap();
    ws
}

async fn next_json(ws: &mut (impl StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin)) -> Value {
    loop {
        match ws.next().await.unwrap().unwrap() {
            Message::Text(t) => return serde_json::from_str(&t).unwrap(),
            _ => continue,
        }
    }
}

#[tokio::test]
async fn pushes_bare_server_info_on_connect() {
    let (addr, _s) = spawn_server(Arc::new(StubController::new(test_server_info()))).await;
    let mut ws = connect(addr).await;
    let hello = next_json(&mut ws).await;
    // Bare object: schema fields at top level, NO message_id envelope.
    assert_eq!(hello["schema_version"], 13);
    assert!(hello.get("message_id").is_none());
}

#[tokio::test]
async fn dispatches_commands_and_echoes_message_id() {
    let (addr, _s) = spawn_server(Arc::new(StubController::new(test_server_info()))).await;
    let mut ws = connect(addr).await;
    let _hello = next_json(&mut ws).await;

    ws.send(Message::text(r#"{"message_id":"abc","command":"get_nodes"}"#)).await.unwrap();
    let resp = next_json(&mut ws).await;
    assert_eq!(resp, json!({"message_id": "abc", "result": []}));

    ws.send(Message::text(r#"{"message_id":"x","command":"frobnicate"}"#)).await.unwrap();
    let resp = next_json(&mut ws).await;
    assert_eq!(resp["message_id"], "x");
    assert_eq!(resp["error_code"], 9);
}

#[tokio::test]
async fn malformed_json_gets_invalid_arguments_error() {
    let (addr, _s) = spawn_server(Arc::new(StubController::new(test_server_info()))).await;
    let mut ws = connect(addr).await;
    let _hello = next_json(&mut ws).await;

    ws.send(Message::text("{not json")).await.unwrap();
    let resp = next_json(&mut ws).await;
    assert_eq!(resp["error_code"], 8);
    assert_eq!(resp["message_id"], "");
}

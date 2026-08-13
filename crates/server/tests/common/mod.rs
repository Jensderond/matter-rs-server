use std::net::SocketAddr;
use std::sync::Arc;

use matter_rs_controller::api::Controller;
use matter_rs_wire::server_info::{ServerInfoMessage, MIN_SUPPORTED_SCHEMA_VERSION, SCHEMA_VERSION};

pub fn test_server_info() -> ServerInfoMessage {
    ServerInfoMessage {
        fabric_id: 1,
        compressed_fabric_id: 0,
        fabric_index: None,
        schema_version: SCHEMA_VERSION,
        min_supported_schema_version: MIN_SUPPORTED_SCHEMA_VERSION,
        sdk_version: "matter-rs-server/test".into(),
        wifi_credentials_set: false,
        wifi_ssid: None,
        thread_credentials_set: false,
        bluetooth_enabled: false,
        ble_proxy_enabled: None,
        controller_node_id: None,
    }
}

/// Serve on an ephemeral port; returns (addr, shutdown_sender-keepalive).
pub async fn spawn_server(
    controller: Arc<dyn Controller>,
) -> (SocketAddr, tokio::sync::watch::Sender<bool>) {
    let (tx, rx) = tokio::sync::watch::channel(false);
    let state = matter_rs_server::http::AppState { controller, shutdown: rx };
    let router = matter_rs_server::http::build_router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (addr, tx)
}

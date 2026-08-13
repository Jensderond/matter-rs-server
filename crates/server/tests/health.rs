use std::sync::Arc;

use matter_rs_controller::stub::StubController;

mod common;
use common::{spawn_server, test_server_info};

#[tokio::test]
async fn health_reports_version_and_node_count() {
    let (addr, _shutdown) = spawn_server(Arc::new(StubController::new(test_server_info()))).await;
    let body: serde_json::Value = reqwest::get(format!("http://{addr}/health"))
        .await.unwrap().json().await.unwrap();
    assert_eq!(body["version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(body["node_count"], 0);
}

#[tokio::test]
async fn health_post_is_405_and_unknown_path_404() {
    let (addr, _shutdown) = spawn_server(Arc::new(StubController::new(test_server_info()))).await;
    let client = reqwest::Client::new();
    let resp = client.post(format!("http://{addr}/health")).send().await.unwrap();
    assert_eq!(resp.status(), 405);
    let resp = client.get(format!("http://{addr}/nope")).send().await.unwrap();
    assert_eq!(resp.status(), 404);
}

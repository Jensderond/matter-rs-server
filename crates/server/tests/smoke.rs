use std::process::Stdio;
use std::time::Duration;

#[tokio::test]
async fn binary_serves_health_and_ws_and_stops_on_sigterm() {
    let dir = std::env::temp_dir().join(format!("mrs-smoke-{}", std::process::id()));
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_matter-rs-server"))
        .args(["--port", "0"]) // 0 = kernel-picked; printed on stdout as "listening on <addr>"
        .args(["--storage-path", dir.to_str().unwrap()])
        .args(["--listen-address", "127.0.0.1"])
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();

    // Parse "listening on 127.0.0.1:PORT" from stdout.
    use tokio::io::{AsyncBufReadExt, BufReader};
    let stdout = child.stdout.take().unwrap();
    let mut lines = BufReader::new(stdout).lines();
    let addr = loop {
        let line = tokio::time::timeout(Duration::from_secs(10), lines.next_line())
            .await.unwrap().unwrap().unwrap();
        if let Some(rest) = line.strip_prefix("listening on ") {
            break rest.trim().to_string();
        }
    };

    let health: serde_json::Value =
        reqwest::get(format!("http://{addr}/health")).await.unwrap().json().await.unwrap();
    assert_eq!(health["node_count"], 0);

    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws")).await.unwrap();
    use futures_util::StreamExt;
    let first = ws.next().await.unwrap().unwrap();
    assert!(first.to_text().unwrap().contains("\"schema_version\":13"));

    // SIGTERM -> clean exit 0.
    send_sigterm(child.id().unwrap());
    let status = tokio::time::timeout(Duration::from_secs(5), child.wait()).await.unwrap().unwrap();
    assert!(status.success(), "exit was {status:?}");
    assert!(dir.exists(), "storage dir must be created");
}

fn send_sigterm(pid: u32) {
    // SIGTERM without adding a nix dependency:
    let _ = std::process::Command::new("kill").args(["-TERM", &pid.to_string()]).status();
}

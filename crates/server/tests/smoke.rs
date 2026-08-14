//! Whole-binary smoke test: boot -> identity generation -> serving -> clean
//! SIGTERM. Since Task 17 this runs the real Matter stack, so it is also the
//! gate that the wiring in `main.rs` (stack boot, ready handshake, controller
//! construction, ordered shutdown) holds together outside the unit tests.

use std::process::Stdio;
use std::time::Duration;

/// The binary generates a CA and a NOC before it prints anything, and mDNS
/// startup can be slow on a loaded machine. The cap exists to turn a hang into a
/// failure, not to police the boot time.
const BOOT_CAP: Duration = Duration::from_secs(60);

/// `main`'s shutdown budget is up to 3s of listener drain plus up to 10s inside
/// `StackHandle::shutdown` (5s for the loop's acknowledgement, 5s for the
/// thread join), so anything under ~13s would be flaky by construction.
const STOP_CAP: Duration = Duration::from_secs(30);

#[tokio::test]
async fn binary_serves_health_and_ws_and_stops_on_sigterm() {
    // A fresh dir per run, removed when `dir` drops — the old fixed-name path
    // leaked across runs and could hand a later run a stale identity.
    let dir = tempfile::tempdir().unwrap();
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_matter-rs-server"))
        .args(["--port", "0"]) // 0 = kernel-picked; printed on stdout as "listening on <addr>"
        .args(["--storage-path", dir.path().to_str().unwrap()])
        .args(["--listen-address", "127.0.0.1"])
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();

    // Parse "listening on 127.0.0.1:PORT" from stdout. Reaching this line at all
    // means the stack answered the ready handshake: `main` exits before binding
    // if it did not.
    use tokio::io::{AsyncBufReadExt, BufReader};
    let stdout = child.stdout.take().unwrap();
    let mut lines = BufReader::new(stdout).lines();
    let addr = loop {
        let line = tokio::time::timeout(BOOT_CAP, lines.next_line())
            .await.unwrap().unwrap()
            .expect("the server exited before it started listening");
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
    let info = first.to_text().unwrap();
    assert!(info.contains("\"schema_version\":13"), "server_info was {info}");
    // The real controller reports the fabric it actually booted, so these prove
    // it is not the stub any more.
    assert!(info.contains("(rs-matter/03bc8f2)"), "server_info was {info}");
    assert!(info.contains("\"controller_node_id\":112233"), "server_info was {info}");

    // SIGTERM -> clean exit 0 (a dead stack would exit 1 instead).
    send_sigterm(child.id().unwrap());
    let status = tokio::time::timeout(STOP_CAP, child.wait()).await.unwrap().unwrap();
    assert!(status.success(), "exit was {status:?}");

    // The identity has to have survived to disk, or every commissioned node
    // would be orphaned by a restart.
    assert!(dir.path().join("server.json").exists(), "identity must be persisted");
}

fn send_sigterm(pid: u32) {
    // SIGTERM without adding a nix dependency:
    let _ = std::process::Command::new("kill").args(["-TERM", &pid.to_string()]).status();
}

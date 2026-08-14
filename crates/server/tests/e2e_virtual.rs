//! End-to-end acceptance against a virtual matter.js device: the real Matter
//! stack, the real binary, a real peer on the LAN.
//!
//! Its own test binary on purpose. There is one Matter stack per *process* —
//! rs-matter keeps the `Matter` instance, the exchange buffer pool and the IM
//! state in process-wide statics that `shutdown()` does not release — so a live
//! stack cannot share a binary with anything else that wants one. (The binary
//! this test spawns is a separate process, so that constraint is about not
//! adding live-stack cases to `smoke.rs` or to this file's sibling tests.)
//!
//! `#[ignore]`d *and* gated on `MRS_E2E=1`: it needs `npx`, a LAN interface, and
//! multicast, none of which a CI runner is guaranteed to have. Best-effort
//! automation of `scripts/e2e-virtual-device.md` — **that runbook is the
//! authoritative gate**, because matter.js startup is flaky enough that a red
//! run here is more often the device's fault than ours.

use std::process::Stdio;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio_tungstenite::tungstenite::Message;

/// Node's first `npx` run downloads ~100 MB; after that the device is online in
/// a couple of seconds. Generous because the cost of being wrong is a spurious
/// failure on a slow link, and the cap only exists to turn a hang into a failure.
const DEVICE_BOOT_CAP: Duration = Duration::from_secs(300);

/// The binary generates a CA and a NOC before it prints anything.
const SERVER_BOOT_CAP: Duration = Duration::from_secs(60);

/// Discovery (30 s browse budget) + PASE + over-PASE configuration + CASE +
/// the wildcard interview, with room for a device that is slow to advertise.
const COMMISSION_CAP: Duration = Duration::from_secs(120);

/// Everything post-commissioning runs over an established subscription, so these
/// are fast; a second-scale cap is only here to keep a hang from hanging the run.
const REPLY_CAP: Duration = Duration::from_secs(30);

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

#[tokio::test]
#[ignore = "needs npx + a LAN interface; set MRS_E2E=1 and see scripts/e2e-virtual-device.md"]
async fn commissions_controls_and_reports_a_virtual_matter_js_device() {
    // Early return, not a panic: `--ignored` runs everything ignored, so a
    // developer running the ignored set without the env var should see a pass
    // with an explanation rather than a failure to triage.
    if std::env::var("MRS_E2E").as_deref() != Ok("1") {
        eprintln!("skipping: set MRS_E2E=1 to run the virtual-device e2e");
        return;
    }
    let iface = std::env::var("MRS_E2E_INTERFACE").unwrap_or_else(|_| "en0".into());

    // Fresh storage for both sides. A half-commissioned fabric left by a failed
    // attempt makes the next one fail for reasons that have nothing to do with
    // the code under test, on either side of the wire.
    let device_dir = tempfile::tempdir().unwrap();
    let server_dir = tempfile::tempdir().unwrap();

    let (mut device, code) = start_device(device_dir.path()).await;
    let (mut server, addr) = start_server(server_dir.path(), &iface).await;

    let mut ws = connect(&addr).await;
    let greeting_by = tokio::time::Instant::now() + REPLY_CAP;
    let info = next_json(&mut ws, "the server_info greeting", greeting_by).await;
    assert_eq!(info["schema_version"], 13, "server_info was {info}");

    // Before commissioning: every event below is dropped for a connection that
    // is not listening.
    send(&mut ws, "1", "start_listening", json!({})).await;
    let listening = expect_result(&mut ws, "1", REPLY_CAP).await;
    assert_eq!(listening, json!([]), "fresh storage must report no nodes");

    // --- commission ---------------------------------------------------------
    send(&mut ws, "2", "commission_with_code", json!({"code": code})).await;
    let node = expect_result(&mut ws, "2", COMMISSION_CAP).await;
    assert_eq!(node["node_id"], 1, "first node gets id 1: {node}");
    let attrs = node["attributes"].as_object().expect("attributes object");
    // Endpoint 1 is the example's OnOffLight; 0/40/1 is its vendor name. Two
    // specific paths rather than a count, so this says what the interview read.
    assert_eq!(attrs["0/40/1"], "matter-node.js", "attributes were {attrs:?}");
    assert!(attrs.contains_key("1/6/0"), "no OnOff state in {attrs:?}");
    // `commission_with_code` returns before the node is subscribed — the
    // supervisor establishes its subscription afterwards — so the result says
    // `available: false` and the flip arrives as a `node_updated` event.
    assert_eq!(node["available"], false, "commissioning cannot report a live subscription yet");
    assert_eq!(node["is_bridge"], false);

    // Waiting for that flip is not politeness, it is required: an
    // `attribute_updated` is a *subscription* report, so a toggle issued before
    // the subscription exists shows up folded into the priming snapshot (a
    // `node_updated`) and no `attribute_updated` ever comes. Which is how the
    // first version of this test failed.
    let up = await_available(&mut ws, true, COMMISSION_CAP).await;
    assert_eq!(up["data"]["node_id"], 1);

    // --- toggle, and the subscription report it provokes ---------------------
    let before = read_on_off(&mut ws, "3").await;
    send(&mut ws, "4", "device_command", json!({
        "node_id": 1, "endpoint_id": 1, "cluster_id": 6,
        "command_name": "toggle", "payload": {},
    })).await;
    // A DefaultSuccess command carries no response payload.
    assert_eq!(expect_result(&mut ws, "4", REPLY_CAP).await, Value::Null);

    // The state change arrives on the subscription, not in the command's reply.
    let ev = await_event(&mut ws, "attribute_updated", REPLY_CAP).await;
    assert_eq!(ev["data"], json!([1, "1/6/0", !before]), "event was {ev}");

    // --- read it back -------------------------------------------------------
    assert_eq!(read_on_off(&mut ws, "5").await, !before);

    // A commissioned node must have an address, and it must be a usable literal
    // — the brackets rs-matter puts around an IPv6 peer used to survive to here.
    send(&mut ws, "6", "get_node_ip_addresses", json!({"node_id": 1})).await;
    let addrs = expect_result(&mut ws, "6", REPLY_CAP).await;
    let first = addrs[0].as_str().unwrap_or_default();
    assert!(!first.is_empty() && !first.contains('['), "addresses were {addrs}");
    assert!(first.parse::<std::net::IpAddr>().is_ok(), "unusable address {first:?}");

    // Kills both children even on an assertion failure above (`kill_on_drop`),
    // but be explicit so the device is gone before `device_dir` is removed.
    let _ = server.kill().await;
    let _ = device.kill().await;
}

/// Spawns the device and scrapes its QR payload, returning `(child, "MT:…")`.
///
/// `-p` is not optional: `npx -y @matter/examples matter-device` fails with
/// "could not determine executable to run" because the package ships nine bins
/// and none matches the package name.
async fn start_device(storage: &std::path::Path) -> (Child, String) {
    let mut child = Command::new("npx")
        .args(["-y", "-p", "@matter/examples", "matter-device"])
        .env("MATTER_STORAGE_PATH", storage)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("npx not on PATH");

    // The device's stdout MUST keep being drained for as long as it runs. It logs
    // every mDNS announcement and every message at debug, which fills the 64 KB
    // pipe in seconds; once full, matter.js blocks in `write` and stops answering
    // on the network entirely. The first version of this test dropped the reader
    // after finding the QR line and the run failed with a browse timeout that
    // looked like our bug — the device had simply gone catatonic.
    let stdout = child.stdout.take().unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let mut lines = BufReader::new(stdout).lines();
        let mut tx = Some(tx);
        while let Ok(Some(line)) = lines.next_line().await {
            // The QR line is `  QR code URL: https://…?data=MT:…`.
            if let Some((_, payload)) = line.rsplit_once("data=") {
                if let Some(tx) = tx.take() {
                    let _ = tx.send(payload.trim().to_string());
                }
            }
        }
    });

    let code = tokio::time::timeout(DEVICE_BOOT_CAP, rx)
        .await
        .expect("the device never printed a pairing code")
        .expect("the device exited before printing a pairing code");
    assert!(code.starts_with("MT:"), "unexpected QR payload {code:?}");
    (child, code)
}

/// Spawns the binary on an ephemeral port, returning `(child, "127.0.0.1:port")`.
async fn start_server(storage: &std::path::Path, iface: &str) -> (Child, String) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_matter-rs-server"))
        .args(["--port", "0"]) // kernel-picked; echoed on stdout
        .args(["--storage-path", storage.to_str().unwrap()])
        .args(["--listen-address", "127.0.0.1"])
        .args(["--primary-interface", iface])
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();

    // Drained for the whole run, same reason as the device's (see `start_device`).
    // Far less output here, but a test that deadlocks on a full pipe is not a
    // failure mode worth leaving in place twice.
    let stdout = child.stdout.take().unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let mut lines = BufReader::new(stdout).lines();
        let mut tx = Some(tx);
        while let Ok(Some(line)) = lines.next_line().await {
            // Reaching this line means the stack answered the ready handshake.
            if let Some(rest) = line.strip_prefix("listening on ") {
                if let Some(tx) = tx.take() {
                    let _ = tx.send(rest.trim().to_string());
                }
            }
        }
    });

    let addr = tokio::time::timeout(SERVER_BOOT_CAP, rx)
        .await
        .expect("the server never started listening")
        .expect("the server exited before it started listening");
    (child, addr)
}

async fn connect(addr: &str) -> Ws {
    let (ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws")).await.unwrap();
    ws
}

async fn send(ws: &mut Ws, id: &str, command: &str, args: Value) {
    let frame = json!({"message_id": id, "command": command, "args": args});
    ws.send(Message::text(frame.to_string())).await.unwrap();
}

/// `want` names what the caller is waiting for, so a timeout says which step of
/// the flow stalled instead of only that some frame never came. Every frame seen
/// is echoed, because with `--nocapture` that transcript interleaved with the
/// server's log is the whole diagnostic value of this test when it fails.
async fn next_json(ws: &mut Ws, want: &str, deadline: tokio::time::Instant) -> Value {
    loop {
        let msg = tokio::time::timeout_at(deadline, ws.next())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for {want}"))
            .expect("the connection closed")
            .unwrap();
        if let Message::Text(t) = msg {
            // Truncated: a `MatterNodeData` frame is ~10 KB of attributes.
            eprintln!("ws <- {:.400}", t.as_str());
            return serde_json::from_str(&t).expect("every frame is JSON");
        }
    }
}

/// The `result` of the reply to `message_id`, skipping any events that arrive
/// first — `start_listening` is sent before anything else, so a command's reply
/// is regularly interleaved with `node_added` / `node_updated` frames.
async fn expect_result(ws: &mut Ws, id: &str, cap: Duration) -> Value {
    let deadline = tokio::time::Instant::now() + cap;
    let want = format!("the reply to message {id}");
    loop {
        let v = next_json(ws, &want, deadline).await;
        if v["message_id"] != id {
            continue;
        }
        assert!(v.get("error_code").is_none(), "command {id} failed: {v}");
        return v["result"].clone();
    }
}

async fn await_event(ws: &mut Ws, event: &str, cap: Duration) -> Value {
    let deadline = tokio::time::Instant::now() + cap;
    let want = format!("a {event} event");
    loop {
        let v = next_json(ws, &want, deadline).await;
        if v["event"] == event {
            return v;
        }
    }
}

/// The first `node_updated` reporting `available == want`.
async fn await_available(ws: &mut Ws, want: bool, cap: Duration) -> Value {
    let deadline = tokio::time::Instant::now() + cap;
    let want_str = format!("a node_updated with available={want}");
    loop {
        let v = next_json(ws, &want_str, deadline).await;
        if v["event"] == "node_updated" && v["data"]["available"] == want {
            return v;
        }
    }
}

async fn read_on_off(ws: &mut Ws, id: &str) -> bool {
    send(ws, id, "read_attribute", json!({"node_id": 1, "attribute_path": "1/6/0"})).await;
    let v = expect_result(ws, id, REPLY_CAP).await;
    v["1/6/0"].as_bool().unwrap_or_else(|| panic!("1/6/0 was not a bool: {v}"))
}

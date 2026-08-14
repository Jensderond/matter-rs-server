//! Boot smoke test: the whole stack thread, end to end, without a device.
//!
//! It is the gate for the runtime wiring — it proves the ready handshake fires
//! (and is not blocked behind mDNS init), that the request loop dispatches and
//! replies, and that `shutdown` joins rather than hangs.
//!
//! One test only, deliberately: the `Matter` instance lives in process-wide
//! statics, so a second `spawn` in this binary cannot boot a stack. The
//! second-spawn behaviour is therefore checked *inside* this test, where the
//! ordering is known.

use std::sync::Arc;
use std::time::Duration;

use matter_rs_controller::stack_api::Stack;
use matter_rs_controller::storage::Storage;

/// The read below has to wait out the stack's own 30s IM budget (there is no
/// device and no mDNS record to resolve), so the cap is comfortably above it —
/// its job is to turn a hang into a failure, not to shorten the operation.
const READ_CAP: Duration = Duration::from_secs(90);

#[tokio::test]
async fn stack_boots_persists_identity_and_shuts_down() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(dir.path()).unwrap());
    let (handle, _events, ready) = matter_rs_stack::spawn(matter_rs_stack::StackConfig {
        storage: storage.clone(),
        fabric_id: 1,
        vendor_id: 0xFFF1,
        fabric_label: "HomeAssistant".into(),
        primary_interface: None,
    });
    let ready = tokio::time::timeout(Duration::from_secs(30), ready)
        .await
        .expect("ready in time")
        .expect("channel")
        .expect("stack up");
    assert_eq!(ready.identity.controller_node_id, 112233);
    assert_ne!(ready.identity.compressed_fabric_id, 0);
    assert_eq!(ready.fabric_index, 1);
    assert!(storage.load_identity().unwrap().is_some());

    // An operation against a nonexistent node fails cleanly (mDNS resolve miss),
    // proving the request loop dispatches and replies.
    let err = tokio::time::timeout(
        READ_CAP,
        handle.read_attributes(
            999,
            vec![matter_rs_controller::stack_api::AttributePathSpec {
                endpoint: Some(0),
                cluster: Some(40),
                attribute: Some(2),
            }],
            false,
        ),
    )
    .await
    .expect("the request loop must reply, not hang")
    .unwrap_err();
    assert!(!err.message.is_empty());

    // A second stack in the same process is refused through the ready channel
    // rather than panicking on the already-initialised statics — and the running
    // stack is unaffected (the `node_addresses` call after this still works).
    let (second, _second_events, second_ready) =
        matter_rs_stack::spawn(matter_rs_stack::StackConfig {
            storage: storage.clone(),
            fabric_id: 1,
            vendor_id: 0xFFF1,
            fabric_label: "HomeAssistant".into(),
            primary_interface: None,
        });
    let refused = tokio::time::timeout(Duration::from_secs(10), second_ready)
        .await
        .expect("the second spawn must answer promptly")
        .expect("the ready channel must carry the refusal, not just close")
        .expect_err("a second stack must not boot");
    assert!(
        refused.contains("already running in this process"),
        "the refusal must say why: {refused}"
    );
    // Its handle is inert, not poisonous.
    assert_eq!(
        second.node_addresses(1).await.unwrap_err().message,
        "stack thread is down"
    );
    second.shutdown().await;

    // The first stack is still serving: an unknown node has no cached addresses,
    // which is an empty answer rather than an error.
    assert_eq!(handle.node_addresses(999).await.unwrap(), Vec::<String>::new());

    // Supervisor start/stop are fire-and-forget; they must not wedge the loop.
    handle.start_supervisor(999).await;
    handle.stop_supervisor(999).await;
    assert_eq!(handle.node_addresses(999).await.unwrap(), Vec::<String>::new());

    // Leave two supervisors *running* into the shutdown, so the `Shutdown` arm
    // drains a non-empty `ctx.supervisors` and the executor has cancelled
    // supervisor futures to drop as `run_stack` returns. Without this the drain
    // is only ever exercised on an empty map and the cancel path that
    // `SubscriptionGuard` exists for is never taken.
    handle.start_supervisor(1001).await;
    handle.start_supervisor(1002).await;
    // Let them be polled at least once, so they are parked in an await rather
    // than sitting unstarted on the executor's queue.
    tokio::time::sleep(Duration::from_millis(200)).await;
    // Still serving — the supervisors did not block the request loop.
    assert_eq!(handle.node_addresses(1001).await.unwrap(), Vec::<String>::new());

    tokio::time::timeout(Duration::from_secs(20), handle.shutdown())
        .await
        .expect("shutdown must return (thread joined), not hang");

    // Post-shutdown the thread is gone, so every method answers cleanly instead
    // of hanging on a channel nobody reads.
    assert_eq!(
        handle.node_addresses(999).await.unwrap_err().message,
        "stack thread is down"
    );
    // And shutting down twice is a no-op.
    handle.shutdown().await;
}

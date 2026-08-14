//! The ONLY crate that imports rs-matter. Everything runs on one dedicated
//! OS thread (rs-matter futures are !Send); the outside world talks to it
//! through [`StackHandle`], which implements
//! [`matter_rs_controller::stack_api::Stack`] over an mpsc channel.

pub(crate) mod ctx;
pub mod identity;
pub(crate) mod mdns;
pub mod migration;
pub(crate) mod ops;
pub(crate) mod reports;
pub(crate) mod runtime;
pub(crate) mod supervisor;
pub mod tlv_json;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use matter_rs_controller::stack_api::{
    AttributePathSpec, CommissionOutcome, CommissionRequest, DeviceFabric, DiscoveredDevice, Stack,
    StackError, StackErrorKind, StackEvent, WindowInfo,
};
use matter_rs_controller::storage::{ServerIdentity, Storage};

use serde_json::Value;
use tokio::sync::{mpsc, oneshot};

use crate::runtime::{Reply, StackRequest};

/// How the stack thread is configured at boot. Everything here comes from the
/// server's CLI/config; nothing is read from the environment.
pub struct StackConfig {
    pub storage: Arc<Storage>,
    pub fabric_id: u64,
    pub vendor_id: u16,
    pub fabric_label: String,
    /// `--primary-interface`: pins the interface mDNS binds to, instead of
    /// letting the heuristic pick (which can land on a docker/VM bridge).
    pub primary_interface: Option<String>,
}

/// What the caller needs before it starts serving: the identity Home Assistant
/// is told about in `server_info`, and our own fabric index.
#[derive(Debug, Clone)]
pub struct ReadyInfo {
    pub identity: ServerIdentity,
    pub fabric_index: u8,
}

/// Stack size for the Matter thread.
///
/// Generous on purpose: rs-matter composes deep future state machines (the
/// responder alone nests four exchange handlers over the IM over the secure
/// channel) and several operations put multi-kilobyte certificate buffers on the
/// stack. It is virtual address space — only touched pages are committed.
const THREAD_STACK_SIZE: usize = 8 * 1024 * 1024;

/// Cap on both halves of `shutdown`: waiting for the loop to acknowledge, and
/// joining the thread afterwards.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// What every method answers when the stack thread is gone — because it never
/// started, because the transport died, or because `shutdown` already ran.
const THREAD_DOWN: &str = "stack thread is down";

/// A cloneable handle onto the stack thread. All methods are `async` and `Send`;
/// the rs-matter side of the boundary is entirely hidden behind the channel.
#[derive(Clone)]
pub struct StackHandle {
    tx: mpsc::UnboundedSender<StackRequest>,
    /// Shared so that `shutdown` can be called through any clone, and so that
    /// the second call finds `None` and returns instead of joining twice.
    thread: Arc<Mutex<Option<JoinHandle<()>>>>,
}

/// Spawn the dedicated rs-matter thread. Await `ready` before serving.
///
/// Returns the handle, the `StackEvent` stream (for the controller's
/// `NodeManager`), and the ready handshake: `Ok(ReadyInfo)` once the identity is
/// established, `Err(message)` if the stack cannot start.
///
/// **One stack per process.** rs-matter's `Matter` instance, exchange buffer pool
/// and IM state live in process-wide statics (they must outlive every future that
/// borrows them). A second `spawn` therefore cannot start a stack: it returns a
/// handle whose thread immediately reports `Err` through the ready channel and
/// exits, after which every method on that handle answers "stack thread is down".
/// It does not panic and it does not disturb the running stack.
pub fn spawn(
    config: StackConfig,
) -> (
    StackHandle,
    mpsc::UnboundedReceiver<StackEvent>,
    oneshot::Receiver<Result<ReadyInfo, String>>,
) {
    let (tx, rx) = mpsc::unbounded_channel();
    let (events_tx, events_rx) = mpsc::unbounded_channel();
    let (ready_tx, ready_rx) = oneshot::channel();

    let thread = std::thread::Builder::new()
        .name("matter-stack".to_string())
        .stack_size(THREAD_STACK_SIZE)
        .spawn(move || runtime::run_stack(config, events_tx, ready_tx, rx));

    let thread = match thread {
        Ok(handle) => Some(handle),
        Err(e) => {
            // `ready_tx` went into the closure and is dropped with it, so the
            // caller sees a closed ready channel — which the server already has
            // to treat as a failed boot. Nothing to unwrap, nothing to panic on.
            tracing::error!("could not spawn the Matter stack thread: {e}");
            None
        }
    };

    (
        StackHandle { tx, thread: Arc::new(Mutex::new(thread)) },
        events_rx,
        ready_rx,
    )
}

impl StackHandle {
    /// Queue a request, mapping a closed channel onto a clean error.
    fn send(&self, req: StackRequest) -> Result<(), StackError> {
        self.tx
            .send(req)
            .map_err(|_| StackError::new(StackErrorKind::Sdk, THREAD_DOWN))
    }

    /// Queue a request and await its reply.
    ///
    /// A dropped reply sender is the same condition as a closed request channel
    /// (the thread died while the operation was in flight), so both map to
    /// `THREAD_DOWN` rather than to a panic on an unwrap.
    async fn request<T>(
        &self,
        make: impl FnOnce(Reply<T>) -> StackRequest,
    ) -> Result<T, StackError> {
        let (reply, rx) = oneshot::channel();
        self.send(make(reply))?;
        match rx.await {
            Ok(result) => result,
            Err(_) => Err(StackError::new(StackErrorKind::Sdk, THREAD_DOWN)),
        }
    }

    /// Take the join handle, tolerating a poisoned mutex.
    ///
    /// Poisoning here would mean a previous holder panicked while swapping an
    /// `Option`; the value is still well-formed, and panicking again inside a
    /// shutdown path is the one thing that would make it worse.
    fn take_thread(&self) -> Option<JoinHandle<()>> {
        match self.thread.lock() {
            Ok(mut guard) => guard.take(),
            Err(poisoned) => poisoned.into_inner().take(),
        }
    }
}

#[async_trait::async_trait]
impl Stack for StackHandle {
    async fn commission(&self, req: CommissionRequest) -> Result<CommissionOutcome, StackError> {
        self.request(|reply| StackRequest::Commission { req, reply }).await
    }

    async fn read_attributes(
        &self,
        node_id: u64,
        paths: Vec<AttributePathSpec>,
        fabric_filtered: bool,
    ) -> Result<Vec<(String, Value)>, StackError> {
        self.request(|reply| StackRequest::Read { node_id, paths, fabric_filtered, reply })
            .await
    }

    async fn write_attribute(
        &self,
        node_id: u64,
        endpoint: u16,
        cluster: u32,
        attribute: u32,
        value: Value,
    ) -> Result<u8, StackError> {
        self.request(|reply| StackRequest::Write {
            node_id,
            endpoint,
            cluster,
            attribute,
            value,
            reply,
        })
        .await
    }

    async fn invoke(
        &self,
        node_id: u64,
        endpoint: u16,
        cluster: u32,
        command_name: String,
        payload: Value,
        timed_ms: Option<u16>,
    ) -> Result<Value, StackError> {
        self.request(|reply| StackRequest::Invoke {
            node_id,
            endpoint,
            cluster,
            command_name,
            payload,
            timed_ms,
            reply,
        })
        .await
    }

    async fn interview(&self, node_id: u64) -> Result<BTreeMap<String, Value>, StackError> {
        self.request(|reply| StackRequest::Interview { node_id, reply }).await
    }

    async fn open_commissioning_window(
        &self,
        node_id: u64,
        timeout_secs: u16,
    ) -> Result<WindowInfo, StackError> {
        self.request(|reply| StackRequest::OpenWindow { node_id, timeout_secs, reply })
            .await
    }

    async fn device_fabrics(&self, node_id: u64) -> Result<Vec<DeviceFabric>, StackError> {
        self.request(|reply| StackRequest::DeviceFabrics { node_id, reply }).await
    }

    async fn remove_device_fabric(&self, node_id: u64, fabric_index: u8) -> Result<(), StackError> {
        self.request(|reply| StackRequest::RemoveDeviceFabric { node_id, fabric_index, reply })
            .await
    }

    async fn update_fabric_label(&self, label: String) -> Result<(), StackError> {
        self.request(|reply| StackRequest::UpdateFabricLabel { label, reply }).await
    }

    async fn start_supervisor(&self, node_id: u64) {
        // No reply to await: the outcome arrives as a `StackEvent`. A closed
        // channel is still worth a line, since the caller cannot see it.
        if self.send(StackRequest::StartSupervisor { node_id }).is_err() {
            tracing::warn!("cannot start the supervisor for node {node_id}: {THREAD_DOWN}");
        }
    }

    async fn stop_supervisor(&self, node_id: u64) {
        if self.send(StackRequest::StopSupervisor { node_id }).is_err() {
            tracing::debug!("cannot stop the supervisor for node {node_id}: {THREAD_DOWN}");
        }
    }

    async fn node_addresses(&self, node_id: u64) -> Result<Vec<String>, StackError> {
        self.request(|reply| StackRequest::NodeAddresses { node_id, reply }).await
    }

    async fn browse_commissionable(
        &self,
        timeout_ms: u32,
    ) -> Result<Vec<DiscoveredDevice>, StackError> {
        self.request(|reply| StackRequest::Browse { timeout_ms, reply }).await
    }

    /// Cancel the supervisors, stop the request loop, and join the stack thread.
    ///
    /// **An abrupt stop, not a drain.** The stack answers `done` and breaks its
    /// loop immediately, which ends the executor, so:
    ///
    /// - in-flight detached requests (a 60s commissioning attempt, say) are
    ///   abandoned mid-operation and their callers see the reply channel close;
    /// - `run_persist_resumption` never gets a final tick, so up to its 500ms
    ///   coalescing window of CASE-resumption records is lost. That costs one
    ///   full CASE handshake on the next connection — the same fallback every
    ///   other resumption miss takes — and is not worth blocking shutdown for.
    ///
    /// Returns either way: both waits are capped at [`SHUTDOWN_TIMEOUT`] (so the
    /// worst case is twice that), and a thread that has already exited — or never
    /// started — is a no-op. Idempotent: the join handle is taken on the first
    /// call, so a second one has nothing left to do.
    ///
    /// Unlike every other method here, this one **requires a Tokio runtime with
    /// the time driver**: it uses `tokio::time::timeout` and `spawn_blocking`,
    /// which panic outside one. The rest of the trait is executor-agnostic,
    /// because `tokio::sync` is.
    async fn shutdown(&self) {
        let (done, ack) = oneshot::channel();
        if self.send(StackRequest::Shutdown { done }).is_ok() {
            // An `Err` from the oneshot means the thread died before answering,
            // which is the outcome we wanted anyway; only a timeout is notable.
            if tokio::time::timeout(SHUTDOWN_TIMEOUT, ack).await.is_err() {
                tracing::warn!(
                    "the Matter stack did not acknowledge shutdown within {}s",
                    SHUTDOWN_TIMEOUT.as_secs()
                );
            }
        }

        let Some(handle) = self.take_thread() else {
            return;
        };

        // `join` blocks, so it goes to the blocking pool; the timeout is what
        // keeps a wedged stack thread from wedging the whole shutdown.
        let joined = tokio::time::timeout(
            SHUTDOWN_TIMEOUT,
            tokio::task::spawn_blocking(move || handle.join()),
        )
        .await;

        match joined {
            Ok(Ok(Ok(()))) => tracing::info!("Matter stack thread joined"),
            Ok(Ok(Err(_))) => tracing::error!("the Matter stack thread panicked"),
            Ok(Err(e)) => tracing::warn!("joining the Matter stack thread failed: {e}"),
            Err(_) => tracing::warn!(
                "the Matter stack thread did not exit within {}s; abandoning it",
                SHUTDOWN_TIMEOUT.as_secs()
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use matter_rs_controller::stack_api::PaseTarget;

    /// A handle whose stack thread never existed: the request channel is closed
    /// and there is nothing to join. Exactly the state a handle is left in after
    /// `shutdown`, or after the transport died.
    fn dead_handle() -> StackHandle {
        let (tx, rx) = mpsc::unbounded_channel();
        drop(rx);
        StackHandle { tx, thread: Arc::new(Mutex::new(None)) }
    }

    #[tokio::test]
    async fn a_closed_channel_is_an_error_not_a_panic() {
        let handle = dead_handle();

        let e = handle
            .read_attributes(1, vec![], false)
            .await
            .expect_err("a closed channel must not look like an empty read");
        assert_eq!(e.kind, StackErrorKind::Sdk);
        // Compat-relevant: this string reaches the client as the error `details`.
        assert_eq!(e.message, "stack thread is down");

        assert_eq!(handle.interview(1).await.unwrap_err().message, THREAD_DOWN);
        assert_eq!(
            handle.node_addresses(1).await.unwrap_err().message,
            THREAD_DOWN
        );
        assert_eq!(
            handle.browse_commissionable(100).await.unwrap_err().message,
            THREAD_DOWN
        );
        assert_eq!(
            handle.update_fabric_label("x".into()).await.unwrap_err().message,
            THREAD_DOWN
        );
        assert_eq!(
            handle.open_commissioning_window(1, 60).await.unwrap_err().message,
            THREAD_DOWN
        );
        assert_eq!(
            handle.write_attribute(1, 0, 6, 0, Value::Bool(true)).await.unwrap_err().message,
            THREAD_DOWN
        );
        assert_eq!(
            handle.invoke(1, 1, 6, "toggle".into(), Value::Null, None).await.unwrap_err().message,
            THREAD_DOWN
        );
        assert_eq!(
            handle.device_fabrics(1).await.unwrap_err().message,
            THREAD_DOWN
        );
        assert_eq!(
            handle.remove_device_fabric(1, 2).await.unwrap_err().message,
            THREAD_DOWN
        );
        let commission = CommissionRequest {
            node_id: 1,
            target: PaseTarget::Code { code: "MT:0000".into() },
            fabric_label: "HomeAssistant".into(),
        };
        assert_eq!(
            handle.commission(commission).await.unwrap_err().message,
            THREAD_DOWN
        );

        // The two fire-and-forget methods return `()` and must simply not panic.
        handle.start_supervisor(1).await;
        handle.stop_supervisor(1).await;
    }

    /// A dropped reply sender is the "thread died mid-operation" case, and has to
    /// map to the same clean error as a closed request channel.
    #[tokio::test]
    async fn a_dropped_reply_is_the_same_clean_error() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let handle = StackHandle { tx, thread: Arc::new(Mutex::new(None)) };

        let call = tokio::spawn({
            let handle = handle.clone();
            async move { handle.interview(7).await }
        });

        // Take the request off the queue and drop it, reply sender included.
        let req = rx.recv().await.expect("the request must arrive");
        drop(req);

        let e = call.await.expect("the task must not panic").expect_err("no reply, no result");
        assert_eq!(e.kind, StackErrorKind::Sdk);
        assert_eq!(e.message, THREAD_DOWN);
    }

    #[tokio::test]
    async fn shutdown_is_safe_on_a_dead_stack_and_safe_twice() {
        let handle = dead_handle();
        handle.shutdown().await;
        handle.shutdown().await;

        // And through a clone, which shares the (already emptied) join handle.
        let clone = handle.clone();
        clone.shutdown().await;
    }

    /// `shutdown` must not wait for a reply that will never come. The channel is
    /// open here (nothing reads it), so the acknowledgement times out — and the
    /// call still has to return.
    #[tokio::test(start_paused = true)]
    async fn shutdown_returns_when_the_stack_never_acknowledges() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let handle = StackHandle { tx, thread: Arc::new(Mutex::new(None)) };
        handle.shutdown().await;
    }

    #[test]
    fn the_handle_is_send_sync_and_static() {
        fn assert_stack<S: Stack>() {}
        assert_stack::<StackHandle>();
    }
}

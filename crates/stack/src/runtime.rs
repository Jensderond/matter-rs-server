//! The stack thread: process-wide statics, the boot sequence, the long-running
//! futures, and the request loop [`crate::StackHandle`] talks to.
//!
//! Everything below runs on one OS thread with a local executor, because
//! rs-matter futures are `!Send`. The only things that cross the thread boundary
//! are plain owned data: [`StackRequest`]s in, `StackEvent`s and oneshot replies
//! out.
//!
//! Failure policy differs per future and is load-bearing:
//!
//! | future                    | exit means                                   |
//! |---------------------------|----------------------------------------------|
//! | `Matter::run` (transport) | fatal — nothing works without it; stop        |
//! | IM responder              | fatal — no reports, no replies; stop          |
//! | built-in mDNS             | warn, keep running (spike finding 3)          |
//! | resumption persist        | warn, keep running (costs a CASE handshake)   |
//! | request loop              | `Shutdown` was requested; stop                |

use core::future::pending;
use core::sync::atomic::{AtomicBool, Ordering};
use std::collections::{BTreeMap, HashMap};
use std::net::UdpSocket;
use std::panic::AssertUnwindSafe;
use std::rc::Rc;

use embassy_futures::select::{select, select4};
use embassy_time::Duration;

use matter_rs_controller::stack_api::{
    CommissionOutcome, CommissionRequest, DeviceFabric, DiscoveredDevice, StackError, StackEvent,
    WindowInfo,
};
use matter_rs_controller::stack_api::AttributePathSpec;

use rs_matter::crypto::{default_crypto, Crypto};
use rs_matter::dm::clusters::net_comm::{DummyNetworks, NetworkType};
use rs_matter::dm::devices::test::{DAC_PRIVKEY, TEST_DEV_ATT, TEST_DEV_COMM, TEST_DEV_DET};
use rs_matter::dm::networks::wireless::NoopWirelessNetCtl;
use rs_matter::dm::{EmptyHandler, Node};
use rs_matter::error::{Error, ErrorCode};
use rs_matter::im::{InteractionModel, InteractionModelState};
use rs_matter::persist::DirKvBlobStore;
use rs_matter::respond::Responder;
use rs_matter::transport::exchange::MatterBuffers;
use rs_matter::transport::network::NoNetwork;
use rs_matter::utils::init::InitMaybeUninit;
use rs_matter::Matter;

use futures_lite::FutureExt as _;
use serde_json::Value;
use socket2::{Domain, Protocol, Socket, Type};
use static_cell::StaticCell;
use tokio::sync::{mpsc, oneshot};

use crate::ctx::StackCtx;
use crate::reports::ReportSink;
use crate::{identity, mdns, ops, supervisor, ReadyInfo, StackConfig};

/// The reply half of a request. A dropped sender means the caller gave up (its
/// WS connection closed, say), which is normal and never an error here.
pub(crate) type Reply<T> = oneshot::Sender<Result<T, StackError>>;

/// What [`crate::StackHandle`] can ask the stack thread to do.
///
/// One variant per `Stack` trait method. `StartSupervisor`/`StopSupervisor` have
/// no reply because the trait's methods return `()`: they are fire-and-forget
/// registry mutations, and the interesting outcome arrives later as a
/// `StackEvent`.
pub(crate) enum StackRequest {
    Commission {
        req: CommissionRequest,
        reply: Reply<CommissionOutcome>,
    },
    Read {
        node_id: u64,
        paths: Vec<AttributePathSpec>,
        fabric_filtered: bool,
        reply: Reply<Vec<(String, Value)>>,
    },
    Write {
        node_id: u64,
        endpoint: u16,
        cluster: u32,
        attribute: u32,
        value: Value,
        reply: Reply<u8>,
    },
    Invoke {
        node_id: u64,
        endpoint: u16,
        cluster: u32,
        command_name: String,
        payload: Value,
        timed_ms: Option<u16>,
        reply: Reply<Value>,
    },
    Interview {
        node_id: u64,
        reply: Reply<BTreeMap<String, Value>>,
    },
    OpenWindow {
        node_id: u64,
        timeout_secs: u16,
        reply: Reply<WindowInfo>,
    },
    DeviceFabrics {
        node_id: u64,
        reply: Reply<Vec<DeviceFabric>>,
    },
    RemoveDeviceFabric {
        node_id: u64,
        fabric_index: u8,
        reply: Reply<()>,
    },
    UpdateFabricLabel {
        label: String,
        reply: Reply<()>,
    },
    StartSupervisor {
        node_id: u64,
    },
    StopSupervisor {
        node_id: u64,
    },
    NodeAddresses {
        node_id: u64,
        reply: Reply<Vec<String>>,
    },
    Browse {
        timeout_ms: u32,
        reply: Reply<Vec<DiscoveredDevice>>,
    },
    Shutdown {
        done: oneshot::Sender<()>,
    },
}

/// Concurrent exchange handlers the IM responder runs, as in
/// `rs-matter-ref/rs-matter/tests/im/subscription_reboot.rs:297`.
const RESPONDER_HANDLERS: usize = 4;

/// Minimum gap between two CASE-resumption flushes. Coalesces a commissioning
/// wave into one write; 500 ms is what rs-matter itself used before the interval
/// became a parameter (`rs-matter/src/lib.rs:712`).
const RESUMPTION_FLUSH_MS: u64 = 500;

/// What a second [`crate::spawn`] in the same process is told.
///
/// Not a panic: `spawn` is a library entry point, and the caller can act on an
/// error through the ready channel. See the note on [`crate::spawn`].
pub(crate) const SECOND_STACK_MESSAGE: &str =
    "a Matter stack is already running in this process (rs-matter's state lives in process-wide \
     statics, so there can only be one)";

// One stack per process. These are `static` because `Matter`, the exchange buffer
// pool and the IM state must outlive every future that borrows them, and those
// futures are spawned on a local executor whose tasks are not scoped to any
// enclosing stack frame.
static MATTER: StaticCell<Matter<'static>> = StaticCell::new();
static IM_BUFFERS: StaticCell<MatterBuffers> = StaticCell::new();
static IM_STATE: StaticCell<InteractionModelState<DummyNetworks>> = StaticCell::new();

/// All-or-nothing gate over the three cells above.
///
/// Each `StaticCell` is individually CAS-guarded, but the three claims are not
/// atomic *as a group*: two concurrent `spawn`s could take one cell each, both
/// then fail — and since a `StaticCell` can never be released, the process would
/// be permanently unable to start any stack. One gate in front makes the claim
/// indivisible.
static STACK_CLAIMED: AtomicBool = AtomicBool::new(false);

/// Take the process-wide statics, or `None` if some earlier stack already did.
///
/// `try_uninit`/`try_init` rather than `uninit`/`init`: the latter panic, and a
/// panic in the body of the stack thread would kill the stack with no diagnosis
/// reaching the caller.
#[allow(clippy::type_complexity)]
fn claim_statics() -> Option<(
    &'static Matter<'static>,
    &'static MatterBuffers,
    &'static InteractionModelState<DummyNetworks>,
)> {
    if STACK_CLAIMED.swap(true, Ordering::SeqCst) {
        return None;
    }

    // Exactly one caller ever gets here, so the `?`s below cannot lose a race.
    // They stay fallible anyway: an unexpectedly-full cell must surface as an
    // `Err` on the ready channel, never as a panic on this thread.

    // Test device details/attestation: the controller is never itself
    // commissioned, so these are only ever used for the node-attestation
    // challenge we do not participate in — exactly as in the spike
    // (`spike/src/main.rs:138`).
    let matter = MATTER.try_uninit()?.init_with(Matter::init(
        &TEST_DEV_DET,
        TEST_DEV_COMM,
        &TEST_DEV_ATT,
        // Local port 0: kernel-picked, matching the ephemeral socket below.
        0,
    ));
    let buffers = IM_BUFFERS.try_uninit()?.init_with(MatterBuffers::init());
    let im_state = IM_STATE.try_init(InteractionModelState::new(DummyNetworks))?;

    Some((matter, buffers, im_state))
}

/// The stack thread body. Returns when the stack stops; never panics.
pub(crate) fn run_stack(
    config: StackConfig,
    events: mpsc::UnboundedSender<StackEvent>,
    ready: oneshot::Sender<Result<ReadyInfo, String>>,
    rx: mpsc::UnboundedReceiver<StackRequest>,
) {
    let Some((matter, buffers, im_state)) = claim_statics() else {
        let _ = ready.send(Err(SECOND_STACK_MESSAGE.to_string()));
        return;
    };

    let socket = match create_dual_stack_socket() {
        Ok(s) => s,
        Err(e) => {
            let _ = ready.send(Err(format!("could not bind the Matter UDP socket: Error::{e}")));
            return;
        }
    };
    match socket.get_ref().local_addr() {
        Ok(addr) => tracing::info!("Matter transport bound on {addr}"),
        // Purely informational; a socket we just bound and cannot name is still
        // a working socket.
        Err(e) => tracing::debug!("could not read the local socket address: {e}"),
    }

    let crypto = default_crypto(rand::thread_rng(), DAC_PRIVKEY);

    // CASE-resumption records (and, in principle, fabric blobs — but the fabric
    // slots stay empty by design: `server.json` owns the identity).
    let kv = DirKvBlobStore::new(config.storage.root().join("sessions"));

    // Warn, not fatal: an unreadable `sessions/` directory costs a full CASE
    // handshake per peer on the first connection, nothing more.
    if let Err(e) = matter.startup(matter.kv(kv.clone())) {
        tracing::warn!(
            "loading persisted CASE-resumption state failed: Error::{e}; peers will do a full \
             CASE handshake"
        );
    }

    let (identity, fab_idx) = match identity::ensure_identity(
        matter,
        &crypto,
        &config.storage,
        config.fabric_id,
        config.vendor_id,
        &config.fabric_label,
    ) {
        Ok(v) => v,
        Err(e) => {
            let _ = ready.send(Err(format!(
                "could not establish the controller identity: Error::{e}"
            )));
            return;
        }
    };

    // The ready handshake, answered before the executor exists — so nothing
    // below, and in particular not the built-in mDNS runner (which may take a
    // while to bind, or fail outright), can delay or block it.
    let _ = ready.send(Ok(ReadyInfo {
        identity: identity.clone(),
        fabric_index: fab_idx.get(),
    }));

    let ctx = Rc::new(StackCtx::new(matter, crypto, fab_idx, identity, events));

    // The controller's own data model: no clusters served (empty node), it only
    // consumes reports. Construction mirrors
    // `rs-matter-ref/rs-matter/tests/im/subscription_reboot.rs:286-298`.
    let im = InteractionModel::new_with_reports(
        matter,
        &ctx.crypto,
        buffers,
        (Node::new(&[]), EmptyHandler),
        matter.kv(kv.clone()),
        NoopWirelessNetCtl::new(NetworkType::Ethernet),
        ReportSink(ctx.clone()),
        im_state,
    );
    let responder = Responder::new_default(&im);

    let ex = async_executor::LocalExecutor::new();

    let transport = async {
        match matter.run(&ctx.crypto, &socket, &socket, NoNetwork).await {
            Ok(()) => tracing::error!("the Matter transport exited; stopping the stack"),
            Err(e) => {
                tracing::error!("the Matter transport exited: Error::{e}; stopping the stack")
            }
        }
    };

    let responder_fut = async {
        match responder.run::<RESPONDER_HANDLERS>().await {
            Ok(()) => tracing::error!("the IM responder exited; stopping the stack"),
            Err(e) => tracing::error!("the IM responder exited: Error::{e}; stopping the stack"),
        }
    };

    // Degraded, not dead: no mDNS means no discovery and no resolving a node we
    // have never talked to, but live sessions and subscriptions keep working
    // (spike finding 3). `pending()` keeps this arm of the select from ending the
    // stack.
    let mdns_fut = async {
        match mdns::run_builtin_mdns(matter, &ctx.crypto, config.primary_interface.as_deref()).await
        {
            Ok(()) => tracing::warn!("mDNS runner exited; discovery and cold-resolve degraded"),
            Err(e) => tracing::warn!(
                "mDNS runner exited: Error::{e}; discovery and cold-resolve degraded"
            ),
        }
        pending::<()>().await
    };

    // Also degraded-not-dead: losing the resumption cache means a full CASE
    // handshake next time, which is the same fallback any resumption miss takes.
    let persist_fut = async {
        if let Err(e) = matter
            .run_persist_resumption(
                matter.kv(kv.clone()),
                Duration::from_millis(RESUMPTION_FLUSH_MS),
            )
            .await
        {
            tracing::warn!("persisting the CASE-resumption cache failed: Error::{e}");
        }
        pending::<()>().await
    };

    let requests = request_loop(&ex, &ctx, rx);

    // `select4` and not five arms: the two warn-and-continue futures both end in
    // `pending()`, so their inner `select` never resolves and they cannot stop
    // the stack on their own.
    futures_lite::future::block_on(ex.run(async {
        select4(
            transport,
            responder_fut,
            requests,
            select(mdns_fut, persist_fut),
        )
        .await;
    }));

    tracing::info!("Matter stack thread exiting");
}

/// Read requests until the channel closes or `Shutdown` arrives.
///
/// Everything except the three registry variants is spawned detached, so a 60s
/// commissioning attempt cannot stall a read behind it. The cost is that
/// concurrent requests complete in an arbitrary order — two writes to the same
/// attribute race, and the WS layer's ordering guarantees stop at the stack
/// boundary. That is inherent to not serialising, and the alternative (one
/// in-flight operation at a time) would make a single unreachable node freeze the
/// whole server for 30s.
///
/// # Dropping a supervisor task is not synchronous
///
/// The `Shutdown` and `StopSupervisor` arms below take tasks out of
/// `ctx.supervisors` in two steps. That is *defensive*, not a fix for a live bug,
/// and the distinction matters — do not "simplify" it back on the strength of
/// what `async_executor::Task` happens to do today:
///
/// - `Task::drop` does **not** run the future's drop glue. It marks the task
///   closed and, if it is neither scheduled nor running, schedules it once more
///   "so that its future gets dropped by the executor"
///   (`async-task-4.7.1/src/task.rs:183-215`). So
///   `supervisor::SubscriptionGuard::drop` — which borrows `ctx.subs` and
///   `ctx.liveness` — runs later, on the executor, never inside the `RefMut` we
///   are holding here. The re-entrant-borrow panic cannot fire today.
/// - After `Shutdown` breaks this loop, `select4` resolves and `ex.run` returns,
///   so those cancelled futures are dropped when `LocalExecutor` is dropped at
///   the end of `run_stack` (`async-executor-1.14.0/src/lib.rs:398-417` drains
///   the active list and the queue). Their guards still reach a live `StackCtx`:
///   each task holds its own `Rc` clone, and `ex` is declared *after* `ctx`
///   (`run_stack`) so it drops first regardless.
///
/// The two-step is kept because it costs one binding and stays correct if
/// `ctx.supervisors` ever holds a handle whose drop glue *is* synchronous — a
/// `JoinHandle`-like type, or async-task's own `Runnable`. Folding it into
/// `clear()`/`remove()` would make that swap a production-only panic.
async fn request_loop<'a, C>(
    ex: &async_executor::LocalExecutor<'a>,
    ctx: &Rc<StackCtx<C>>,
    mut rx: mpsc::UnboundedReceiver<StackRequest>,
) where
    C: Crypto + 'a,
{
    while let Some(req) = rx.recv().await {
        match req {
            // Inline, not spawned: the loop has to actually break, and `done`
            // has to be answered by the same task that stops reading.
            StackRequest::Shutdown { done } => {
                // Drain into a local, end the borrow, then let the vector drop —
                // the two-step documented above. Cancelling here only *marks*
                // these tasks; their futures (and so their `SubscriptionGuard`s)
                // are dropped by the executor as `run_stack` returns.
                let tasks: Vec<_> = ctx.supervisors.borrow_mut().drain().map(|(_, t)| t).collect();
                drop(tasks);
                let _ = done.send(());
                break;
            }
            StackRequest::StartSupervisor { node_id } => {
                let task = ex.spawn(watch_supervisor(ctx.clone(), node_id));
                // `insert` returns any displaced task, which is then dropped —
                // so the same two-step applies: bind it, let the `RefMut` die at
                // the `;`, drop after.
                let displaced = ctx.supervisors.borrow_mut().insert(node_id, task);
                if displaced.is_some() {
                    tracing::debug!("node {node_id}: replacing a running supervisor");
                }
                drop(displaced);
            }
            StackRequest::StopSupervisor { node_id } => {
                // Same two-step, same reason as in `Shutdown` above.
                let task = ctx.supervisors.borrow_mut().remove(&node_id);
                drop(task);

                // `subs` and `liveness` are the guard's to release (it runs when
                // the executor drops the cancelled future). The node-lifetime
                // caches are ours — see the cleanup contract on
                // `StackCtx::supervisors`.
                forget_node(
                    &mut ctx.last_event.borrow_mut(),
                    &mut ctx.addrs.borrow_mut(),
                    node_id,
                );
            }
            // Detached: nothing ever awaits these tasks. One consequence is worth
            // knowing when reading a bug report — a panic inside an op is caught
            // by async-executor (`propagate_panic(true)`) and stored as the task's
            // output, which for a detached task is simply dropped. The reply
            // sender goes with it, so the client is told "stack thread is down"
            // by `StackHandle::request` while this thread is in fact alive and
            // still serving. The panic itself is on stderr; the WS error is a red
            // herring.
            other => ex.spawn(handle_request(ctx.clone(), other)).detach(),
        }
    }
}

/// Run one node's supervisor and make its *exit* audible.
///
/// `supervisor::supervise` is an infinite loop: the only ways out are a panic or
/// someone adding a `return`. Neither is visible otherwise — a panic is caught by
/// async-executor (`propagate_panic(true)`) and stored as the task's output,
/// which a detached/parked task never reads, so the task simply sits *completed*
/// in `ctx.supervisors` and that node silently stops resubscribing forever. Both
/// exits get an `error!` naming the node instead.
///
/// Cancellation — the normal `stop_supervisor`/`Shutdown` path — drops the future
/// mid-await, so neither arm runs and nothing is logged. That is the point: only
/// an *unexpected* exit is worth a line.
async fn watch_supervisor<C: Crypto>(ctx: Rc<StackCtx<C>>, node_id: u64) {
    // `AssertUnwindSafe` because the future holds an `Rc<StackCtx>`, which is not
    // `UnwindSafe`. Sound here in the sense that matters: on the unwind path this
    // task is finished and the `Rc` is dropped, so no other code observes state
    // that a half-run supervisor left behind.
    match AssertUnwindSafe(supervisor::supervise(ctx, node_id))
        .catch_unwind()
        .await
    {
        Ok(()) => tracing::error!(
            "node {node_id}: supervisor returned unexpectedly; the node will not resubscribe \
             until its supervisor is restarted"
        ),
        Err(_) => tracing::error!(
            "node {node_id}: supervisor panicked; the node will not resubscribe until its \
             supervisor is restarted"
        ),
    }
}

/// Drop the per-node caches that outlive any single subscription.
///
/// `subs` and `liveness` are deliberately absent: they belong to the
/// subscription, and `supervisor::SubscriptionGuard` releases them when the
/// supervisor task is dropped. Clearing them here would be at best redundant and
/// at worst a race with a supervisor that is being replaced.
///
/// Takes the maps rather than a `StackCtx` so it is testable — a `StackCtx` needs
/// a `&'static Matter` — the same shape as `ctx::note_event` and
/// `supervisor::disown`.
fn forget_node(
    last_event: &mut HashMap<u64, u64>,
    addrs: &mut HashMap<u64, Vec<String>>,
    node_id: u64,
) {
    // Or a node re-commissioned under the same id inherits the old high-water
    // mark and silently drops its first events.
    last_event.remove(&node_id);
    // Or `node_addresses` keeps answering for a node that is gone.
    addrs.remove(&node_id);
}

/// Run one request and answer its `reply`.
///
/// Every arm ignores the send result: a caller that gave up is normal.
async fn handle_request<C: Crypto>(ctx: Rc<StackCtx<C>>, req: StackRequest) {
    match req {
        StackRequest::Commission { req, reply } => {
            let _ = reply.send(ops::commission::commission(&ctx, req).await);
        }
        StackRequest::Read { node_id, paths, fabric_filtered, reply } => {
            let r = ops::interact::read_attributes(&ctx, node_id, &paths, fabric_filtered).await;
            let _ = reply.send(r);
        }
        StackRequest::Write { node_id, endpoint, cluster, attribute, value, reply } => {
            let r =
                ops::interact::write_attribute(&ctx, node_id, endpoint, cluster, attribute, &value)
                    .await;
            let _ = reply.send(r);
        }
        StackRequest::Invoke {
            node_id,
            endpoint,
            cluster,
            command_name,
            payload,
            timed_ms,
            reply,
        } => {
            let r = ops::interact::invoke(
                &ctx,
                node_id,
                endpoint,
                cluster,
                &command_name,
                &payload,
                timed_ms,
            )
            .await;
            let _ = reply.send(r);
        }
        StackRequest::Interview { node_id, reply } => {
            let _ = reply.send(ops::interact::interview(&ctx, node_id).await);
        }
        StackRequest::OpenWindow { node_id, timeout_secs, reply } => {
            let _ = reply.send(ops::window::open_window(&ctx, node_id, timeout_secs).await);
        }
        StackRequest::DeviceFabrics { node_id, reply } => {
            let _ = reply.send(ops::fabrics::device_fabrics(&ctx, node_id).await);
        }
        StackRequest::RemoveDeviceFabric { node_id, fabric_index, reply } => {
            let r = ops::fabrics::remove_device_fabric(&ctx, node_id, fabric_index).await;
            let _ = reply.send(r);
        }
        StackRequest::UpdateFabricLabel { label, reply } => {
            let _ = reply.send(ops::fabrics::update_fabric_label(&ctx, &label).await);
        }
        StackRequest::NodeAddresses { node_id, reply } => {
            // The address cache only. Merging the peer addresses of the node's
            // live CASE sessions is not possible at this rev: `MatterState`
            // exposes `fabrics` but keeps `sessions` private
            // (`rs-matter-ref/rs-matter/src/lib.rs:771`), so there is no way to
            // reach `Session::get_peer_addr` from outside the crate. Not a real
            // gap — the controller merges the addresses cached on the node record
            // into whatever this returns.
            let addrs = ctx.addrs.borrow().get(&node_id).cloned().unwrap_or_default();
            let _ = reply.send(Ok(addrs));
        }
        StackRequest::Browse { timeout_ms, reply } => {
            let _ = reply.send(ops::discovery::browse(&ctx, timeout_ms).await);
        }
        // Handled inline by `request_loop`; routed here only if someone adds a
        // second dispatch site. Logged rather than `unreachable!`, because a
        // panic on the stack thread takes the whole stack down.
        StackRequest::Shutdown { .. }
        | StackRequest::StartSupervisor { .. }
        | StackRequest::StopSupervisor { .. } => {
            tracing::error!("a control request reached handle_request; it belongs in request_loop");
        }
    }
}

/// Dual-stack UDP socket on an ephemeral port.
///
/// Ported verbatim from `spike/src/main.rs:420-435` (itself from rs-matter's
/// `tests/commissioning.rs`): IPv6 socket with `only_v6` off, so one socket
/// serves both families, on a kernel-picked port to match `Matter::init`'s
/// `port = 0`.
fn create_dual_stack_socket() -> Result<async_io::Async<UdpSocket>, Error> {
    let socket = Socket::new(Domain::IPV6, Type::DGRAM, Some(Protocol::UDP))
        .map_err(|_| ErrorCode::NoNetworkInterface)?;
    socket
        .set_reuse_address(true)
        .map_err(|_| ErrorCode::NoNetworkInterface)?;
    socket
        .set_only_v6(false)
        .map_err(|_| ErrorCode::NoNetworkInterface)?;
    let bind_addr = std::net::SocketAddrV6::new(std::net::Ipv6Addr::UNSPECIFIED, 0, 0, 0);
    socket
        .bind(&bind_addr.into())
        .map_err(|_| ErrorCode::NoNetworkInterface)?;
    let socket: UdpSocket = socket.into();
    async_io::Async::new_nonblocking(socket).map_err(|_| ErrorCode::NoNetworkInterface.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stopping_a_supervisor_drops_the_node_lifetime_caches() {
        let mut last_event = HashMap::from([(1u64, 42u64), (2, 7)]);
        let mut addrs = HashMap::from([
            (1u64, vec!["192.168.1.10".to_string()]),
            (2, vec!["192.168.1.11".to_string()]),
        ]);

        forget_node(&mut last_event, &mut addrs, 1);

        // Or the same node id, re-commissioned, would inherit node 1's event
        // high-water mark and drop everything below 42.
        assert_eq!(last_event.get(&1), None);
        // Or `node_addresses` would keep answering for a removed node.
        assert_eq!(addrs.get(&1), None);
        // Strictly per-node: the other supervisor is untouched.
        assert_eq!(last_event.get(&2), Some(&7));
        assert_eq!(addrs.get(&2).map(Vec::len), Some(1));
    }

    #[test]
    fn forgetting_an_unknown_node_is_a_no_op() {
        let mut last_event = HashMap::new();
        let mut addrs = HashMap::new();
        forget_node(&mut last_event, &mut addrs, 99);
        assert!(last_event.is_empty());
        assert!(addrs.is_empty());
    }

    /// `subs` and `liveness` are the guard's, not ours: `forget_node` does not
    /// take them at all, which is what this pins. If someone "completes" the
    /// cleanup by adding them here, they will have to change this signature and
    /// read the comment explaining why the guard owns them.
    #[test]
    fn the_subscription_lifetime_maps_are_not_touched_here() {
        fn assert_two_maps(
            _f: fn(&mut HashMap<u64, u64>, &mut HashMap<u64, Vec<String>>, u64),
        ) {
        }
        assert_two_maps(forget_node);
    }

    #[test]
    fn the_dual_stack_socket_binds_an_ephemeral_dual_stack_port() {
        let socket = create_dual_stack_socket().expect("binding :: on port 0 must work");
        let addr = socket.get_ref().local_addr().expect("a bound socket has an address");
        assert!(addr.is_ipv6(), "the socket must be the v6 one that also serves v4");
        assert_ne!(addr.port(), 0, "port 0 must have been resolved by the kernel");
    }
}

//! One task per commissioned node: establish a single wildcard subscription
//! (attributes *and* events, like matter.js), feed the priming report to the
//! controller, then watch liveness and resubscribe with backoff.
//!
//! The division of labour with [`crate::reports`] is: this task owns
//! establishment, the liveness timeout and the `Connected`/`Reconnecting`
//! signals; `ReportSink` owns everything that arrives *after* establishment.
//! They meet at `ctx.subs`, and the handshake around that map is the delicate
//! part — see [`establish`].
//!
//! The task runs until dropped. The runtime keeps the handle in
//! `ctx.supervisors` (`crate::runtime::request_loop`), so dropping it is how
//! `stop_supervisor` cancels the loop — which means cancellation happens at an
//! arbitrary await, almost always the watchdog's `Timer`. Everything a live
//! subscription owns in `ctx` therefore has to be released from a `Drop`, not
//! from the code path after the await: [`SubscriptionGuard`].

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;

use embassy_time::{Duration, Instant, Timer};
use matter_rs_controller::stack_api::{NodeConnState, StackError, StackEvent};
use rs_matter::crypto::Crypto;
use rs_matter::im::client::{ImClient as _, SubscribeEstablished, SubscribeOutcome, TxOutcome};
use rs_matter::im::{AttrPath, EventPath, GenericPath};
use rs_matter::transport::exchange::Exchange;

use crate::ctx::{
    map_err, map_err_established, with_timeout_mapped, StackCtx, INTERVIEW_TIMEOUT_SECS,
};
use crate::reports::{walk_events, AttrAccumulator};

/// Report no *more* often than this. 0 = "as soon as something changes", which
/// is what a home-automation client wants; rate limiting is the device's job via
/// its own minimum.
const MIN_INTERVAL_FLOOR_SECS: u16 = 0;

/// Report at least this often even when nothing changes. Doubles as the liveness
/// heartbeat: the device MUST send something within the `max_int` it grants (and
/// it may grant less than this ceiling), which is what makes silence detectable.
const MAX_INTERVAL_CEIL_SECS: u16 = 60;

/// Grace on top of the granted `max_int` before declaring a subscription dead.
/// Absorbs one lost heartbeat plus MRP retransmission time, so a single dropped
/// UDP packet does not tear down and rebuild a working subscription.
const LIVENESS_SLACK_SECS: u64 = 15;

/// How often the watchdog looks at the last-report timestamp. Cheap (a map
/// lookup) and unrelated to the deadline, so it only bounds how late a silent
/// subscription is noticed.
const LIVENESS_POLL_SECS: u64 = 5;

/// Reconnect delays, in order, saturating at the last entry. Starts short
/// because the common cause is a device that rebooted and is seconds away from
/// re-announcing, and ends at a minute so a permanently absent node costs one
/// mDNS resolve per minute.
const BACKOFF_SCHEDULE_SECS: [u64; 5] = [2, 5, 10, 30, 60];

pub(crate) async fn supervise<C: Crypto>(ctx: Rc<StackCtx<C>>, node_id: u64) {
    let mut backoff_idx = 0usize;

    loop {
        match establish(&ctx, node_id).await {
            Ok(established) => {
                // Reset only on success: a subscription that establishes and
                // then goes silent immediately retries at delay 0, because the
                // device was demonstrably reachable a moment ago.
                backoff_idx = 0;

                // From here on the entries `establish` wrote are owned by a guard,
                // so they are released on *every* exit — including the task being
                // dropped mid-`Timer` by `stop_supervisor`. `establish` cannot
                // suspend between its insert and its return, so no cancellation
                // can slip in before this line either.
                let owned = SubscriptionGuard::new(&ctx, node_id, established.subscription_id);

                watch_liveness(&ctx, node_id, established.max_int).await;

                // Explicit, because releasing before announcing the loss is the
                // point: any straggling report is then answered
                // `InvalidSubscription` and the device tears its half down instead
                // of feeding a subscription we have given up on.
                drop(owned);
                tracing::warn!(
                    "node {node_id}: subscription {} went silent, resubscribing",
                    established.subscription_id
                );
                let _ = ctx.events.send(StackEvent::NodeState {
                    node_id,
                    state: NodeConnState::Reconnecting,
                });
            }
            Err(e) => {
                tracing::debug!("node {node_id}: subscribe attempt failed: {}", e.message);
                let _ = ctx.events.send(StackEvent::NodeState {
                    node_id,
                    state: NodeConnState::Reconnecting,
                });
                Timer::after(backoff_delay(backoff_idx)).await;
                // Saturating rather than wrapping: the index is only ever used
                // clamped, but letting it count up forever is a needless
                // overflow waiting for a long-absent node.
                backoff_idx = backoff_idx.saturating_add(1);
            }
        }
    }
}

/// CASE + wildcard subscribe, then announce the node and publish its snapshot.
///
/// Four ordering rules, all of them load-bearing:
///
/// 1. `ctx.subs` must name this subscription *before* the device's first
///    post-priming report can arrive, because `ReportSink` answers
///    `InvalidSubscription` for anything it does not recognise — deliberately, as
///    that is how a device is told to drop a subscription left over from before a
///    controller restart. Nothing awaits between `Established` and the insert, so
///    on the single-threaded stack no report can be handled in between. There is a
///    residual window this rev cannot close — see the note in [`crate::reports`] —
///    but it is sub-millisecond and self-healing.
/// 2. The event high-water mark is cleared on *every* establishment. A device
///    that does not persist its event counter restarts numbering at 0, and
///    without the reset every subsequent event compares as a replay and is
///    dropped forever (see [`StackCtx::reset_event_high_water`]).
/// 3. The priming report's own events seed that mark rather than being
///    forwarded: they are the device's event log, which the client has either
///    already seen or does not want replayed as news.
/// 4. `NodeState::Connected` is emitted *before* `PrimingSnapshot`, so the
///    sequence reads "this node is connected, and here is its state" rather than
///    the reverse. Both are sent from here for that reason — sending `Connected`
///    from the caller would necessarily put it second.
async fn establish<C: Crypto>(
    ctx: &StackCtx<C>,
    node_id: u64,
) -> Result<SubscribeEstablished, StackError> {
    // Before the subscribe, so the priming events below re-seed a clean mark.
    ctx.reset_event_high_water(node_id);
    // A resubscribe orphans any report the old subscription left half-sent.
    ctx.pending_reports.borrow_mut().forget(node_id);

    // All-`None` paths: every attribute and every event on every endpoint, which
    // is the single subscription matter.js maintains per node.
    let attr_paths = [AttrPath::from_gp(&GenericPath::new(None, None, None))];
    let event_paths = [EventPath::from_gp(&GenericPath::new(None, None, None))];

    // A wildcard priming read is interview-sized — a bridge chunks for a long
    // time — so it gets the interview budget rather than the per-transaction one.
    // Phase split for the error mapping: once `initiate` has returned Ok, a
    // `NotFound` is the device's IM status, not an mDNS resolution failure.
    let case_up = core::cell::Cell::new(false);
    with_timeout_mapped(INTERVIEW_TIMEOUT_SECS, async {
        let exchange = Exchange::initiate(ctx.matter, &ctx.crypto, ctx.fab_idx, node_id).await?;
        case_up.set(true);
        let mut sender = exchange.subscribe_sender().await?;

        let mut chunk = loop {
            match sender.tx().await? {
                TxOutcome::BuildRequest(builder) => {
                    sender = builder
                        // `false`: terminate whatever this device still thinks it
                        // owes us. We keep exactly one subscription per node, and
                        // any other one is a leftover we would never read.
                        .keep_subs(false)?
                        .min_int_floor(MIN_INTERVAL_FLOOR_SECS)?
                        .max_int_ceil(MAX_INTERVAL_CEIL_SECS)?
                        .attr_requests_from(&attr_paths)?
                        .event_requests_from(&event_paths)?
                        // Unfiltered: the controller's cache mirrors the whole
                        // node, including other fabrics' entries in fabric-scoped
                        // lists.
                        .fabric_filtered(false)?
                        .end()?;
                }
                TxOutcome::GotResponse(c) => break c,
            }
        };

        let who = format!("node {node_id} priming");
        // One accumulator across every priming chunk: that is exactly the span a
        // chunked list attribute is split over, and merging it is why this shares
        // `read_attributes`' walk instead of having its own.
        let mut acc = AttrAccumulator::default();
        let established = loop {
            {
                let resp = chunk.response()?;
                acc.absorb(&resp, &who);
                // Rule 3: seed the mark, do not forward. Shares `ReportSink`'s
                // event triage — unparseable entries logged and skipped, per-path
                // statuses expected — because getting that wrong in one of two
                // copies is exactly how the two paths drift.
                walk_events(&resp, &who, |data| {
                    ctx.note_event(node_id, data.event_number);
                });
            }
            match chunk.complete().await? {
                SubscribeOutcome::NextChunk(next) => chunk = next,
                SubscribeOutcome::Established(est) => break est,
            }
        };

        // Rule 1. No `.await` from here to the `Ok`.
        ctx.subs.borrow_mut().insert(node_id, established.subscription_id);
        ctx.liveness.borrow_mut().insert(node_id, Instant::now());

        if acc.failures() > 0 {
            tracing::warn!("{who}: {} attribute report(s) skipped", acc.failures());
        }
        let attributes: BTreeMap<String, serde_json::Value> = acc.into_pairs().into_iter().collect();
        tracing::info!(
            "node {node_id}: subscription {} established, max_int {}s, {} attribute(s)",
            established.subscription_id,
            established.max_int,
            attributes.len()
        );
        // Rule 4: available first, then its state.
        let _ = ctx.events.send(StackEvent::NodeState {
            node_id,
            state: NodeConnState::Connected { max_interval_secs: established.max_int },
        });
        let _ = ctx.events.send(StackEvent::PrimingSnapshot { node_id, attributes });

        Ok(established)
    }, |e| if case_up.get() { map_err_established(e) } else { map_err(e) })
    .await
}

/// Owns the per-node subscription state that [`establish`] wrote, and releases it
/// on `Drop`.
///
/// A guard rather than a call after the await, because the supervisor's documented
/// stop mechanism is *dropping the task* (`stop_supervisor`, wired at
/// `crates/controller/src/commands/nodes.rs:54` for `remove_node`), and the task
/// spends essentially its whole steady-state life parked in the watchdog's
/// `Timer`. Cancelled there without a guard, `ctx.subs[node_id]` survives — so
/// `ReportSink` keeps *matching* that device's reports and answering `Ok`, the
/// device is never told `InvalidSubscription`, and it holds a subscription plus a
/// CASE session against us forever, burning one of the three subscription slots a
/// typical device has. That is the exact inverse of the "one entry per node, and
/// reports for a superseded id are answered `InvalidSubscription`" invariant
/// `ctx.rs` states.
///
/// It covers `subs` and `liveness` — the two maps whose lifetime is the
/// *subscription's*. `last_event` and `addrs` outlive any single subscription
/// (they are per-node caches), so clearing them belongs to the `StopSupervisor`
/// arm of `crate::runtime::request_loop`, which does it in `runtime::forget_node`
/// alongside removing the `ctx.supervisors` entry.
///
/// `pending_reports` is also subscription-lifetime, like `subs`/`liveness` —
/// but it is deliberately NOT guard-owned: a half-received report must be
/// dropped as soon as a *new* subscription replaces the old one, which is
/// earlier than this guard's `Drop` (that only runs once the task itself
/// ends). So it is released by [`establish`] on every (re)subscribe and, on
/// node removal, by `runtime::forget_node` — the same two places that already
/// clear `last_event`/`addrs`, keeping this doc's contract and `ctx.rs`'s
/// cleanup contract in agreement.
struct SubscriptionGuard<'a> {
    subs: &'a RefCell<HashMap<u64, u32>>,
    liveness: &'a RefCell<HashMap<u64, Instant>>,
    node_id: u64,
    sub_id: u32,
}

impl<'a> SubscriptionGuard<'a> {
    fn new<C: Crypto>(ctx: &'a StackCtx<C>, node_id: u64, sub_id: u32) -> Self {
        Self { subs: &ctx.subs, liveness: &ctx.liveness, node_id, sub_id }
    }
}

impl Drop for SubscriptionGuard<'_> {
    fn drop(&mut self) {
        disown(self.subs, self.node_id, self.sub_id);
        self.liveness.borrow_mut().remove(&self.node_id);
    }
}

/// Block until the device stops reporting within its promised interval.
async fn watch_liveness<C: Crypto>(ctx: &StackCtx<C>, node_id: u64, max_int: u16) {
    let deadline = liveness_deadline(max_int);
    let established_at = Instant::now();

    loop {
        Timer::after(Duration::from_secs(LIVENESS_POLL_SECS)).await;
        let last = ctx.liveness.borrow().get(&node_id).copied();
        if liveness_expired(last, established_at, Instant::now(), deadline) {
            return;
        }
    }
}

/// Whether the subscription counts as silent.
///
/// `established_at` stands in for a missing entry rather than the watch breaking
/// out immediately: the entry is written at establishment and refreshed by every
/// report, so its absence means something else removed it, and treating that as
/// "silent since establishment" degrades into a resubscribe instead of a spin.
fn liveness_expired(
    last: Option<Instant>,
    established_at: Instant,
    now: Instant,
    deadline: Duration,
) -> bool {
    let last = last.unwrap_or(established_at);
    // Saturating: an entry stamped in the future (a clock the report path and this
    // one disagree on) must read as "just heard from", never as an underflow.
    now.saturating_duration_since(last) >= deadline
}

/// Give up ownership of `sub_id` for `node_id`.
///
/// Compare-then-remove because a blind `remove` would delete an entry that some
/// other establishment had already replaced. The runtime keeps one supervisor
/// per node, so this cannot happen today; it costs one comparison to keep it
/// from becoming a silent cross-wiring if that ever changes.
///
/// Takes the map rather than the whole `StackCtx` so it is testable: a `StackCtx`
/// needs a `&'static Matter`, and this comparison is the only non-obvious logic in
/// the file. Same shape, and for the same reason, as `ctx::note_event`.
fn disown(subs: &RefCell<HashMap<u64, u32>>, node_id: u64, sub_id: u32) {
    let mut subs = subs.borrow_mut();
    if subs.get(&node_id) == Some(&sub_id) {
        subs.remove(&node_id);
    }
}

/// How long a device may stay silent before its subscription is declared dead:
/// the interval it committed to, plus slack for one lost heartbeat.
fn liveness_deadline(max_int: u16) -> Duration {
    Duration::from_secs(u64::from(max_int) + LIVENESS_SLACK_SECS)
}

/// Delay before reconnect attempt number `idx` (0-based), clamped to the last
/// step of the schedule.
fn backoff_delay(idx: usize) -> Duration {
    let secs = BACKOFF_SCHEDULE_SECS[idx.min(BACKOFF_SCHEDULE_SECS.len() - 1)];
    Duration::from_secs(secs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_backoff_schedule_is_walked_then_held() {
        let secs: Vec<u64> = (0..8).map(|i| backoff_delay(i).as_secs()).collect();
        assert_eq!(secs, [2, 5, 10, 30, 60, 60, 60, 60]);
    }

    /// The index is only ever used clamped, so it must be safe at the extreme
    /// rather than merely unlikely to get there.
    #[test]
    fn a_saturated_backoff_index_still_indexes() {
        assert_eq!(backoff_delay(usize::MAX).as_secs(), 60);
    }

    #[test]
    fn the_liveness_deadline_is_the_granted_interval_plus_slack() {
        assert_eq!(liveness_deadline(60).as_secs(), 60 + LIVENESS_SLACK_SECS);
        // A device may grant far more than we asked for.
        assert_eq!(liveness_deadline(u16::MAX).as_secs(), u64::from(u16::MAX) + LIVENESS_SLACK_SECS);
        // ...or, in principle, 0 — in which case slack alone keeps the watchdog
        // from firing on the very first poll.
        assert_eq!(liveness_deadline(0).as_secs(), LIVENESS_SLACK_SECS);
        assert!(liveness_deadline(0).as_secs() > LIVENESS_POLL_SECS);
    }

    /// The subscribe request must ask for a heartbeat that the watchdog can
    /// actually observe: a ceiling above the deadline would make silence
    /// indistinguishable from a slow device.
    #[test]
    fn the_requested_ceiling_is_inside_the_deadline_it_implies() {
        assert!(liveness_deadline(MAX_INTERVAL_CEIL_SECS).as_secs() > u64::from(MAX_INTERVAL_CEIL_SECS));
        assert_eq!(MIN_INTERVAL_FLOOR_SECS, 0, "report as soon as anything changes");
    }

    // ------------------------------------------------------------- expiry

    fn at(secs: u64) -> Instant {
        Instant::from_secs(0) + Duration::from_secs(secs)
    }

    #[test]
    fn a_report_inside_the_deadline_keeps_the_subscription() {
        let deadline = liveness_deadline(60); // 75s
        let established = at(0);
        // Heard from at t=70, checked at t=100: 30s of silence, well inside 75.
        assert!(!liveness_expired(Some(at(70)), established, at(100), deadline));
        // Exactly at the boundary counts as expired, so the watchdog cannot sit
        // one tick short of firing forever.
        assert!(liveness_expired(Some(at(25)), established, at(100), deadline));
        assert!(liveness_expired(Some(at(24)), established, at(100), deadline));
        assert!(!liveness_expired(Some(at(26)), established, at(100), deadline));
    }

    /// The plan's version broke out of the watch the instant `ctx.liveness` had no
    /// entry. Measuring from establishment instead means a removed entry produces
    /// one resubscribe rather than an immediate spin.
    #[test]
    fn a_missing_entry_is_measured_from_establishment_not_treated_as_dead() {
        let deadline = liveness_deadline(60);
        let established = at(1_000);
        assert!(!liveness_expired(None, established, at(1_010), deadline));
        assert!(liveness_expired(None, established, at(1_100), deadline));
    }

    /// An entry stamped ahead of `now` must read as "just heard from"; the
    /// underflow it would otherwise cause is a panic in `duration_since`.
    #[test]
    fn a_timestamp_in_the_future_is_not_an_underflow() {
        let deadline = liveness_deadline(60);
        assert!(!liveness_expired(Some(at(500)), at(0), at(100), deadline));
    }

    // ------------------------------------------------------------- disown

    #[test]
    fn disown_releases_the_entry_it_owns() {
        let subs = RefCell::new(HashMap::from([(5u64, 7u32), (6, 8)]));
        disown(&subs, 5, 7);
        assert_eq!(*subs.borrow(), HashMap::from([(6u64, 8u32)]), "only node 5 released");
    }

    /// The compare half. A newer establishment may already have replaced the
    /// entry; deleting it would leave that live subscription untracked, and
    /// `ReportSink` would then disown every one of its reports.
    #[test]
    fn disown_leaves_an_entry_a_newer_subscription_already_replaced() {
        let subs = RefCell::new(HashMap::from([(5u64, 9u32)]));
        disown(&subs, 5, 7); // stale id
        assert_eq!(subs.borrow().get(&5), Some(&9));
    }

    #[test]
    fn disowning_twice_is_harmless() {
        let subs = RefCell::new(HashMap::from([(5u64, 7u32)]));
        disown(&subs, 5, 7);
        disown(&subs, 5, 7);
        assert!(subs.borrow().is_empty());
    }

    /// What the guard's `Drop` does, which is the whole point of item 1: a
    /// supervisor cancelled while parked in the watchdog must leave neither map
    /// naming the subscription, or the device keeps it alive forever.
    #[test]
    fn dropping_the_guard_releases_both_maps() {
        let subs = RefCell::new(HashMap::from([(5u64, 7u32)]));
        let liveness = RefCell::new(HashMap::from([(5u64, at(3))]));
        {
            let _owned = SubscriptionGuard { subs: &subs, liveness: &liveness, node_id: 5, sub_id: 7 };
            assert_eq!(subs.borrow().get(&5), Some(&7), "held while the guard lives");
        }
        assert!(subs.borrow().is_empty());
        assert!(liveness.borrow().is_empty());
    }

    /// Other nodes' supervisors must be untouched by one being cancelled.
    #[test]
    fn dropping_a_guard_leaves_other_nodes_alone() {
        let subs = RefCell::new(HashMap::from([(5u64, 7u32), (6, 8)]));
        let liveness = RefCell::new(HashMap::from([(5u64, at(3)), (6, at(4))]));
        drop(SubscriptionGuard { subs: &subs, liveness: &liveness, node_id: 5, sub_id: 7 });
        assert_eq!(*subs.borrow(), HashMap::from([(6u64, 8u32)]));
        assert_eq!(liveness.borrow().len(), 1);
        assert!(liveness.borrow().contains_key(&6));
    }
}

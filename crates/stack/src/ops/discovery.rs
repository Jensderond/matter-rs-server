//! Commissionable-device discovery (`discover` / `discover_commissionable_nodes`),
//! plus the bounded single-browse primitive the commissioning flow shares.
//!
//! rs-matter's `Transport::browse_commissionable` is deliberately
//! single-result: it parks one browse request in a rendezvous slot and returns
//! the *first* advertisement matching the filter and not in `exclude`
//! (`rs-matter-ref/rs-matter/src/transport.rs:494`). Enumerating a whole network
//! therefore means calling it repeatedly, feeding back every instance already
//! seen, until it reports `NotFound`.

use core::pin::pin;

use embassy_futures::select::{select, Either};
use embassy_time::{Duration, Instant, Timer};
use matter_rs_controller::stack_api::{DiscoveredDevice, StackError, StackErrorKind};
use rs_matter::crypto::Crypto;
use rs_matter::transport::network::mdns::CommissionableFilter;
use rs_matter::transport::network::Address;

use crate::ctx::{map_err, StackCtx};
use crate::ops::addr_to_string;

/// Hard cap on one sweep, dictated by rs-matter rather than chosen: the
/// exclude list is a `heapless::Vec` of `MAX_BROWSE_EXCLUDE = 6`
/// (`rs-matter-ref/rs-matter/src/transport/network/mdns.rs:745`, `pub(crate)` so
/// it cannot be referenced), and a 7th id makes `browse_commissionable` fail
/// with `ResourceExhausted` instead of browsing.
const MAX_SWEEP: usize = 6;

// TODO(task16): remove the allow — `StackHandle::browse_commissionable` is the
// caller.
#[allow(dead_code)]
pub(crate) async fn browse<C: Crypto>(
    ctx: &StackCtx<C>,
    timeout_ms: u32,
) -> Result<Vec<DiscoveredDevice>, StackError> {
    // Only devices actually in commissioning mode (`_CM` subtype); every other
    // field wildcard, since this is an enumeration and not a search.
    let filter = CommissionableFilter { commissioning_mode_only: true, ..Default::default() };

    // `timeout_ms` bounds the whole sweep, not each browse in it. Deriving every
    // per-call budget from what is left of this deadline is what enforces that:
    // `browse_one` is hard-bounded by the value it is given, so N iterations can
    // never together outlast the deadline. Without it, `timeout_ms` applied
    // per-iteration and a 3s request could take 21s over six slow devices.
    let deadline = deadline_after(Instant::now(), Duration::from_millis(u64::from(timeout_ms)));

    let mut sweep = Sweep::default();
    loop {
        if sweep.is_full() {
            // Cannot be raised: `MAX_BROWSE_EXCLUDE` is rs-matter's limit. Say so,
            // because someone setting up seven devices otherwise sees six with no
            // hint that the list was cut.
            tracing::info!(
                "commissionable browse stopped at {MAX_SWEEP} devices (rs-matter's exclude-list \
                 limit); any further devices are not reported"
            );
            break;
        }

        let remaining = remaining_budget(deadline, Instant::now());
        if remaining.as_ticks() == 0 {
            tracing::debug!(
                "commissionable browse spent its {timeout_ms}ms budget after {} device(s)",
                sweep.len()
            );
            break;
        }

        match browse_one(ctx, &filter, sweep.exclude(), remaining).await {
            Ok((addr, instance)) => sweep.push(&addr_to_string(&addr), instance),
            Err(e) => {
                // Degrade rather than fail: the devices already found are useful,
                // and the common terminator is the browse simply timing out with
                // nothing new to report.
                report_sweep_end(&e, sweep.len());
                break;
            }
        }
    }

    Ok(sweep.into_devices())
}

/// `browse_commissionable` under an outer wall-clock bound.
///
/// The bound is not redundant with the primitive's own `timeout_ms`: acquiring
/// the single browse rendezvous slot is a bare, *untimed* `wait(..).await`
/// (`rs-matter-ref/rs-matter/src/transport.rs:505-518`) and `timeout_ms` only
/// arms once the slot is held. A `commission` sitting in its 30s browse can
/// therefore park a concurrent `discover` for 30s before that caller's own budget
/// even starts — which would make this the one transport operation in the crate
/// with no upper bound at all (`ctx.rs`, `with_timeout`).
pub(crate) async fn browse_one<C: Crypto>(
    ctx: &StackCtx<C>,
    filter: &CommissionableFilter,
    exclude: &[u64],
    budget: Duration,
) -> Result<(Address, u64), StackError> {
    let ms = u32::try_from(budget.as_millis()).unwrap_or(u32::MAX);
    let fut = pin!(ctx.matter.transport().browse_commissionable(filter, exclude, ms));
    let timer = pin!(Timer::after(budget));

    match select(fut, timer).await {
        Either::First(r) => r.map_err(map_err),
        // Only reachable while another browse holds the slot, since the inner
        // call would otherwise have returned `NotFound` at the same instant.
        Either::Second(()) => Err(StackError::new(
            StackErrorKind::Timeout,
            format!(
                "mDNS browse timed out after {ms}ms without getting a turn (another discovery or \
                 commissioning was holding the browse slot)"
            ),
        )),
    }
}

/// Log the error that ended a sweep.
///
/// The empty case is the one that needs saying: the caller is about to be handed
/// "no commissionable devices found", and this line is the only way to tell that
/// apart from discovery being broken — no mDNS responder running,
/// `ResourceExhausted`, an interface fault. `NotFound` is *also* what a genuinely
/// empty network produces (it is the browse timeout), so it cannot be classified
/// further from here and gets `info` rather than `warn`; every other code means
/// something is actually wrong and gets `warn`.
fn report_sweep_end(e: &StackError, found: usize) {
    if found > 0 {
        tracing::debug!("commissionable browse ended after {found} device(s): {}", e.message);
    } else if e.kind == StackErrorKind::NodeUnreachable {
        // What `map_err` makes of `ErrorCode::NotFound`.
        tracing::info!("commissionable browse found nothing: {}", e.message);
    } else {
        tracing::warn!(
            "commissionable browse failed and is reporting an empty list: {}",
            e.message
        );
    }
}

/// `now + total`, saturating. Cannot overflow at any `u32` millisecond budget
/// (~49 days); saturating rather than unwrapping so it stays a total function.
fn deadline_after(now: Instant, total: Duration) -> Instant {
    now.checked_add(total).unwrap_or(Instant::MAX)
}

/// What is left of the sweep budget, `0` once the deadline has passed.
fn remaining_budget(deadline: Instant, now: Instant) -> Duration {
    deadline.saturating_duration_since(now)
}

/// The accumulation half of [`browse`], separated from the await so the
/// exclude-list feedback and the cap are testable without a network.
#[derive(Debug, Default)]
struct Sweep {
    out: Vec<DiscoveredDevice>,
    exclude: Vec<u64>,
}

impl Sweep {
    fn push(&mut self, address: &str, instance: u64) {
        // Upper-case hex, 16 digits: Node's `instance_name` shape.
        self.out.push(DiscoveredDevice {
            instance_name: format!("{instance:016X}"),
            address: address.to_string(),
        });
        self.exclude.push(instance);
    }

    fn exclude(&self) -> &[u64] {
        &self.exclude
    }

    fn is_full(&self) -> bool {
        self.exclude.len() >= MAX_SWEEP
    }

    fn len(&self) -> usize {
        self.out.len()
    }

    fn into_devices(self) -> Vec<DiscoveredDevice> {
        self.out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rs_matter::error::ErrorCode;

    /// Every found instance must be fed back as an exclusion, otherwise the very
    /// next browse returns the same device and the sweep never advances.
    #[test]
    fn each_result_is_excluded_from_the_next_query() {
        let mut s = Sweep::default();
        assert!(s.exclude().is_empty(), "the first query excludes nothing");
        s.push("192.168.1.50:5540", 0x1122_3344_5566_7788);
        assert_eq!(s.exclude(), [0x1122_3344_5566_7788]);
        s.push("192.168.1.51:5540", 1);
        assert_eq!(s.exclude(), [0x1122_3344_5566_7788, 1]);
    }

    #[test]
    fn instance_name_is_16_upper_hex_digits() {
        let mut s = Sweep::default();
        s.push("192.168.1.50:5540", 0xabcd);
        s.push("[fe80::1%2]:5540", u64::MAX);
        let names: Vec<String> = s.into_devices().into_iter().map(|d| d.instance_name).collect();
        assert_eq!(names, ["000000000000ABCD", "FFFFFFFFFFFFFFFF"]);
    }

    /// The cap is rs-matter's `MAX_BROWSE_EXCLUDE`, not a preference: a 7th
    /// exclusion turns the next browse into `ResourceExhausted`.
    #[test]
    fn the_sweep_stops_at_the_exclude_list_capacity() {
        let mut s = Sweep::default();
        for i in 0..MAX_SWEEP {
            assert!(!s.is_full(), "room for device {i}");
            s.push("192.168.1.50:5540", i as u64);
        }
        assert!(s.is_full());
        assert_eq!(s.len(), MAX_SWEEP);
    }

    #[test]
    fn an_empty_network_yields_an_empty_list_not_an_error() {
        let s = Sweep::default();
        assert_eq!(s.len(), 0);
        assert!(s.into_devices().is_empty());
    }

    /// The whole point of the budget arithmetic: successive browses share one
    /// deadline, so the sweep cannot outlast the caller's request.
    #[test]
    fn per_call_budgets_are_carved_out_of_one_deadline() {
        let start = Instant::from_ticks(0);
        let deadline = deadline_after(start, Duration::from_millis(3_000));

        // Nothing spent yet: the first browse may use the whole budget.
        assert_eq!(remaining_budget(deadline, start).as_millis(), 3_000);
        // Two slow devices later, the third browse gets only what is left —
        // rather than a fresh 3s each, which is how a 3s request became 21s.
        let after_two = start + Duration::from_millis(2_500);
        assert_eq!(remaining_budget(deadline, after_two).as_millis(), 500);
    }

    /// Zero is the loop's termination signal, so an expired or exactly-met
    /// deadline must produce it and never wrap into a huge budget.
    #[test]
    fn an_expired_deadline_leaves_no_budget() {
        let start = Instant::from_ticks(0);
        let deadline = deadline_after(start, Duration::from_millis(1_000));
        assert_eq!(remaining_budget(deadline, deadline).as_ticks(), 0);
        assert_eq!(
            remaining_budget(deadline, start + Duration::from_millis(5_000)).as_ticks(),
            0
        );
        // A zero-millisecond request is legal and must not sweep at all.
        let none = deadline_after(start, Duration::from_millis(0));
        assert_eq!(remaining_budget(none, start).as_ticks(), 0);
    }

    #[test]
    fn the_deadline_saturates_instead_of_overflowing() {
        assert_eq!(deadline_after(Instant::MAX, Duration::from_millis(1)), Instant::MAX);
        // The largest budget the WS API can ask for is still ~49 days out.
        let far = deadline_after(Instant::from_ticks(0), Duration::from_millis(u64::from(u32::MAX)));
        assert!(far < Instant::MAX);
    }

    /// The failure that used to be discarded entirely: an empty sweep must always
    /// leave a trace, and a transport fault must be louder than an empty network.
    #[test]
    fn an_empty_sweep_always_reports_why() {
        // Not asserting on log output (no subscriber here); asserting on the
        // classification the levels are chosen from, which is the part that can
        // regress.
        let not_found = map_err(ErrorCode::NotFound.into());
        assert_eq!(not_found.kind, StackErrorKind::NodeUnreachable);
        let broken = map_err(ErrorCode::ResourceExhausted.into());
        assert_ne!(broken.kind, StackErrorKind::NodeUnreachable);

        // Exercised for the panic-freedom of every branch.
        report_sweep_end(&not_found, 0);
        report_sweep_end(&broken, 0);
        report_sweep_end(&not_found, 3);
    }
}

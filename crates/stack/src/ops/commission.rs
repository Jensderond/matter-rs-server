//! Commissioning a new node onto our fabric, RCAC-direct.
//!
//! Line-for-line the spike flow (`spike/src/main.rs:229-350`, which passed
//! against matter.js at this rs-matter rev), with the offline CA chain replaced
//! by the persisted identity: PASE + over-PASE configuration
//! (`Commissioner::commission`), then CASE + `CommissioningComplete`
//! (`complete_via_case`). Both phases carry an outer timeout, because a device
//! whose failsafe is still armed from a previous attempt answers everything
//! `Busy` for ~60s (spike finding 2) and rs-matter's MRP retries would otherwise
//! keep the request alive far past any caller's patience.

use core::pin::pin;

use embassy_futures::select::{select, Either};
use embassy_time::{Duration, Timer};
use matter_rs_controller::stack_api::{
    CommissionOutcome, CommissionRequest, PaseTarget, StackError, StackErrorKind,
};
use rs_matter::cert::gen::VALID_FOREVER;
use rs_matter::cert::{MAX_CERT_TLV_AND_ASN1_LEN, MAX_CERT_TLV_LEN};
use rs_matter::crypto::Crypto;
use rs_matter::onboard::noc::NocGenerator;
use rs_matter::onboard::{CommissionOptions, Commissioner};
use rs_matter::pairing::qr::QrPayload;
use rs_matter::transport::network::mdns::CommissionableFilter;
use rs_matter::transport::network::Address;

use crate::ctx::{map_err, StackCtx, COMMISSION_TIMEOUT_SECS};
use crate::ops::{addr_to_string, discovery, fabrics, ip_of};

/// How long to wait for a commissionable advertisement. Generous because a
/// device that has just been power-cycled may take seconds to start advertising,
/// and this is a user-initiated, foreground operation.
const BROWSE_TIMEOUT: Duration = Duration::from_secs(30);

/// Scratch for `QrPayload::parse`: the base38 body never decodes to more bytes
/// than the input string, and a `MT:` code with optional data stays well under
/// this (`rs-matter-ref/rs-matter/src/pairing/qr.rs:426`).
const QR_BUF_LEN: usize = 512;

pub(crate) async fn commission<C: Crypto>(
    ctx: &StackCtx<C>,
    req: CommissionRequest,
) -> Result<CommissionOutcome, StackError> {
    // 1. Resolve the passcode and the address to run PASE against.
    let mut qr_buf = [0u8; QR_BUF_LEN];
    let (passcode, addr) = resolve_target(ctx, &req.target, &mut qr_buf).await?;

    // 2. A NocGenerator over the persisted CA. RCAC-direct: the signer *is* the
    //    root key and the ICAC is empty, because matter.js rejects rs-matter's
    //    ICAC (spike finding 1).
    //
    //    `create_with_fabric_id`, not `create`: a fabric migrated from
    //    matter.js has an RCAC whose subject DN legally omits the FabricId
    //    RDN, which the plain constructor rejects — and the device NOCs it
    //    mints must mirror the root's actual subject shape in their issuer DN
    //    either way. The RCAC's own RDN still wins when present (exactly what
    //    `create` always read — the stored scalar is warn-only on mismatch,
    //    see `identity::install`); the scalar is only the fallback for the
    //    FabricId-less migrated shape, where it equals the fabric id the
    //    controller NOC's subject carries.
    let ca_key = crate::identity::canon_secret_key(&ctx.identity.ca_private_key).map_err(map_err)?;
    let noc_fabric_id = rs_matter::cert::CertRef::new(rs_matter::tlv::TLVElement::new(
        &ctx.identity.rcac_tlv,
    ))
    .get_fabric_id()
    .unwrap_or(ctx.identity.fabric_id);
    let mut noc_buf = [0u8; MAX_CERT_TLV_AND_ASN1_LEN];
    let mut noc_generator = NocGenerator::create_with_fabric_id(
        ca_key.reference(),
        &ctx.identity.rcac_tlv,
        &[],
        noc_fabric_id,
        &mut noc_buf,
    )
    .map_err(map_err)?;
    let mut commissioner_buf = [0u8; MAX_CERT_TLV_LEN];
    let mut commissioner = Commissioner::new(
        ctx.matter,
        &ctx.crypto,
        ctx.fab_idx,
        &mut noc_generator,
        &mut commissioner_buf,
    );
    let opts = CommissionOptions {
        // Mandatory: rs-matter has no PAA-chain verification path yet, and
        // `false` fails outright. Accepted v1 gap.
        allow_test_attestation: true,
        ..CommissionOptions::new()
    };

    // 3. Phase 1 — ArmFailSafe .. AddNOC over PASE.
    let phase1 = {
        let fut = pin!(commissioner.commission(addr, passcode, &opts, req.node_id, VALID_FOREVER));
        let timeout = pin!(Timer::after(Duration::from_secs(COMMISSION_TIMEOUT_SECS)));
        match select(fut, timeout).await {
            Either::First(r) => r.map_err(map_err)?,
            Either::Second(()) => {
                return Err(StackError::new(
                    StackErrorKind::Timeout,
                    format!(
                        "commissioning timed out after {COMMISSION_TIMEOUT_SECS}s (a previous \
                         failed attempt may hold the device's PASE session for ~60s)"
                    ),
                ))
            }
        }
    };

    // 4. Phase 2 — CASE against the freshly-installed operational identity, then
    //    CommissioningComplete.
    //
    //    The two 60s budgets add up to 120s against a failsafe the device armed
    //    for 60 (`CommissionOptions::new`, `onboard.rs:110`), which
    //    `complete_via_case` never re-arms — so a phase 1 that took 55s leaves
    //    phase 2 about 5 useful seconds no matter what its own timeout says. The
    //    two-phase 60s structure is kept deliberately (it is the spike's, and
    //    shortening phase 2 would only move the failure earlier), but the message
    //    has to name the real cause: the user's next action is a retry, which will
    //    hit `BUSY_MESSAGE` until the failsafe finishes rolling back.
    {
        let fut = pin!(commissioner.complete_via_case(addr, &phase1));
        let timeout = pin!(Timer::after(Duration::from_secs(COMMISSION_TIMEOUT_SECS)));
        match select(fut, timeout).await {
            Either::First(r) => r.map_err(map_err)?,
            Either::Second(()) => {
                return Err(StackError::new(
                    StackErrorKind::Timeout,
                    format!(
                        "CASE completion timed out after {COMMISSION_TIMEOUT_SECS}s; the device's \
                         60s failsafe has expired, so it has rolled back the partial commissioning \
                         and may report Busy for ~60s"
                    ),
                ))
            }
        }
    }

    // 5. Name our fabric on the new node, so other administrators see it. Purely
    //    cosmetic, and the node is already commissioned — never fail here.
    fabrics::push_fabric_label(ctx, req.node_id, &req.fabric_label).await;

    let address = addr_to_string(&addr);
    // The address PASE succeeded on is the best `node_addresses` answer we have
    // until the supervisor's first CASE session refreshes it.
    ctx.addrs.borrow_mut().insert(req.node_id, vec![ip_of(&address)]);

    Ok(CommissionOutcome { device_fabric_index: phase1.fabric_index.get(), address })
}

/// `(passcode, address)` for the PASE handshake.
///
/// `qr_buf` is the caller's scratch because a parsed `QrPayload` borrows from it.
async fn resolve_target<C: Crypto>(
    ctx: &StackCtx<C>,
    target: &PaseTarget,
    qr_buf: &mut [u8],
) -> Result<(u32, Address), StackError> {
    let (passcode, filter) = match target {
        PaseTarget::Code { code } => parse_pairing_code(code.trim(), qr_buf)?,
        PaseTarget::OnNetwork { passcode, long_discriminator, short_discriminator, vendor_id } => (
            *passcode,
            CommissionableFilter {
                discriminator: *long_discriminator,
                short_discriminator: *short_discriminator,
                vendor_id: *vendor_id,
                product_id: None,
                device_type: None,
                // With no discriminator or vendor to select on, the only way to
                // avoid picking up an already-commissioned node is the `_CM`
                // subtype.
                commissioning_mode_only: long_discriminator.is_none()
                    && short_discriminator.is_none()
                    && vendor_id.is_none(),
            },
        ),
        // No discovery needed: the caller told us exactly where to knock.
        PaseTarget::Address { passcode, addr } => return Ok((*passcode, parse_socket_addr(addr)?)),
    };

    // Through `browse_one` rather than the transport directly, so the wait for the
    // shared browse slot is bounded too — see its doc comment.
    let (addr, instance) = discovery::browse_one(ctx, &filter, &[], BROWSE_TIMEOUT).await?;
    tracing::info!("discovered commissionable {instance:016X} at {addr}");

    Ok((passcode, addr))
}

/// A `MT:` QR string or an 11-/21-digit manual pairing code -> the passcode plus
/// the mDNS filter that finds the device it describes.
///
/// The distinction matters beyond parsing: a QR carries the full 12-bit
/// discriminator and yields a filter that can pin down one device, while a manual
/// code carries only its top 4 bits, so the filter may match several and PASE
/// then decides.
fn parse_pairing_code(
    code: &str,
    qr_buf: &mut [u8],
) -> Result<(u32, CommissionableFilter), StackError> {
    if code.starts_with("MT:") {
        let qr = QrPayload::parse(code, qr_buf).map_err(|e| invalid_code("QR code", e))?;
        Ok((qr.passcode(), qr.commissionable_filter()))
    } else {
        let manual =
            QrPayload::parse_pairing_code(code).map_err(|e| invalid_code("pairing code", e))?;
        Ok((manual.passcode(), manual.commissionable_filter()))
    }
}

/// A malformed code is the caller's mistake, not the SDK's — a mistyped digit
/// must read as `InvalidArguments` rather than as an opaque stack failure.
fn invalid_code(what: &str, e: rs_matter::error::Error) -> StackError {
    StackError::new(StackErrorKind::InvalidArguments, format!("invalid {what}: Error::{e}"))
}

/// `"ip:port"` (or a bracketed IPv6 form) -> a UDP [`Address`].
fn parse_socket_addr(addr: &str) -> Result<Address, StackError> {
    addr.parse::<std::net::SocketAddr>().map(Address::Udp).map_err(|e| {
        StackError::new(StackErrorKind::InvalidArguments, format!("invalid ip_addr: {e}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The canonical Matter test device's QR: passcode 20202021, discriminator
    /// 3840. The filter must carry the *long* discriminator, because that is what
    /// makes a QR-driven commissioning target exactly one device.
    #[test]
    fn a_qr_code_yields_the_passcode_and_a_long_discriminator_filter() {
        let mut buf = [0u8; QR_BUF_LEN];
        let (passcode, filter) =
            parse_pairing_code("MT:-24J0AFN00KA0648G00", &mut buf).expect("valid QR");
        assert_eq!(passcode, 20_202_021);
        assert_eq!(filter.discriminator, Some(3840));
        assert_eq!(filter.short_discriminator, None);
        // Deliberately absent even though the QR carries them: a device may
        // advertise an anonymized product id (`qr.rs:770`).
        assert_eq!(filter.vendor_id, None);
        assert_eq!(filter.product_id, None);
        assert!(!filter.commissioning_mode_only);
    }

    /// The same device's manual code. Only the top 4 discriminator bits survive
    /// the encoding, so the filter must use `short_discriminator` — filtering on
    /// `discriminator` here would silently never match.
    #[test]
    fn a_manual_code_yields_a_short_discriminator_filter() {
        let mut buf = [0u8; QR_BUF_LEN];
        let (passcode, filter) = parse_pairing_code("34970112332", &mut buf).expect("valid code");
        assert_eq!(passcode, 20_202_021);
        assert_eq!(filter.short_discriminator, Some(15)); // 3840 >> 8
        assert_eq!(filter.discriminator, None);
    }

    #[test]
    fn the_pretty_manual_code_form_is_accepted() {
        let mut buf = [0u8; QR_BUF_LEN];
        let (passcode, _) = parse_pairing_code("3497-0112-332", &mut buf).expect("valid code");
        assert_eq!(passcode, 20_202_021);
    }

    /// A mistyped code must be the caller's fault, with the reason visible.
    #[test]
    fn a_malformed_code_is_invalid_arguments() {
        let cases = [
            ("MT:not-base38!!", "QR code"),
            ("MT:", "QR code"),
            ("34970112333", "pairing code"), // bad Verhoeff check digit
            ("123", "pairing code"),
            ("", "pairing code"),
            ("abcdefghijk", "pairing code"),
        ];
        for (code, what) in cases {
            let mut buf = [0u8; QR_BUF_LEN];
            let e = parse_pairing_code(code, &mut buf).expect_err("must be rejected");
            assert_eq!(e.kind, StackErrorKind::InvalidArguments, "for {code:?}");
            assert!(e.message.starts_with(&format!("invalid {what}: Error::")), "{}", e.message);
            // Never the `Debug` spelling: rs-matter is built with `backtrace`, so
            // `{e:?}` would put a whole stack trace in the WS `details` field.
            assert!(!e.message.contains("stack backtrace"), "{}", e.message);
        }
    }

    #[test]
    fn an_ip_address_target_becomes_a_udp_address() {
        assert_eq!(
            parse_socket_addr("192.168.1.50:5540").expect("v4"),
            Address::Udp("192.168.1.50:5540".parse().unwrap())
        );
        assert_eq!(
            parse_socket_addr("[fe80::1%2]:5540").expect("scoped v6"),
            Address::Udp("[fe80::1%2]:5540".parse().unwrap())
        );
    }

    /// The message is the one the WS client sees, so pin its shape.
    #[test]
    fn an_unparseable_address_names_itself() {
        for bad in ["192.168.1.50", "not-an-address", "", "fe80::1:5540"] {
            let e = parse_socket_addr(bad).expect_err("must be rejected");
            assert_eq!(e.kind, StackErrorKind::InvalidArguments);
            assert!(e.message.starts_with("invalid ip_addr: "), "{}", e.message);
        }
    }
}

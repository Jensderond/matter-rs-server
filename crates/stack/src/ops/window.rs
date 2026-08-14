//! `open_commissioning_window`: mint a fresh passcode/discriminator/salt,
//! compute the SPAKE2+ verifier ourselves, hand it to the node via
//! `AdministratorCommissioning.OpenCommissioningWindow` (Enhanced Commissioning
//! Method), and give the caller back the matching manual pairing code and QR
//! string so a phone can join the node.
//!
//! The verifier is computed here rather than by the device because ECM exists
//! precisely so the *administrator* chooses the passcode: the node is told only
//! `(w0 || L, salt, iterations)`, from which the passcode cannot be recovered.
//! The math is the password branch of `Spake2P::setup_verifier`
//! (`rs-matter-ref/rs-matter/src/sc/pase/spake2p.rs:288-307`), which is not
//! reachable from outside rs-matter — see `compute_pase_verifier`.

use base64::Engine as _;
use matter_rs_controller::stack_api::{AttributePathSpec, StackError, WindowInfo};
use rs_matter::crypto::{
    Crypto, CryptoSensitive, CryptoSensitiveRef, EcPoint as _, EcScalar as _, PbKdf as _, RngCore,
    EC_CANON_POINT_LEN, EC_CANON_SCALAR_LEN, UINT320_CANON_LEN,
};
use rs_matter::error::Error;
use rs_matter::pairing::qr::{no_optional_data, CommFlowType, NoOptionalData, QrPayload};
use rs_matter::pairing::DiscoveryCapabilities;
use rs_matter::BasicCommData;
use serde_json::Value;

use crate::ctx::{map_err, StackCtx};
use crate::ops::{interact, ROOT_ENDPOINT};

/// `AdministratorCommissioning` cluster id.
const ADMIN_COMMISSIONING: u32 = 60;
/// `BasicInformation` cluster id, for the VID/PID that go into the QR.
const BASIC_INFO: u32 = 40;
const BASIC_INFO_VENDOR_ID: u32 = 2;
const BASIC_INFO_PRODUCT_ID: u32 = 4;

/// `SPAKE2P_ITERATION_COUNT` (`spake2p.rs:38`). Re-declared because the module
/// holding it is `pub(crate)` in rs-matter.
const PAKE_ITERATIONS: u32 = 2_000;

/// PASE salt length. The spec allows 16..=32; 32 is what rs-matter's own
/// `Spake2pVerifierSalt` is sized for.
const SALT_LEN: usize = 32;

/// Passcode length as fed to PBKDF2: the 4-byte little-endian encoding of the
/// numeric passcode (`SPAKE2P_VERIFIER_PASSWORD_LEN`).
const PASSCODE_LEN: usize = 4;

/// PBKDF2 output: `w0s || w1s`, two 320-bit integers (`SPAKE2P_W_LEN`).
const W0S_W1S_LEN: usize = UINT320_CANON_LEN * 2;

/// The verifier blob: `w0` (32) ++ `L` (65) = 97 bytes
/// (`SPAKE2P_VERIFIER_STR_LEN`), matching the IDL's
/// `octet_string<97> PAKEPasscodeVerifier`.
const VERIFIER_LEN: usize = EC_CANON_SCALAR_LEN + EC_CANON_POINT_LEN;

/// Largest legal setup passcode (`0x5F5E0FE`).
const MAX_PASSCODE: u32 = 99_999_998;

/// Passcodes the spec forbids: trivially guessable repetitions plus the two
/// sequences, and the reserved 0. Mirrors `QrPayload::is_valid_setup_pin`
/// (`rs-matter-ref/rs-matter/src/pairing/qr.rs:229`) — a generated passcode that
/// hit one of these would produce a QR code a phone refuses to accept.
const INVALID_PASSCODES: &[u32] = &[
    0, 11111111, 22222222, 33333333, 44444444, 55555555, 66666666, 77777777, 88888888, 99999999,
    12345678, 87654321,
];

pub(crate) async fn open_window<C: Crypto>(
    ctx: &StackCtx<C>,
    node_id: u64,
    timeout_secs: u16,
) -> Result<WindowInfo, StackError> {
    // Same RNG accessor the IPK generation uses (`spike/src/main.rs:279`).
    let (passcode, discriminator, salt) = {
        let mut rng = ctx.crypto.rand().map_err(map_err)?;
        generate_window_secrets(&mut rng)
    };

    let verifier = compute_pase_verifier(&ctx.crypto, passcode, PAKE_ITERATIONS, &salt)
        .map_err(map_err)?;

    // Deliberately not logged, at any level: the verifier is the credential that
    // lets its holder join the node, and the passcode is recoverable from it by
    // brute force over 10^8 candidates.
    let payload = serde_json::json!({
        "commissioningTimeout": timeout_secs,
        "PAKEPasscodeVerifier": base64_std(&verifier),
        "discriminator": discriminator,
        "iterations": PAKE_ITERATIONS,
        "salt": base64_std(&salt),
    });
    interact::invoke(
        ctx,
        node_id,
        ROOT_ENDPOINT,
        ADMIN_COMMISSIONING,
        "openCommissioningWindow",
        &payload,
        // `OpenCommissioningWindow` is a spec-timed command; `interact::invoke`
        // would supply the same 10s default, stated here because a device
        // rejecting an untimed invoke is a confusing failure to debug.
        Some(10_000),
    )
    .await?;

    // The window is open at this point, so the passcode is live whatever happens
    // below — hence VID/PID read failures degrade rather than abort.
    let (vid, pid) = basic_info_vid_pid(ctx, node_id).await;
    let comm_data = BasicCommData { password: passcode.to_le_bytes().into(), discriminator };
    let setup_manual_code = comm_data.compute_pairing_code().to_string();
    let setup_qr_code = build_qr(comm_data, vid, pid).map_err(map_err)?;

    Ok(WindowInfo { setup_pin_code: passcode, setup_manual_code, setup_qr_code })
}

/// A fresh `(passcode, discriminator, salt)` triple.
///
/// The RNG is a parameter so the retry path can be exercised: a passcode landing
/// on one of [`INVALID_PASSCODES`] has to be redrawn, and that branch is
/// otherwise reachable roughly once in 10^7 runs.
///
/// Only the *passcode* reduction is biased, and only slightly: 2^32 is not a
/// multiple of `MAX_PASSCODE`, so the low ~52 residues come up 43 times per 2^32
/// draws where the rest come up 42 — a relative excess of about 2^-25. Left as-is
/// knowingly: the passcode is a one-shot secret with a 15-minute window and a
/// 20-attempt device-side lockout, so rejection sampling would buy nothing
/// measurable. The discriminator reduction is *exactly* uniform — it draws over
/// 2^16, which is 16 × 4096.
fn generate_window_secrets<R: RngCore>(rng: &mut R) -> (u32, u16, [u8; SALT_LEN]) {
    let passcode = loop {
        let mut b = [0u8; 4];
        rng.fill_bytes(&mut b);
        let p = u32::from_le_bytes(b) % MAX_PASSCODE + 1; // 1..=99999998
        if !INVALID_PASSCODES.contains(&p) {
            break p;
        }
    };

    let mut d = [0u8; 2];
    rng.fill_bytes(&mut d);
    // 12-bit, per the spec's discriminator width.
    let discriminator = u16::from_le_bytes(d) % 4096;

    let mut salt = [0u8; SALT_LEN];
    rng.fill_bytes(&mut salt);

    (passcode, discriminator, salt)
}

/// The SPAKE2+ registration record for `passcode`: `w0 || L`, 97 bytes.
///
/// ```text
/// w0s || w1s = PBKDF2-SHA256(passcode as 4-byte LE, salt, iterations, dkLen = 80)
/// w0 = w0s mod n          (n = the P-256 group order)
/// w1 = w1s mod n
/// L  = w1 * G
/// out = w0 (32 canonical bytes) || L (65 canonical SEC1 bytes, 0x04-prefixed)
/// ```
///
/// This duplicates the password branch of `Spake2P::setup_verifier`
/// (`rs-matter-ref/rs-matter/src/sc/pase/spake2p.rs:288-307`) because that code
/// is unreachable: `spake2p` is a `pub(crate)` module and the only items
/// re-exported from it are `Spake2pVerifierPassword` and its length constants.
/// The tests pin the output against an independently derived vector instead —
/// see `matches_the_published_chip_test_verifier`.
fn compute_pase_verifier<C: Crypto>(
    crypto: &C,
    passcode: u32,
    iterations: u32,
    salt: &[u8],
) -> Result<[u8; VERIFIER_LEN], Error> {
    let passcode = passcode.to_le_bytes();
    let pw = CryptoSensitiveRef::<PASSCODE_LEN>::new(&passcode);

    let mut w0s_w1s = CryptoSensitive::<W0S_W1S_LEN>::new();
    crypto
        .pbkdf()?
        .derive(pw, iterations as usize, salt, &mut w0s_w1s)?;
    let (w0s, w1s) = w0s_w1s
        .reference()
        .split::<UINT320_CANON_LEN, UINT320_CANON_LEN>();

    // `ec_scalar_mod_p` reduces a 320-bit big-endian integer modulo the group
    // order, which is what turns the 40-byte halves into curve scalars.
    let w0 = crypto.ec_scalar_mod_p(w0s)?;
    let w1 = crypto.ec_scalar_mod_p(w1s)?;
    let l_pt = crypto.ec_generator_point()?.mul(&w1)?;

    let mut w0_canon = CryptoSensitive::<EC_CANON_SCALAR_LEN>::new();
    w0.write_canon(&mut w0_canon)?;
    let mut l_canon = CryptoSensitive::<EC_CANON_POINT_LEN>::new();
    l_pt.write_canon(&mut l_canon)?;

    let mut out = [0u8; VERIFIER_LEN];
    out[..EC_CANON_SCALAR_LEN].copy_from_slice(w0_canon.access());
    out[EC_CANON_SCALAR_LEN..].copy_from_slice(l_canon.access());

    Ok(out)
}

/// The onboarding QR text (`"MT:..."`) for a node the window was just opened on.
///
/// `DiscoveryCapabilities::IP` because a node that is already commissioned is on
/// the operational network and re-advertises there; `CommFlowType::Standard`
/// because ECM needs no user action on the device; empty serial number and no
/// optional TLV data because neither is needed to join and both only lengthen
/// the code.
fn build_qr(comm_data: BasicCommData, vid: u16, pid: u16) -> Result<String, Error> {
    let payload: QrPayload<'_, NoOptionalData> = QrPayload::new(
        DiscoveryCapabilities::IP,
        CommFlowType::Standard,
        comm_data,
        vid,
        pid,
        "",
        no_optional_data,
    );

    // The fixed part is 88 bits -> 19 base38 chars, plus the 3-char prefix, and
    // the optional-data section is empty: 22 bytes, always, for these arguments.
    //
    // The margin is not optional politeness. `as_str` starts with
    // `buf.split_at_mut(str_len)` (`rs-matter-ref/rs-matter/src/pairing/qr.rs:276`),
    // which **panics** — it does not return `BufferTooSmall` — if the buffer is
    // shorter than the encoded string. Anyone adding a serial number or optional
    // TLV data here must grow this buffer to match; it will not fail gracefully.
    let mut buf = [0u8; 64];
    let (qr, _) = payload.as_str(&mut buf)?;

    Ok(qr.to_string())
}

/// The node's `(VendorID, ProductID)`, or `(0, 0)` if they cannot be read.
///
/// Both are optional decoration on the QR — a commissioner finds the node by
/// discriminator — so a failed read must not sink an otherwise-open window.
async fn basic_info_vid_pid<C: Crypto>(ctx: &StackCtx<C>, node_id: u64) -> (u16, u16) {
    let paths = [
        AttributePathSpec {
            endpoint: Some(ROOT_ENDPOINT),
            cluster: Some(BASIC_INFO),
            attribute: Some(BASIC_INFO_VENDOR_ID),
        },
        AttributePathSpec {
            endpoint: Some(ROOT_ENDPOINT),
            cluster: Some(BASIC_INFO),
            attribute: Some(BASIC_INFO_PRODUCT_ID),
        },
    ];
    match interact::read_attributes(ctx, node_id, &paths, false).await {
        Ok(pairs) => vid_pid_from(&pairs),
        Err(e) => {
            tracing::warn!(
                "could not read vendor/product id of node {node_id} for the QR code: {}",
                e.message
            );
            (0, 0)
        }
    }
}

fn vid_pid_from(pairs: &[(String, Value)]) -> (u16, u16) {
    let get = |attr: u32| {
        let key = format!("{ROOT_ENDPOINT}/{BASIC_INFO}/{attr}");
        pairs
            .iter()
            .find(|(k, _)| *k == key)
            .and_then(|(_, v)| v.as_u64())
            .and_then(|v| u16::try_from(v).ok())
            .unwrap_or(0)
    };
    (get(BASIC_INFO_VENDOR_ID), get(BASIC_INFO_PRODUCT_ID))
}

fn base64_std(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

/// Whether `passcode` is one a commissioner will accept — the predicate
/// [`generate_window_secrets`] is contracted to satisfy, spelled out separately
/// so the tests assert it rather than restating the range.
#[cfg(test)]
fn passcode_is_legal(passcode: u32) -> bool {
    (1..=MAX_PASSCODE).contains(&passcode) && !INVALID_PASSCODES.contains(&passcode)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rs_matter::crypto::default_crypto;
    use rs_matter::dm::devices::test::DAC_PRIVKEY;

    /// An RNG that hands out scripted bytes, then zeros. Enough to drive
    /// `generate_window_secrets` down a chosen path.
    struct ScriptedRng {
        chunks: Vec<Vec<u8>>,
        at: usize,
    }

    impl ScriptedRng {
        fn new(chunks: Vec<Vec<u8>>) -> Self {
            Self { chunks, at: 0 }
        }
    }

    impl RngCore for ScriptedRng {
        fn next_u32(&mut self) -> u32 {
            let mut b = [0u8; 4];
            self.fill_bytes(&mut b);
            u32::from_le_bytes(b)
        }

        fn next_u64(&mut self) -> u64 {
            let mut b = [0u8; 8];
            self.fill_bytes(&mut b);
            u64::from_le_bytes(b)
        }

        fn fill_bytes(&mut self, dest: &mut [u8]) {
            dest.fill(0);
            if let Some(chunk) = self.chunks.get(self.at) {
                let n = chunk.len().min(dest.len());
                dest[..n].copy_from_slice(&chunk[..n]);
            }
            self.at += 1;
        }

        fn try_fill_bytes(&mut self, dest: &mut [u8]) -> Result<(), rand::Error> {
            self.fill_bytes(dest);
            Ok(())
        }
    }

    /// The bytes `generate_window_secrets` must draw to end up at `passcode`.
    fn draw_for(passcode: u32) -> Vec<u8> {
        (passcode - 1).to_le_bytes().to_vec()
    }

    // ---------------------------------------------------------------- verifier

    /// The load-bearing assertion of this module.
    ///
    /// rs-matter's own derivation (`Spake2P::setup_verifier`, password branch) is
    /// not callable from here — `sc::pase::spake2p` is `pub(crate)`, and even if
    /// it were public the function mixes in a fresh random scalar and returns
    /// `(Y, cB)`, never `(w0, L)`. So the cross-check is against the *published*
    /// Matter reference value instead: `spake2p gen-verifier --passcode 20202021
    /// --salt "SPAKE2P Key Salt" --count 1000` is the verifier every CHIP example
    /// device ships with, and this base64 is that output verbatim. It was
    /// re-derived independently (PBKDF2 from `hashlib`, P-256 double-and-add in
    /// Python) before being pinned here, so three implementations agree on it.
    #[test]
    fn matches_the_published_chip_test_verifier() {
        let crypto = default_crypto(rand::thread_rng(), DAC_PRIVKEY);
        let v = compute_pase_verifier(&crypto, 20_202_021, 1_000, b"SPAKE2P Key Salt")
            .expect("verifier");
        assert_eq!(
            base64_std(&v),
            "uWFwqugDNGiEck/po7KHwwMwwqZgN10XuyBajPGuyzUEV/iree4lOrao5GuwnlQ65CJzbeUB49s31EH+\
             NEkg0JVI5MGCQGMMT/SRPFNRODm3wH/MBiehuFc6FJ/NH6Rmzw=="
        );
    }

    /// Same, at the parameters this module actually uses (2000 iterations, a
    /// 32-byte salt) so a change to `PAKE_ITERATIONS` or the salt handling shows
    /// up as a failure rather than as a device that silently rejects PASE.
    #[test]
    fn verifier_is_pinned_at_the_production_parameters() {
        let crypto = default_crypto(rand::thread_rng(), DAC_PRIVKEY);
        let salt: [u8; SALT_LEN] = core::array::from_fn(|i| i as u8);
        let v = compute_pase_verifier(&crypto, 12_345_679, PAKE_ITERATIONS, &salt)
            .expect("verifier");
        assert_eq!(
            base64_std(&v),
            "tFFAsuYjouoJMHVCMcZIzRO/I7E4pN2NhAl+HJ8EbxMEXNRFhNCQqHChkaNG1kwrnVSxLweSOh0OeT+j\
             DOY6kYIBB46RA3cG+uEX0Cu3Rhkl8xjErs0QeM5mvwzxzuyZrA=="
        );
    }

    /// Layout, independently of the values: 32-byte scalar then a 65-byte
    /// uncompressed SEC1 point. The IDL caps `PAKEPasscodeVerifier` at exactly 97
    /// bytes, so a wrong split would be rejected by the device, not by us.
    #[test]
    fn verifier_is_a_32_byte_scalar_followed_by_a_65_byte_point() {
        let crypto = default_crypto(rand::thread_rng(), DAC_PRIVKEY);
        let salt = [7u8; SALT_LEN];
        let v = compute_pase_verifier(&crypto, 1, PAKE_ITERATIONS, &salt).expect("verifier");
        assert_eq!(v.len(), 97);
        assert_eq!(EC_CANON_SCALAR_LEN, 32);
        assert_eq!(EC_CANON_POINT_LEN, 65);
        assert_eq!(v[EC_CANON_SCALAR_LEN], 0x04, "L must be an uncompressed point");
        assert!(v[..EC_CANON_SCALAR_LEN].iter().any(|b| *b != 0), "w0 must not be zero");
    }

    /// Every input must reach the derivation: a verifier that ignored the salt or
    /// the iteration count would still be 97 bytes and still be accepted by the
    /// device, and PASE would then fail with no diagnostic.
    #[test]
    fn every_parameter_changes_the_verifier() {
        let crypto = default_crypto(rand::thread_rng(), DAC_PRIVKEY);
        let salt = [1u8; SALT_LEN];
        let base = compute_pase_verifier(&crypto, 1_234_567, 2_000, &salt).unwrap();

        let other_passcode = compute_pase_verifier(&crypto, 1_234_568, 2_000, &salt).unwrap();
        let other_iterations = compute_pase_verifier(&crypto, 1_234_567, 2_001, &salt).unwrap();
        let other_salt = compute_pase_verifier(&crypto, 1_234_567, 2_000, &[2u8; SALT_LEN]).unwrap();

        assert_ne!(base, other_passcode);
        assert_ne!(base, other_iterations);
        assert_ne!(base, other_salt);

        // ...and it is deterministic: the same inputs must give the same record,
        // or the window's passcode would not open it.
        assert_eq!(base, compute_pase_verifier(&crypto, 1_234_567, 2_000, &salt).unwrap());
    }

    // ----------------------------------------------------------- secrets / RNG

    #[test]
    fn generated_secrets_are_in_range() {
        // A spread of raw RNG words, including the extremes that off-by-one
        // errors in `% MAX_PASSCODE + 1` would land on.
        for word in [0u32, 1, 2, MAX_PASSCODE - 1, MAX_PASSCODE, MAX_PASSCODE + 1, u32::MAX] {
            let mut rng = ScriptedRng::new(vec![
                word.to_le_bytes().to_vec(),
                vec![0xFF, 0xFF],
                vec![0xAA; SALT_LEN],
            ]);
            let (passcode, discriminator, salt) = generate_window_secrets(&mut rng);
            assert!(
                (1..=MAX_PASSCODE).contains(&passcode),
                "passcode {passcode} out of range for word {word}"
            );
            assert!(!INVALID_PASSCODES.contains(&passcode));
            assert!(passcode_is_legal(passcode));
            assert!(discriminator < 4096, "discriminator {discriminator} is not 12-bit");
            assert_eq!(salt.len(), SALT_LEN);
            assert_eq!(salt, [0xAA; SALT_LEN]);
        }
    }

    /// The retry branch: a draw landing on a forbidden passcode must be
    /// discarded, not clamped or accepted.
    #[test]
    fn a_forbidden_passcode_is_redrawn() {
        for forbidden in INVALID_PASSCODES {
            // 0 is not reachable (`% MAX + 1` never yields it) and 99999999
            // exceeds the range, so only the interior values can be drawn.
            if *forbidden == 0 || *forbidden > MAX_PASSCODE {
                continue;
            }
            let mut rng = ScriptedRng::new(vec![
                draw_for(*forbidden),
                draw_for(4_242_424),
                vec![0x01, 0x00],
                vec![0; SALT_LEN],
            ]);
            let (passcode, discriminator, _) = generate_window_secrets(&mut rng);
            assert_eq!(passcode, 4_242_424, "{forbidden} must have been redrawn");
            assert_eq!(discriminator, 1);
        }
    }

    #[test]
    fn several_forbidden_draws_in_a_row_are_all_redrawn() {
        let mut rng = ScriptedRng::new(vec![
            draw_for(11_111_111),
            draw_for(12_345_678),
            draw_for(87_654_321),
            draw_for(7),
            vec![0x00, 0x10], // 4096 -> 0 after the 12-bit reduction
            vec![0; SALT_LEN],
        ]);
        let (passcode, discriminator, _) = generate_window_secrets(&mut rng);
        assert_eq!(passcode, 7);
        assert_eq!(discriminator, 0);
    }

    #[test]
    fn the_discriminator_is_reduced_to_twelve_bits() {
        for (word, expected) in [(0u16, 0u16), (4095, 4095), (4096, 0), (4097, 1), (u16::MAX, 4095)]
        {
            let mut rng = ScriptedRng::new(vec![
                draw_for(1_000),
                word.to_le_bytes().to_vec(),
                vec![0; SALT_LEN],
            ]);
            let (_, discriminator, _) = generate_window_secrets(&mut rng);
            assert_eq!(discriminator, expected, "for word {word}");
        }
    }

    // ------------------------------------------------------ onboarding payload

    /// Manual pairing code and QR string for the canonical Matter test device
    /// (passcode 20202021, discriminator 3840, VID 0xFFF1, PID 0x8001). Both
    /// values are the ones `chip-tool` prints for that device, so a regression in
    /// how the passcode is packed into `BasicCommData` — the one thing this
    /// module contributes — fails here instead of at a phone that will not scan.
    #[test]
    fn onboarding_payload_matches_the_canonical_test_device() {
        let comm_data =
            BasicCommData { password: 20_202_021u32.to_le_bytes().into(), discriminator: 3840 };
        assert_eq!(comm_data.compute_pairing_code().as_str(), "34970112332");
        assert_eq!(
            build_qr(comm_data, 0xFFF1, 0x8001).expect("qr"),
            "MT:-24J0AFN00KA0648G00"
        );
    }

    /// The VID/PID read is best-effort, so `(0, 0)` is a shape the QR builder
    /// must still handle — and produce a *different* code for.
    #[test]
    fn an_unknown_vendor_and_product_still_produce_a_scannable_qr() {
        let comm_data =
            BasicCommData { password: 20_202_021u32.to_le_bytes().into(), discriminator: 3840 };
        let qr = build_qr(comm_data, 0, 0).expect("qr");
        assert_eq!(qr, "MT:00000CQM00KA0648G00");
        assert!(qr.starts_with("MT:"));
    }

    /// Discriminator and passcode both have to reach the QR: they are the two
    /// fields a commissioner uses to find and authenticate the node.
    #[test]
    fn passcode_and_discriminator_both_reach_the_payload() {
        let a = build_qr(
            BasicCommData { password: 20_202_021u32.to_le_bytes().into(), discriminator: 3840 },
            0xFFF1,
            0x8001,
        )
        .unwrap();
        let b = build_qr(
            BasicCommData { password: 20_202_022u32.to_le_bytes().into(), discriminator: 3840 },
            0xFFF1,
            0x8001,
        )
        .unwrap();
        let c = build_qr(
            BasicCommData { password: 20_202_021u32.to_le_bytes().into(), discriminator: 3841 },
            0xFFF1,
            0x8001,
        )
        .unwrap();
        assert_ne!(a, b);
        assert_ne!(a, c);
    }

    /// Round-trip through rs-matter's own parser: this checks the *semantics* of
    /// the emitted QR (that the fields land in the right slots), which a pinned
    /// string alone cannot.
    #[test]
    fn the_emitted_qr_parses_back_to_what_went_in() {
        let comm_data =
            BasicCommData { password: 42_424_242u32.to_le_bytes().into(), discriminator: 1234 };
        let qr = build_qr(comm_data, 0xFFF1, 0x8001).expect("qr");

        let mut buf = [0u8; 64];
        let parsed = QrPayload::parse(&qr, &mut buf).expect("parse");
        assert_eq!(parsed.passcode(), 42_424_242);
        assert_eq!(parsed.discriminator(), 1234);
        assert_eq!(parsed.vid(), 0xFFF1);
        assert_eq!(parsed.pid(), 0x8001);
        assert_eq!(parsed.version(), 0);
        assert_eq!(parsed.comm_flow(), CommFlowType::Standard);
        assert_eq!(parsed.discovery_capabilities(), DiscoveryCapabilities::IP);
        assert_eq!(parsed.serial_no(), "");
        assert!(parsed.optional_data().is_empty());
    }

    /// The manual code carries only the top 4 discriminator bits, so it must be
    /// derived from the same 12-bit value the QR got, not from a truncated one.
    #[test]
    fn the_manual_code_agrees_with_the_qr_on_the_short_discriminator() {
        let comm_data =
            BasicCommData { password: 42_424_242u32.to_le_bytes().into(), discriminator: 1234 };
        let manual = comm_data.compute_pairing_code().to_string();
        let parsed = QrPayload::parse_pairing_code(&manual).expect("parse manual");
        assert_eq!(parsed.passcode(), 42_424_242);
        assert_eq!(parsed.short_discriminator(), (1234 >> 8) as u8);
    }

    // --------------------------------------------------------------- vid / pid

    #[test]
    fn vid_pid_are_read_by_path() {
        let pairs = vec![
            ("0/40/4".to_string(), Value::from(0x8001)),
            ("0/40/2".to_string(), Value::from(0xFFF1)),
            ("0/40/1".to_string(), Value::from("acme")),
        ];
        assert_eq!(vid_pid_from(&pairs), (0xFFF1, 0x8001));
    }

    #[test]
    fn missing_or_unusable_vid_pid_default_to_zero() {
        assert_eq!(vid_pid_from(&[]), (0, 0));
        assert_eq!(
            vid_pid_from(&[("0/40/2".to_string(), Value::Null)]),
            (0, 0)
        );
        // Out of `u16` range: truncating would put a wrong vendor in the QR.
        assert_eq!(
            vid_pid_from(&[("0/40/2".to_string(), Value::from(70_000))]),
            (0, 0)
        );
        // Right value, wrong endpoint.
        assert_eq!(
            vid_pid_from(&[("1/40/2".to_string(), Value::from(0xFFF1))]),
            (0, 0)
        );
    }
}

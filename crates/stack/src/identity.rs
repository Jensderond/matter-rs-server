//! The controller's fabric identity: loaded from `server.json` or minted on
//! first run, then installed as a fabric on the `Matter` instance.
//!
//! RCAC-direct always: the controller NOC is signed by the RCAC and the ICAC is
//! empty. matter.js rejects rs-matter's ICAC (spike finding 1), so there is no
//! ICAC-tier mode to switch to.
//!
//! rs-matter keeps fabrics in RAM, so every start re-adds the fabric from the
//! stored blobs. A stored identity therefore always wins over the CLI flags:
//! minting a new fabric because `--fabricid` changed would orphan every node
//! already commissioned onto the old one.

use core::num::NonZeroU8;

use matter_rs_controller::storage::{ServerIdentity, Storage};

use rs_matter::cert::gen::VALID_FOREVER;
use rs_matter::cert::{CertRef, MAX_CERT_TLV_AND_ASN1_LEN};
use rs_matter::crypto::{
    CanonAeadKey, CanonAeadKeyRef, CanonPkcSecretKey, CanonPkcSecretKeyRef, Crypto, RngCore as _,
    SecretKey, SigningSecretKey,
};
use rs_matter::error::{Error, ErrorCode};
use rs_matter::onboard::cac::RcacGenerator;
use rs_matter::onboard::noc::NocGenerator;
use rs_matter::tlv::TLVElement;
use rs_matter::Matter;

/// Operational node id of this controller on its own fabric, matching the Node
/// server so an existing `server.json` stays usable.
pub const CONTROLLER_NODE_ID: u64 = 112233;

/// `Fabric::label` is a `heapless::String<32>` — 32 *bytes*, where the stored
/// label is capped at 32 *chars* (Node-compatible, deliberately).
pub(crate) const FABRIC_LABEL_MAX_BYTES: usize = 32;

/// Load-or-generate the controller identity and install it as a fabric on the
/// Matter instance. Returns the identity — freshly written to `server.json` on
/// first run, otherwise the stored one — and the local fabric index.
///
/// A `server.json` that exists but cannot be read or parsed is a hard error:
/// generating over it would destroy still-recoverable key material.
pub fn ensure_identity<C: Crypto>(
    matter: &Matter<'_>,
    crypto: &C,
    storage: &Storage,
    fabric_id: u64,
    vendor_id: u16,
    fabric_label: &str,
) -> Result<(ServerIdentity, NonZeroU8), Error> {
    let (identity, fab_idx) = match storage.load_identity()? {
        Some(mut stored) => {
            if stored.fabric_id != fabric_id || stored.vendor_id != vendor_id {
                tracing::warn!(
                    "stored identity (fabric id {}, vendor id {:#06x}) differs from the requested \
                     fabric id {} / vendor id {vendor_id:#06x}; keeping the stored one — \
                     regenerating would orphan every commissioned node",
                    stored.fabric_id,
                    stored.vendor_id,
                    fabric_id,
                );
            }
            let (fab_idx, compressed) = install(matter, crypto, &stored)?;
            if compressed != stored.compressed_fabric_id {
                // Derived from the RCAC pubkey + fabric id, i.e. from the key
                // blobs themselves, so the derived value is the authoritative
                // one and the stored scalar is the corrupt half. It reaches
                // Home Assistant verbatim in server_info, so correct it.
                tracing::warn!(
                    "stored compressed fabric id {:#x} disagrees with the {compressed:#x} derived \
                     from the stored certificates; using and persisting the derived value",
                    stored.compressed_fabric_id,
                );
                stored.compressed_fabric_id = compressed;
                if let Err(e) = storage.save_identity(&stored) {
                    // The in-memory value is already right; a read-only or full
                    // disk must not keep the controller from starting.
                    tracing::warn!("could not persist the corrected compressed fabric id: {e}");
                }
            }
            (stored, fab_idx)
        }
        None => {
            tracing::info!("no stored identity; generating a fabric (id {fabric_id}) for this controller");
            let mut identity = generate(crypto, fabric_id, vendor_id)?;
            let (fab_idx, compressed) = install(matter, crypto, &identity)?;
            identity.compressed_fabric_id = compressed;
            if let Err(e) = storage.create_identity(&identity) {
                // A fabric nothing can reproduce after a restart is worse than
                // no fabric: back it out so the failure is clean.
                if let Err(e) = matter.with_state(|s| s.fabrics.remove(fab_idx)) {
                    tracing::warn!("could not roll back fabric {fab_idx} after a failed write: {e:?}");
                }
                return Err(e.into());
            }
            (identity, fab_idx)
        }
    };

    // Cosmetic only (it is what other admins see in the Fabrics cluster), so a
    // duplicate/over-long label must not abort the bootstrap. rs-matter clears
    // the label before pushing, so an over-long one would be left EMPTY rather
    // than truncated — clamp to the byte budget first.
    let label = truncate_to_bytes(fabric_label, FABRIC_LABEL_MAX_BYTES);
    if label.len() < fabric_label.len() {
        tracing::warn!("fabric label {fabric_label:?} exceeds {FABRIC_LABEL_MAX_BYTES} bytes; using {label:?}");
    }
    if let Err(e) = matter.with_state(|s| s.fabrics.update_label(fab_idx, label).map(|_| ())) {
        tracing::warn!("could not set fabric label {label:?}: {e:?}");
    }

    Ok((identity, fab_idx))
}

/// Add the identity's fabric to the Matter instance. Returns its fabric index
/// and the compressed fabric id rs-matter derived for it.
fn install<C: Crypto>(
    matter: &Matter<'_>,
    crypto: &C,
    id: &ServerIdentity,
) -> Result<(NonZeroU8, u64), Error> {
    // rs-matter takes `case_admin_subject` on trust, so a hand-edited
    // controller_node_id would silently disagree with the NOC we install under
    // it and only surface as a CASE failure later.
    let noc_node_id = CertRef::new(TLVElement::new(&id.controller_noc_tlv)).get_node_id()?;
    if noc_node_id != id.controller_node_id {
        tracing::error!(
            "stored controller_node_id {} does not match the {noc_node_id} in the stored NOC",
            id.controller_node_id
        );
        return Err(ErrorCode::InvalidData.into());
    }

    // References, not owned copies: nothing here needs a second copy of the
    // operational secret key on the stack.
    let controller_key = CanonPkcSecretKeyRef::try_new(&id.controller_private_key)?;
    let ipk = CanonAeadKeyRef::try_new(&id.ipk)?;

    matter.with_state(|s| {
        s.fabrics
            .add(
                crypto,
                controller_key,
                &id.rcac_tlv,
                &id.controller_noc_tlv,
                &[], // RCAC-direct: no ICAC, ever
                Some(ipk),
                id.vendor_id,
                id.controller_node_id,
            )
            .map(|f| (f.fab_idx(), f.compressed_fabric_id()))
    })
}

/// Longest prefix of `s` that fits in `max_bytes` without splitting a char.
///
/// Shared with `ops::fabrics::update_fabric_label`, which has to clamp for the
/// same reason: `Fabrics::update_label` rejects an over-long label outright
/// instead of truncating it.
pub(crate) fn truncate_to_bytes(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

/// Mint a fresh CA chain and controller NOC. The generated blobs are all the
/// caller needs to reproduce this fabric on a later start.
fn generate<C: Crypto>(crypto: &C, fabric_id: u64, vendor_id: u16) -> Result<ServerIdentity, Error> {
    let mut rcac_buf = [0u8; MAX_CERT_TLV_AND_ASN1_LEN];
    let mut rcac_gen = RcacGenerator::new(&mut rcac_buf);
    let (rcac_priv, rcac) = rcac_gen.generate(crypto, fabric_id, VALID_FOREVER)?;

    // Controller operational keypair -> CSR -> NOC signed by the RCAC.
    let controller_secret_key = crypto.generate_secret_key()?;
    let mut csr_buf = [0u8; 256];
    let csr = controller_secret_key.csr(&mut csr_buf)?;
    let mut controller_key = CanonPkcSecretKey::new();
    controller_secret_key.write_canon(&mut controller_key)?;

    let mut noc_buf = [0u8; MAX_CERT_TLV_AND_ASN1_LEN];
    let mut noc_gen = NocGenerator::create(rcac_priv.reference(), rcac, &[], &mut noc_buf)?;
    let controller_noc = noc_gen.generate(crypto, csr, CONTROLLER_NODE_ID, &[], VALID_FOREVER)?;

    // Fabric IPK: 16 random bytes.
    let mut ipk = CanonAeadKey::new();
    crypto.rand()?.fill_bytes(ipk.access_mut());

    Ok(ServerIdentity {
        fabric_id,
        vendor_id,
        controller_node_id: CONTROLLER_NODE_ID,
        // Derived by rs-matter when the fabric is added; filled in by the caller.
        compressed_fabric_id: 0,
        ca_private_key: rcac_priv.access().to_vec(),
        rcac_tlv: rcac.to_vec(),
        controller_private_key: controller_key.access().to_vec(),
        controller_noc_tlv: controller_noc.to_vec(),
        ipk: ipk.access().to_vec(),
    })
}

/// Rebuild an owned P-256 secret key from its persisted canonical bytes.
/// Needed where the key must outlive the borrow of the identity: a
/// `NocGenerator` holds a reference to the CA key for a whole commissioning
/// flow. `install` here needs only a borrow; `ops::commission` is the caller
/// that needs the owned form.
pub(crate) fn canon_secret_key(bytes: &[u8]) -> Result<CanonPkcSecretKey, Error> {
    CanonPkcSecretKey::try_from(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use matter_rs_controller::storage::Storage;
    use rs_matter::crypto::default_crypto;
    use rs_matter::dm::clusters::time_sync::UtcTime;
    use rs_matter::dm::devices::test::{DAC_PRIVKEY, TEST_DEV_ATT, TEST_DEV_COMM, TEST_DEV_DET};
    use rs_matter::utils::init::InitMaybeUninit;
    use rs_matter::Matter;
    use static_cell::StaticCell;

    fn init(cell: &'static StaticCell<Matter<'static>>) -> &'static Matter<'static> {
        cell.uninit()
            .init_with(Matter::init(&TEST_DEV_DET, TEST_DEV_COMM, &TEST_DEV_ATT, 0))
    }

    #[test]
    fn generates_then_reloads_identical_identity() {
        static M1: StaticCell<Matter> = StaticCell::new();
        static M2: StaticCell<Matter> = StaticCell::new();
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::open(dir.path()).unwrap();
        let crypto = default_crypto(rand::thread_rng(), DAC_PRIVKEY);

        let m1 = M1.uninit().init_with(Matter::init(&TEST_DEV_DET, TEST_DEV_COMM, &TEST_DEV_ATT, 0));
        let (id1, idx1) = ensure_identity(m1, &crypto, &storage, 1, 0xFFF1, "HomeAssistant").unwrap();
        assert_eq!(id1.controller_node_id, CONTROLLER_NODE_ID);
        assert_ne!(id1.compressed_fabric_id, 0);
        assert!(!id1.ca_private_key.is_empty());
        assert!(storage.load_identity().unwrap().is_some());

        // Fresh Matter instance (new "process"): identity must LOAD, not regenerate.
        let m2 = M2.uninit().init_with(Matter::init(&TEST_DEV_DET, TEST_DEV_COMM, &TEST_DEV_ATT, 0));
        let (id2, idx2) = ensure_identity(m2, &crypto, &storage, 1, 0xFFF1, "HomeAssistant").unwrap();
        assert_eq!(id1.rcac_tlv, id2.rcac_tlv);
        assert_eq!(id1.compressed_fabric_id, id2.compressed_fabric_id);
        // Plan-mandated, but vacuous: each call gets a fresh `Matter`, so the
        // first fabric added is always index 1 on both.
        assert_eq!(idx1, idx2);
        // The reloaded blobs must reproduce the same fabric, not just the same
        // JSON: compare what rs-matter itself derived on the second instance.
        let recomputed = m2.with_state(|s| s.fabrics.get(idx2).map(|f| f.compressed_fabric_id()));
        assert_eq!(recomputed, Some(id1.compressed_fabric_id));
    }

    /// The orphan-every-node guard: changed flags must not remint the fabric.
    #[test]
    fn stored_identity_wins_over_conflicting_arguments() {
        static M1: StaticCell<Matter> = StaticCell::new();
        static M2: StaticCell<Matter> = StaticCell::new();
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::open(dir.path()).unwrap();
        let crypto = default_crypto(rand::thread_rng(), DAC_PRIVKEY);

        let (id1, _) = ensure_identity(init(&M1), &crypto, &storage, 1, 0xFFF1, "HomeAssistant").unwrap();
        let (id2, _) = ensure_identity(init(&M2), &crypto, &storage, 42, 0x1234, "Other").unwrap();
        assert_eq!(id2.fabric_id, 1);
        assert_eq!(id2.vendor_id, 0xFFF1);
        assert_eq!(id1.rcac_tlv, id2.rcac_tlv);
        assert_eq!(id1.controller_noc_tlv, id2.controller_noc_tlv);
        assert_eq!(id1.compressed_fabric_id, id2.compressed_fabric_id);
        // ...and nothing was rewritten on disk.
        let on_disk = storage.load_identity().unwrap().unwrap();
        assert_eq!(on_disk.fabric_id, 1);
        assert_eq!(on_disk.vendor_id, 0xFFF1);
    }

    /// A `server.json` that exists but cannot be parsed must NEVER read as
    /// "first run": generating would rename over recoverable key material and
    /// orphan every commissioned node. The file must also be left untouched so
    /// it can still be repaired by hand.
    #[test]
    fn corrupt_identity_file_errors_and_is_left_intact() {
        static M0: StaticCell<Matter> = StaticCell::new();
        static M1: StaticCell<Matter> = StaticCell::new();
        static M2: StaticCell<Matter> = StaticCell::new();
        static M3: StaticCell<Matter> = StaticCell::new();
        static M4: StaticCell<Matter> = StaticCell::new();
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::open(dir.path()).unwrap();
        let crypto = default_crypto(rand::thread_rng(), DAC_PRIVKEY);
        let server = dir.path().join("server.json");

        let (id, _) = ensure_identity(init(&M0), &crypto, &storage, 1, 0xFFF1, "HA").unwrap();
        let good = std::fs::read(&server).unwrap();

        let mut non_utf8 = good.clone();
        non_utf8[good.len() / 2] = 0x80; // lone continuation byte
        let truncated = good[..good.len() / 2].to_vec();
        let missing_field = br#"{"fabric_id":1,"vendor_id":65521}"#.to_vec();

        for (cell, corrupt) in [
            (&M1, non_utf8),
            (&M2, missing_field),
            (&M3, truncated),
        ] {
            std::fs::write(&server, &corrupt).unwrap();
            let matter = init(cell);
            assert!(
                ensure_identity(matter, &crypto, &storage, 1, 0xFFF1, "HA").is_err(),
                "corrupt server.json was accepted"
            );
            // The one assertion that matters: nothing was rewritten.
            assert_eq!(std::fs::read(&server).unwrap(), corrupt);
            assert!(matter.with_state(|s| s.fabrics.iter().next().is_none()));
        }

        // ...and the original file still works, i.e. corruption was survivable.
        std::fs::write(&server, &good).unwrap();
        let (restored, _) = ensure_identity(init(&M4), &crypto, &storage, 1, 0xFFF1, "HA").unwrap();
        assert_eq!(restored.rcac_tlv, id.rcac_tlv);
        assert_eq!(restored.ca_private_key, id.ca_private_key);
    }

    /// The stored scalar is the corrupt half when it disagrees with what the
    /// stored certificates derive: the derived value wins and gets persisted,
    /// because it is what `server_info` reports to Home Assistant.
    #[test]
    fn wrong_stored_compressed_fabric_id_is_corrected_and_persisted() {
        static M1: StaticCell<Matter> = StaticCell::new();
        static M2: StaticCell<Matter> = StaticCell::new();
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::open(dir.path()).unwrap();
        let crypto = default_crypto(rand::thread_rng(), DAC_PRIVKEY);

        let (id1, _) = ensure_identity(init(&M1), &crypto, &storage, 1, 0xFFF1, "HA").unwrap();

        let mut tampered = storage.load_identity().unwrap().unwrap();
        tampered.compressed_fabric_id = 0;
        storage.save_identity(&tampered).unwrap();

        let (id2, _) = ensure_identity(init(&M2), &crypto, &storage, 1, 0xFFF1, "HA").unwrap();
        assert_eq!(id2.compressed_fabric_id, id1.compressed_fabric_id);
        assert_eq!(
            storage.load_identity().unwrap().unwrap().compressed_fabric_id,
            id1.compressed_fabric_id
        );
    }

    /// Nothing in `ensure_identity` reads `ca_private_key` back, so without this
    /// a regression in its canon form would only surface as an opaque crypto
    /// error at the first commissioning. This is Task 15's dependency, pinned.
    #[test]
    fn persisted_ca_key_still_signs_a_device_noc() {
        static M: StaticCell<Matter> = StaticCell::new();
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::open(dir.path()).unwrap();
        let crypto = default_crypto(rand::thread_rng(), DAC_PRIVKEY);
        ensure_identity(init(&M), &crypto, &storage, 1, 0xFFF1, "HA").unwrap();

        // Simulated restart: the CA comes off disk, exactly as ops/commission will.
        let stored = storage.load_identity().unwrap().unwrap();
        let ca_key = canon_secret_key(&stored.ca_private_key).unwrap();
        let mut noc_buf = [0u8; MAX_CERT_TLV_AND_ASN1_LEN];
        let mut noc_gen =
            NocGenerator::create(ca_key.reference(), &stored.rcac_tlv, &[], &mut noc_buf).unwrap();

        let device_key = crypto.generate_secret_key().unwrap();
        let mut csr_buf = [0u8; 256];
        let csr = device_key.csr(&mut csr_buf).unwrap();
        let device_noc = noc_gen.generate(&crypto, csr, 4242, &[], VALID_FOREVER).unwrap();

        // RCAC-direct: the NOC must chain straight to the persisted RCAC.
        let noc = CertRef::new(TLVElement::new(device_noc));
        let rcac = CertRef::new(TLVElement::new(&stored.rcac_tlv));
        let mut scratch = [0u8; MAX_CERT_TLV_AND_ASN1_LEN];
        noc.verify_chain_start(
            &crypto,
            UtcTime::Reliable(VALID_FOREVER.not_before as u64 * 1_000_000),
        )
        .add_cert(&rcac, &mut scratch)
        .unwrap()
        .finalise(&mut scratch)
        .unwrap();
        assert_eq!(noc.get_node_id().unwrap(), 4242);
    }

    /// `Fabric::label` is 32 BYTES while the stored label is 32 CHARS, and
    /// rs-matter clears the label before pushing — so an over-long label used to
    /// leave it empty instead of truncated.
    #[test]
    fn multibyte_fabric_label_is_truncated_not_emptied() {
        static M: StaticCell<Matter> = StaticCell::new();
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::open(dir.path()).unwrap();
        let crypto = default_crypto(rand::thread_rng(), DAC_PRIVKEY);

        let matter = init(&M);
        let label = "é".repeat(32); // 32 chars, 64 bytes: legal per normalize_fabric_label
        let (_, idx) = ensure_identity(matter, &crypto, &storage, 1, 0xFFF1, &label).unwrap();

        let installed = matter
            .with_state(|s| s.fabrics.get(idx).map(|f| f.label().to_string()))
            .unwrap();
        assert_eq!(installed, "é".repeat(16));
        assert!(installed.len() <= FABRIC_LABEL_MAX_BYTES);
    }

    /// A hand-edited `controller_node_id` would be installed as the CASE admin
    /// subject while the NOC says something else — rs-matter does not check.
    #[test]
    fn controller_node_id_must_match_the_stored_noc() {
        static M1: StaticCell<Matter> = StaticCell::new();
        static M2: StaticCell<Matter> = StaticCell::new();
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::open(dir.path()).unwrap();
        let crypto = default_crypto(rand::thread_rng(), DAC_PRIVKEY);
        ensure_identity(init(&M1), &crypto, &storage, 1, 0xFFF1, "HA").unwrap();

        let mut tampered = storage.load_identity().unwrap().unwrap();
        tampered.controller_node_id = CONTROLLER_NODE_ID + 1;
        storage.save_identity(&tampered).unwrap();
        assert!(ensure_identity(init(&M2), &crypto, &storage, 1, 0xFFF1, "HA").is_err());
    }

    #[test]
    fn canon_secret_key_rejects_wrong_lengths() {
        assert!(canon_secret_key(&[0u8; 32]).is_ok());
        assert!(canon_secret_key(&[0u8; 31]).is_err());
        assert!(canon_secret_key(&[]).is_err());
    }

    #[test]
    fn truncate_to_bytes_never_splits_a_char() {
        assert_eq!(truncate_to_bytes("abc", 32), "abc");
        assert_eq!(truncate_to_bytes("ééé", 5), "éé");
        assert_eq!(truncate_to_bytes("é", 1), "");
    }
}

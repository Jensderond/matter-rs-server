//! rs-matter-backed helpers for the one-shot fabric migration tool
//! (`crates/migrate`). The architecture confines rs-matter to this crate, and
//! `identity::ensure_identity`'s whole job is the generate-or-load decision the
//! tool bypasses — so the tool's rs-matter needs (minting a NOC against a
//! preserved CA, deriving the compressed fabric id and the operational IPK)
//! live here, behind rs-matter-free signatures. Everything is additive: no
//! existing signature, behaviour or stored format changes.

use matter_rs_controller::stack_api::{StackError, StackErrorKind};
use matter_rs_controller::storage::{ServerIdentity, IDENTITY_VERSION};

use rs_matter::cert::gen::VALID_FOREVER;
use rs_matter::cert::{CertRef, MAX_CERT_TLV_AND_ASN1_LEN};
use rs_matter::crypto::{
    default_crypto, CanonAeadKeyRef, CanonPkcSecretKey, Crypto, SecretKey, SigningSecretKey,
};
use rs_matter::dm::clusters::time_sync::UtcTime;
use rs_matter::dm::devices::test::DAC_PRIVKEY;
use rs_matter::fabric::Fabrics;
use rs_matter::group_keys::KeySet;
use rs_matter::onboard::noc::NocGenerator;
use rs_matter::tlv::TLVElement;

use crate::tlv_json::MATTER_EPOCH_OFFSET_S;

/// Map an rs-matter error to the crate-boundary type. `Error::{e}` and never
/// `{e:?}`: rs-matter is built with `backtrace`, so its `Debug` dumps a whole
/// captured backtrace (same rule as `identity.rs`).
fn sdk_err(context: &str, e: rs_matter::error::Error) -> StackError {
    StackError::new(StackErrorKind::Sdk, format!("{context}: Error::{e}"))
}

pub fn identity_from_preserved_ca(
    ca_private_key: &[u8],
    rcac_tlv: &[u8],
    fabric_id: u64,
    vendor_id: u16,
    node_id: u64,
    ipk_epoch_key: &[u8],
) -> Result<ServerIdentity, StackError> {
    let crypto = default_crypto(rand::thread_rng(), DAC_PRIVKEY);

    // Fail fast on self-inconsistency: NocGenerator takes the fabric id from
    // the RCAC, so a mismatch here would otherwise surface later as
    // self-check 1 failing with no hint of why. This is also where a DER blob
    // in rootCertBytes dies, with the certificate named as the problem.
    //
    // A real matter.js RCAC parses fine but carries NO FabricId in its
    // subject DN at all (verified in matter.js v0.17.9's
    // CertificateAuthority.ts: RCAC `subject: { rcacId }` only). That is a
    // different failure than "not a cert", so `get_fabric_id()` erroring is
    // disambiguated against `get_ca_id()` on the same cert before either is
    // reported as a parse failure.
    let cert_ref = CertRef::new(TLVElement::new(rcac_tlv));
    match cert_ref.get_fabric_id() {
        Ok(rcac_fabric_id) if rcac_fabric_id == fabric_id => {}
        Ok(rcac_fabric_id) => {
            return Err(StackError::new(
                StackErrorKind::Sdk,
                format!("the preserved RCAC carries fabric id {rcac_fabric_id}, not the requested {fabric_id}"),
            ));
        }
        Err(e) => {
            if cert_ref.get_ca_id().is_ok() {
                return Err(StackError::new(
                    StackErrorKind::Sdk,
                    "the preserved RCAC carries no FabricId in its subject DN — the normal \
                     matter.js shape. rs-matter's NocGenerator cannot yet mint a controller NOC \
                     whose issuer DN mirrors such a root (devices validate the NOC's issuer DN \
                     against the root's subject DN, so a mismatched mint would be rejected at \
                     CASE). Migration of this store is blocked on an rs-matter change; see the \
                     spec's Risks section."
                        .to_string(),
                ));
            }
            return Err(sdk_err("rcac_tlv does not parse as a Matter TLV certificate", e));
        }
    }

    let ca_key = CanonPkcSecretKey::try_from(ca_private_key)
        .map_err(|e| sdk_err("ca_private_key is not a canonical P-256 secret key", e))?;

    // Controller operational keypair -> CSR -> NOC signed DIRECTLY by the
    // preserved root (RCAC-direct, the shape ServerIdentity encodes). The old
    // matter.js ICAC is deliberately never seen: the chain still terminates at
    // the root devices trust, and the subject still matches their ACLs.
    let controller_secret_key = crypto
        .generate_secret_key()
        .map_err(|e| sdk_err("generating the controller key pair", e))?;
    let mut csr_buf = [0u8; 256];
    let csr = controller_secret_key
        .csr(&mut csr_buf)
        .map_err(|e| sdk_err("building the controller CSR", e))?;
    let mut controller_key = CanonPkcSecretKey::new();
    controller_secret_key
        .write_canon(&mut controller_key)
        .map_err(|e| sdk_err("serialising the controller key", e))?;

    let mut noc_buf = [0u8; MAX_CERT_TLV_AND_ASN1_LEN];
    let mut noc_gen = NocGenerator::create(ca_key.reference(), rcac_tlv, &[], &mut noc_buf)
        .map_err(|e| sdk_err("preparing the NOC generator over the preserved CA", e))?;
    let controller_noc = noc_gen
        .generate(&crypto, csr, node_id, &[], VALID_FOREVER)
        .map_err(|e| sdk_err("minting the controller NOC", e))?;

    // Compressed fabric id through the exact code path the server boots with
    // (Fabric::update -> compute_compressed_fabric_id), not a re-implemented
    // KDF. A standalone `Fabrics` is enough; no Matter instance involved.
    let ipk = CanonAeadKeyRef::try_new(ipk_epoch_key)
        .map_err(|e| sdk_err("ipk_epoch_key is not a 16-byte AEAD key", e))?;
    let mut fabrics = Fabrics::new();
    let compressed_fabric_id = fabrics
        .add(
            &crypto,
            controller_key.reference(),
            rcac_tlv,
            controller_noc,
            &[], // RCAC-direct: no ICAC, ever
            Some(ipk),
            vendor_id,
            node_id,
        )
        .map(|f| f.compressed_fabric_id())
        .map_err(|e| sdk_err("installing the preserved fabric", e))?;

    let identity = ServerIdentity {
        version: IDENTITY_VERSION,
        fabric_id,
        vendor_id,
        controller_node_id: node_id,
        compressed_fabric_id,
        ca_private_key: ca_private_key.to_vec(),
        rcac_tlv: rcac_tlv.to_vec(),
        controller_private_key: controller_key.access().to_vec(),
        controller_noc_tlv: controller_noc.to_vec(),
        ipk: ipk_epoch_key.to_vec(),
    };
    // Belt to the braces above: the identity we hand back is one the minted
    // NOC actually proves.
    verify_identity(&identity)?;
    Ok(identity)
}

/// Self-check 2's substance: the stored controller NOC verifies against the
/// stored RCAC, and its subject node id equals `controller_node_id`.
pub fn verify_identity(id: &ServerIdentity) -> Result<(), StackError> {
    let crypto = default_crypto(rand::thread_rng(), DAC_PRIVKEY);
    let noc = CertRef::new(TLVElement::new(&id.controller_noc_tlv));
    let rcac = CertRef::new(TLVElement::new(&id.rcac_tlv));

    let noc_node_id = noc
        .get_node_id()
        .map_err(|e| sdk_err("controller_noc_tlv does not parse as a Matter TLV certificate", e))?;
    if noc_node_id != id.controller_node_id {
        return Err(StackError::new(
            StackErrorKind::Sdk,
            format!("the minted NOC's subject node id {noc_node_id} does not match controller_node_id {}", id.controller_node_id),
        ));
    }

    // Verify at the CURRENT time, not at VALID_FOREVER.not_before: the
    // preserved RCAC was minted by matter.js with a real notBefore, which the
    // year-2000 instant our own test certs use would fall outside of.
    // UtcTime::Reliable is Matter-epoch MICROseconds.
    let now_matter_micros = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .saturating_sub(MATTER_EPOCH_OFFSET_S)
        * 1_000_000;
    let mut scratch = [0u8; MAX_CERT_TLV_AND_ASN1_LEN];
    noc.verify_chain_start(&crypto, UtcTime::Reliable(now_matter_micros))
        .add_cert(&rcac, &mut scratch)
        .map_err(|e| sdk_err("the controller NOC does not chain to the RCAC", e))?
        .finalise(&mut scratch)
        .map_err(|e| sdk_err("the RCAC did not verify as a chain root", e))?;
    Ok(())
}

/// Matter's operational-IPK derivation (KeySet::update — HKDF over the epoch
/// key, salted with the big-endian compressed fabric id). Self-check 3 uses
/// this to prove `identityProtectionKey` really is the epoch key.
pub fn derive_operational_ipk(
    ipk_epoch_key: &[u8],
    compressed_fabric_id: u64,
) -> Result<Vec<u8>, StackError> {
    let crypto = default_crypto(rand::thread_rng(), DAC_PRIVKEY);
    let epoch = CanonAeadKeyRef::try_new(ipk_epoch_key)
        .map_err(|e| sdk_err("ipk_epoch_key is not a 16-byte AEAD key", e))?;
    let mut keys = KeySet::new();
    keys.update(&crypto, epoch, &compressed_fabric_id)
        .map_err(|e| sdk_err("deriving the operational IPK", e))?;
    Ok(keys.op_key.access().to_vec())
}

/// The root's uncompressed public key point (65 bytes), for matching device
/// fabric tables by `rootPublicKey`.
pub fn rcac_public_key(rcac_tlv: &[u8]) -> Result<Vec<u8>, StackError> {
    CertRef::new(TLVElement::new(rcac_tlv))
        .pubkey()
        .map(|pk| pk.to_vec())
        .map_err(|e| sdk_err("rcac_tlv does not parse as a Matter TLV certificate", e))
}

/// Whether the RCAC's serial is already canonical DER — a migrated matter.js
/// root often is not, which trips the server's boot-time warning. The tool
/// detects it up front so the dry-run report can say it is expected and
/// harmless (every device already accepted this root).
pub fn rcac_serial_is_der_canonical(rcac_tlv: &[u8]) -> Result<bool, StackError> {
    crate::identity::serial_is_der_canonical(rcac_tlv)
        .map_err(|e| sdk_err("rcac_tlv does not parse as a Matter TLV certificate", e))
}

/// Fixture/test helper: a fresh CA in exactly the preserved shape the tool
/// reads out of a matter.js store — `(ca_private_key, rcac_tlv)`.
pub fn generate_ca(fabric_id: u64) -> Result<(Vec<u8>, Vec<u8>), StackError> {
    let crypto = default_crypto(rand::thread_rng(), DAC_PRIVKEY);
    let (ca_key, rcac) = crate::identity::generate_usable_rcac(&crypto, fabric_id)
        .map_err(|e| sdk_err("generating a CA", e))?;
    Ok((ca_key.access().to_vec(), rcac))
}

#[cfg(test)]
mod tests {
    use super::*;

    const EPOCH_KEY: [u8; 16] = [0x5a; 16];

    #[test]
    fn preserved_ca_round_trip_mints_a_verifiable_identity() {
        let (ca_key, rcac) = generate_ca(1).unwrap();
        let id = identity_from_preserved_ca(&ca_key, &rcac, 1, 0xFFF1, 112233, &EPOCH_KEY).unwrap();

        assert_eq!(id.version, matter_rs_controller::storage::IDENTITY_VERSION);
        assert_eq!(id.fabric_id, 1);
        assert_eq!(id.vendor_id, 0xFFF1);
        assert_eq!(id.controller_node_id, 112233);
        assert_ne!(id.compressed_fabric_id, 0);
        // The preserved inputs are stored verbatim...
        assert_eq!(id.ca_private_key, ca_key);
        assert_eq!(id.rcac_tlv, rcac);
        // ...and the IPK slot holds the EPOCH key (the server derives the
        // operational key itself when it installs the fabric).
        assert_eq!(id.ipk, EPOCH_KEY.to_vec());
        assert_eq!(id.controller_private_key.len(), 32);
        assert!(!id.controller_noc_tlv.is_empty());

        // Self-check 2's substance: NOC chains to the RCAC, subject is 112233.
        verify_identity(&id).unwrap();
    }

    /// The compressed fabric id is a pure KDF of (root public key, fabric id):
    /// two independent mints from the SAME CA must agree on it — that is what
    /// lets self-check 1 prove "same fabric" offline.
    #[test]
    fn compressed_fabric_id_is_deterministic_per_ca_and_fabric_id() {
        let (ca_key, rcac) = generate_ca(1).unwrap();
        let a = identity_from_preserved_ca(&ca_key, &rcac, 1, 0xFFF1, 112233, &EPOCH_KEY).unwrap();
        let b = identity_from_preserved_ca(&ca_key, &rcac, 1, 0xFFF1, 112233, &EPOCH_KEY).unwrap();
        assert_eq!(a.compressed_fabric_id, b.compressed_fabric_id);
        // Freshly minted each time, though: the operational keys differ.
        assert_ne!(a.controller_private_key, b.controller_private_key);
        assert_ne!(a.controller_noc_tlv, b.controller_noc_tlv);
        // And a different CA lands on a different fabric.
        let (ca2, rcac2) = generate_ca(1).unwrap();
        let c = identity_from_preserved_ca(&ca2, &rcac2, 1, 0xFFF1, 112233, &EPOCH_KEY).unwrap();
        assert_ne!(a.compressed_fabric_id, c.compressed_fabric_id);
    }

    /// Pin the operational-IPK KDF to rs-matter's own: what `Fabric` derives
    /// when the fabric is installed must be exactly what we derive standalone —
    /// this is what makes self-check 3 a proof rather than a re-implementation.
    #[test]
    fn derive_operational_ipk_matches_what_the_installed_fabric_derives() {
        use rs_matter::crypto::{default_crypto, CanonAeadKeyRef, CanonPkcSecretKeyRef};
        use rs_matter::dm::devices::test::DAC_PRIVKEY;
        use rs_matter::fabric::Fabrics;

        let (ca_key, rcac) = generate_ca(1).unwrap();
        let id = identity_from_preserved_ca(&ca_key, &rcac, 1, 0xFFF1, 112233, &EPOCH_KEY).unwrap();

        let ours = derive_operational_ipk(&EPOCH_KEY, id.compressed_fabric_id).unwrap();
        assert_eq!(ours.len(), 16);
        assert_ne!(ours, EPOCH_KEY.to_vec());

        let crypto = default_crypto(rand::thread_rng(), DAC_PRIVKEY);
        let mut fabrics = Fabrics::new();
        let fabric = fabrics
            .add(
                &crypto,
                CanonPkcSecretKeyRef::try_new(&id.controller_private_key).unwrap(),
                &id.rcac_tlv,
                &id.controller_noc_tlv,
                &[],
                Some(CanonAeadKeyRef::try_new(&EPOCH_KEY).unwrap()),
                0xFFF1,
                112233,
            )
            .unwrap();
        assert_eq!(fabric.compressed_fabric_id(), id.compressed_fabric_id);
        assert_eq!(fabric.ipk().op_key.access().to_vec(), ours);
    }

    #[test]
    fn a_fabric_id_that_disagrees_with_the_rcac_is_refused_up_front() {
        let (ca_key, rcac) = generate_ca(1).unwrap();
        let err = identity_from_preserved_ca(&ca_key, &rcac, 42, 0xFFF1, 112233, &EPOCH_KEY).unwrap_err();
        assert!(err.message.contains("fabric id"), "unhelpful error: {}", err.message);
    }

    /// The real matter.js shape: an RCAC whose subject DN carries a RootCaId
    /// but no FabricId at all. `get_fabric_id()` errors on this (there is
    /// nothing to find), but the cert otherwise parses fine — `get_ca_id()`
    /// finds its RootCaId. The tool must tell these apart: this is NOT the
    /// same failure as a DER blob or garbage bytes in `rootCertBytes`.
    ///
    /// Hand-built minimal TLV (see `rs-matter`'s `cert.rs`: `CertRef::subject`
    /// reads context tag 6 of the outer struct as a `List` of DN entries;
    /// each DN entry is itself a context-tagged uint, tag = `DNTag` ordinal —
    /// `RootCaId` = 20, `FabricId` = 21):
    ///
    /// ```text
    /// 0x15                 outer struct, anonymous
    ///   0x37 0x06            context tag 6 (Subject), List
    ///     0x24 0x14 0x01       context tag 20 (RootCaId), U8 value = 1
    ///   0x18                 end of list
    /// 0x18                 end of struct
    /// ```
    ///
    /// No FabricId (tag 21) DN entry is present, and no other cert field
    /// (serial, issuer, signature, ...) is needed — neither `get_fabric_id`
    /// nor `get_ca_id` reads past the subject list.
    fn rcac_with_ca_id_but_no_fabric_id() -> Vec<u8> {
        vec![0x15, 0x37, 0x06, 0x24, 0x14, 0x01, 0x18, 0x18]
    }

    #[test]
    fn a_fabricid_less_matterjs_rcac_gets_a_precise_diagnosis_not_a_parse_error() {
        let (ca_key, _) = generate_ca(1).unwrap();
        let rcac = rcac_with_ca_id_but_no_fabric_id();

        // Sanity: the cert really does parse (get_ca_id succeeds) but really
        // does lack a FabricId (get_fabric_id fails) — otherwise this test
        // would not be exercising the branch it claims to.
        let cert_ref = CertRef::new(TLVElement::new(&rcac));
        assert!(cert_ref.get_ca_id().is_ok(), "test fixture must parse as a cert");
        assert!(cert_ref.get_fabric_id().is_err(), "test fixture must lack a FabricId");

        let err = identity_from_preserved_ca(&ca_key, &rcac, 1, 0xFFF1, 112233, &EPOCH_KEY).unwrap_err();
        assert!(
            err.message.contains("no FabricId in its subject DN"),
            "unhelpful error: {}",
            err.message
        );
        assert!(
            !err.message.contains("does not parse"),
            "a fabricId-less-but-parseable RCAC must not be reported as unparseable: {}",
            err.message
        );
    }

    #[test]
    fn garbage_and_wrong_length_inputs_are_clean_errors() {
        let (ca_key, rcac) = generate_ca(1).unwrap();
        // A non-TLV rcac (e.g. what a DER blob in rootCertBytes would look like)
        // must name the certificate as the problem — this is the spec's
        // "rootCertBytes encoding is unconfirmed" risk turned into a clear error.
        let err = identity_from_preserved_ca(&ca_key, b"not a cert", 1, 0xFFF1, 112233, &EPOCH_KEY).unwrap_err();
        assert!(err.message.to_lowercase().contains("certificate"), "unhelpful error: {}", err.message);
        assert!(rcac_public_key(b"not a cert").is_err());
        assert!(identity_from_preserved_ca(&[0u8; 31], &rcac, 1, 0xFFF1, 112233, &EPOCH_KEY).is_err());
        assert!(identity_from_preserved_ca(&ca_key, &rcac, 1, 0xFFF1, 112233, &[0u8; 15]).is_err());
        assert!(derive_operational_ipk(&[0u8; 15], 1).is_err());
    }

    #[test]
    fn verify_identity_catches_a_node_id_that_does_not_match_the_noc() {
        let (ca_key, rcac) = generate_ca(1).unwrap();
        let mut id = identity_from_preserved_ca(&ca_key, &rcac, 1, 0xFFF1, 112233, &EPOCH_KEY).unwrap();
        id.controller_node_id = 112234;
        assert!(verify_identity(&id).is_err());
    }

    #[test]
    fn rcac_public_key_is_the_65_byte_uncompressed_point() {
        let (_, rcac) = generate_ca(1).unwrap();
        let pk = rcac_public_key(&rcac).unwrap();
        assert_eq!(pk.len(), 65);
        assert_eq!(pk[0], 0x04); // uncompressed-point marker
    }

    #[test]
    fn serial_canonicality_passthrough_works_on_generated_cas() {
        let (_, rcac) = generate_ca(1).unwrap();
        // generate_ca redraws until canonical, so this must be true.
        assert!(rcac_serial_is_der_canonical(&rcac).unwrap());
        assert!(rcac_serial_is_der_canonical(b"not a cert").is_err());
    }
}

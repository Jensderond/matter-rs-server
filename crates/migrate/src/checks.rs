//! The five offline self-checks (spec, "Self-checks"). All run in both modes;
//! any failure aborts before the first write. Checks 1-3 are what make booting
//! the migrated server against live devices a low-risk cutover step instead of
//! an experiment.

use crate::convert::{FabricIndexSource, NodePlan, SourceFabric};
use matter_rs_controller::storage::{ConfigData, ServerIdentity};
use matter_rs_stack::migration::{derive_operational_ipk, verify_identity};

/// One self-check's result: a stable machine name (used by callers to look up
/// a specific outcome), whether it passed, and a detail that is diagnosable on
/// its own — no other context needed to understand a failure from the report.
#[derive(Debug)]
pub struct CheckOutcome {
    pub name: &'static str,
    pub passed: bool,
    pub detail: String,
}

fn outcome(name: &'static str, passed: bool, detail: String) -> CheckOutcome {
    CheckOutcome { name, passed, detail }
}

/// Check 1: the identity we would boot with is the same fabric the source
/// store trusts. `identity.compressed_fabric_id`, big-endian, IS matter.js's
/// `operationalId` — a pure function of (root public key, fabric id), so
/// agreement here means "same fabric", full stop.
fn check_fabric_identity(identity: &ServerIdentity, source: &SourceFabric) -> CheckOutcome {
    let ours = identity.compressed_fabric_id.to_be_bytes();
    let passed = ours[..] == source.operational_id[..];
    let detail = format!(
        "ours {:016x} vs source {}",
        identity.compressed_fabric_id,
        hex::encode(&source.operational_id)
    );
    outcome("fabric-identity", passed, detail)
}

/// Check 2: the minted controller NOC verifies against the preserved RCAC, and
/// its subject is the same admin node id the source fabric already used —
/// otherwise every device's existing ACL for the controller would reject it.
fn check_admin_identity(identity: &ServerIdentity, source: &SourceFabric) -> CheckOutcome {
    match verify_identity(identity) {
        Ok(()) if identity.controller_node_id == source.controller_node_id => outcome(
            "admin-identity",
            true,
            format!("controller node id {} verifies against the RCAC", identity.controller_node_id),
        ),
        Ok(()) => outcome(
            "admin-identity",
            false,
            format!(
                "identity.controller_node_id {} != source.controller_node_id {}",
                identity.controller_node_id, source.controller_node_id
            ),
        ),
        Err(e) => outcome(
            "admin-identity",
            false,
            format!("{:?}: {}", e.kind, e.message),
        ),
    }
}

/// Check 3: `identityProtectionKey` really is the epoch key, not the already-
/// derived operational key matter.js also stores next to it — the spec's exact
/// worry, since either looks plausible without this proof.
fn check_ipk_choice(identity: &ServerIdentity, source: &SourceFabric) -> CheckOutcome {
    match derive_operational_ipk(&identity.ipk, identity.compressed_fabric_id) {
        Ok(derived) if derived == source.operational_ipk => {
            outcome("ipk-choice", true, "derived operational IPK matches the source's".to_string())
        }
        Ok(derived) => outcome(
            "ipk-choice",
            false,
            format!(
                "derived operational IPK {} != source's operational_ipk {}",
                hex::encode(&derived),
                hex::encode(&source.operational_ipk)
            ),
        ),
        Err(e) => outcome("ipk-choice", false, format!("{:?}: {}", e.kind, e.message)),
    }
}

/// Check 4: `next_node_id` cannot collide with any commissioned node, and the
/// planned file count matches the commissioned-node count (by construction
/// here; the write path re-verifies this against what actually landed on
/// disk).
fn check_node_accounting(config: &ConfigData, nodes: &[NodePlan]) -> CheckOutcome {
    let colliding: Vec<u64> = nodes
        .iter()
        .map(|n| n.record.node_id)
        .filter(|id| *id >= config.next_node_id)
        .collect();
    let passed = colliding.is_empty();
    let detail = if passed {
        format!("next_node_id {} is past every one of {} planned node(s)", config.next_node_id, nodes.len())
    } else {
        format!(
            "next_node_id {} would collide with commissioned node id(s) {:?}",
            config.next_node_id, colliding
        )
    };
    outcome("node-accounting", passed, detail)
}

/// Check 5: every planned node's fabric index either really was matched
/// against our root public key, or is the fallback 0 — and every fallback is
/// listed with its reason, because that list is the operator's warning that
/// removing those devices leaves our fabric behind on them. A non-zero index
/// paired with a `FallbackZero` source is impossible by construction in
/// `convert`; this check exists to catch a future refactor breaking that
/// invariant.
fn check_fabric_index_sanity(nodes: &[NodePlan]) -> CheckOutcome {
    let mut fallbacks: Vec<String> = Vec::new();
    let mut inconsistent: Vec<String> = Vec::new();
    for n in nodes {
        match &n.fabric_index {
            FabricIndexSource::MatchedByRootPublicKey => {
                if n.record.device_fabric_index == 0 {
                    inconsistent.push(format!(
                        "node {} is MatchedByRootPublicKey but device_fabric_index is 0",
                        n.record.node_id
                    ));
                }
            }
            FabricIndexSource::FallbackZero(reason) => {
                if n.record.device_fabric_index != 0 {
                    inconsistent.push(format!(
                        "node {} has device_fabric_index {} but its fabric_index source is FallbackZero ({reason})",
                        n.record.node_id, n.record.device_fabric_index
                    ));
                } else {
                    fallbacks.push(format!("node {} — FALLBACK 0 — {reason}", n.record.node_id));
                }
            }
        }
    }
    let passed = inconsistent.is_empty();
    let detail = if !inconsistent.is_empty() {
        inconsistent.join("; ")
    } else if fallbacks.is_empty() {
        "every planned node's fabric index was matched by root public key".to_string()
    } else {
        fallbacks.join("; ")
    };
    outcome("fabric-index-sanity", passed, detail)
}

/// Run every self-check, in spec order. Always all five, even after an early
/// failure — the report is meant to be read whole, not stopped at the first
/// red line.
pub fn run_all(
    identity: &ServerIdentity,
    source: &SourceFabric,
    config: &ConfigData,
    nodes: &[NodePlan],
) -> Vec<CheckOutcome> {
    vec![
        check_fabric_identity(identity, source),
        check_admin_identity(identity, source),
        check_ipk_choice(identity, source),
        check_node_accounting(config, nodes),
        check_fabric_index_sanity(nodes),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::convert::{FabricIndexSource, NodePlan, SourceFabric};
    use matter_rs_controller::storage::{ConfigData, NodeRecord};
    use matter_rs_stack::migration::{derive_operational_ipk, generate_ca, identity_from_preserved_ca};

    const EPOCH_KEY: [u8; 16] = [0x5a; 16];

    /// A fully self-consistent (identity, source) pair — what a correct
    /// migration of a healthy store produces.
    fn consistent_pair() -> (matter_rs_controller::storage::ServerIdentity, SourceFabric) {
        let (ca_key, rcac) = generate_ca(1).unwrap();
        let identity = identity_from_preserved_ca(&ca_key, &rcac, 1, 0xFFF1, 112233, &EPOCH_KEY).unwrap();
        let source = SourceFabric {
            fabric_id: 1,
            controller_node_id: 112233,
            vendor_id: 0xFFF1,
            label: String::new(),
            ipk_epoch_key: EPOCH_KEY.to_vec(),
            operational_ipk: derive_operational_ipk(&EPOCH_KEY, identity.compressed_fabric_id).unwrap(),
            operational_id: identity.compressed_fabric_id.to_be_bytes().to_vec(),
            ca_private_key: ca_key,
            rcac_tlv: rcac,
        };
        (identity, source)
    }

    fn node(id: u64, dfi: u8, src: FabricIndexSource) -> NodePlan {
        NodePlan {
            record: NodeRecord {
                node_id: id,
                date_commissioned: "d".into(),
                last_interview: "d".into(),
                device_fabric_index: dfi,
                addresses: vec![],
                attributes: serde_json::Map::new(),
            },
            fabric_index: src,
        }
    }

    fn all_passed(outcomes: &[CheckOutcome]) -> bool {
        outcomes.iter().all(|c| c.passed)
    }

    #[test]
    fn a_consistent_migration_passes_all_five_checks() {
        let (identity, source) = consistent_pair();
        let nodes = vec![
            node(10, 3, FabricIndexSource::MatchedByRootPublicKey),
            node(23, 0, FabricIndexSource::FallbackZero("no cache".into())),
        ];
        let config = ConfigData { next_node_id: 24, ..ConfigData::default() };
        let outcomes = run_all(&identity, &source, &config, &nodes);
        assert_eq!(outcomes.len(), 5);
        assert!(all_passed(&outcomes), "{outcomes:?}");
        // Check 5's detail is the operator's warning list.
        let sanity = outcomes.iter().find(|c| c.name == "fabric-index-sanity").unwrap();
        assert!(sanity.detail.contains("23"), "fallback node not listed: {}", sanity.detail);
    }

    #[test]
    fn check_1_catches_a_different_fabric() {
        let (identity, mut source) = consistent_pair();
        source.operational_id[0] ^= 0xFF;
        let outcomes = run_all(&identity, &source, &ConfigData::default(), &[]);
        let c = outcomes.iter().find(|c| c.name == "fabric-identity").unwrap();
        assert!(!c.passed);
        assert!(c.detail.contains(&hex::encode(&source.operational_id)), "{}", c.detail);
    }

    /// The spec's exact worry for check 3: the OTHER stored key looks just as
    /// plausible, and picking it produces a fabric that looks correct and then
    /// fails subtly. Swap them; the check must fail.
    #[test]
    fn check_3_catches_the_wrong_ipk_choice() {
        let (ca_key, rcac) = generate_ca(1).unwrap();
        let good = identity_from_preserved_ca(&ca_key, &rcac, 1, 0xFFF1, 112233, &EPOCH_KEY).unwrap();
        let operational = derive_operational_ipk(&EPOCH_KEY, good.compressed_fabric_id).unwrap();
        // The tool "chose" the operational key as the epoch key:
        let wrong =
            identity_from_preserved_ca(&ca_key, &rcac, 1, 0xFFF1, 112233, &operational).unwrap();
        let source = SourceFabric {
            fabric_id: 1,
            controller_node_id: 112233,
            vendor_id: 0xFFF1,
            label: String::new(),
            ipk_epoch_key: operational.clone(), // the wrong choice, propagated
            operational_ipk: operational,
            operational_id: good.compressed_fabric_id.to_be_bytes().to_vec(),
            ca_private_key: ca_key,
            rcac_tlv: rcac,
        };
        let outcomes = run_all(&wrong, &source, &ConfigData::default(), &[]);
        assert!(!outcomes.iter().find(|c| c.name == "ipk-choice").unwrap().passed);
    }

    #[test]
    fn check_2_catches_a_foreign_node_id() {
        let (mut identity, source) = consistent_pair();
        identity.controller_node_id = 999;
        let outcomes = run_all(&identity, &source, &ConfigData::default(), &[]);
        assert!(!outcomes.iter().find(|c| c.name == "admin-identity").unwrap().passed);
    }

    #[test]
    fn check_4_catches_a_next_node_id_that_would_collide() {
        let (identity, source) = consistent_pair();
        let nodes = vec![node(23, 0, FabricIndexSource::FallbackZero("x".into()))];
        let config = ConfigData { next_node_id: 23, ..ConfigData::default() };
        let outcomes = run_all(&identity, &source, &config, &nodes);
        assert!(!outcomes.iter().find(|c| c.name == "node-accounting").unwrap().passed);
    }

    #[test]
    fn check_5_catches_a_nonzero_index_that_was_not_matched() {
        let (identity, source) = consistent_pair();
        // Impossible by construction in convert — which is why the check exists.
        let nodes = vec![node(10, 1, FabricIndexSource::FallbackZero("but wrote 1?!".into()))];
        let outcomes = run_all(&identity, &source, &ConfigData { next_node_id: 11, ..ConfigData::default() }, &nodes);
        assert!(!outcomes.iter().find(|c| c.name == "fabric-index-sanity").unwrap().passed);
    }
}

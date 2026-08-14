//! The spec's mapping table, executable. Reads replayed matter.js state and
//! produces the inputs for `server.json` / `config.json` / `nodes/<id>.json`.
//! Identity fields are strict (a broken fabric must not migrate); the
//! per-device fabric-index match is best-effort with a loud fallback to 0
//! (invalid in Matter, so RemoveFabric(0) is rejected instead of evicting
//! someone else's admin — fail safe, never plausible).

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::Value;

use crate::decode;
use crate::jsdb::JsDb;
use matter_rs_controller::addr::ip_of;
use matter_rs_controller::storage::{format_node_date, normalize_fabric_label, ConfigData, NodeRecord};

#[derive(Debug, thiserror::Error)]
pub enum ConvertError {
    #[error("missing {0} in the matter.js store")]
    Missing(&'static str),
    #[error("{context}: {source}")]
    Decode {
        context: String,
        #[source]
        source: crate::decode::DecodeError,
    },
    #[error("{0}")]
    Invalid(String),
}

/// The source fabric's identity: everything `server.json` needs, plus the
/// root CA key pair. Read strictly — a broken fabric must not migrate.
pub struct SourceFabric {
    pub fabric_id: u64,
    pub controller_node_id: u64, // fabric.nodeId (112233 on the reference install)
    pub vendor_id: u16,          // fabric.rootVendorId
    pub label: String,           // fabric.label ("" on the reference install)
    pub ipk_epoch_key: Vec<u8>,  // fabric.identityProtectionKey (16 bytes)
    pub operational_ipk: Vec<u8>, // fabric.operationalIdentityProtectionKey (16 bytes; check-3 oracle)
    pub operational_id: Vec<u8>, // fabric.operationalId (8 bytes; check-1 oracle)
    pub ca_private_key: Vec<u8>, // certificates.rootKeyPair.privateKey (32 bytes)
    pub rcac_tlv: Vec<u8>,       // certificates.rootCertBytes
}

/// Hand-written, NOT derived: mirrors `storage::ServerIdentity`'s rationale —
/// `ipk_epoch_key`, `operational_ipk` and `ca_private_key` are the fabric's
/// trust anchor, and a derived `Debug` (reachable from a stray `{:?}`, or from
/// `unwrap_err`'s panic message on a test failure) would print them byte by
/// byte. The certificate is public, so it is summarised by length only for
/// symmetry, not because it is secret.
impl std::fmt::Debug for SourceFabric {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceFabric")
            .field("fabric_id", &self.fabric_id)
            .field("controller_node_id", &self.controller_node_id)
            .field("vendor_id", &self.vendor_id)
            .field("label", &self.label)
            .field("ipk_epoch_key", &format_args!("[redacted; {} bytes]", self.ipk_epoch_key.len()))
            .field("operational_ipk", &format_args!("[redacted; {} bytes]", self.operational_ipk.len()))
            .field("operational_id", &format_args!("[{} bytes]", self.operational_id.len()))
            .field("ca_private_key", &format_args!("[redacted; {} bytes]", self.ca_private_key.len()))
            .field("rcac_tlv", &format_args!("[{} bytes]", self.rcac_tlv.len()))
            .finish()
    }
}

/// Where a node's `device_fabric_index` came from: an exact match against our
/// root public key in the node's cached Operational Credentials attribute, or
/// a fallback to 0 with a full-sentence reason. A matching failure is never an
/// abort and never a guess.
#[derive(Debug, Clone, PartialEq)]
pub enum FabricIndexSource {
    MatchedByRootPublicKey,
    FallbackZero(String),
}

pub struct NodePlan {
    pub record: NodeRecord,
    pub fabric_index: FabricIndexSource,
}

/// Fetch a required field from a JSON object, naming the full dotted path on
/// absence.
fn need<'a>(
    obj: &'a serde_json::Map<String, Value>,
    key: &str,
    full: &'static str,
) -> Result<&'a Value, ConvertError> {
    obj.get(key).ok_or(ConvertError::Missing(full))
}

fn as_object<'a>(v: &'a Value, full: &'static str) -> Result<&'a serde_json::Map<String, Value>, ConvertError> {
    v.as_object()
        .ok_or_else(|| ConvertError::Invalid(format!("{full} is not an object")))
}

fn u64_field(v: &Value, path: &'static str) -> Result<u64, ConvertError> {
    decode::as_u64(v).map_err(|source| ConvertError::Decode { context: path.to_string(), source })
}

fn bytes_field(v: &Value, path: &'static str) -> Result<Vec<u8>, ConvertError> {
    decode::as_bytes(v).map_err(|source| ConvertError::Decode { context: path.to_string(), source })
}

fn str_field<'a>(v: &'a Value, path: &'static str) -> Result<&'a str, ConvertError> {
    decode::as_str(v).map_err(|source| ConvertError::Decode { context: path.to_string(), source })
}

/// `Invalid`, naming the field and both lengths (expected vs. got).
fn check_len(bytes: &[u8], expected: usize, field: &'static str) -> Result<(), ConvertError> {
    if bytes.len() != expected {
        return Err(ConvertError::Invalid(format!(
            "{field} must be {expected} bytes, got {}",
            bytes.len()
        )));
    }
    Ok(())
}

/// Read the source fabric's identity out of the replayed matter.js store.
/// Strict: every missing context/field names its full dotted path, and every
/// wrong-length key is refused rather than migrated.
pub fn read_source_fabric(db: &JsDb) -> Result<SourceFabric, ConvertError> {
    let credentials = db.get("credentials").ok_or(ConvertError::Missing("credentials"))?;
    let fabric = need(credentials, "fabric", "credentials.fabric")?;
    let fabric = as_object(fabric, "credentials.fabric")?;

    let fabric_id = u64_field(
        need(fabric, "fabricId", "credentials.fabric.fabricId")?,
        "credentials.fabric.fabricId",
    )?;
    let controller_node_id = u64_field(
        need(fabric, "nodeId", "credentials.fabric.nodeId")?,
        "credentials.fabric.nodeId",
    )?;
    let root_vendor_id = u64_field(
        need(fabric, "rootVendorId", "credentials.fabric.rootVendorId")?,
        "credentials.fabric.rootVendorId",
    )?;
    let vendor_id: u16 = u16::try_from(root_vendor_id).map_err(|_| {
        ConvertError::Invalid(format!(
            "credentials.fabric.rootVendorId {root_vendor_id} does not fit in a u16"
        ))
    })?;

    let label = match fabric.get("label") {
        Some(v) => str_field(v, "credentials.fabric.label")?.to_string(),
        None => String::new(),
    };

    let ipk_epoch_key = bytes_field(
        need(fabric, "identityProtectionKey", "credentials.fabric.identityProtectionKey")?,
        "credentials.fabric.identityProtectionKey",
    )?;
    check_len(&ipk_epoch_key, 16, "credentials.fabric.identityProtectionKey")?;

    let operational_ipk = bytes_field(
        need(
            fabric,
            "operationalIdentityProtectionKey",
            "credentials.fabric.operationalIdentityProtectionKey",
        )?,
        "credentials.fabric.operationalIdentityProtectionKey",
    )?;
    check_len(&operational_ipk, 16, "credentials.fabric.operationalIdentityProtectionKey")?;

    let operational_id = bytes_field(
        need(fabric, "operationalId", "credentials.fabric.operationalId")?,
        "credentials.fabric.operationalId",
    )?;
    check_len(&operational_id, 8, "credentials.fabric.operationalId")?;

    let certificates = db.get("certificates").ok_or(ConvertError::Missing("certificates"))?;
    let root_key_pair = need(certificates, "rootKeyPair", "certificates.rootKeyPair")?;
    let root_key_pair = as_object(root_key_pair, "certificates.rootKeyPair")?;
    let ca_private_key = bytes_field(
        need(root_key_pair, "privateKey", "certificates.rootKeyPair.privateKey")?,
        "certificates.rootKeyPair.privateKey",
    )?;
    check_len(&ca_private_key, 32, "certificates.rootKeyPair.privateKey")?;

    let rcac_tlv = bytes_field(
        need(certificates, "rootCertBytes", "certificates.rootCertBytes")?,
        "certificates.rootCertBytes",
    )?;

    Ok(SourceFabric {
        fabric_id,
        controller_node_id,
        vendor_id,
        label,
        ipk_epoch_key,
        operational_ipk,
        operational_id,
        ca_private_key,
        rcac_tlv,
    })
}

/// Match a node's cached Operational Credentials `fabrics` attribute
/// (`nodes.peer{node_id}.endpoints.0.62` field `"1"`) against our root public
/// key. Best-effort: zero, multiple, out-of-range, or unparseable results all
/// fall back to index 0 with a full-sentence reason — never an abort, never a
/// guess.
fn match_fabric_index(db: &JsDb, node_id: u64, root_public_key: &[u8]) -> (u8, FabricIndexSource) {
    let context = format!("nodes.peer{node_id}.endpoints.0.62");
    let Some(fabrics_value) = db.field(&context, "1") else {
        return (
            0,
            FabricIndexSource::FallbackZero(format!(
                "no cached fabrics attribute at {context} — removing this device will leave our fabric behind on it"
            )),
        );
    };
    let Some(array) = fabrics_value.as_array() else {
        return (
            0,
            FabricIndexSource::FallbackZero(format!(
                "cached fabrics attribute at {context} is not an array — removing this device will leave our fabric behind on it"
            )),
        );
    };

    let mut matches: Vec<u64> = Vec::new();
    for entry in array {
        let Some(obj) = entry.as_object() else { continue };
        let Some(pk_value) = obj.get("rootPublicKey") else { continue };
        let Ok(pk) = decode::as_bytes(pk_value) else { continue };
        if pk != root_public_key {
            continue;
        }
        let Some(index_value) = obj.get("fabricIndex") else { continue };
        let Ok(index) = decode::as_u64(index_value) else { continue };
        matches.push(index);
    }

    match matches.len() {
        1 => {
            let index = matches[0];
            if (1..=254).contains(&index) {
                (index as u8, FabricIndexSource::MatchedByRootPublicKey)
            } else {
                (
                    0,
                    FabricIndexSource::FallbackZero(format!(
                        "the cached fabric entry matching our root public key has an out-of-range fabricIndex {index}"
                    )),
                )
            }
        }
        0 => (
            0,
            FabricIndexSource::FallbackZero(
                "no fabric in the cached table carries our root public key".to_string(),
            ),
        ),
        n => (
            0,
            FabricIndexSource::FallbackZero(format!(
                "{n} fabrics in the cached table carry our root public key; refusing to pick"
            )),
        ),
    }
}

/// Build the planned `NodeRecord`s from `nodes.commissionedNodes`. An absent
/// field or context is a fabric with zero commissioned nodes — legal, not an
/// error. Sorted by `node_id` ascending for deterministic reports and files.
pub fn plan_nodes(db: &JsDb, root_public_key: &[u8]) -> Result<Vec<NodePlan>, ConvertError> {
    let Some(nodes_ctx) = db.get("nodes") else { return Ok(Vec::new()) };
    let Some(commissioned) = nodes_ctx.get("commissionedNodes") else { return Ok(Vec::new()) };

    let entries = decode::as_map_entries(commissioned).map_err(|source| ConvertError::Decode {
        context: "nodes.commissionedNodes".to_string(),
        source,
    })?;

    let mut plans = Vec::with_capacity(entries.len());
    for (key, value) in entries {
        let node_id = u64_field(&key, "nodes.commissionedNodes[key]")?;
        let obj = as_object(&value, "nodes.commissionedNodes[value]")?;

        let discovered_ms = match obj.get("discoveryData").and_then(|d| d.get("discoveredAt")) {
            Some(v) => Some(u64_field(v, "nodes.commissionedNodes[value].discoveryData.discoveredAt")?),
            None => None,
        };
        let date = match discovered_ms {
            Some(ms) => format_node_date(UNIX_EPOCH + Duration::from_millis(ms)),
            None => format_node_date(SystemTime::now()),
        };

        let addresses = match obj.get("operationalServerAddress").and_then(|a| a.get("ip")) {
            Some(v) => {
                let ip = str_field(v, "nodes.commissionedNodes[value].operationalServerAddress.ip")?;
                vec![ip_of(ip)]
            }
            None => Vec::new(),
        };

        let (device_fabric_index, fabric_index) = match_fabric_index(db, node_id, root_public_key);

        plans.push(NodePlan {
            record: NodeRecord {
                node_id,
                date_commissioned: date.clone(),
                last_interview: date,
                device_fabric_index,
                addresses,
                attributes: serde_json::Map::new(),
            },
            fabric_index,
        });
    }

    plans.sort_by_key(|p| p.record.node_id);
    Ok(plans)
}

/// Build the target `config.json` contents from the source fabric and the
/// planned nodes.
pub fn config_from(source: &SourceFabric, nodes: &[NodePlan]) -> ConfigData {
    let fabric_label = normalize_fabric_label(Some(&source.label));
    let next_node_id = nodes.iter().map(|p| p.record.node_id).max().map_or(1, |max| max + 1);
    ConfigData {
        fabric_label,
        next_node_id,
        wifi_credentials: std::collections::BTreeMap::new(),
        thread_datasets: std::collections::BTreeMap::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jsdb::JsDb;
    use serde_json::{json, Map, Value};
    use std::collections::BTreeMap;

    fn bigint(n: u64) -> Value {
        Value::String(format!("{{\"__object__\":\"BigInt\",\"__value__\":\"{n}\"}}"))
    }
    fn bytes(b: &[u8]) -> Value {
        Value::String(format!("{{\"__object__\":\"Uint8Array\",\"__value__\":\"{}\"}}", hex::encode(b)))
    }
    fn map_tag(entries: Vec<(Value, Value)>) -> Value {
        let pairs: Vec<Value> = entries.into_iter().map(|(k, v)| json!([k, v])).collect();
        let inner = serde_json::to_string(&Value::Array(pairs)).unwrap();
        Value::String(format!(
            "{{\"__object__\":\"Map\",\"__value__\":{}}}",
            serde_json::to_string(&inner).unwrap()
        ))
    }
    fn obj(v: Value) -> Map<String, Value> {
        v.as_object().unwrap().clone()
    }

    /// 65-byte uncompressed-point stand-in; matching is byte equality, so no
    /// real crypto is needed at this layer.
    fn root_pk(fill: u8) -> Vec<u8> {
        let mut pk = vec![0x04];
        pk.extend_from_slice(&[fill; 64]);
        pk
    }

    fn fabric_entry(pk: &[u8], index: u64, label: &str) -> Value {
        json!({
            "rootPublicKey": bytes(pk),
            "vendorId": 4996,
            "fabricId": bigint(1),
            "nodeId": bigint(112233),
            "label": label,
            "fabricIndex": index,
        })
    }

    fn store_with_fabric() -> BTreeMap<String, Map<String, Value>> {
        let mut data = BTreeMap::new();
        data.insert("credentials".to_string(), obj(json!({
            "fabric": {
                "fabricId": bigint(1),
                "nodeId": bigint(112233),
                "rootVendorId": 65521,
                "identityProtectionKey": bytes(&[0x11; 16]),
                "operationalIdentityProtectionKey": bytes(&[0x22; 16]),
                "operationalId": bytes(&[0xca, 0x88, 0xe6, 0x79, 0xa3, 0x50, 0x5b, 0x0a]),
                "label": "",
            }
        })));
        data.insert("certificates".to_string(), obj(json!({
            "rootKeyPair": {"privateKey": bytes(&[0x33; 32]), "publicKey": bytes(&root_pk(0xAA))},
            "rootCertBytes": bytes(&[0x15, 0x30, 0x01, 0x08]),
        })));
        data
    }

    #[test]
    fn source_fabric_reads_every_field_with_the_right_types() {
        let db = JsDb::from_data(store_with_fabric());
        let s = read_source_fabric(&db).unwrap();
        assert_eq!(s.fabric_id, 1);
        assert_eq!(s.controller_node_id, 112233);
        assert_eq!(s.vendor_id, 0xFFF1);
        assert_eq!(s.label, "");
        assert_eq!(s.ipk_epoch_key, vec![0x11; 16]);
        assert_eq!(s.operational_ipk, vec![0x22; 16]);
        assert_eq!(s.operational_id, vec![0xca, 0x88, 0xe6, 0x79, 0xa3, 0x50, 0x5b, 0x0a]);
        assert_eq!(s.ca_private_key, vec![0x33; 32]);
        assert_eq!(s.rcac_tlv, vec![0x15, 0x30, 0x01, 0x08]);
    }

    #[test]
    fn missing_fields_are_named_with_their_full_path() {
        let mut data = store_with_fabric();
        data.get_mut("credentials").unwrap().remove("fabric");
        let err = read_source_fabric(&JsDb::from_data(data)).unwrap_err();
        assert!(err.to_string().contains("credentials.fabric"), "{err}");

        let err = read_source_fabric(&JsDb::from_data(BTreeMap::new())).unwrap_err();
        assert!(err.to_string().contains("credentials"), "{err}");

        let mut data = store_with_fabric();
        data.get_mut("certificates").unwrap().remove("rootCertBytes");
        let err = read_source_fabric(&JsDb::from_data(data)).unwrap_err();
        assert!(err.to_string().contains("rootCertBytes"), "{err}");
    }

    #[test]
    fn wrong_key_lengths_are_refused() {
        for (field, len) in [
            ("identityProtectionKey", 15usize),
            ("operationalIdentityProtectionKey", 17),
            ("operationalId", 7),
        ] {
            let mut data = store_with_fabric();
            let fabric = data.get_mut("credentials").unwrap().get_mut("fabric").unwrap();
            fabric.as_object_mut().unwrap().insert(field.into(), bytes(&vec![0u8; len]));
            let err = read_source_fabric(&JsDb::from_data(data)).unwrap_err();
            assert!(err.to_string().contains(field), "for {field}: {err}");
        }
    }

    fn commissioned(node_id: u64, addr_ip: Option<&str>, discovered_ms: Option<u64>) -> (Value, Value) {
        let mut v = json!({"deviceData": {}});
        if let Some(ms) = discovered_ms {
            v["discoveryData"] = json!({"discoveredAt": ms});
        }
        if let Some(ip) = addr_ip {
            v["operationalServerAddress"] = json!({"type": "udp", "ip": ip, "port": 5540});
        }
        (bigint(node_id), v)
    }

    fn store_with_nodes() -> BTreeMap<String, Map<String, Value>> {
        let mut data = store_with_fabric();
        data.insert("nodes".to_string(), obj(json!({
            "commissionedNodes": map_tag(vec![
                commissioned(10, Some("192.168.1.60"), Some(1_699_999_999_999)),
                commissioned(22, Some("[fe80::1%eth1]"), Some(1_700_000_000_000)),
                commissioned(23, None, None),
            ]),
        })));
        // node 10: three fabrics cached; OURS (root pk 0xAA) at index 3 — the
        // spec's peer1 scenario, where guessing "1" would evict "Mijn huis".
        data.insert("nodes.peer10.endpoints.0.62".to_string(), obj(json!({
            "1": [
                fabric_entry(&root_pk(0xBB), 1, "Mijn huis"),
                fabric_entry(&root_pk(0xCC), 2, ""),
                fabric_entry(&root_pk(0xAA), 3, "HomeAssistant"),
            ],
        })));
        // node 22: cache exists but no entry matches our root -> fallback 0.
        data.insert("nodes.peer22.endpoints.0.62".to_string(), obj(json!({
            "1": [fabric_entry(&root_pk(0xBB), 1, "Mijn huis")],
        })));
        // node 23: no cache at all -> fallback 0.
        data
    }

    #[test]
    fn nodes_map_dates_addresses_and_matched_fabric_indices() {
        let db = JsDb::from_data(store_with_nodes());
        let plans = plan_nodes(&db, &root_pk(0xAA)).unwrap();
        assert_eq!(plans.iter().map(|p| p.record.node_id).collect::<Vec<_>>(), vec![10, 22, 23]);

        let n10 = &plans[0];
        assert_eq!(n10.record.device_fabric_index, 3);
        assert_eq!(n10.fabric_index, FabricIndexSource::MatchedByRootPublicKey);
        assert_eq!(n10.record.addresses, vec!["192.168.1.60".to_string()]);
        assert_eq!(n10.record.date_commissioned, n10.record.last_interview);
        // format_node_date shape: local time, ".SSS000" tail, no timezone.
        assert!(n10.record.date_commissioned.ends_with("000"));
        assert!(n10.record.attributes.is_empty());

        let n22 = &plans[1];
        assert_eq!(n22.record.device_fabric_index, 0);
        assert!(matches!(&n22.fabric_index, FabricIndexSource::FallbackZero(r) if r.contains("root public key")));
        // bracket-free per controller::addr, scope id kept
        assert_eq!(n22.record.addresses, vec!["fe80::1%eth1".to_string()]);

        let n23 = &plans[2];
        assert_eq!(n23.record.device_fabric_index, 0);
        assert!(matches!(&n23.fabric_index, FabricIndexSource::FallbackZero(_)));
        assert!(n23.record.addresses.is_empty());
    }

    /// Never a value that was inferred: several matches is as disqualifying
    /// as none, and an out-of-range fabricIndex cannot be "clamped" into use.
    #[test]
    fn ambiguous_or_invalid_matches_fall_back_to_zero() {
        let mut data = store_with_nodes();
        data.insert("nodes.peer10.endpoints.0.62".to_string(), obj(json!({
            "1": [fabric_entry(&root_pk(0xAA), 2, "a"), fabric_entry(&root_pk(0xAA), 3, "b")],
        })));
        let plans = plan_nodes(&JsDb::from_data(data), &root_pk(0xAA)).unwrap();
        assert_eq!(plans[0].record.device_fabric_index, 0);
        assert!(matches!(&plans[0].fabric_index, FabricIndexSource::FallbackZero(_)));

        for bad_index in [0u64, 255, 300] {
            let mut data = store_with_nodes();
            data.insert("nodes.peer10.endpoints.0.62".to_string(), obj(json!({
                "1": [fabric_entry(&root_pk(0xAA), bad_index, "x")],
            })));
            let plans = plan_nodes(&JsDb::from_data(data), &root_pk(0xAA)).unwrap();
            assert_eq!(plans[0].record.device_fabric_index, 0, "for index {bad_index}");
        }

        // An unparseable cache entry is a fallback REASON, not an abort.
        let mut data = store_with_nodes();
        data.insert("nodes.peer10.endpoints.0.62".to_string(), obj(json!({
            "1": [{"rootPublicKey": "not tagged", "fabricIndex": 3}],
        })));
        let plans = plan_nodes(&JsDb::from_data(data), &root_pk(0xAA)).unwrap();
        assert_eq!(plans[0].record.device_fabric_index, 0);
    }

    #[test]
    fn no_commissioned_nodes_is_a_valid_empty_fabric() {
        let db = JsDb::from_data(store_with_fabric());
        assert!(plan_nodes(&db, &root_pk(0xAA)).unwrap().is_empty());
    }

    #[test]
    fn config_gets_the_default_label_and_the_next_node_id_arithmetic() {
        let db = JsDb::from_data(store_with_nodes());
        let source = read_source_fabric(&db).unwrap();
        let nodes = plan_nodes(&db, &root_pk(0xAA)).unwrap();
        let cfg = config_from(&source, &nodes);
        assert_eq!(cfg.fabric_label, "HomeAssistant"); // empty label -> default
        assert_eq!(cfg.next_node_id, 24);              // max(10,22,23) + 1
        assert!(cfg.wifi_credentials.is_empty());
        assert!(cfg.thread_datasets.is_empty());

        let cfg_empty = config_from(&source, &[]);
        assert_eq!(cfg_empty.next_node_id, 1);

        let mut source_labeled = source;
        source_labeled.label = "  Casa  ".to_string();
        assert_eq!(config_from(&source_labeled, &nodes).fabric_label, "Casa");
    }
}

//! In-memory node registry: the attribute cache + availability, mirroring
//! nodes/<id>.json. Availability is CACHED here (never recomputed) so the
//! serialized `available` and the event stream can never disagree.

use std::collections::BTreeMap;
use std::sync::Mutex;

use matter_rs_wire::node::MatterNodeData;
use serde_json::Value;

use crate::lock::lock;
use crate::storage::NodeRecord;

pub struct NodeEntry {
    pub record: NodeRecord,
    pub available: bool,
}

pub struct Registry {
    inner: Mutex<BTreeMap<u64, NodeEntry>>,
}

impl Registry {
    pub fn new(records: Vec<NodeRecord>) -> Self {
        let inner = records.into_iter()
            .map(|record| (record.node_id, NodeEntry { record, available: false }))
            .collect();
        Self { inner: Mutex::new(inner) }
    }

    pub fn contains(&self, node_id: u64) -> bool { lock(&self.inner).contains_key(&node_id) }
    pub fn node_ids(&self) -> Vec<u64> { lock(&self.inner).keys().copied().collect() }
    pub fn len(&self) -> usize { lock(&self.inner).len() }
    pub fn is_empty(&self) -> bool { self.len() == 0 }

    pub fn insert(&self, record: NodeRecord) {
        lock(&self.inner).insert(record.node_id, NodeEntry { record, available: false });
    }
    pub fn remove(&self, node_id: u64) -> bool {
        lock(&self.inner).remove(&node_id).is_some()
    }

    pub fn with_entry<R>(&self, node_id: u64, f: impl FnOnce(&mut NodeEntry) -> R) -> Option<R> {
        lock(&self.inner).get_mut(&node_id).map(f)
    }

    /// Returns Some(changed) or None when the node is unknown.
    pub fn set_available(&self, node_id: u64, available: bool) -> Option<bool> {
        self.with_entry(node_id, |e| {
            let changed = e.available != available;
            e.available = available;
            changed
        })
    }

    pub fn node_data(&self, node_id: u64) -> Option<MatterNodeData> {
        lock(&self.inner).get(&node_id).map(|e| build_node_data(&e.record, e.available))
    }

    pub fn all_node_data(&self, only_available: bool) -> Vec<MatterNodeData> {
        lock(&self.inner).values()
            .filter(|e| !only_available || e.available)
            .map(|e| build_node_data(&e.record, e.available))
            .collect()
    }

    pub fn snapshot_record(&self, node_id: u64) -> Option<NodeRecord> {
        lock(&self.inner).get(&node_id).map(|e| e.record.clone())
    }
}

pub fn build_node_data(record: &NodeRecord, available: bool) -> MatterNodeData {
    MatterNodeData {
        node_id: record.node_id,
        date_commissioned: record.date_commissioned.clone(),
        last_interview: record.last_interview.clone(),
        interview_version: 6,
        available,
        is_bridge: is_bridge(&record.attributes),
        attributes: record.attributes.clone(),
        attribute_subscriptions: vec![],
        matter_version: matter_version(&record.attributes),
    }
}

/// Node quirk kept on purpose: checks endpoint 1's Descriptor DeviceTypeList
/// for an Aggregator (14) entry.
fn is_bridge(attributes: &serde_json::Map<String, Value>) -> bool {
    attributes.get("1/29/0")
        .and_then(Value::as_array)
        .is_some_and(|list| list.iter().any(|e| e.get("0").and_then(Value::as_u64) == Some(14)))
}

fn matter_version(attributes: &serde_json::Map<String, Value>) -> Option<String> {
    if let Some(v) = attributes.get("0/40/21").and_then(Value::as_u64) {
        return Some(format!("{}.{}.{}", (v >> 24) & 0xFF, (v >> 16) & 0xFF, (v >> 8) & 0xFF));
    }
    match attributes.get("0/40/0").and_then(Value::as_u64) {
        Some(r) if r <= 16 => Some("<1.2.0".into()),
        Some(17) => Some("1.2.0".into()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::NodeRecord;
    use serde_json::json;

    fn rec(node_id: u64, attrs: serde_json::Value) -> NodeRecord {
        NodeRecord {
            node_id,
            date_commissioned: "2026-08-13T10:00:00.000000".into(),
            last_interview: "2026-08-13T10:00:00.000000".into(),
            device_fabric_index: 1,
            addresses: vec![],
            attributes: attrs.as_object().unwrap().clone(),
        }
    }

    #[test]
    fn is_bridge_from_endpoint_1_descriptor() {
        let bridge = rec(1, json!({"1/29/0": [{"0": 14, "1": 1}]}));
        assert!(build_node_data(&bridge, true).is_bridge);
        let light = rec(2, json!({"1/29/0": [{"0": 257, "1": 1}]}));
        assert!(!build_node_data(&light, true).is_bridge);
        let none = rec(3, json!({}));
        assert!(!build_node_data(&none, true).is_bridge);
    }

    #[test]
    fn matter_version_from_spec_version_or_data_model_revision() {
        // 0x01040000 -> "1.4.0"
        let n = rec(1, json!({"0/40/21": 0x0104_0000u32}));
        assert_eq!(build_node_data(&n, true).matter_version.as_deref(), Some("1.4.0"));
        let n = rec(2, json!({"0/40/0": 17}));
        assert_eq!(build_node_data(&n, true).matter_version.as_deref(), Some("1.2.0"));
        let n = rec(3, json!({"0/40/0": 16}));
        assert_eq!(build_node_data(&n, true).matter_version.as_deref(), Some("<1.2.0"));
        let n = rec(4, json!({}));
        assert_eq!(build_node_data(&n, true).matter_version, None);
    }

    #[test]
    fn registry_availability_and_filtering() {
        let r = Registry::new(vec![rec(1, json!({})), rec(2, json!({}))]);
        assert_eq!(r.len(), 2);
        assert!(!r.node_data(1).unwrap().available); // starts unavailable
        assert_eq!(r.set_available(1, true), Some(true));  // changed
        assert_eq!(r.set_available(1, true), Some(false)); // unchanged
        assert_eq!(r.set_available(99, true), None);
        assert_eq!(r.all_node_data(true).len(), 1);
        assert_eq!(r.all_node_data(false).len(), 2);
        assert!(r.remove(2));
        assert!(!r.contains(2));
    }

    #[test]
    fn attribute_updates_via_with_entry() {
        let r = Registry::new(vec![rec(1, json!({"1/6/0": false}))]);
        r.with_entry(1, |e| { e.record.attributes.insert("1/6/0".into(), json!(true)); });
        assert_eq!(r.node_data(1).unwrap().attributes["1/6/0"], json!(true));
    }
}

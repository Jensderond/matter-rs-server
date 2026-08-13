//! Consumes StackEvents from the stack thread and applies them to the
//! registry + storage, fanning wire events out on the broadcast channel.
//! Owns the Node-server availability semantics: 3-minute reconnect grace.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use matter_rs_wire::envelope::EventMessage;
use matter_rs_wire::node::MatterNodeEvent;
use serde_json::{json, Value};
use tokio::sync::{broadcast, mpsc};

use crate::registry::Registry;
use crate::stack_api::{NodeConnState, NodeEventData, StackEvent};
use crate::storage::{format_node_date, Storage};

pub const RECONNECT_GRACE: std::time::Duration = std::time::Duration::from_secs(180);
pub const EVENT_HISTORY_SIZE: usize = 25;

pub struct NodeManager;

impl NodeManager {
    /// Spawns the consumer task. `events` is the broadcast sender OWNED BY
    /// MatterController for its whole life (carryover: never rotate it).
    pub fn spawn(
        registry: Arc<Registry>,
        storage: Arc<Storage>,
        events: broadcast::Sender<EventMessage>,
        history: Arc<Mutex<VecDeque<Value>>>,
        rx: mpsc::UnboundedReceiver<StackEvent>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(run(registry, storage, events, history, rx))
    }
}

async fn run(
    registry: Arc<Registry>,
    storage: Arc<Storage>,
    events: broadcast::Sender<EventMessage>,
    history: Arc<Mutex<VecDeque<Value>>>,
    mut rx: mpsc::UnboundedReceiver<StackEvent>,
) {
    // (node_id -> timer task). Timer tasks send the expired node_id over grace_tx.
    let (grace_tx, mut grace_rx) = mpsc::unbounded_channel::<u64>();
    let mut grace_timers: HashMap<u64, tokio::task::JoinHandle<()>> = HashMap::new();

    loop {
        tokio::select! {
            ev = rx.recv() => {
                let Some(ev) = ev else { break };
                handle_event(&registry, &storage, &events, &history, &mut grace_timers, &grace_tx, ev);
            }
            Some(node_id) = grace_rx.recv() => {
                grace_timers.remove(&node_id);
                if registry.set_available(node_id, false) == Some(true) {
                    tracing::warn!("Node {node_id} offline grace period expired, marking unavailable");
                    emit_node_updated(&registry, &events, node_id);
                }
            }
        }
    }
    for (_, t) in grace_timers { t.abort(); }
}

fn handle_event(
    registry: &Arc<Registry>,
    storage: &Arc<Storage>,
    events: &broadcast::Sender<EventMessage>,
    history: &Arc<Mutex<VecDeque<Value>>>,
    grace_timers: &mut HashMap<u64, tokio::task::JoinHandle<()>>,
    grace_tx: &mpsc::UnboundedSender<u64>,
    ev: StackEvent,
) {
    match ev {
        StackEvent::NodeState { node_id, state } => {
            if !registry.contains(node_id) {
                tracing::debug!("state event for unknown node {node_id}, dropping");
                return;
            }
            match state {
                NodeConnState::Connected { .. } => {
                    if let Some(t) = grace_timers.remove(&node_id) { t.abort(); }
                    if registry.set_available(node_id, true) == Some(true) {
                        tracing::info!("Node {node_id} availability changed to true");
                        emit_node_updated(registry, events, node_id);
                    }
                }
                NodeConnState::Reconnecting => {
                    let available = registry.node_data(node_id).map(|n| n.available).unwrap_or(false);
                    if available && !grace_timers.contains_key(&node_id) {
                        let tx = grace_tx.clone();
                        grace_timers.insert(node_id, tokio::spawn(async move {
                            tokio::time::sleep(RECONNECT_GRACE).await;
                            let _ = tx.send(node_id);
                        }));
                    }
                }
            }
        }
        StackEvent::PrimingSnapshot { node_id, attributes } => {
            if !registry.contains(node_id) {
                tracing::debug!("priming snapshot for unknown node {node_id}, dropping");
                return;
            }
            registry.with_entry(node_id, |e| {
                e.record.attributes = attributes.into_iter().collect();
                e.record.last_interview = format_node_date(std::time::SystemTime::now());
            });
            persist(registry, storage, node_id);
            emit_node_updated(registry, events, node_id);
        }
        StackEvent::AttributesChanged { node_id, changes } => {
            if !registry.contains(node_id) {
                tracing::debug!("attribute change for unknown node {node_id}, dropping");
                return;
            }
            for (path, value) in &changes {
                registry.with_entry(node_id, |e| {
                    e.record.attributes.insert(path.clone(), value.clone());
                });
                let _ = events.send(EventMessage {
                    event: "attribute_updated".into(),
                    data: json!([node_id, path, value]),
                });
            }
            persist(registry, storage, node_id);
        }
        StackEvent::NodeEvent { node_id, event } => {
            if !registry.contains(node_id) {
                tracing::debug!("node event for unknown node {node_id}, dropping");
                return;
            }
            let payload = build_node_event(node_id, event);
            let data = serde_json::to_value(&payload).expect("MatterNodeEvent serializes");
            {
                let mut h = history.lock().unwrap();
                if h.len() >= EVENT_HISTORY_SIZE { h.pop_front(); }
                h.push_back(data.clone());
            }
            let _ = events.send(EventMessage { event: "node_event".into(), data });
        }
    }
}

fn build_node_event(node_id: u64, event: NodeEventData) -> MatterNodeEvent {
    MatterNodeEvent {
        node_id,
        endpoint_id: event.endpoint_id,
        cluster_id: event.cluster_id,
        event_id: event.event_id,
        event_number: event.event_number,
        priority: event.priority,
        timestamp: event.timestamp,
        timestamp_type: event.timestamp_type,
        data: event.data,
    }
}

fn emit_node_updated(registry: &Registry, events: &broadcast::Sender<EventMessage>, node_id: u64) {
    if let Some(nd) = registry.node_data(node_id) {
        let _ = events.send(EventMessage {
            event: "node_updated".into(),
            data: serde_json::to_value(&nd).expect("MatterNodeData serializes"),
        });
    }
}

fn persist(registry: &Registry, storage: &Storage, node_id: u64) {
    if let Some(rec) = registry.snapshot_record(node_id) {
        if let Err(e) = storage.save_node(&rec) {
            tracing::error!("failed to persist node {node_id}: {e} (still serving from memory)");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::Registry;
    use crate::stack_api::{NodeConnState, NodeEventData, StackEvent};
    use crate::storage::{NodeRecord, Storage};
    use serde_json::json;
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    struct Rig {
        registry: Arc<Registry>,
        tx: tokio::sync::mpsc::UnboundedSender<StackEvent>,
        events: tokio::sync::broadcast::Receiver<matter_rs_wire::envelope::EventMessage>,
        history: Arc<Mutex<VecDeque<serde_json::Value>>>,
        _dir: tempfile::TempDir,
    }

    fn rig() -> Rig {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(Storage::open(dir.path()).unwrap());
        let rec = NodeRecord { node_id: 7, date_commissioned: "d".into(), last_interview: "l".into(),
                               device_fabric_index: 1, addresses: vec![],
                               attributes: serde_json::Map::new() };
        storage.save_node(&rec).unwrap();
        let registry = Arc::new(Registry::new(vec![rec]));
        let (btx, brx) = tokio::sync::broadcast::channel(64);
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let history = Arc::new(Mutex::new(VecDeque::new()));
        NodeManager::spawn(registry.clone(), storage, btx, history.clone(), rx);
        Rig { registry, tx, events: brx, history, _dir: dir }
    }

    async fn next_event(rx: &mut tokio::sync::broadcast::Receiver<matter_rs_wire::envelope::EventMessage>)
        -> matter_rs_wire::envelope::EventMessage {
        tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv()).await.unwrap().unwrap()
    }

    #[tokio::test]
    async fn connected_marks_available_and_emits_node_updated_once() {
        let mut r = rig();
        r.tx.send(StackEvent::NodeState { node_id: 7, state: NodeConnState::Connected { max_interval_secs: 60 } }).unwrap();
        let ev = next_event(&mut r.events).await;
        assert_eq!(ev.event, "node_updated");
        assert_eq!(ev.data["node_id"], 7);
        assert_eq!(ev.data["available"], true);
        // second Connected: no change, no event; prove by sending a snapshot next
        r.tx.send(StackEvent::NodeState { node_id: 7, state: NodeConnState::Connected { max_interval_secs: 60 } }).unwrap();
        r.tx.send(StackEvent::PrimingSnapshot { node_id: 7, attributes: [("1/6/0".to_string(), json!(true))].into() }).unwrap();
        let ev = next_event(&mut r.events).await;
        assert_eq!(ev.event, "node_updated"); // the snapshot's, not a duplicate availability one
        assert_eq!(ev.data["attributes"]["1/6/0"], true);
    }

    #[tokio::test(start_paused = true)]
    async fn reconnecting_keeps_available_through_grace_then_drops() {
        let mut r = rig();
        r.tx.send(StackEvent::NodeState { node_id: 7, state: NodeConnState::Connected { max_interval_secs: 60 } }).unwrap();
        let _ = next_event(&mut r.events).await;
        r.tx.send(StackEvent::NodeState { node_id: 7, state: NodeConnState::Reconnecting }).unwrap();
        tokio::task::yield_now().await;
        assert!(r.registry.node_data(7).unwrap().available); // grace holds
        tokio::time::advance(RECONNECT_GRACE + std::time::Duration::from_secs(1)).await;
        let ev = next_event(&mut r.events).await;
        assert_eq!(ev.event, "node_updated");
        assert_eq!(ev.data["available"], false);
    }

    #[tokio::test(start_paused = true)]
    async fn reconnect_within_grace_cancels_timer() {
        let mut r = rig();
        r.tx.send(StackEvent::NodeState { node_id: 7, state: NodeConnState::Connected { max_interval_secs: 60 } }).unwrap();
        let _ = next_event(&mut r.events).await;
        r.tx.send(StackEvent::NodeState { node_id: 7, state: NodeConnState::Reconnecting }).unwrap();
        tokio::time::advance(std::time::Duration::from_secs(30)).await;
        r.tx.send(StackEvent::NodeState { node_id: 7, state: NodeConnState::Connected { max_interval_secs: 60 } }).unwrap();
        tokio::time::advance(RECONNECT_GRACE * 2).await;
        tokio::task::yield_now().await;
        assert!(r.registry.node_data(7).unwrap().available);
    }

    #[tokio::test]
    async fn attribute_change_emits_three_tuple_and_updates_cache() {
        let mut r = rig();
        r.tx.send(StackEvent::AttributesChanged { node_id: 7, changes: vec![("1/6/0".into(), json!(true))] }).unwrap();
        let ev = next_event(&mut r.events).await;
        assert_eq!(ev.event, "attribute_updated");
        assert_eq!(ev.data, json!([7, "1/6/0", true]));
        assert_eq!(r.registry.node_data(7).unwrap().attributes["1/6/0"], json!(true));
    }

    #[tokio::test]
    async fn node_event_goes_to_broadcast_and_history() {
        let mut r = rig();
        r.tx.send(StackEvent::NodeEvent { node_id: 7, event: NodeEventData {
            endpoint_id: 1, cluster_id: 59, event_id: 1, event_number: 5, priority: 1,
            timestamp: 1_700_000_000_000, timestamp_type: 1, data: json!({"newPosition": 1}) } }).unwrap();
        let ev = next_event(&mut r.events).await;
        assert_eq!(ev.event, "node_event");
        assert_eq!(ev.data["node_id"], 7);
        assert_eq!(ev.data["data"]["newPosition"], 1);
        assert_eq!(r.history.lock().unwrap().len(), 1);
    }
}

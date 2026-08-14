//! MatterController: the rs-matter-backed Controller implementation.
//! Dispatch lives here; per-family handlers live in crate::commands::*.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use matter_rs_wire::envelope::{CommandMessage, EventMessage};
use matter_rs_wire::error::ServerErrorCode;
use matter_rs_wire::server_info::{ServerInfoMessage, MIN_SUPPORTED_SCHEMA_VERSION, SCHEMA_VERSION};
use serde_json::Value;
use tokio::sync::{broadcast, mpsc};

use crate::api::{CommandError, ConnId, Controller};
use crate::commands;
use crate::lock::lock;
use crate::node_manager::NodeManager;
use crate::registry::Registry;
use crate::stack_api::{Stack, StackEvent};
use crate::storage::{ConfigData, ServerIdentity, Storage};

pub trait LogLevels: Send + Sync + 'static {
    fn get(&self) -> (String, Option<String>);
    fn set(&self, console: Option<&str>, file: Option<&str>);
}

pub struct MatterController {
    pub(crate) stack: Arc<dyn Stack>,
    pub(crate) storage: Arc<Storage>,
    pub(crate) registry: Arc<Registry>,
    pub(crate) identity: ServerIdentity,
    pub(crate) fabric_index: u8,
    pub(crate) sdk_version: String,
    pub(crate) config: Mutex<ConfigData>,
    /// Serializes the whole config read-modify-write (see `update_config`).
    /// Separate from `config` on purpose: a `std::sync::Mutex` must not be held
    /// across an await, and `config` exists so `config_snapshot` stays a cheap
    /// synchronous clone.
    pub(crate) config_write: tokio::sync::Mutex<()>,
    pub(crate) alloc_lock: tokio::sync::Mutex<()>,
    pub(crate) events: broadcast::Sender<EventMessage>,
    pub(crate) history: Arc<Mutex<VecDeque<Value>>>,
    pub(crate) label_locked: bool,
    pub(crate) label_owner: Mutex<Option<ConnId>>,
    pub(crate) log: Arc<dyn LogLevels>,
}

impl MatterController {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        stack: Arc<dyn Stack>,
        storage: Arc<Storage>,
        identity: ServerIdentity,
        fabric_index: u8,
        sdk_version: String,
        label_locked: bool,
        log: Arc<dyn LogLevels>,
        stack_events: mpsc::UnboundedReceiver<StackEvent>,
    ) -> Arc<Self> {
        let registry = Arc::new(Registry::new(storage.load_nodes()));
        let config = storage.load_config();
        let (events, _) = broadcast::channel(1024);
        let history = Arc::new(Mutex::new(VecDeque::new()));

        NodeManager::spawn(registry.clone(), storage.clone(), events.clone(), history.clone(), stack_events);

        let ctrl = Arc::new(Self {
            stack, storage, registry, identity, fabric_index, sdk_version,
            config: Mutex::new(config), config_write: tokio::sync::Mutex::new(()),
            alloc_lock: tokio::sync::Mutex::new(()),
            events, history, label_locked, label_owner: Mutex::new(None), log,
        });

        // Kick off supervisors for every already-commissioned node.
        let c = ctrl.clone();
        tokio::spawn(async move {
            for node_id in c.registry.node_ids() {
                c.stack.start_supervisor(node_id).await;
            }
        });

        ctrl
    }

    pub(crate) fn config_snapshot(&self) -> ConfigData { lock(&self.config).clone() }

    /// Clone-mutate-save-writeback of the config, serialized against every other
    /// caller. `f` may return a value derived from the mutation (e.g. an
    /// allocated node id).
    ///
    /// **The `config_write` lock is the point of this function.** Commands from
    /// two different WS connections run as independent tasks (`ws.rs` serializes
    /// only *within* one connection), and five commands mutate the config. With
    /// the read-modify-write unserialized, T1 clones, T2 clones, T1 saves, T2
    /// saves — and T1's mutation is gone from both disk and memory. Only
    /// `do_commission` used to hold anything (`alloc_lock`), which is why this
    /// lock is here and not there.
    ///
    /// It is a *tokio* mutex because it spans the save; nothing below actually
    /// awaits, but a `std::sync::Mutex` held across a future's yield point is
    /// exactly the shape that must never appear here, and the type makes that
    /// impossible rather than merely true today. `self.config` stays a std mutex
    /// and is held only for a clone or an assignment.
    ///
    /// The in-memory value commits even when persistence fails, deliberately:
    /// `do_commission` needs its allocated node id to stay reserved for the rest
    /// of this process run, or a second commissioning would hand out the same id.
    /// So the returned `io::Result` means "did this survive a restart", not "was
    /// this applied" — the credential family reports it to the client on that
    /// understanding.
    pub(crate) async fn update_config<R>(
        &self, f: impl FnOnce(&mut ConfigData) -> R,
    ) -> (R, std::io::Result<()>) {
        let _write = self.config_write.lock().await;
        let mut cfg = self.config_snapshot();
        let result = f(&mut cfg);
        let persisted = self.storage.save_config(&cfg);
        if let Err(e) = &persisted {
            tracing::error!("persist config: {e}");
        }
        *lock(&self.config) = cfg;
        (result, persisted)
    }

    pub(crate) fn ensure_node(&self, node_id: u64) -> Result<(), CommandError> {
        if self.registry.contains(node_id) { Ok(()) } else {
            Err(CommandError::new(ServerErrorCode::NodeNotExists, format!("Node {node_id} does not exist")))
        }
    }

    pub(crate) fn build_server_info(&self) -> ServerInfoMessage {
        let cfg = self.config_snapshot();
        let wifi = cfg.wifi_credentials.get("default").filter(|c| !c.password.is_empty());
        ServerInfoMessage {
            fabric_id: self.identity.fabric_id,
            compressed_fabric_id: self.identity.compressed_fabric_id,
            fabric_index: Some(self.fabric_index),
            schema_version: SCHEMA_VERSION,
            min_supported_schema_version: MIN_SUPPORTED_SCHEMA_VERSION,
            sdk_version: self.sdk_version.clone(),
            wifi_credentials_set: wifi.is_some(),
            wifi_ssid: wifi.map(|c| c.ssid.clone()),
            thread_credentials_set: cfg.thread_datasets.contains_key("default"),
            bluetooth_enabled: false,
            ble_proxy_enabled: Some(false),
            controller_node_id: Some(self.identity.controller_node_id),
        }
    }

    pub(crate) fn broadcast_server_info_updated(&self) {
        let _ = self.events.send(matter_rs_wire::envelope::EventMessage {
            event: "server_info_updated".into(),
            data: serde_json::to_value(self.build_server_info()).unwrap(),
        });
    }
}

#[async_trait::async_trait]
impl Controller for MatterController {
    fn server_info(&self) -> ServerInfoMessage { self.build_server_info() }
    fn node_count(&self) -> usize { self.registry.len() }

    async fn handle_command(&self, conn: ConnId, cmd: &CommandMessage) -> Result<Value, CommandError> {
        let args = &cmd.args;
        match cmd.command.as_str() {
            "server_info" => Ok(serde_json::to_value(self.build_server_info()).unwrap()),
            "start_listening" => commands::nodes::get_nodes(self, &Default::default()).await,
            "get_nodes" => commands::nodes::get_nodes(self, args).await,
            "get_node" => commands::nodes::get_node(self, args).await,
            "diagnostics" => commands::nodes::diagnostics(self, args).await,
            "interview_node" => commands::nodes::interview_node(self, args).await,
            "remove_node" => commands::nodes::remove_node(self, args).await,
            "ping_node" => commands::nodes::ping_node(self, args).await,
            "get_node_ip_addresses" => commands::nodes::get_node_ip_addresses(self, args).await,
            "read_attribute" => commands::interaction::read_attribute(self, args).await,
            "write_attribute" => commands::interaction::write_attribute(self, args).await,
            "device_command" => commands::interaction::device_command(self, args).await,
            "commission_with_code" => commands::commissioning::commission_with_code(self, args).await,
            "commission_on_network" => commands::commissioning::commission_on_network(self, args).await,
            "open_commissioning_window" => commands::commissioning::open_commissioning_window(self, args).await,
            "discover" | "discover_commissionable_nodes" => commands::commissioning::discover(self, args).await,
            "set_wifi_credentials" => commands::credentials::set_wifi(self, args).await,
            "set_thread_dataset" => commands::credentials::set_thread(self, args).await,
            "remove_wifi_credentials" => commands::credentials::remove_wifi(self, args).await,
            "remove_thread_dataset" => commands::credentials::remove_thread(self, args).await,
            "get_all_credentials" => commands::credentials::get_all(self, args).await,
            "set_default_fabric_label" => commands::fabrics::set_default_fabric_label(self, conn, args).await,
            "get_fabric_label" => commands::fabrics::get_fabric_label(self, args).await,
            "get_matter_fabrics" => commands::fabrics::get_matter_fabrics(self, args).await,
            "remove_matter_fabric" => commands::fabrics::remove_matter_fabric(self, args).await,
            "set_acl_entry" => commands::fabrics::set_acl_entry(self, args).await,
            "set_node_binding" => commands::fabrics::set_node_binding(self, args).await,
            "get_vendor_names" => commands::misc::get_vendor_names(self, args).await,
            "get_loglevel" => commands::misc::get_loglevel(self, args).await,
            "set_loglevel" => commands::misc::set_loglevel(self, args).await,
            "get_icd_state" | "register_icd" | "unregister_icd" => commands::misc::icd_state(self, args).await,
            "resync_icd" => commands::misc::resync_icd(self, args).await,
            "check_node_update" => commands::misc::check_node_update(self, args).await,
            "update_node" => commands::misc::update_node(self, args).await,
            // The catch-all stays last.
            other => Err(CommandError::new(
                ServerErrorCode::InvalidCommand, format!("Unknown command: {other}"))),
        }
    }

    fn connection_closed(&self, conn: ConnId) {
        let mut owner = lock(&self.label_owner);
        if *owner == Some(conn) { *owner = None; }
    }

    fn subscribe_events(&self) -> broadcast::Receiver<EventMessage> { self.events.subscribe() }
}

#[cfg(test)]
pub(crate) mod test_rig {
    use super::*;
    use crate::stack_api::fake::FakeStack;
    use crate::storage::{NodeRecord, ServerIdentity, Storage};
    use std::sync::Arc;

    pub struct NopLog;
    impl crate::real::LogLevels for NopLog {
        fn get(&self) -> (String, Option<String>) { ("info".into(), None) }
        fn set(&self, _c: Option<&str>, _f: Option<&str>) {}
    }

    pub fn identity() -> ServerIdentity {
        ServerIdentity { fabric_id: 1, vendor_id: 0xFFF1, controller_node_id: 112233,
            compressed_fabric_id: 0xC0FFEE, ca_private_key: vec![], rcac_tlv: vec![],
            controller_private_key: vec![], controller_noc_tlv: vec![], ipk: vec![] }
    }

    pub struct Rig {
        pub ctrl: Arc<MatterController>,
        pub stack: Arc<FakeStack>,
        // Never read, but must outlive `ctrl`: dropping the `TempDir` deletes the
        // storage dir out from under `Storage`, and dropping the sender would
        // close `stack_rx` and end the controller's event-relay loop early.
        // Underscore-prefixed (rather than `#[allow(dead_code)]`) so the compiler
        // still catches an actual accidental read turning into a real bug.
        pub _dir: tempfile::TempDir,
        pub _stack_tx: tokio::sync::mpsc::UnboundedSender<crate::stack_api::StackEvent>,
    }

    pub fn rig_with_nodes(records: Vec<NodeRecord>) -> Rig {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(Storage::open(dir.path()).unwrap());
        for r in &records { storage.save_node(r).unwrap(); }
        let stack = Arc::new(FakeStack::new());
        let (stack_tx, stack_rx) = tokio::sync::mpsc::unbounded_channel();
        let ctrl = MatterController::new(
            stack.clone(), storage, identity(), 1,
            "matter-rs-server/test (rs-matter/03bc8f2)".into(),
            false, Arc::new(NopLog), stack_rx);
        Rig { ctrl, stack, _dir: dir, _stack_tx: stack_tx }
    }

    pub fn rig() -> Rig { rig_with_nodes(vec![]) }

    pub fn node_record(node_id: u64) -> NodeRecord {
        NodeRecord { node_id, date_commissioned: "2026-08-13T10:00:00.000000".into(),
            last_interview: "2026-08-13T10:00:00.000000".into(), device_fabric_index: 2,
            addresses: vec!["192.168.1.50".into()], attributes: serde_json::Map::new() }
    }

    pub fn cmd(name: &str, args: serde_json::Value) -> matter_rs_wire::envelope::CommandMessage {
        serde_json::from_value(serde_json::json!({"message_id": "1", "command": name, "args": args})).unwrap()
    }

    pub async fn call(rig: &Rig, name: &str, args: serde_json::Value)
        -> Result<serde_json::Value, crate::api::CommandError> {
        use crate::api::{ConnId, Controller};
        rig.ctrl.handle_command(ConnId(1), &cmd(name, args)).await
    }
}

#[cfg(test)]
mod tests {
    use super::test_rig::*;
    use serde_json::json;

    #[tokio::test]
    async fn server_info_reflects_identity_and_config() {
        let r = rig();
        let v = call(&r, "server_info", json!({})).await.unwrap();
        assert_eq!(v["fabric_id"], 1);
        assert_eq!(v["compressed_fabric_id"], 0xC0FFEEu64);
        assert_eq!(v["controller_node_id"], 112233);
        assert_eq!(v["fabric_index"], 1);
        assert_eq!(v["schema_version"], 13);
        assert_eq!(v["wifi_credentials_set"], false);
        assert_eq!(v["bluetooth_enabled"], false);
    }

    #[tokio::test]
    async fn get_nodes_and_start_listening_return_node_data() {
        let r = rig_with_nodes(vec![node_record(5)]);
        let v = call(&r, "start_listening", json!({})).await.unwrap();
        assert_eq!(v.as_array().unwrap().len(), 1);
        assert_eq!(v[0]["node_id"], 5);
        assert_eq!(v[0]["available"], false);
        assert_eq!(v[0]["interview_version"], 6);
        // only_available filters
        let v = call(&r, "get_nodes", json!({"only_available": true})).await.unwrap();
        assert_eq!(v.as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn get_node_unknown_gives_exact_error() {
        let r = rig();
        let e = call(&r, "get_node", json!({"node_id": 42})).await.unwrap_err();
        assert_eq!(e.code.code(), 5);
        assert_eq!(e.details, "Node 42 does not exist");
    }

    #[tokio::test]
    async fn interview_node_updates_cache_and_emits() {
        use crate::api::Controller;
        let r = rig_with_nodes(vec![node_record(5)]);
        *r.stack.interview_response.lock().unwrap() =
            Some(Ok([("1/6/0".to_string(), json!(true))].into()));
        let mut events = r.ctrl.subscribe_events();
        let v = call(&r, "interview_node", json!({"node_id": 5})).await.unwrap();
        assert_eq!(v, serde_json::Value::Null);
        let ev = events.recv().await.unwrap();
        assert_eq!(ev.event, "node_updated");
        assert_eq!(ev.data["attributes"]["1/6/0"], true);
    }

    #[tokio::test]
    async fn remove_node_full_flow() {
        use crate::api::Controller;
        let r = rig_with_nodes(vec![node_record(5)]);
        let mut events = r.ctrl.subscribe_events();
        let v = call(&r, "remove_node", json!({"node_id": 5})).await.unwrap();
        assert_eq!(v, serde_json::Value::Null);
        let calls = r.stack.calls();
        assert!(calls.iter().any(|c| c == "stop_supervisor 5"));
        assert!(calls.iter().any(|c| c == "remove_device_fabric node=5 idx=2"));
        let ev = events.recv().await.unwrap();
        assert_eq!(ev.event, "node_removed");
        assert_eq!(ev.data, json!(5));
        let e = call(&r, "get_node", json!({"node_id": 5})).await.unwrap_err();
        assert_eq!(e.code.code(), 5);
    }

    #[tokio::test]
    async fn ping_node_empty_addresses_gives_empty_object() {
        let r = rig_with_nodes(vec![{ let mut n = node_record(5); n.addresses = vec![]; n }]);
        *r.stack.addresses_response.lock().unwrap() = Some(Ok(vec![]));
        let v = call(&r, "ping_node", json!({"node_id": 5})).await.unwrap();
        assert_eq!(v, json!({}));
    }

    #[tokio::test]
    async fn get_node_ip_addresses_strips_scope_unless_scoped() {
        let r = rig_with_nodes(vec![node_record(5)]);
        *r.stack.addresses_response.lock().unwrap() =
            Some(Ok(vec!["fe80::1%eth0".into(), "fd12::5".into()]));
        let v = call(&r, "get_node_ip_addresses", json!({"node_id": 5})).await.unwrap();
        assert_eq!(v, json!(["fe80::1", "fd12::5", "192.168.1.50"]));
        *r.stack.addresses_response.lock().unwrap() =
            Some(Ok(vec!["fe80::1%eth0".into()]));
        let v = call(&r, "get_node_ip_addresses", json!({"node_id": 5, "scoped": true})).await.unwrap();
        assert_eq!(v, json!(["fe80::1%eth0", "192.168.1.50"]));
    }

    #[tokio::test]
    async fn diagnostics_shape() {
        let r = rig_with_nodes(vec![node_record(5)]);
        let v = call(&r, "diagnostics", json!({})).await.unwrap();
        assert!(v["info"]["schema_version"] == 13);
        assert_eq!(v["nodes"].as_array().unwrap().len(), 1);
        assert_eq!(v["events"], json!([]));
    }

    #[tokio::test]
    async fn unknown_command_exact_error() {
        let r = rig();
        let e = call(&r, "frobnicate", json!({})).await.unwrap_err();
        assert_eq!(e.code.code(), 9);
        assert_eq!(e.details, "Unknown command: frobnicate");
    }

    /// Important-1 regression. `update_config` is a read-modify-write and the
    /// commands that call it run as independent tasks (`ws.rs` serializes only
    /// within one connection, and two connections is the normal case: HA plus a
    /// debug client, or a reconnect while the old one still drains). Before the
    /// `config_write` lock, T1 could clone, T2 clone, T1 save, T2 save — and T1's
    /// mutation was gone from both disk and memory.
    ///
    /// Verified to fail with the lock removed: "lost wifi credentials".
    ///
    /// Two OS threads, each driving its own current-thread runtime, rather than
    /// two tasks: `update_config`'s body has no await inside it, so two tasks on
    /// one worker thread never interleave and the bug is invisible. (A
    /// `multi_thread` test runtime would also do it, but that means adding
    /// tokio's `rt-multi-thread` to this crate's dev-dependencies — a feature-set
    /// change that costs a full rs-matter rebuild — and it would leave the
    /// interleaving up to the scheduler instead of guaranteeing it.)
    #[test]
    fn concurrent_config_writers_never_lose_each_others_mutations() {
        use crate::storage::WifiCredential;
        const N: u64 = 50;

        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let r = rt.block_on(async { rig() });

        std::thread::scope(|scope| {
            let c = &r.ctrl;
            scope.spawn(move || {
                block_on_current_thread(async {
                    for i in 0..N {
                        c.update_config(|cfg| {
                            cfg.wifi_credentials.insert(
                                format!("wifi{i}"),
                                WifiCredential { ssid: format!("ssid{i}"), password: "pw".into() },
                            );
                        })
                        .await;
                    }
                });
            });
            scope.spawn(move || {
                block_on_current_thread(async {
                    for i in 0..N {
                        c.update_config(|cfg| {
                            cfg.thread_datasets.insert(format!("thread{i}"), "0e08".into());
                        })
                        .await;
                    }
                });
            });
        });

        let in_memory = r.ctrl.config_snapshot();
        // ...and what the NEXT BOOT would see, re-read from config.json.
        let on_disk = r.ctrl.storage.load_config();
        for cfg in [&in_memory, &on_disk] {
            assert_eq!(cfg.wifi_credentials.len(), N as usize, "lost wifi credentials");
            assert_eq!(cfg.thread_datasets.len(), N as usize, "lost thread datasets");
            for i in 0..N {
                assert!(cfg.wifi_credentials.contains_key(&format!("wifi{i}")));
                assert!(cfg.thread_datasets.contains_key(&format!("thread{i}")));
            }
        }
    }

    /// The same race through the actual command surface, which is how it is
    /// reachable: `set_wifi_credentials` and `set_thread_dataset` are two of the
    /// five config writers and neither holds any lock of its own. Two connections,
    /// because `ws.rs` serializes commands only within one.
    #[test]
    fn concurrent_credential_commands_from_two_connections_both_survive() {
        use crate::api::{ConnId, Controller};
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let r = rt.block_on(async { rig() });

        std::thread::scope(|scope| {
            let c = &r.ctrl;
            scope.spawn(move || {
                block_on_current_thread(async {
                    for i in 0..20 {
                        c.handle_command(ConnId(1), &cmd("set_wifi_credentials",
                            json!({"ssid": format!("iot{i}"), "credentials": "pw", "id": format!("w{i}")})))
                            .await.unwrap();
                    }
                });
            });
            scope.spawn(move || {
                block_on_current_thread(async {
                    for i in 0..20 {
                        c.handle_command(ConnId(2), &cmd("set_thread_dataset",
                            json!({"dataset": "0e080000000000010000", "id": format!("t{i}")})))
                            .await.unwrap();
                    }
                });
            });
        });

        let on_disk = r.ctrl.storage.load_config();
        assert_eq!(on_disk.wifi_credentials.len(), 20, "lost wifi credentials: {on_disk:?}");
        assert_eq!(on_disk.thread_datasets.len(), 20, "lost thread datasets: {on_disk:?}");
    }

    /// Drives one future to completion on the calling thread. Needed by the two
    /// concurrency tests above, which use real OS threads (see there) and so
    /// cannot borrow the test's runtime.
    fn block_on_current_thread<F: std::future::Future>(f: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(f)
    }

    #[tokio::test]
    async fn supervisors_started_for_known_nodes() {
        let r = rig_with_nodes(vec![node_record(5), node_record(6)]);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let calls = r.stack.calls();
        assert!(calls.contains(&"start_supervisor 5".to_string()));
        assert!(calls.contains(&"start_supervisor 6".to_string()));
    }
}

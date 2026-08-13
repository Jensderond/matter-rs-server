//! The boundary between the protocol/orchestration side (this crate, tokio,
//! Send futures) and the rs-matter side (`crates/stack`, single-threaded).
//! Everything here is plain owned data — no rs-matter types.

use std::collections::BTreeMap;

use serde_json::Value;

/// Stack-side failure, mapped to wire error codes by the controller.
#[derive(Debug, Clone)]
pub struct StackError {
    pub kind: StackErrorKind,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StackErrorKind {
    /// Peer unreachable / mDNS resolve failed / no session.
    NodeUnreachable,
    /// PASE lockout or device busy (spike finding 2).
    Busy,
    /// Operation timed out.
    Timeout,
    /// Caller passed something invalid (unknown cluster/command/field...).
    InvalidArguments,
    /// Any other rs-matter error (maps to SDKStackError, code 7).
    Sdk,
}

impl StackError {
    pub fn new(kind: StackErrorKind, message: impl Into<String>) -> Self {
        Self { kind, message: message.into() }
    }
}

/// Connection-lifecycle state for a supervised node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeConnState {
    /// Subscription established; max report interval as granted by the device.
    Connected { max_interval_secs: u16 },
    /// Subscription/liveness lost; supervisor is retrying with backoff.
    Reconnecting,
}

/// Pushed by the stack thread; consumed by the controller's NodeManager.
#[derive(Debug, Clone)]
pub enum StackEvent {
    NodeState { node_id: u64, state: NodeConnState },
    /// Full attribute snapshot from a priming report (replaces the cache).
    PrimingSnapshot { node_id: u64, attributes: BTreeMap<String, Value> },
    /// Incremental attribute changes from a subscription report.
    AttributesChanged { node_id: u64, changes: Vec<(String, Value)> },
    NodeEvent { node_id: u64, event: NodeEventData },
}

/// One device event, already converted (data is name-based JSON or Null).
#[derive(Debug, Clone)]
pub struct NodeEventData {
    pub endpoint_id: u16,
    pub cluster_id: u32,
    pub event_id: u32,
    pub event_number: u64,
    pub priority: u8,
    pub timestamp: i64,
    pub timestamp_type: u8,
    pub data: Value,
}

/// How to reach the commissionee for PASE. QR/manual-code PARSING lives in
/// the stack (rs-matter QrPayload), so the raw code is passed through.
#[derive(Debug, Clone)]
pub enum PaseTarget {
    /// A pairing code: "MT:..." QR string or 11-digit manual code.
    Code { code: String },
    /// commission_on_network: browse mDNS with this filter.
    OnNetwork { passcode: u32, long_discriminator: Option<u16>,
                short_discriminator: Option<u8>, vendor_id: Option<u16> },
    /// Direct address (commission_on_network with ip_addr).
    Address { passcode: u32, addr: String /* "ip:port" */ },
}

#[derive(Debug, Clone)]
pub struct CommissionRequest {
    /// Pre-allocated by the controller (config.json next_node_id).
    pub node_id: u64,
    pub target: PaseTarget,
    pub fabric_label: String,
}

#[derive(Debug, Clone)]
pub struct CommissionOutcome {
    /// The fabric index the DEVICE assigned to our fabric (needed for RemoveFabric).
    pub device_fabric_index: u8,
    /// Address we commissioned over, e.g. "192.168.1.50:5540".
    pub address: String,
}

#[derive(Debug, Clone)]
pub struct WindowInfo {
    pub setup_pin_code: u32,
    pub setup_manual_code: String,
    pub setup_qr_code: String,
}

#[derive(Debug, Clone)]
pub struct DeviceFabric {
    pub fabric_id: u64,
    pub vendor_id: u16,
    pub fabric_index: u8,
    pub fabric_label: String,
}

#[derive(Debug, Clone)]
pub struct DiscoveredDevice {
    pub instance_name: String,
    pub address: String, // "ip:port"
}

/// One attribute path; None = wildcard on that segment.
#[derive(Debug, Clone, Copy)]
pub struct AttributePathSpec {
    pub endpoint: Option<u16>,
    pub cluster: Option<u32>,
    pub attribute: Option<u32>,
}

#[async_trait::async_trait]
pub trait Stack: Send + Sync + 'static {
    async fn commission(&self, req: CommissionRequest) -> Result<CommissionOutcome, StackError>;
    /// Read attributes; returns concrete ("e/c/a", tag-based JSON) pairs.
    async fn read_attributes(&self, node_id: u64, paths: Vec<AttributePathSpec>, fabric_filtered: bool)
        -> Result<Vec<(String, Value)>, StackError>;
    /// Write one attribute; returns the IM status code (0 = success).
    async fn write_attribute(&self, node_id: u64, endpoint: u16, cluster: u32, attribute: u32, value: Value)
        -> Result<u8, StackError>;
    /// Invoke by command NAME (Node camelizes; lookup is case-insensitive).
    /// Returns the name-based JSON response, or Null for DefaultSuccess.
    async fn invoke(&self, node_id: u64, endpoint: u16, cluster: u32, command_name: String,
                    payload: Value, timed_ms: Option<u16>) -> Result<Value, StackError>;
    /// Full wildcard read (fabric_filtered=true), for interviews.
    async fn interview(&self, node_id: u64) -> Result<BTreeMap<String, Value>, StackError>;
    async fn open_commissioning_window(&self, node_id: u64, timeout_secs: u16)
        -> Result<WindowInfo, StackError>;
    /// Device's OperationalCredentials fabrics list (fabric_filtered=false).
    async fn device_fabrics(&self, node_id: u64) -> Result<Vec<DeviceFabric>, StackError>;
    async fn remove_device_fabric(&self, node_id: u64, fabric_index: u8) -> Result<(), StackError>;
    /// Update our own fabric's label (and best-effort UpdateFabricLabel on connected nodes).
    async fn update_fabric_label(&self, label: String) -> Result<(), StackError>;
    /// Start/stop the per-node subscription supervisor.
    async fn start_supervisor(&self, node_id: u64);
    async fn stop_supervisor(&self, node_id: u64);
    /// Known/live addresses for the node ("ip" or "ip%iface", no port).
    async fn node_addresses(&self, node_id: u64) -> Result<Vec<String>, StackError>;
    async fn browse_commissionable(&self, timeout_ms: u32) -> Result<Vec<DiscoveredDevice>, StackError>;
    /// Stop supervisors, flush persistence, join the stack thread.
    async fn shutdown(&self);
}

/// Scriptable fake for controller unit tests. Each method returns the queued
/// response (or a default), records the call. Not cfg(test): command tests in
/// this crate and smoke tests in `server` use it.
pub mod fake {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    pub struct FakeStack {
        pub calls: Mutex<Vec<String>>,
        pub read_response: Mutex<Option<Result<Vec<(String, Value)>, StackError>>>,
        pub invoke_response: Mutex<Option<Result<Value, StackError>>>,
        pub write_response: Mutex<Option<Result<u8, StackError>>>,
        pub commission_response: Mutex<Option<Result<CommissionOutcome, StackError>>>,
        pub interview_response: Mutex<Option<Result<BTreeMap<String, Value>, StackError>>>,
        pub window_response: Mutex<Option<Result<WindowInfo, StackError>>>,
        pub fabrics_response: Mutex<Option<Result<Vec<DeviceFabric>, StackError>>>,
        pub addresses_response: Mutex<Option<Result<Vec<String>, StackError>>>,
        pub browse_response: Mutex<Option<Result<Vec<DiscoveredDevice>, StackError>>>,
    }

    impl FakeStack {
        pub fn new() -> Self { Self::default() }
        fn log(&self, s: String) { self.calls.lock().unwrap().push(s); }
        pub fn calls(&self) -> Vec<String> { self.calls.lock().unwrap().clone() }
    }

    fn sdk_err() -> StackError { StackError::new(StackErrorKind::Sdk, "fake: no scripted response") }

    #[async_trait::async_trait]
    impl Stack for FakeStack {
        async fn commission(&self, req: CommissionRequest) -> Result<CommissionOutcome, StackError> {
            self.log(format!("commission node_id={}", req.node_id));
            self.commission_response.lock().unwrap().take().unwrap_or_else(|| Err(sdk_err()))
        }
        async fn read_attributes(&self, node_id: u64, paths: Vec<AttributePathSpec>, fabric_filtered: bool)
            -> Result<Vec<(String, Value)>, StackError> {
            self.log(format!("read node={node_id} paths={} ff={fabric_filtered}", paths.len()));
            self.read_response.lock().unwrap().take().unwrap_or_else(|| Err(sdk_err()))
        }
        async fn write_attribute(&self, node_id: u64, endpoint: u16, cluster: u32, attribute: u32, _value: Value)
            -> Result<u8, StackError> {
            self.log(format!("write node={node_id} {endpoint}/{cluster}/{attribute}"));
            self.write_response.lock().unwrap().take().unwrap_or(Ok(0))
        }
        async fn invoke(&self, node_id: u64, endpoint: u16, cluster: u32, command_name: String,
                        _payload: Value, timed_ms: Option<u16>) -> Result<Value, StackError> {
            self.log(format!("invoke node={node_id} {endpoint}/{cluster} {command_name} timed={timed_ms:?}"));
            self.invoke_response.lock().unwrap().take().unwrap_or(Ok(Value::Null))
        }
        async fn interview(&self, node_id: u64) -> Result<BTreeMap<String, Value>, StackError> {
            self.log(format!("interview node={node_id}"));
            self.interview_response.lock().unwrap().take().unwrap_or_else(|| Ok(BTreeMap::new()))
        }
        async fn open_commissioning_window(&self, node_id: u64, timeout_secs: u16)
            -> Result<WindowInfo, StackError> {
            self.log(format!("ocw node={node_id} timeout={timeout_secs}"));
            self.window_response.lock().unwrap().take().unwrap_or_else(|| Err(sdk_err()))
        }
        async fn device_fabrics(&self, node_id: u64) -> Result<Vec<DeviceFabric>, StackError> {
            self.log(format!("device_fabrics node={node_id}"));
            self.fabrics_response.lock().unwrap().take().unwrap_or_else(|| Ok(vec![]))
        }
        async fn remove_device_fabric(&self, node_id: u64, fabric_index: u8) -> Result<(), StackError> {
            self.log(format!("remove_device_fabric node={node_id} idx={fabric_index}"));
            Ok(())
        }
        async fn update_fabric_label(&self, label: String) -> Result<(), StackError> {
            self.log(format!("update_fabric_label {label}"));
            Ok(())
        }
        async fn start_supervisor(&self, node_id: u64) { self.log(format!("start_supervisor {node_id}")); }
        async fn stop_supervisor(&self, node_id: u64) { self.log(format!("stop_supervisor {node_id}")); }
        async fn node_addresses(&self, node_id: u64) -> Result<Vec<String>, StackError> {
            self.log(format!("node_addresses {node_id}"));
            self.addresses_response.lock().unwrap().take().unwrap_or_else(|| Ok(vec![]))
        }
        async fn browse_commissionable(&self, timeout_ms: u32) -> Result<Vec<DiscoveredDevice>, StackError> {
            self.log(format!("browse {timeout_ms}"));
            self.browse_response.lock().unwrap().take().unwrap_or_else(|| Ok(vec![]))
        }
        async fn shutdown(&self) { self.log("shutdown".into()); }
    }
}

#[cfg(test)]
mod tests {
    use super::fake::FakeStack;
    use super::*;

    #[tokio::test]
    async fn fake_stack_scripts_and_records() {
        let s = FakeStack::new();
        *s.read_response.lock().unwrap() =
            Some(Ok(vec![("1/6/0".into(), serde_json::json!(true))]));
        let r = s.read_attributes(5, vec![AttributePathSpec { endpoint: Some(1), cluster: Some(6), attribute: Some(0) }], false).await.unwrap();
        assert_eq!(r[0].0, "1/6/0");
        assert_eq!(s.calls()[0], "read node=5 paths=1 ff=false");
        // unscripted read errors as Sdk
        assert_eq!(s.read_attributes(5, vec![], false).await.unwrap_err().kind, StackErrorKind::Sdk);
    }
}

# matter-rs-server Plan 1: Protocol Skeleton Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A running `matter-rs-server` daemon that speaks the python-matter-server WebSocket protocol shape on :5580 (`/ws` + `/health`), backed by a stub controller — the foundation plans 2 (rs-matter core) and 3 (wire-perfect converters) build on.

**Architecture:** Cargo workspace with three crates: `wire` (protocol message types, error codes — pure data, no I/O), `controller` (the `Controller` trait + `StubController`; plan 2 adds the real implementation), `server` (binary: CLI config, logging, axum HTTP/WS, connection actor, graceful shutdown). The server only knows the `Controller` trait, so swapping the stub for the real controller in plan 2 touches nothing here.

**Tech Stack:** Rust 2021 (toolchain ≥1.96), tokio 1, axum 0.8 (`ws` feature), serde/serde_json, clap 4 (derive + env), async-trait, tracing + tracing-subscriber, tokio-tungstenite (tests only).

## Global Constraints

- Spec: `docs/superpowers/specs/2026-08-13-matter-rs-server-design.md`; spike findings: `spike/SPIKE-RESULTS.md`.
- `SCHEMA_VERSION = 13`, `MIN_SUPPORTED_SCHEMA_VERSION = 11`.
- Defaults: port `5580`, storage path `~/.matter_server`, vendor id `0xFFF1` (65521), fabric id `1`.
- `node_id`/`fabric_id`/`compressed_fabric_id` are u64 serialized as **unquoted JSON numbers** (serde_json does this natively; never stringify).
- On WS connect the server pushes the **bare** `ServerInfoMessage` object (NO `{message_id,...}` envelope). Command responses ARE enveloped.
- Events are `{"event": "...", "data": ...}` and flow only after that connection sent `start_listening`.
- Error codes (python-compatible): 0 Unknown, 1 NodeCommissionFailed, 2 NodeInterviewFailed, 3 NodeNotReady, 4 NodeNotResolving, 5 NodeNotExists, 6 VersionMismatch, 7 SDKStackError, 8 InvalidArguments, 9 InvalidCommand, 10 UpdateCheckError, 11 UpdateError, 100 IcdMultiAdmin, 101 OtaUploadError.
- Out-of-scope Node flags (`--bluetooth-adapter`, `--ble-proxy`, `--disable-ota`, `--ota-provider-dir`, `--disable-dashboard`, ...) must parse, warn, and be ignored — an existing unit file must never fail to start.
- Reference for wire shapes: `matterjs-server/docs/websockets_api.md` (gitignored clone at repo root).
- Commit messages: conventional style (`feat:`, `test:`, `chore:`), each ending with the trailer `Claude-Session: https://claude.ai/code/session_01BxfHyF8XvzcwxUtWUcDuYM`.
- All work on branch `plan1-protocol-skeleton`.

## File Structure

```
Cargo.toml                      # workspace: crates/*
crates/
├── wire/
│   ├── Cargo.toml              # serde, serde_json, thiserror
│   └── src/
│       ├── lib.rs              # pub mod envelope; pub mod error; pub mod server_info;
│       ├── envelope.rs         # CommandMessage, SuccessResult, ErrorResult, EventMessage
│       ├── error.rs            # ServerErrorCode
│       └── server_info.rs      # ServerInfoMessage
├── controller/
│   ├── Cargo.toml              # wire, async-trait, tokio (sync), serde_json, thiserror
│   └── src/
│       ├── lib.rs              # pub mod api; pub mod stub;
│       ├── api.rs              # Controller trait, CommandError
│       └── stub.rs             # StubController
└── server/
    ├── Cargo.toml              # bin "matter-rs-server": wire, controller, tokio, axum, clap, tracing...
    ├── src/
    │   ├── main.rs             # wiring: config -> logging -> StubController -> serve
    │   ├── config.rs           # clap Config incl. ignored legacy flags
    │   ├── logging.rs          # level-string mapping -> tracing init
    │   ├── http.rs             # build_router(), serve(): /health + /ws upgrade + 404
    │   └── ws.rs               # per-connection actor: push server_info, dispatch, events
    └── tests/
        ├── ws_protocol.rs      # integration: envelope, errors, events, gating
        └── health.rs           # integration: /health
```

---

### Task 1: Workspace scaffold

**Files:**
- Create: `Cargo.toml`, `crates/wire/Cargo.toml`, `crates/wire/src/lib.rs`, `crates/controller/Cargo.toml`, `crates/controller/src/lib.rs`, `crates/server/Cargo.toml`, `crates/server/src/main.rs`
- Modify: `.gitignore` (root `target/` already ignored)

**Interfaces:**
- Consumes: nothing
- Produces: workspace where `cargo build && cargo test` passes; crate names `matter-rs-wire`, `matter-rs-controller`, `matter-rs-server` (lib/bin paths as above)

- [ ] **Step 1: Create branch**

```bash
git checkout -b plan1-protocol-skeleton
```

- [ ] **Step 2: Write workspace + crate manifests**

`Cargo.toml` (root):
```toml
[workspace]
resolver = "2"
members = ["crates/wire", "crates/controller", "crates/server"]

[workspace.package]
version = "0.1.0"
edition = "2021"
license = "Apache-2.0"

[workspace.dependencies]
serde = { version = "1", features = ["derive"] }
serde_json = "1"
thiserror = "2"
tokio = { version = "1", features = ["full"] }
async-trait = "0.1"
tracing = "0.1"
```

`crates/wire/Cargo.toml`:
```toml
[package]
name = "matter-rs-wire"
version.workspace = true
edition.workspace = true
license.workspace = true

[dependencies]
serde.workspace = true
serde_json.workspace = true
thiserror.workspace = true
```

`crates/wire/src/lib.rs`:
```rust
pub mod envelope;
pub mod error;
pub mod server_info;
```
(Create empty `envelope.rs`, `error.rs`, `server_info.rs` files so it compiles.)

`crates/controller/Cargo.toml`:
```toml
[package]
name = "matter-rs-controller"
version.workspace = true
edition.workspace = true
license.workspace = true

[dependencies]
matter-rs-wire = { path = "../wire" }
serde_json.workspace = true
thiserror.workspace = true
tokio = { version = "1", features = ["sync", "rt"] }
async-trait.workspace = true
```

`crates/controller/src/lib.rs`:
```rust
pub mod api;
pub mod stub;
```
(Create empty `api.rs`, `stub.rs`.)

`crates/server/Cargo.toml`:
```toml
[package]
name = "matter-rs-server"
version.workspace = true
edition.workspace = true
license.workspace = true

[[bin]]
name = "matter-rs-server"
path = "src/main.rs"

[dependencies]
matter-rs-wire = { path = "../wire" }
matter-rs-controller = { path = "../controller" }
tokio.workspace = true
axum = { version = "0.8", features = ["ws"] }
clap = { version = "4", features = ["derive", "env"] }
serde_json.workspace = true
tracing.workspace = true
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
futures-util = "0.3"

[dev-dependencies]
tokio-tungstenite = "0.24"
reqwest = { version = "0.12", default-features = false, features = ["json"] }
```

`crates/server/src/main.rs`:
```rust
fn main() {
    println!("matter-rs-server scaffold");
}
```

- [ ] **Step 3: Verify it builds**

Run: `cargo build && cargo test`
Expected: builds; 0 tests, no failures.

- [ ] **Step 4: Commit**

```bash
git add Cargo.toml Cargo.lock crates
git commit -m "chore: scaffold cargo workspace (wire, controller, server)"
```

---

### Task 2: `wire` — error codes

**Files:**
- Modify: `crates/wire/src/error.rs`

**Interfaces:**
- Produces: `ServerErrorCode` (Copy enum) with `code() -> u16`; used by `ErrorResult` and `CommandError`.

- [ ] **Step 1: Write the failing test** (bottom of `error.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_match_python_matter_server() {
        assert_eq!(ServerErrorCode::UnknownError.code(), 0);
        assert_eq!(ServerErrorCode::NodeCommissionFailed.code(), 1);
        assert_eq!(ServerErrorCode::NodeInterviewFailed.code(), 2);
        assert_eq!(ServerErrorCode::NodeNotReady.code(), 3);
        assert_eq!(ServerErrorCode::NodeNotResolving.code(), 4);
        assert_eq!(ServerErrorCode::NodeNotExists.code(), 5);
        assert_eq!(ServerErrorCode::VersionMismatch.code(), 6);
        assert_eq!(ServerErrorCode::SdkStackError.code(), 7);
        assert_eq!(ServerErrorCode::InvalidArguments.code(), 8);
        assert_eq!(ServerErrorCode::InvalidCommand.code(), 9);
        assert_eq!(ServerErrorCode::UpdateCheckError.code(), 10);
        assert_eq!(ServerErrorCode::UpdateError.code(), 11);
        assert_eq!(ServerErrorCode::IcdMultiAdmin.code(), 100);
        assert_eq!(ServerErrorCode::OtaUploadError.code(), 101);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p matter-rs-wire`
Expected: FAIL — `ServerErrorCode` not defined.

- [ ] **Step 3: Implement**

```rust
/// python-matter-server compatible error codes (see design spec, Error handling).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerErrorCode {
    UnknownError,
    NodeCommissionFailed,
    NodeInterviewFailed,
    NodeNotReady,
    NodeNotResolving,
    NodeNotExists,
    VersionMismatch,
    SdkStackError,
    InvalidArguments,
    InvalidCommand,
    UpdateCheckError,
    UpdateError,
    IcdMultiAdmin,
    OtaUploadError,
}

impl ServerErrorCode {
    pub fn code(self) -> u16 {
        match self {
            Self::UnknownError => 0,
            Self::NodeCommissionFailed => 1,
            Self::NodeInterviewFailed => 2,
            Self::NodeNotReady => 3,
            Self::NodeNotResolving => 4,
            Self::NodeNotExists => 5,
            Self::VersionMismatch => 6,
            Self::SdkStackError => 7,
            Self::InvalidArguments => 8,
            Self::InvalidCommand => 9,
            Self::UpdateCheckError => 10,
            Self::UpdateError => 11,
            Self::IcdMultiAdmin => 100,
            Self::OtaUploadError => 101,
        }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p matter-rs-wire`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/wire/src/error.rs
git commit -m "feat(wire): python-compatible server error codes"
```

---

### Task 3: `wire` — message envelope

**Files:**
- Modify: `crates/wire/src/envelope.rs`

**Interfaces:**
- Produces:
  - `CommandMessage { message_id: String, command: String, args: serde_json::Map<String, Value> }` (Deserialize; `args` defaults to empty)
  - `SuccessResult { message_id: String, result: Value }` (Serialize)
  - `ErrorResult { message_id: String, error_code: u16, details: String }` (Serialize) + `ErrorResult::new(message_id, ServerErrorCode, details)`
  - `EventMessage { event: String, data: Value }` (Serialize, Clone)

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_command_with_and_without_args() {
        let m: CommandMessage =
            serde_json::from_str(r#"{"message_id":"1","command":"get_node","args":{"node_id":42}}"#)
                .unwrap();
        assert_eq!(m.message_id, "1");
        assert_eq!(m.command, "get_node");
        assert_eq!(m.args.get("node_id"), Some(&json!(42)));

        let m: CommandMessage =
            serde_json::from_str(r#"{"message_id":"2","command":"server_info"}"#).unwrap();
        assert!(m.args.is_empty());
    }

    #[test]
    fn success_result_shape() {
        let s = SuccessResult { message_id: "1".into(), result: json!([]) };
        assert_eq!(serde_json::to_string(&s).unwrap(), r#"{"message_id":"1","result":[]}"#);
    }

    #[test]
    fn error_result_shape() {
        let e = ErrorResult::new("1".into(), crate::error::ServerErrorCode::InvalidCommand, "nope".into());
        assert_eq!(
            serde_json::to_string(&e).unwrap(),
            r#"{"message_id":"1","error_code":9,"details":"nope"}"#
        );
    }

    #[test]
    fn event_shape_and_big_u64_stays_numeric() {
        // node ids can exceed 2^53; they must serialize as unquoted numbers.
        let ev = EventMessage { event: "node_added".into(), data: json!({"node_id": 18446744073709551615u64}) };
        assert_eq!(
            serde_json::to_string(&ev).unwrap(),
            r#"{"event":"node_added","data":{"node_id":18446744073709551615}}"#
        );
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p matter-rs-wire`
Expected: FAIL — types not defined.

- [ ] **Step 3: Implement**

```rust
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::ServerErrorCode;

/// Inbound request: {"message_id", "command", "args"?}.
#[derive(Debug, Clone, Deserialize)]
pub struct CommandMessage {
    pub message_id: String,
    pub command: String,
    #[serde(default)]
    pub args: serde_json::Map<String, Value>,
}

/// Outbound success: {"message_id", "result"}.
#[derive(Debug, Clone, Serialize)]
pub struct SuccessResult {
    pub message_id: String,
    pub result: Value,
}

/// Outbound error: {"message_id", "error_code", "details"}.
#[derive(Debug, Clone, Serialize)]
pub struct ErrorResult {
    pub message_id: String,
    pub error_code: u16,
    pub details: String,
}

impl ErrorResult {
    pub fn new(message_id: String, code: ServerErrorCode, details: String) -> Self {
        Self { message_id, error_code: code.code(), details }
    }
}

/// Outbound event: {"event", "data"} — only after start_listening.
#[derive(Debug, Clone, Serialize)]
pub struct EventMessage {
    pub event: String,
    pub data: Value,
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p matter-rs-wire`
Expected: PASS (4 tests)

- [ ] **Step 5: Commit**

```bash
git add crates/wire/src/envelope.rs
git commit -m "feat(wire): request/response/event envelope types"
```

---

### Task 4: `wire` — ServerInfoMessage

**Files:**
- Modify: `crates/wire/src/server_info.rs`

**Interfaces:**
- Produces: `ServerInfoMessage` (Serialize + Deserialize + Clone) with exactly the fields below; optional OHF-only fields skip serialization when `None`. Constants `SCHEMA_VERSION: u8 = 13`, `MIN_SUPPORTED_SCHEMA_VERSION: u8 = 11`.

- [ ] **Step 1: Write the failing test**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serializes_like_node_server_and_skips_absent_optionals() {
        let info = ServerInfoMessage {
            fabric_id: 1,
            compressed_fabric_id: 9876543210,
            fabric_index: None,
            schema_version: SCHEMA_VERSION,
            min_supported_schema_version: MIN_SUPPORTED_SCHEMA_VERSION,
            sdk_version: "matter-rs-server/0.1.0 (rs-matter/03bc8f2)".into(),
            wifi_credentials_set: false,
            wifi_ssid: None,
            thread_credentials_set: false,
            bluetooth_enabled: false,
            ble_proxy_enabled: None,
            controller_node_id: None,
        };
        let v: serde_json::Value = serde_json::to_value(&info).unwrap();
        assert_eq!(v["schema_version"], 13);
        assert_eq!(v["min_supported_schema_version"], 11);
        assert_eq!(v["compressed_fabric_id"], serde_json::json!(9876543210u64));
        assert!(v.get("fabric_index").is_none());
        assert!(v.get("wifi_ssid").is_none());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p matter-rs-wire`
Expected: FAIL — type not defined.

- [ ] **Step 3: Implement**

```rust
use serde::{Deserialize, Serialize};

pub const SCHEMA_VERSION: u8 = 13;
pub const MIN_SUPPORTED_SCHEMA_VERSION: u8 = 11;

/// Pushed bare (unenveloped) on WS connect; also the result of `server_info`.
/// Field set mirrors matterjs-server's ServerInfoMessage (ws-client model.ts).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerInfoMessage {
    pub fabric_id: u64,
    pub compressed_fabric_id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fabric_index: Option<u8>,
    pub schema_version: u8,
    pub min_supported_schema_version: u8,
    pub sdk_version: String,
    pub wifi_credentials_set: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wifi_ssid: Option<String>,
    pub thread_credentials_set: bool,
    pub bluetooth_enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ble_proxy_enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub controller_node_id: Option<u64>,
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p matter-rs-wire`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/wire/src/server_info.rs
git commit -m "feat(wire): ServerInfoMessage with schema 13 constants"
```

---

### Task 5: `controller` — Controller trait + StubController

**Files:**
- Modify: `crates/controller/src/api.rs`, `crates/controller/src/stub.rs`

**Interfaces:**
- Consumes: `matter_rs_wire::{envelope::{CommandMessage, EventMessage}, error::ServerErrorCode, server_info::ServerInfoMessage}`
- Produces (plan 2 implements this trait for the real controller — signatures are load-bearing):

```rust
pub struct CommandError { pub code: ServerErrorCode, pub details: String }

#[async_trait::async_trait]
pub trait Controller: Send + Sync + 'static {
    /// Snapshot for /health and the server_info push/command.
    fn server_info(&self) -> ServerInfoMessage;
    fn node_count(&self) -> usize;
    /// Handle one command; Ok(result) -> SuccessResult, Err -> ErrorResult.
    async fn handle_command(&self, cmd: &CommandMessage) -> Result<serde_json::Value, CommandError>;
    /// Every connection subscribes; events are fanned out post-start_listening.
    fn subscribe_events(&self) -> tokio::sync::broadcast::Receiver<EventMessage>;
}
```
- `StubController::new(info: ServerInfoMessage) -> Self`, plus `StubController::event_sender(&self) -> broadcast::Sender<EventMessage>` (tests inject events through it).

- [ ] **Step 1: Write the failing tests** (bottom of `stub.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::Controller;
    use matter_rs_wire::envelope::CommandMessage;
    use matter_rs_wire::server_info::{ServerInfoMessage, MIN_SUPPORTED_SCHEMA_VERSION, SCHEMA_VERSION};
    use serde_json::json;

    fn test_info() -> ServerInfoMessage {
        ServerInfoMessage {
            fabric_id: 1,
            compressed_fabric_id: 0,
            fabric_index: None,
            schema_version: SCHEMA_VERSION,
            min_supported_schema_version: MIN_SUPPORTED_SCHEMA_VERSION,
            sdk_version: "test".into(),
            wifi_credentials_set: false,
            wifi_ssid: None,
            thread_credentials_set: false,
            bluetooth_enabled: false,
            ble_proxy_enabled: None,
            controller_node_id: None,
        }
    }

    fn cmd(name: &str) -> CommandMessage {
        serde_json::from_value(json!({"message_id": "1", "command": name})).unwrap()
    }

    #[tokio::test]
    async fn known_read_commands_return_empty_shapes() {
        let c = StubController::new(test_info());
        assert_eq!(c.handle_command(&cmd("get_nodes")).await.unwrap(), json!([]));
        assert_eq!(c.handle_command(&cmd("start_listening")).await.unwrap(), json!([]));
        let si = c.handle_command(&cmd("server_info")).await.unwrap();
        assert_eq!(si["schema_version"], 13);
        let diag = c.handle_command(&cmd("diagnostics")).await.unwrap();
        assert_eq!(diag["nodes"], json!([]));
        assert_eq!(diag["events"], json!([]));
    }

    #[tokio::test]
    async fn get_node_returns_node_not_exists() {
        let c = StubController::new(test_info());
        let err = c.handle_command(&cmd("get_node")).await.unwrap_err();
        assert_eq!(err.code.code(), 5);
    }

    #[tokio::test]
    async fn unknown_command_returns_invalid_command() {
        let c = StubController::new(test_info());
        let err = c.handle_command(&cmd("frobnicate")).await.unwrap_err();
        assert_eq!(err.code.code(), 9);
    }

    #[tokio::test]
    async fn events_flow_through_broadcast() {
        let c = StubController::new(test_info());
        let mut rx = c.subscribe_events();
        c.event_sender()
            .send(matter_rs_wire::envelope::EventMessage { event: "node_added".into(), data: json!({}) })
            .unwrap();
        assert_eq!(rx.recv().await.unwrap().event, "node_added");
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p matter-rs-controller`
Expected: FAIL — types not defined.

- [ ] **Step 3: Implement**

`api.rs`:
```rust
use matter_rs_wire::envelope::{CommandMessage, EventMessage};
use matter_rs_wire::error::ServerErrorCode;
use matter_rs_wire::server_info::ServerInfoMessage;

#[derive(Debug)]
pub struct CommandError {
    pub code: ServerErrorCode,
    pub details: String,
}

impl CommandError {
    pub fn new(code: ServerErrorCode, details: impl Into<String>) -> Self {
        Self { code, details: details.into() }
    }
}

#[async_trait::async_trait]
pub trait Controller: Send + Sync + 'static {
    fn server_info(&self) -> ServerInfoMessage;
    fn node_count(&self) -> usize;
    async fn handle_command(&self, cmd: &CommandMessage) -> Result<serde_json::Value, CommandError>;
    fn subscribe_events(&self) -> tokio::sync::broadcast::Receiver<EventMessage>;
}
```

`stub.rs`:
```rust
use serde_json::{json, Value};
use tokio::sync::broadcast;

use matter_rs_wire::envelope::{CommandMessage, EventMessage};
use matter_rs_wire::error::ServerErrorCode;
use matter_rs_wire::server_info::ServerInfoMessage;

use crate::api::{CommandError, Controller};

/// Protocol-skeleton stand-in: real shapes for session commands, honest
/// errors for the rest. Replaced by the rs-matter-backed controller in plan 2.
pub struct StubController {
    info: ServerInfoMessage,
    events: broadcast::Sender<EventMessage>,
}

impl StubController {
    pub fn new(info: ServerInfoMessage) -> Self {
        let (events, _) = broadcast::channel(256);
        Self { info, events }
    }

    pub fn event_sender(&self) -> broadcast::Sender<EventMessage> {
        self.events.clone()
    }
}

#[async_trait::async_trait]
impl Controller for StubController {
    fn server_info(&self) -> ServerInfoMessage {
        self.info.clone()
    }

    fn node_count(&self) -> usize {
        0
    }

    async fn handle_command(&self, cmd: &CommandMessage) -> Result<Value, CommandError> {
        match cmd.command.as_str() {
            "server_info" => Ok(serde_json::to_value(self.server_info()).unwrap()),
            "start_listening" | "get_nodes" => Ok(json!([])),
            "diagnostics" => Ok(json!({
                "info": serde_json::to_value(self.server_info()).unwrap(),
                "nodes": [],
                "events": [],
            })),
            "get_node" | "interview_node" | "remove_node" | "ping_node"
            | "get_node_ip_addresses" | "read_attribute" | "write_attribute"
            | "device_command" => Err(CommandError::new(
                ServerErrorCode::NodeNotExists,
                "stub controller has no nodes",
            )),
            other => Err(CommandError::new(
                ServerErrorCode::InvalidCommand,
                format!("unknown command: {other}"),
            )),
        }
    }

    fn subscribe_events(&self) -> broadcast::Receiver<EventMessage> {
        self.events.subscribe()
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p matter-rs-controller`
Expected: PASS (4 tests)

- [ ] **Step 5: Commit**

```bash
git add crates/controller/src
git commit -m "feat(controller): Controller trait and StubController"
```

---

### Task 6: `server` — CLI config

**Files:**
- Create: `crates/server/src/config.rs`
- Modify: `crates/server/src/main.rs` (add `mod config;`)

**Interfaces:**
- Produces: `Config` (clap Parser) with `port: u16` (default 5580, env `PORT`), `listen_address: Vec<String>` (repeatable, env `LISTEN_ADDRESS`), `storage_path: PathBuf` (default `~/.matter_server`, env `STORAGE_PATH`), `vendor_id: u16` (default 0xFFF1, env `VENDOR_ID`), `fabric_id: u64` (default 1, env `FABRIC_ID`), `log_level: String` (default `"info"`, env `LOG_LEVEL`), `log_file: Option<PathBuf>` (env `LOG_FILE`), `primary_interface: Option<String>` (env `PRIMARY_INTERFACE`), `default_fabric_label: Option<String>` (env `DEFAULT_FABRIC_LABEL`); plus hidden ignored legacy flags and `Config::warn_ignored(&self)`.

- [ ] **Step 1: Write the failing tests** (bottom of `config.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn defaults_match_node_server() {
        let c = Config::try_parse_from(["matter-rs-server"]).unwrap();
        assert_eq!(c.port, 5580);
        assert_eq!(c.vendor_id, 0xFFF1);
        assert_eq!(c.fabric_id, 1);
        assert_eq!(c.log_level, "info");
        assert!(c.listen_address.is_empty());
        assert!(c.storage_path.ends_with(".matter_server"));
    }

    #[test]
    fn parses_node_server_style_invocation() {
        let c = Config::try_parse_from([
            "matter-rs-server",
            "--storage-path", "/var/lib/matter-rs-server",
            "--port", "5581",
            "--listen-address", "127.0.0.1",
            "--listen-address", "::1",
            "--log-level", "debug",
        ])
        .unwrap();
        assert_eq!(c.port, 5581);
        assert_eq!(c.listen_address, vec!["127.0.0.1", "::1"]);
        assert_eq!(c.storage_path, std::path::PathBuf::from("/var/lib/matter-rs-server"));
    }

    #[test]
    fn legacy_out_of_scope_flags_parse_and_are_ignored() {
        // An existing matterjs-server unit file must never fail to start.
        let c = Config::try_parse_from([
            "matter-rs-server",
            "--bluetooth-adapter", "0",
            "--ble-proxy",
            "--disable-ota",
            "--ota-provider-dir", "/tmp/ota",
            "--disable-dashboard",
            "--enable-test-net-dcl",
            "--production-mode",
        ])
        .unwrap();
        assert!(c.ble_proxy); // captured, warned about at startup, never acted on
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p matter-rs-server`
Expected: FAIL — `Config` not defined.

- [ ] **Step 3: Implement**

```rust
use std::path::PathBuf;

use clap::Parser;

fn default_storage_path() -> PathBuf {
    dirs_next_home().join(".matter_server")
}

fn dirs_next_home() -> PathBuf {
    std::env::var_os("HOME").map(PathBuf::from).unwrap_or_else(|| PathBuf::from("."))
}

/// CLI compatible with matterjs-server's (docs/cli.md there). Out-of-scope
/// flags are accepted + warned + ignored so existing unit files keep starting.
#[derive(Debug, Parser)]
#[command(name = "matter-rs-server", version)]
pub struct Config {
    #[arg(long, env = "PORT", default_value_t = 5580)]
    pub port: u16,

    /// Repeatable. Empty -> bind all interfaces.
    #[arg(long = "listen-address", env = "LISTEN_ADDRESS")]
    pub listen_address: Vec<String>,

    #[arg(long = "storage-path", env = "STORAGE_PATH", default_value_os_t = default_storage_path())]
    pub storage_path: PathBuf,

    #[arg(long = "vendorid", env = "VENDOR_ID", default_value_t = 0xFFF1)]
    pub vendor_id: u16,

    #[arg(long = "fabricid", env = "FABRIC_ID", default_value_t = 1)]
    pub fabric_id: u64,

    /// fatal|critical|error|warning|warn|notice|info|debug|verbose
    #[arg(long = "log-level", env = "LOG_LEVEL", default_value = "info")]
    pub log_level: String,

    #[arg(long = "log-file", env = "LOG_FILE")]
    pub log_file: Option<PathBuf>,

    #[arg(long = "primary-interface", env = "PRIMARY_INTERFACE")]
    pub primary_interface: Option<String>,

    #[arg(long = "default-fabric-label", env = "DEFAULT_FABRIC_LABEL")]
    pub default_fabric_label: Option<String>,

    // ---- accepted-but-ignored (out of scope in v1; see design spec) ----
    #[arg(long = "bluetooth-adapter", env = "BLUETOOTH_ADAPTER", hide = true)]
    pub bluetooth_adapter: Option<u32>,
    #[arg(long = "ble-proxy", env = "BLE_PROXY", hide = true, default_value_t = false)]
    pub ble_proxy: bool,
    #[arg(long = "disable-ota", hide = true, default_value_t = false)]
    pub disable_ota: bool,
    #[arg(long = "ota-provider-dir", hide = true)]
    pub ota_provider_dir: Option<PathBuf>,
    #[arg(long = "disable-dashboard", hide = true, default_value_t = false)]
    pub disable_dashboard: bool,
    #[arg(long = "enable-test-net-dcl", hide = true, default_value_t = false)]
    pub enable_test_net_dcl: bool,
    #[arg(long = "production-mode", hide = true, default_value_t = false)]
    pub production_mode: bool,
}

impl Config {
    /// Log one warning per supplied out-of-scope flag.
    pub fn warn_ignored(&self) {
        let mut ignored: Vec<&str> = Vec::new();
        if self.bluetooth_adapter.is_some() { ignored.push("--bluetooth-adapter"); }
        if self.ble_proxy { ignored.push("--ble-proxy"); }
        if self.disable_ota { ignored.push("--disable-ota"); }
        if self.ota_provider_dir.is_some() { ignored.push("--ota-provider-dir"); }
        if self.disable_dashboard { ignored.push("--disable-dashboard"); }
        if self.enable_test_net_dcl { ignored.push("--enable-test-net-dcl"); }
        if self.production_mode { ignored.push("--production-mode"); }
        for flag in ignored {
            tracing::warn!("{flag} is not supported by matter-rs-server v1 and is ignored");
        }
    }
}
```

In `main.rs` add `mod config;` (keep the scaffold `main`).

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p matter-rs-server`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add crates/server/src/config.rs crates/server/src/main.rs
git commit -m "feat(server): matterjs-server-compatible CLI config"
```

---

### Task 7: `server` — logging init

**Files:**
- Create: `crates/server/src/logging.rs`
- Modify: `crates/server/src/main.rs` (add `mod logging;`)

**Interfaces:**
- Produces: `map_level(&str) -> Option<tracing::Level>` (matterjs level names → tracing levels; `fatal|critical|error`→ERROR, `warning|warn`→WARN, `notice|info`→INFO, `debug`→DEBUG, `verbose`→TRACE, unknown→None) and `init(config: &Config)` installing tracing-subscriber (stderr; plus non-rotating file layer when `--log-file` set — rotation is a later plan).

- [ ] **Step 1: Write the failing test** (bottom of `logging.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_node_server_level_names() {
        use tracing::Level;
        assert_eq!(map_level("fatal"), Some(Level::ERROR));
        assert_eq!(map_level("critical"), Some(Level::ERROR));
        assert_eq!(map_level("error"), Some(Level::ERROR));
        assert_eq!(map_level("warning"), Some(Level::WARN));
        assert_eq!(map_level("warn"), Some(Level::WARN));
        assert_eq!(map_level("notice"), Some(Level::INFO));
        assert_eq!(map_level("info"), Some(Level::INFO));
        assert_eq!(map_level("debug"), Some(Level::DEBUG));
        assert_eq!(map_level("verbose"), Some(Level::TRACE));
        assert_eq!(map_level("nonsense"), None);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p matter-rs-server`
Expected: FAIL — `map_level` not defined.

- [ ] **Step 3: Implement**

```rust
use tracing::Level;
use tracing_subscriber::prelude::*;

use crate::config::Config;

pub fn map_level(name: &str) -> Option<Level> {
    match name {
        "fatal" | "critical" | "error" => Some(Level::ERROR),
        "warning" | "warn" => Some(Level::WARN),
        "notice" | "info" => Some(Level::INFO),
        "debug" => Some(Level::DEBUG),
        "verbose" => Some(Level::TRACE),
        _ => None,
    }
}

pub fn init(config: &Config) {
    let level = map_level(&config.log_level).unwrap_or(Level::INFO);
    let filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(level.into())
        .from_env_lossy();

    let stderr_layer = tracing_subscriber::fmt::layer().with_writer(std::io::stderr);

    if let Some(path) = &config.log_file {
        // Plain append for v1; rotation matching the Node server is a later plan.
        let file = std::fs::OpenOptions::new().create(true).append(true).open(path)
            .expect("cannot open --log-file");
        let file_layer = tracing_subscriber::fmt::layer().with_writer(file).with_ansi(false);
        tracing_subscriber::registry().with(filter).with(stderr_layer).with(file_layer).init();
    } else {
        tracing_subscriber::registry().with(filter).with(stderr_layer).init();
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p matter-rs-server`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/server/src/logging.rs crates/server/src/main.rs
git commit -m "feat(server): tracing init with node-server level names"
```

---

### Task 8: `server` — HTTP router with /health

**Files:**
- Create: `crates/server/src/http.rs`, `crates/server/tests/health.rs`
- Modify: `crates/server/src/main.rs` (add `mod http;`)

**Interfaces:**
- Consumes: `matter_rs_controller::api::Controller`
- Produces:
  - `AppState { controller: Arc<dyn Controller>, shutdown: tokio_util-free simple broadcast — see ws task }` — concretely: `pub struct AppState { pub controller: std::sync::Arc<dyn matter_rs_controller::api::Controller>, pub shutdown: tokio::sync::watch::Receiver<bool> }` (Clone)
  - `pub fn build_router(state: AppState) -> axum::Router`
  - `GET /health` → `200 {"version": "<crate version>", "node_count": <usize>}`; any other method on /health → `405` with `Allow: GET`; unknown paths → 404.

- [ ] **Step 1: Write the failing integration test** (`crates/server/tests/health.rs`)

```rust
use std::sync::Arc;

use matter_rs_controller::stub::StubController;

mod common;
use common::{spawn_server, test_server_info};

#[tokio::test]
async fn health_reports_version_and_node_count() {
    let (addr, _shutdown) = spawn_server(Arc::new(StubController::new(test_server_info()))).await;
    let body: serde_json::Value = reqwest::get(format!("http://{addr}/health"))
        .await.unwrap().json().await.unwrap();
    assert_eq!(body["version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(body["node_count"], 0);
}

#[tokio::test]
async fn health_post_is_405_and_unknown_path_404() {
    let (addr, _shutdown) = spawn_server(Arc::new(StubController::new(test_server_info()))).await;
    let client = reqwest::Client::new();
    let resp = client.post(format!("http://{addr}/health")).send().await.unwrap();
    assert_eq!(resp.status(), 405);
    let resp = client.get(format!("http://{addr}/nope")).send().await.unwrap();
    assert_eq!(resp.status(), 404);
}
```

Also create `crates/server/tests/common/mod.rs` (the shared helper used by all three integration test files):

```rust
use std::net::SocketAddr;
use std::sync::Arc;

use matter_rs_controller::api::Controller;
use matter_rs_wire::server_info::{ServerInfoMessage, MIN_SUPPORTED_SCHEMA_VERSION, SCHEMA_VERSION};

pub fn test_server_info() -> ServerInfoMessage {
    ServerInfoMessage {
        fabric_id: 1,
        compressed_fabric_id: 0,
        fabric_index: None,
        schema_version: SCHEMA_VERSION,
        min_supported_schema_version: MIN_SUPPORTED_SCHEMA_VERSION,
        sdk_version: "matter-rs-server/test".into(),
        wifi_credentials_set: false,
        wifi_ssid: None,
        thread_credentials_set: false,
        bluetooth_enabled: false,
        ble_proxy_enabled: None,
        controller_node_id: None,
    }
}

/// Serve on an ephemeral port; returns (addr, shutdown_sender-keepalive).
pub async fn spawn_server(
    controller: Arc<dyn Controller>,
) -> (SocketAddr, tokio::sync::watch::Sender<bool>) {
    let (tx, rx) = tokio::sync::watch::channel(false);
    let state = matter_rs_server::http::AppState { controller, shutdown: rx };
    let router = matter_rs_server::http::build_router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (addr, tx)
}
```

Note: this requires the server crate to ALSO be a lib. Add to `crates/server/Cargo.toml`:
```toml
[lib]
name = "matter_rs_server"
path = "src/lib.rs"
```
and create `crates/server/src/lib.rs`:
```rust
pub mod config;
pub mod http;
pub mod logging;
pub mod ws;
```
(`main.rs` switches to `use matter_rs_server::{config, http, logging};` — drop its `mod` declarations. Create an empty `src/ws.rs` with just a doc comment for now; Task 9 fills it.)

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p matter-rs-server --test health`
Expected: FAIL — `http` module empty / `build_router` not defined.

- [ ] **Step 3: Implement** (`crates/server/src/http.rs`)

```rust
use std::sync::Arc;

use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};

use matter_rs_controller::api::Controller;

#[derive(Clone)]
pub struct AppState {
    pub controller: Arc<dyn Controller>,
    pub shutdown: tokio::sync::watch::Receiver<bool>,
}

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health).fallback(method_not_allowed))
        .route("/ws", get(crate::ws::ws_upgrade))
        .with_state(state)
}

async fn health(State(state): State<AppState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "version": env!("CARGO_PKG_VERSION"),
        "node_count": state.controller.node_count(),
    }))
}

async fn method_not_allowed() -> Response {
    (StatusCode::METHOD_NOT_ALLOWED, [(header::ALLOW, "GET")], "").into_response()
}
```

For this task, `src/ws.rs` needs a compilable placeholder handler (Task 9 replaces the body):
```rust
use axum::extract::{State, WebSocketUpgrade};
use axum::response::Response;

use crate::http::AppState;

pub async fn ws_upgrade(ws: WebSocketUpgrade, State(_state): State<AppState>) -> Response {
    ws.on_upgrade(|_socket| async {})
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p matter-rs-server --test health`
Expected: PASS (2 tests)

- [ ] **Step 5: Commit**

```bash
git add crates/server
git commit -m "feat(server): axum router with /health and server lib target"
```

---

### Task 9: `server` — WS connection actor (server_info push, dispatch, errors)

**Files:**
- Modify: `crates/server/src/ws.rs`
- Create: `crates/server/tests/ws_protocol.rs`

**Interfaces:**
- Consumes: `AppState`, `Controller::handle_command`, wire envelope types
- Produces: `/ws` behavior — on connect push bare `ServerInfoMessage`; then per text frame: parse `CommandMessage` → `handle_command` → `SuccessResult`/`ErrorResult`; malformed JSON or missing fields → `ErrorResult{message_id: "", error_code: 8}` (provisional; plan 3 verifies against Node fixtures); `start_listening` additionally flips the connection into listening mode (Task 10 uses it).

- [ ] **Step 1: Write the failing integration tests** (`crates/server/tests/ws_protocol.rs`)

```rust
use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use matter_rs_controller::stub::StubController;
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::Message;

mod common;
use common::{spawn_server, test_server_info};

async fn connect(addr: std::net::SocketAddr)
-> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    let (ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws")).await.unwrap();
    ws
}

async fn next_json(ws: &mut (impl StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin)) -> Value {
    loop {
        match ws.next().await.unwrap().unwrap() {
            Message::Text(t) => return serde_json::from_str(&t).unwrap(),
            _ => continue,
        }
    }
}

#[tokio::test]
async fn pushes_bare_server_info_on_connect() {
    let (addr, _s) = spawn_server(Arc::new(StubController::new(test_server_info()))).await;
    let mut ws = connect(addr).await;
    let hello = next_json(&mut ws).await;
    // Bare object: schema fields at top level, NO message_id envelope.
    assert_eq!(hello["schema_version"], 13);
    assert!(hello.get("message_id").is_none());
}

#[tokio::test]
async fn dispatches_commands_and_echoes_message_id() {
    let (addr, _s) = spawn_server(Arc::new(StubController::new(test_server_info()))).await;
    let mut ws = connect(addr).await;
    let _hello = next_json(&mut ws).await;

    ws.send(Message::text(r#"{"message_id":"abc","command":"get_nodes"}"#)).await.unwrap();
    let resp = next_json(&mut ws).await;
    assert_eq!(resp, json!({"message_id": "abc", "result": []}));

    ws.send(Message::text(r#"{"message_id":"x","command":"frobnicate"}"#)).await.unwrap();
    let resp = next_json(&mut ws).await;
    assert_eq!(resp["message_id"], "x");
    assert_eq!(resp["error_code"], 9);
}

#[tokio::test]
async fn malformed_json_gets_invalid_arguments_error() {
    let (addr, _s) = spawn_server(Arc::new(StubController::new(test_server_info()))).await;
    let mut ws = connect(addr).await;
    let _hello = next_json(&mut ws).await;

    ws.send(Message::text("{not json")).await.unwrap();
    let resp = next_json(&mut ws).await;
    assert_eq!(resp["error_code"], 8);
    assert_eq!(resp["message_id"], "");
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p matter-rs-server --test ws_protocol`
Expected: FAIL — connect works but no server_info frame arrives (placeholder closes).

- [ ] **Step 3: Implement** (`crates/server/src/ws.rs`, replacing the placeholder)

```rust
use axum::extract::ws::{Message, WebSocket};
use axum::extract::{State, WebSocketUpgrade};
use axum::response::Response;

use matter_rs_wire::envelope::{CommandMessage, ErrorResult, SuccessResult};
use matter_rs_wire::error::ServerErrorCode;

use crate::http::AppState;

pub async fn ws_upgrade(ws: WebSocketUpgrade, State(state): State<AppState>) -> Response {
    ws.on_upgrade(move |socket| handle_connection(socket, state))
}

async fn handle_connection(mut socket: WebSocket, state: AppState) {
    // 1. Unsolicited, bare server_info (matterjs-server behavior).
    let info = state.controller.server_info();
    if socket
        .send(Message::Text(serde_json::to_string(&info).unwrap().into()))
        .await
        .is_err()
    {
        return;
    }

    let mut listening = false;

    while let Some(Ok(msg)) = socket.recv().await {
        let Message::Text(text) = msg else { continue };

        let cmd: CommandMessage = match serde_json::from_str(&text) {
            Ok(c) => c,
            Err(e) => {
                let err = ErrorResult::new(String::new(), ServerErrorCode::InvalidArguments, e.to_string());
                if socket.send(Message::Text(serde_json::to_string(&err).unwrap().into())).await.is_err() {
                    return;
                }
                continue;
            }
        };

        let is_start_listening = cmd.command == "start_listening";
        let frame = match state.controller.handle_command(&cmd).await {
            Ok(result) => {
                if is_start_listening {
                    listening = true; // Task 10 forwards events based on this
                }
                serde_json::to_string(&SuccessResult { message_id: cmd.message_id, result }).unwrap()
            }
            Err(e) => serde_json::to_string(&ErrorResult::new(cmd.message_id, e.code, e.details)).unwrap(),
        };
        if socket.send(Message::Text(frame.into())).await.is_err() {
            return;
        }
    }
    let _ = listening; // consumed for real in Task 10
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p matter-rs-server --test ws_protocol`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add crates/server/src/ws.rs crates/server/tests
git commit -m "feat(server): WS connection actor with server_info push and dispatch"
```

---

### Task 10: `server` — event fan-out with start_listening gating + shutdown event

**Files:**
- Modify: `crates/server/src/ws.rs`, `crates/server/tests/ws_protocol.rs`

**Interfaces:**
- Consumes: `Controller::subscribe_events`, `AppState.shutdown` (watch channel; `true` = shutting down)
- Produces: events forwarded as `{"event","data"}` frames ONLY after that connection sent `start_listening`; on shutdown signal every connection (listening or not) receives `{"event":"server_shutdown","data":null}` and is closed.

- [ ] **Step 1: Write the failing tests** (append to `ws_protocol.rs`)

```rust
#[tokio::test]
async fn events_only_after_start_listening() {
    let stub = Arc::new(StubController::new(test_server_info()));
    let sender = stub.event_sender();
    let (addr, _s) = spawn_server(stub).await;
    let mut ws = connect(addr).await;
    let _hello = next_json(&mut ws).await;

    // Event before start_listening: must NOT be delivered.
    sender.send(matter_rs_wire::envelope::EventMessage { event: "node_added".into(), data: json!({"node_id": 5}) }).unwrap();

    ws.send(Message::text(r#"{"message_id":"1","command":"start_listening"}"#)).await.unwrap();
    let resp = next_json(&mut ws).await;
    assert_eq!(resp["message_id"], "1"); // the pre-listening event was dropped

    sender.send(matter_rs_wire::envelope::EventMessage { event: "node_updated".into(), data: json!({"node_id": 6}) }).unwrap();
    let ev = next_json(&mut ws).await;
    assert_eq!(ev, json!({"event": "node_updated", "data": {"node_id": 6}}));
}

#[tokio::test]
async fn shutdown_sends_server_shutdown_event_and_closes() {
    let stub = Arc::new(StubController::new(test_server_info()));
    let (addr, shutdown) = spawn_server(stub).await;
    let mut ws = connect(addr).await;
    let _hello = next_json(&mut ws).await;
    ws.send(Message::text(r#"{"message_id":"1","command":"start_listening"}"#)).await.unwrap();
    let _resp = next_json(&mut ws).await;

    shutdown.send(true).unwrap();
    let ev = next_json(&mut ws).await;
    assert_eq!(ev["event"], "server_shutdown");
    // Then the server closes the socket.
    loop {
        match ws.next().await {
            None | Some(Err(_)) => break,
            Some(Ok(Message::Close(_))) => break,
            Some(Ok(_)) => continue,
        }
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p matter-rs-server --test ws_protocol`
Expected: the two new tests FAIL (no events forwarded / no shutdown frame).

- [ ] **Step 3: Implement** — rework `handle_connection` into a select loop:

```rust
async fn handle_connection(mut socket: WebSocket, state: AppState) {
    let info = state.controller.server_info();
    if socket.send(Message::Text(serde_json::to_string(&info).unwrap().into())).await.is_err() {
        return;
    }

    let mut events = state.controller.subscribe_events();
    let mut shutdown = state.shutdown.clone();
    let mut listening = false;

    loop {
        tokio::select! {
            msg = socket.recv() => {
                let Some(Ok(msg)) = msg else { return };
                let Message::Text(text) = msg else { continue };
                let frame = handle_text_frame(&state, &text, &mut listening).await;
                if socket.send(Message::Text(frame.into())).await.is_err() { return; }
            }
            ev = events.recv() => {
                match ev {
                    Ok(ev) if listening => {
                        let frame = serde_json::to_string(&ev).unwrap();
                        if socket.send(Message::Text(frame.into())).await.is_err() { return; }
                    }
                    Ok(_) => {}                       // not listening: drop
                    Err(_) => {}                      // lagged/closed: keep serving commands
                }
            }
            res = shutdown.changed() => {
                // A dropped sender (Err) is treated as shutdown too — otherwise
                // changed() returns Err instantly forever and this arm busy-loops.
                if res.is_err() || *shutdown.borrow() {
                    let bye = serde_json::json!({"event": "server_shutdown", "data": null});
                    let _ = socket.send(Message::Text(bye.to_string().into())).await;
                    let _ = socket.close().await;
                    return;
                }
            }
        }
    }
}

async fn handle_text_frame(state: &AppState, text: &str, listening: &mut bool) -> String {
    let cmd: CommandMessage = match serde_json::from_str(text) {
        Ok(c) => c,
        Err(e) => {
            return serde_json::to_string(&ErrorResult::new(
                String::new(), ServerErrorCode::InvalidArguments, e.to_string(),
            )).unwrap();
        }
    };
    let is_start_listening = cmd.command == "start_listening";
    match state.controller.handle_command(&cmd).await {
        Ok(result) => {
            if is_start_listening { *listening = true; }
            serde_json::to_string(&SuccessResult { message_id: cmd.message_id, result }).unwrap()
        }
        Err(e) => serde_json::to_string(&ErrorResult::new(cmd.message_id, e.code, e.details)).unwrap(),
    }
}
```

- [ ] **Step 4: Run all server tests**

Run: `cargo test -p matter-rs-server`
Expected: PASS (all tests incl. Tasks 8–9's)

- [ ] **Step 5: Commit**

```bash
git add crates/server/src/ws.rs crates/server/tests/ws_protocol.rs
git commit -m "feat(server): event fan-out with start_listening gating and shutdown event"
```

---

### Task 11: `server` — main() wiring + binary smoke test

**Files:**
- Modify: `crates/server/src/main.rs`
- Create: `crates/server/tests/smoke.rs`
- Modify: `crates/server/Cargo.toml` (dev-dependency `assert_cmd = "2"` not needed — we spawn via `env!("CARGO_BIN_EXE_matter-rs-server")`)

**Interfaces:**
- Consumes: everything above
- Produces: `matter-rs-server --port N --storage-path P` runs: creates the storage dir (0700) if missing, binds `--listen-address` list (or all interfaces), serves /health and /ws, handles SIGTERM/SIGINT by flipping the shutdown watch, waits max 3 s for connections to drain, exits 0.

- [ ] **Step 1: Write the failing smoke test** (`crates/server/tests/smoke.rs`)

```rust
use std::process::Stdio;
use std::time::Duration;

#[tokio::test]
async fn binary_serves_health_and_ws_and_stops_on_sigterm() {
    let dir = std::env::temp_dir().join(format!("mrs-smoke-{}", std::process::id()));
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_matter-rs-server"))
        .args(["--port", "0"]) // 0 = kernel-picked; printed on stdout as "listening on <addr>"
        .args(["--storage-path", dir.to_str().unwrap()])
        .args(["--listen-address", "127.0.0.1"])
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();

    // Parse "listening on 127.0.0.1:PORT" from stdout.
    use tokio::io::{AsyncBufReadExt, BufReader};
    let stdout = child.stdout.take().unwrap();
    let mut lines = BufReader::new(stdout).lines();
    let addr = loop {
        let line = tokio::time::timeout(Duration::from_secs(10), lines.next_line())
            .await.unwrap().unwrap().unwrap();
        if let Some(rest) = line.strip_prefix("listening on ") {
            break rest.trim().to_string();
        }
    };

    let health: serde_json::Value =
        reqwest::get(format!("http://{addr}/health")).await.unwrap().json().await.unwrap();
    assert_eq!(health["node_count"], 0);

    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws")).await.unwrap();
    use futures_util::StreamExt;
    let first = ws.next().await.unwrap().unwrap();
    assert!(first.to_text().unwrap().contains("\"schema_version\":13"));

    // SIGTERM -> clean exit 0.
    send_sigterm(child.id().unwrap());
    let status = tokio::time::timeout(Duration::from_secs(5), child.wait()).await.unwrap().unwrap();
    assert!(status.success(), "exit was {status:?}");
    assert!(dir.exists(), "storage dir must be created");
}

fn send_sigterm(pid: u32) {
    // SIGTERM without adding a nix dependency:
    let _ = std::process::Command::new("kill").args(["-TERM", &pid.to_string()]).status();
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p matter-rs-server --test smoke`
Expected: FAIL — binary still prints scaffold text, no "listening on" line.

- [ ] **Step 3: Implement `main.rs`**

```rust
use std::sync::Arc;

use clap::Parser;

use matter_rs_controller::stub::StubController;
use matter_rs_server::{config::Config, http, logging};
use matter_rs_wire::server_info::{ServerInfoMessage, MIN_SUPPORTED_SCHEMA_VERSION, SCHEMA_VERSION};

#[tokio::main]
async fn main() {
    let config = Config::parse();
    logging::init(&config);
    config.warn_ignored();

    // Storage dir now (plan 2 stores fabric data in it).
    std::fs::create_dir_all(&config.storage_path).expect("cannot create --storage-path");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&config.storage_path, std::fs::Permissions::from_mode(0o700));
    }

    // Plan 1: stub controller. Plan 2 replaces this with the rs-matter one.
    let info = ServerInfoMessage {
        fabric_id: config.fabric_id,
        compressed_fabric_id: 0,
        fabric_index: None,
        schema_version: SCHEMA_VERSION,
        min_supported_schema_version: MIN_SUPPORTED_SCHEMA_VERSION,
        sdk_version: format!("matter-rs-server/{} (rs-matter/pending)", env!("CARGO_PKG_VERSION")),
        wifi_credentials_set: false,
        wifi_ssid: None,
        thread_credentials_set: false,
        bluetooth_enabled: false,
        ble_proxy_enabled: None,
        controller_node_id: None,
    };
    let controller = Arc::new(StubController::new(info));

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let state = http::AppState { controller, shutdown: shutdown_rx };
    let router = http::build_router(state);

    // Bind each --listen-address (or all interfaces when none given).
    let addrs: Vec<String> = if config.listen_address.is_empty() {
        tracing::warn!("no --listen-address given; binding all interfaces");
        vec![format!("[::]:{}", config.port)]
    } else {
        config.listen_address.iter().map(|a| {
            if a.contains(':') { format!("[{}]:{}", a, config.port) } else { format!("{}:{}", a, config.port) }
        }).collect()
    };

    let mut servers = tokio::task::JoinSet::new();
    for addr in addrs {
        let listener = tokio::net::TcpListener::bind(&addr).await
            .unwrap_or_else(|e| panic!("cannot bind {addr}: {e}"));
        println!("listening on {}", listener.local_addr().unwrap());
        let router = router.clone();
        let mut rx = shutdown_tx.subscribe();
        servers.spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async move { let _ = rx.changed().await; })
                .await
                .unwrap();
        });
    }

    // SIGTERM/SIGINT -> shutdown.
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
    tokio::select! {
        _ = sigterm.recv() => {},
        _ = tokio::signal::ctrl_c() => {},
    }
    tracing::info!("shutting down");
    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while servers.join_next().await.is_some() {}
    }).await;
}
```

- [ ] **Step 4: Run the full test suite**

Run: `cargo test`
Expected: PASS — all wire, controller, and server tests including smoke.

- [ ] **Step 5: Commit**

```bash
git add crates/server
git commit -m "feat(server): main wiring, multi-listen, graceful SIGTERM shutdown"
```

---

### Task 12: README + finish branch

**Files:**
- Create: `README.md`

- [ ] **Step 1: Write README**

```markdown
# matter-rs-server

Rust port of the OHF matterjs-server: a Matter controller daemon with the
python-matter-server-compatible WebSocket API used by Home Assistant.
Status: plan 1 (protocol skeleton, stub controller). See
`docs/superpowers/specs/` for the design and `spike/SPIKE-RESULTS.md` for
the rs-matter validation.

## Run

    cargo run -p matter-rs-server -- --storage-path /tmp/mrs --listen-address 127.0.0.1

- `GET /health` -> `{"version", "node_count"}`
- `ws://host:5580/ws` -> python-matter-server WS API (schema 13)

## systemd (target deployment)

    [Service]
    ExecStart=/usr/local/bin/matter-rs-server --storage-path /var/lib/matter-rs-server
    Restart=on-failure
    RestartSec=5

Thread devices require the host to accept RA route-info
(`net.ipv6.conf.eth0.accept_ra_rt_info_max_plen = 64`) — see spike finding 4.

## Test

    cargo test
```

- [ ] **Step 2: Verify the whole workspace once more**

Run: `cargo test && cargo build --release`
Expected: all green; release binary at `target/release/matter-rs-server`.

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: README with run/deploy instructions"
```

- [ ] **Step 4: Finish the branch**

Use the superpowers:finishing-a-development-branch skill (merge to master, push).

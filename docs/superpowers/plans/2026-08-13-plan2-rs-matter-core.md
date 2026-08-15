# matter-rs-server Plan 2: rs-matter Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `StubController` with a real rs-matter-backed controller: fabric identity + storage, commissioning (RCAC-direct), per-node wildcard subscriptions with availability tracking, and functional implementations of all 31 in-scope WS commands.

**Architecture:** Two new crates. `gen` (build-time cluster-metadata tables parsed from the CSA `.matter` IDL — command/struct/event/attribute names, ids, types) and `stack` (the ONLY crate importing rs-matter; runs the whole Matter stack on one dedicated OS thread with a local executor, because rs-matter futures are !Send — upstream's own work-stealing example documents it doesn't compile). The `controller` crate gains storage, a node registry, a node-manager task, and `MatterController` implementing the plan-1 `Controller` trait; it talks to the stack only through a `Stack` trait (async, Send) defined in `controller::stack_api` and implemented by `stack::StackHandle` over an mpsc channel, so all controller logic is unit-testable against a `FakeStack`.

**Tech Stack:** rs-matter pinned to `03bc8f2aeb7765a93e7863e2263f73c7bbc3d401` (the spike-validated rev), embassy-futures/embassy-time + async-io + futures-lite (stack thread), tokio (everything else), chrono (Node-compatible date strings), base64, serde/serde_json.

## Global Constraints

- Spec: `docs/superpowers/specs/2026-08-13-matter-rs-server-design.md`. Spike findings: `spike/SPIKE-RESULTS.md`. Carryover: `docs/superpowers/plans/2026-08-13-plan2-carryover.md`.
- Node-server reference semantics (exact args/results/error strings) were extracted from `matterjs-server/packages/ws-controller/src` — each task below embeds the relevant facts; where docs and Node code disagreed, the code was trusted.
- **RCAC-direct mode always** (NOC signed by the RCAC, empty ICAC) — matter.js rejects rs-matter's ICAC (spike finding 1).
- **rs-matter runs single-threaded** on a dedicated OS thread. Never spawn rs-matter futures on the tokio runtime. Every IM operation consumes an `Exchange`; responses borrow the exchange RX buffer, so convert TLV→owned JSON inside the borrow scope. `use rs_matter::im::client::ImClient;` is needed for the generic read/write/invoke/subscribe methods (trait defaults, not inherent). Build closures passed to senders are `FnMut` re-run on retransmit — keep them pure.
- `CommissionOptions { allow_test_attestation: true, .. }` is mandatory (no PAA verification upstream yet — accepted v1 gap).
- Controller identity constants: controller node id `112233`, fabric id from `--fabricid` (default 1), admin vendor id from `--vendorid` (default 0xFFF1).
- Wire invariants (compat-critical): u64 ids as unquoted JSON numbers; attribute values tag-based (decimal-string field-id keys); command responses & `node_event.data` name-based (camelCase, from `gen` tables); attribute paths decimal `endpoint/cluster/attr`; octet strings ↔ base64; epoch conversions with `MATTER_EPOCH_OFFSET_S = 946_684_800`.
- Dates (`date_commissioned`, `last_interview`): **local time**, format `YYYY-MM-DDTHH:mm:ss.SSS000` (millis + literal `"000"`, no timezone suffix) — chrono format string `"%Y-%m-%dT%H:%M:%S%.3f000"`.
- Error codes as in plan 1. `details` strings must match the Node server where specified in tasks (`Node <id> does not exist`, `Unknown command: <cmd>`, etc.).
- Storage: all writes atomic (temp file in same dir + rename); `server.json` and `config.json` mode 0600. Layout: `server.json`, `config.json`, `nodes/<node-id>.json`, `sessions/` (rs-matter `DirKvBlobStore` for fabric blob + CASE resumption; deviation from the spec's `sessions.json` — same best-effort intent, documented in README).
- Commit style: conventional (`feat:`, `test:`, `chore:`), each ending with trailer `Claude-Session: https://claude.ai/code/session_01BxfHyF8XvzcwxUtWUcDuYM`.
- All work on branch `plan2-rs-matter-core`.
- After EVERY task: `cargo test --workspace` must pass before committing.

### Known, accepted v1 deviations from the Node server (documented here so nobody "fixes" them silently)

1. `read_attribute` issues one IM read for all paths (rs-matter chunks responses) instead of Node's 9-path batching.
2. Tag-based epoch conversion is applied only for top-level attribute values whose type is known from `gen`; nested epoch fields inside unknown structs pass through numerically (plan 3 fixtures will tighten).
3. `node_updated` is emitted on priming/interview/availability changes, not on Node's 6-second basic-info debounce.
4. `set_loglevel` drives one global filter; `file_loglevel` mirrors `console_loglevel` when `--log-file` is set, `null` otherwise.
5. `discover`/`discover_commissionable_nodes` return instance + address only (rs-matter browse doesn't expose TXT metadata); other fields get Node's own defaults (`host_name: "000000000000"`, `product_id: -1`, ...).
6. Backpressure send-classes (reliable/ordered/coalescable) stay deferred (plan 3); broadcast channel from plan 1 is kept.
7. `get_vendor_names` is the static table only (no DCL).

## File Structure

```
crates/gen/                        # NEW — no rs-matter dependency
├── Cargo.toml                     # name matter-rs-gen
├── build.rs                       # parses idl/controller-clusters-V1.6.0.0.matter -> OUT_DIR/tables.rs
├── idl/controller-clusters-V1.6.0.0.matter   # vendored from rs-matter-codegen (Apache-2.0)
└── src/lib.rs                     # Cluster/Attr/Cmd/Struct/Field/Event types + lookups + include!
crates/wire/src/
├── lib.rs                         # + pub mod node;
└── node.rs                        # MatterNodeData, MatterNodeEvent, CommissionableNodeData, MatterFabricData, IcdState
crates/controller/
├── Cargo.toml                     # + chrono, tracing; tokio macros -> dev-only; drop thiserror
└── src/
    ├── lib.rs                     # + storage, stack_api, registry, node_manager, real, vendors, commands
    ├── api.rs                     # Controller trait: + ConnId param, connection_closed()
    ├── storage.rs                 # atomic JSON store: ServerIdentity, ConfigData, NodeRecord
    ├── stack_api.rs               # Stack trait, StackEvent, StackError, request/response types
    ├── registry.rs                # Registry, MatterNodeData building (is_bridge, matter_version, dates)
    ├── node_manager.rs            # StackEvent consumer: cache/persist/broadcast + 3-min grace
    ├── vendors.rs                 # static vendor id -> name table
    ├── real.rs                    # MatterController + dispatch
    └── commands/                  # one module per command family
        ├── mod.rs  nodes.rs  interaction.rs  commissioning.rs  credentials.rs  fabrics.rs  misc.rs
crates/stack/                      # NEW — the ONLY crate importing rs-matter
├── Cargo.toml
└── src/
    ├── lib.rs                     # pub spawn(), StackHandle, StackConfig
    ├── tlv_json.rs                # TLV<->JSON (tag-based + name-based), type-driven via gen
    ├── identity.rs                # RCAC-direct identity generate/load -> Matter fabric bootstrap
    ├── mdns.rs                    # builtin mDNS runner (ported from spike, --primary-interface)
    ├── runtime.rs                 # stack thread: executor, matter.run, responder, request loop
    ├── reports.rs                 # ReportDataHandler -> StackEvent channel
    ├── supervisor.rs              # per-node subscribe loop, liveness watchdog, backoff
    └── ops/
        ├── mod.rs  interact.rs  commission.rs  window.rs  fabrics.rs  discovery.rs
crates/server/src/
    ├── main.rs                    # real controller wiring, ready handshake, shutdown order
    ├── logging.rs                 # reload handle (LogControl) for set_loglevel
    ├── config.rs                  # LISTEN_ADDRESS value_delimiter, env-isolated tests
    └── ws.rs                      # ConnId plumbing, connection_closed, Lagged warn
```

Dependency directions: `gen` ← `stack`; `wire` ← `controller` ← `stack` ← `server`; `controller` never sees rs-matter, `stack` never sees axum.

---

### Task 1: `gen` crate — cluster metadata tables from the .matter IDL

**Files:**
- Create: `crates/gen/Cargo.toml`, `crates/gen/build.rs`, `crates/gen/src/lib.rs`, `crates/gen/idl/controller-clusters-V1.6.0.0.matter`
- Modify: root `Cargo.toml` (workspace members)

**Interfaces:**
- Consumes: nothing (build-time text parsing only).
- Produces (used by `stack` in Tasks 12/15/16):
  - `matter_rs_gen::cluster(code: u32) -> Option<&'static Cluster>`
  - `Cluster { code: u32, name: &'static str, attributes: &'static [Attr], commands: &'static [Cmd], structs: &'static [Struct], events: &'static [Event] }`
  - `Attr { code: u32, name: &'static str, ty: &'static str, is_list: bool }`
  - `Cmd { code: u32, name: &'static str, input: Option<&'static str>, output: Option<&'static str>, is_timed: bool }` (`output: None` = DefaultSuccess)
  - `Struct { name: &'static str, fields: &'static [Field] }`, `Field { code: u32, name: &'static str, ty: &'static str, is_list: bool }`
  - `Event { code: u32, name: &'static str, fields: &'static [Field] }`
  - `Cluster::find_command_ci(&self, name: &str) -> Option<&'static Cmd>` (case-insensitive — Node camelizes incoming `command_name`), `Cluster::find_struct(&self, name) -> Option<&'static Struct>`, `Cluster::attr(&self, code) -> Option<&'static Attr>`, `Cluster::event(&self, code) -> Option<&'static Event>`

**Background:** rs-matter-codegen embeds the CSA IDL as `CSA_STANDARD_CLUSTERS_IDL_V1_6_0_0` but its `idl` parser module is **private**, so we vendor the file and parse the four line-shapes we need ourselves. The relevant IDL grammar (verified against the real file):

```text
cluster OnOff = 6 {                      <- also "provisional cluster", "internal cluster"
  readonly attribute boolean onOff = 0;  <- qualifiers: readonly/optional/nosubscribe/attribute access(...)
  attribute DeviceTypeStruct deviceTypeList[] = 0;      <- "[]" after name = list
  struct TargetStruct { node_id node = 1; ... }
  request struct MoveToLevelRequest { int8u level = 0; optional nullable ... }
  response struct NOCResponse = 8 { enum8 statusCode = 0; ... }
  critical event StartUp = 0 { int32u softwareVersion = 0; }
  command Off(): DefaultSuccess = 0;
  command MoveToLevel(MoveToLevelRequest): DefaultSuccess = 0;
  timed command access(invoke: administer) OpenCommissioningWindow(OpenCommissioningWindowRequest): DefaultSuccess = 0;
  enum ... { }  bitmap ... { }           <- skip these blocks entirely
}
```

- [ ] **Step 1: Create branch and vendor the IDL**

```bash
git checkout -b plan2-rs-matter-core
mkdir -p crates/gen/idl crates/gen/src
cp rs-matter-ref/rs-matter-codegen/src/idl/parser/controller-clusters-V1.6.0.0.matter crates/gen/idl/
```

(The `rs-matter-ref/` clone is gitignored; the vendored copy keeps its Apache-2.0 header. Same IDL version rs-matter 03bc8f2 itself is generated from.)

- [ ] **Step 2: Write manifests + lib skeleton with failing tests**

Root `Cargo.toml` members: `["crates/gen", "crates/wire", "crates/controller", "crates/server"]` (add `crates/stack` later, Task 12).

`crates/gen/Cargo.toml`:
```toml
[package]
name = "matter-rs-gen"
version.workspace = true
edition.workspace = true
license.workspace = true
build = "build.rs"
```

`crates/gen/src/lib.rs`:
```rust
//! Build-time cluster metadata from the CSA .matter IDL (vendored V1.6.0.0).
//! Used for: device_command name->id, name-based JSON for command responses
//! and events, and TLV type hints for JSON->TLV encoding.

#[derive(Debug)]
pub struct Cluster {
    pub code: u32,
    pub name: &'static str,
    pub attributes: &'static [Attr],
    pub commands: &'static [Cmd],
    pub structs: &'static [Struct],
    pub events: &'static [Event],
}

#[derive(Debug)]
pub struct Attr { pub code: u32, pub name: &'static str, pub ty: &'static str, pub is_list: bool }
#[derive(Debug)]
pub struct Cmd { pub code: u32, pub name: &'static str, pub input: Option<&'static str>, pub output: Option<&'static str>, pub is_timed: bool }
#[derive(Debug)]
pub struct Struct { pub name: &'static str, pub fields: &'static [Field] }
#[derive(Debug)]
pub struct Field { pub code: u32, pub name: &'static str, pub ty: &'static str, pub is_list: bool }
#[derive(Debug)]
pub struct Event { pub code: u32, pub name: &'static str, pub fields: &'static [Field] }

include!(concat!(env!("OUT_DIR"), "/tables.rs")); // defines: static CLUSTERS: &[Cluster] (sorted by code)

/// Look up a cluster by its Matter cluster id.
pub fn cluster(code: u32) -> Option<&'static Cluster> {
    CLUSTERS.binary_search_by_key(&code, |c| c.code).ok().map(|i| &CLUSTERS[i])
}

impl Cluster {
    pub fn find_command_ci(&self, name: &str) -> Option<&'static Cmd> {
        self.commands.iter().find(|c| c.name.eq_ignore_ascii_case(name))
    }
    pub fn find_struct(&self, name: &str) -> Option<&'static Struct> {
        self.structs.iter().find(|s| s.name == name)
    }
    pub fn attr(&self, code: u32) -> Option<&'static Attr> {
        self.attributes.iter().find(|a| a.code == code)
    }
    pub fn event(&self, code: u32) -> Option<&'static Event> {
        self.events.iter().find(|e| e.code == code)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn onoff_commands() {
        let c = cluster(6).expect("OnOff cluster");
        assert_eq!(c.name, "OnOff");
        let toggle = c.find_command_ci("toggle").expect("Toggle");
        assert_eq!(toggle.code, 2);
        assert_eq!(toggle.input, None);
        assert_eq!(toggle.output, None); // DefaultSuccess
    }

    #[test]
    fn level_control_input_struct_fields() {
        let c = cluster(8).expect("LevelControl");
        let mv = c.find_command_ci("moveToLevel").expect("MoveToLevel");
        let input = c.find_struct(mv.input.expect("has input")).expect("request struct");
        let level = input.fields.iter().find(|f| f.name == "level").unwrap();
        assert_eq!(level.code, 0);
        assert_eq!(level.ty, "int8u");
    }

    #[test]
    fn operational_credentials_response_struct() {
        let c = cluster(62).expect("OperationalCredentials");
        let rf = c.find_command_ci("removeFabric").expect("RemoveFabric");
        assert_eq!(rf.code, 10);
        let out = c.find_struct(rf.output.expect("NOCResponse")).unwrap();
        assert!(out.fields.iter().any(|f| f.name == "statusCode" && f.code == 0));
        assert!(out.fields.iter().any(|f| f.name == "fabricIndex" && f.code == 1));
    }

    #[test]
    fn admin_commissioning_is_timed() {
        let c = cluster(60).expect("AdministratorCommissioning");
        let ocw = c.find_command_ci("openCommissioningWindow").unwrap();
        assert!(ocw.is_timed);
    }

    #[test]
    fn descriptor_device_type_list_is_list_attr() {
        let c = cluster(29).expect("Descriptor");
        let a = c.attr(0).unwrap();
        assert_eq!(a.name, "deviceTypeList");
        assert!(a.is_list);
    }

    #[test]
    fn events_with_fields() {
        // BasicInformation StartUp event carries softwareVersion.
        let c = cluster(40).unwrap();
        let e = c.event(0).expect("StartUp");
        assert_eq!(e.name, "StartUp");
        assert!(e.fields.iter().any(|f| f.name == "softwareVersion"));
    }
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cargo test -p matter-rs-gen`
Expected: FAIL — build.rs missing / `tables.rs` not generated.

- [ ] **Step 4: Implement `build.rs`**

A line-oriented extractor. It must be deliberately forgiving: skip `enum`/`bitmap` blocks by brace counting, ignore lines it doesn't recognize (the vendored file is fixed, so silent-skip is safe — the unit tests pin the clusters we rely on).

```rust
//! Parses idl/controller-clusters-V1.6.0.0.matter into static Rust tables.
//! Focused extractor, not a general IDL parser: clusters, attributes,
//! commands, (request/response/plain) structs, events. Enum/bitmap blocks
//! are skipped. Unrecognized lines are ignored.

use std::fmt::Write as _;
use std::path::PathBuf;

#[derive(Default)]
struct Cluster {
    name: String,
    code: u64,
    attrs: Vec<(u64, String, String, bool)>,      // code, name, ty, is_list
    cmds: Vec<(u64, String, Option<String>, Option<String>, bool)>, // code, name, input, output, timed
    structs: Vec<(String, Vec<(u64, String, String, bool)>)>,
    events: Vec<(u64, String, Vec<(u64, String, String, bool)>)>,
}

fn main() {
    println!("cargo:rerun-if-changed=idl/controller-clusters-V1.6.0.0.matter");
    println!("cargo:rerun-if-changed=build.rs");
    let src = std::fs::read_to_string("idl/controller-clusters-V1.6.0.0.matter").unwrap();
    let clusters = parse(&src);
    let out = render(&clusters);
    let dest = PathBuf::from(std::env::var("OUT_DIR").unwrap()).join("tables.rs");
    std::fs::write(dest, out).unwrap();
}

/// "int8u level = 0;" / "optional nullable CharString<32> foo = 1;" /
/// "DeviceTypeStruct deviceTypeList[] = 0;" -> (code, name, ty, is_list)
fn parse_field(line: &str) -> Option<(u64, String, String, bool)> {
    let line = line.trim().trim_end_matches(';');
    let (decl, code) = line.rsplit_once('=')?;
    let code: u64 = code.trim().parse().ok()?;
    let mut toks: Vec<&str> = decl.split_whitespace().collect();
    // strip qualifiers from the front
    while matches!(toks.first().copied(), Some("optional" | "nullable" | "readonly" | "fabric_idx" | "fabric_sensitive")) {
        toks.remove(0);
    }
    if toks.len() < 2 { return None; }
    let name_tok = toks[toks.len() - 1];
    let ty_tok = toks[toks.len() - 2];
    let is_list = name_tok.ends_with("[]");
    let name = name_tok.trim_end_matches("[]").to_string();
    // "octet_string<32>" -> "octet_string"
    let ty = ty_tok.split('<').next().unwrap_or(ty_tok).to_string();
    Some((code, name, ty, is_list))
}

fn parse(src: &str) -> Vec<Cluster> {
    let mut clusters: Vec<Cluster> = Vec::new();
    let mut cur: Option<Cluster> = None;
    // (kind, name/code, fields) for the struct/event block being collected
    enum Block { Skip, Struct(String), Event(u64, String) }
    let mut block: Option<(Block, Vec<(u64, String, String, bool)>)> = None;
    let mut depth_in_block = 0i32;

    for raw in src.lines() {
        let line = raw.trim();
        if line.starts_with("//") || line.is_empty() { continue; }

        if let Some((kind, fields)) = block.as_mut() {
            if line.contains('{') { depth_in_block += 1; }
            if line.contains('}') {
                depth_in_block -= 1;
                if depth_in_block <= 0 {
                    let (kind, fields) = block.take().unwrap();
                    if let Some(c) = cur.as_mut() {
                        match kind {
                            Block::Struct(name) => c.structs.push((name, fields)),
                            Block::Event(code, name) => c.events.push((code, name, fields)),
                            Block::Skip => {}
                        }
                    }
                    continue;
                }
            }
            if !matches!(kind, Block::Skip) {
                if let Some(f) = parse_field(line) { fields.push(f); }
            }
            continue;
        }

        let words: Vec<&str> = line.split_whitespace().collect();
        if words.is_empty() { continue; }

        // cluster header: "[provisional|internal|deprecated]* cluster Name = N {"
        if let Some(pos) = words.iter().position(|w| *w == "cluster") {
            if words.get(pos + 2) == Some(&"=") && line.ends_with('{') {
                if let Some(c) = cur.take() { clusters.push(c); }
                cur = Some(Cluster {
                    name: words[pos + 1].to_string(),
                    code: words[pos + 3].trim_end_matches(['{', ' ']).parse().unwrap_or(u64::MAX),
                    ..Default::default()
                });
                continue;
            }
        }
        let Some(c) = cur.as_mut() else { continue };

        if line == "}" { clusters.push(cur.take().unwrap()); continue; }

        if words.contains(&"enum") || words.contains(&"bitmap") {
            if line.ends_with('{') { block = Some((Block::Skip, Vec::new())); depth_in_block = 1; }
            continue;
        }
        if let Some(pos) = words.iter().position(|w| *w == "struct") {
            // "request struct X {" | "response struct X = 8 {" | "struct X {"
            let name = words.get(pos + 1).unwrap_or(&"").trim_end_matches('{').to_string();
            if line.ends_with('{') { block = Some((Block::Struct(name), Vec::new())); depth_in_block = 1; }
            continue;
        }
        if let Some(pos) = words.iter().position(|w| *w == "event") {
            // "critical event StartUp = 0 {"
            if words.get(pos + 2) == Some(&"=") && line.ends_with('{') {
                let name = words[pos + 1].to_string();
                let code = words[pos + 3].trim_end_matches(['{', ' ']).parse().unwrap_or(0);
                block = Some((Block::Event(code, name), Vec::new()));
                depth_in_block = 1;
            }
            continue;
        }
        if words.contains(&"attribute") {
            // strip everything up to and including "attribute" and any "access(...)"
            let rest = line.split("attribute").nth(1).unwrap_or("");
            let rest = strip_access(rest);
            if let Some(f) = parse_field(&rest) { c.attrs.push(f); }
            continue;
        }
        if let Some(pos) = words.iter().position(|w| *w == "command") {
            // "[timed|fabric]* command [access(...)] Name(Input?): Output = N;"
            let is_timed = words[..pos].contains(&"timed");
            let rest = line.split("command").nth(1).unwrap_or("");
            let rest = strip_access(rest);
            if let Some(cap) = parse_command(&rest) {
                let (name, input, output, code) = cap;
                c.cmds.push((code, name, input, output, is_timed));
            }
            continue;
        }
    }
    if let Some(c) = cur.take() { clusters.push(c); }
    clusters.retain(|c| c.code != u64::MAX);
    clusters.sort_by_key(|c| c.code);
    clusters.dedup_by_key(|c| c.code);
    clusters
}

/// remove an "access(...)" group anywhere in the fragment
fn strip_access(s: &str) -> String {
    if let Some(start) = s.find("access(") {
        if let Some(end) = s[start..].find(')') {
            let mut out = String::new();
            out.push_str(&s[..start]);
            out.push_str(&s[start + end + 1..]);
            return out;
        }
    }
    s.to_string()
}

/// " Name(Input): Output = N;" -> (name, input, output, code)
fn parse_command(s: &str) -> Option<(String, Option<String>, Option<String>, u64)> {
    let s = s.trim().trim_end_matches(';');
    let (sig, code) = s.rsplit_once('=')?;
    let code: u64 = code.trim().parse().ok()?;
    let (call, output) = sig.rsplit_once(':')?;
    let output = output.trim();
    let output = if output == "DefaultSuccess" { None } else { Some(output.to_string()) };
    let call = call.trim();
    let open = call.find('(')?;
    let name = call[..open].trim().to_string();
    let inner = call[open + 1..call.rfind(')')?].trim();
    let input = if inner.is_empty() { None } else { Some(inner.to_string()) };
    Some((name, input, output, code))
}

fn render(clusters: &[Cluster]) -> String {
    let mut o = String::new();
    let esc = |s: &str| s.replace('"', "\\\"");
    let fields = |o: &mut String, fs: &[(u64, String, String, bool)]| {
        for (code, name, ty, is_list) in fs {
            writeln!(o, "        Field {{ code: {code}, name: \"{}\", ty: \"{}\", is_list: {is_list} }},", esc(name), esc(ty)).unwrap();
        }
    };
    writeln!(o, "static CLUSTERS: &[Cluster] = &[").unwrap();
    for c in clusters {
        writeln!(o, "Cluster {{ code: {}, name: \"{}\", attributes: &[", c.code, esc(&c.name)).unwrap();
        for (code, name, ty, is_list) in &c.attrs {
            writeln!(o, "    Attr {{ code: {code}, name: \"{}\", ty: \"{}\", is_list: {is_list} }},", esc(name), esc(ty)).unwrap();
        }
        writeln!(o, "], commands: &[").unwrap();
        for (code, name, input, output, timed) in &c.cmds {
            let i = input.as_ref().map(|s| format!("Some(\"{}\")", esc(s))).unwrap_or("None".into());
            let out = output.as_ref().map(|s| format!("Some(\"{}\")", esc(s))).unwrap_or("None".into());
            writeln!(o, "    Cmd {{ code: {code}, name: \"{}\", input: {i}, output: {out}, is_timed: {timed} }},", esc(name)).unwrap();
        }
        writeln!(o, "], structs: &[").unwrap();
        for (name, fs) in &c.structs {
            writeln!(o, "    Struct {{ name: \"{}\", fields: &[", esc(name)).unwrap();
            fields(&mut o, fs);
            writeln!(o, "    ] }},").unwrap();
        }
        writeln!(o, "], events: &[").unwrap();
        for (code, name, fs) in &c.events {
            writeln!(o, "    Event {{ code: {code}, name: \"{}\", fields: &[", esc(name)).unwrap();
            fields(&mut o, fs);
            writeln!(o, "    ] }},").unwrap();
        }
        writeln!(o, "] }},").unwrap();
    }
    writeln!(o, "];").unwrap();
    o
}
```

Note: attribute field codes in `Cluster.attrs` come out of `parse_field` on the fragment after `attribute`, which already handles list markers and `Type<len>`.

- [ ] **Step 5: Run tests until they pass**

Run: `cargo test -p matter-rs-gen`
Expected: PASS (6 tests). If a test fails, inspect the actual IDL lines for that cluster in the vendored file (`grep -n "cluster OnOff" crates/gen/idl/*.matter` etc.) and adjust the extractor — the tests are the contract, the extractor is disposable.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml crates/gen
git commit -m "feat(gen): build-time cluster metadata tables from vendored CSA IDL"
```

---

### Task 2: `wire` — node/event/fabric wire models

**Files:**
- Create: `crates/wire/src/node.rs`
- Modify: `crates/wire/src/lib.rs` (add `pub mod node;`)

**Interfaces:**
- Consumes: serde only.
- Produces (exact wire shapes; Tasks 5, 8–11 build these):

```rust
MatterNodeData { node_id: u64, date_commissioned: String, last_interview: String,
                 interview_version: u8 /* always 6 */, available: bool, is_bridge: bool,
                 attributes: serde_json::Map<String, Value>,
                 attribute_subscriptions: Vec<Value> /* always [] */,
                 matter_version: Option<String> /* skip if None */ }
MatterNodeEvent { node_id: u64, endpoint_id: u16, cluster_id: u32, event_id: u32,
                  event_number: u64, priority: u8, timestamp: i64, timestamp_type: u8,
                  data: Value }
CommissionableNodeData { instance_name: Option<String>, host_name: String /* "000000000000" */,
                 port: Option<u16>, long_discriminator: Option<u16>, vendor_id: i32,
                 product_id: i32, commissioning_mode: u8, device_type: Option<u32>,
                 device_name: Option<String>, pairing_instruction: Option<String>,
                 pairing_hint: u32, mrp_retry_interval_idle: Option<u32>,
                 mrp_retry_interval_active: Option<u32>, supports_tcp: bool,
                 addresses: Vec<String>, rotating_id: Option<String> }
MatterFabricData { fabric_id: u64, vendor_id: u16, fabric_index: u8,
                   fabric_label: Option<String>, vendor_name: Option<String> /* skip if None */ }
IcdState { supported: bool, lit_supported: bool, registered: bool,
           operating_mode: Option<String>, awake: Option<bool>, available: Option<bool>,
           next_expected_checkin: Option<i64> }  // all-null "not registered" default
```

- [ ] **Step 1: Write the failing tests** (bottom of `node.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn matter_node_data_shape() {
        let mut attributes = serde_json::Map::new();
        attributes.insert("0/40/2".into(), json!(65521));
        attributes.insert("1/6/0".into(), json!(true));
        let n = MatterNodeData {
            node_id: 4,
            date_commissioned: "2026-08-13T10:15:42.123000".into(),
            last_interview: "2026-08-13T10:15:42.123000".into(),
            interview_version: 6,
            available: true,
            is_bridge: false,
            attributes,
            attribute_subscriptions: vec![],
            matter_version: None,
        };
        let v = serde_json::to_value(&n).unwrap();
        assert_eq!(v["node_id"], 4);
        assert_eq!(v["interview_version"], 6);
        assert_eq!(v["attributes"]["1/6/0"], true);
        assert_eq!(v["attribute_subscriptions"], json!([]));
        assert!(v.get("matter_version").is_none());
    }

    #[test]
    fn node_event_shape() {
        let e = MatterNodeEvent {
            node_id: 1, endpoint_id: 1, cluster_id: 59, event_id: 1,
            event_number: 12345, priority: 1, timestamp: 1704067200000,
            timestamp_type: 1, data: json!({"newPosition": 1}),
        };
        let v = serde_json::to_value(&e).unwrap();
        assert_eq!(v["event_number"], 12345);
        assert_eq!(v["data"]["newPosition"], 1);
    }

    #[test]
    fn icd_state_not_registered_default() {
        let v = serde_json::to_value(IcdState::not_registered()).unwrap();
        assert_eq!(v["supported"], false);
        assert_eq!(v["registered"], false);
        assert_eq!(v["operating_mode"], serde_json::Value::Null);
        assert_eq!(v["next_expected_checkin"], serde_json::Value::Null);
    }

    #[test]
    fn fabric_data_skips_absent_vendor_name() {
        let f = MatterFabricData { fabric_id: 1, vendor_id: 0xFFF1, fabric_index: 1,
                                   fabric_label: Some("HomeAssistant".into()), vendor_name: None };
        let v = serde_json::to_value(&f).unwrap();
        assert!(v.get("vendor_name").is_none());
        assert_eq!(v["fabric_label"], "HomeAssistant");
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p matter-rs-wire`
Expected: FAIL — module/types missing.

- [ ] **Step 3: Implement `node.rs`**

```rust
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The python-matter-server MatterNodeData shape (schema 13).
/// `attributes` keys are decimal "endpoint/cluster/attribute" paths; values
/// are tag-based JSON. `interview_version` is a compat constant (always 6).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatterNodeData {
    pub node_id: u64,
    pub date_commissioned: String,
    pub last_interview: String,
    pub interview_version: u8,
    pub available: bool,
    pub is_bridge: bool,
    pub attributes: serde_json::Map<String, Value>,
    pub attribute_subscriptions: Vec<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matter_version: Option<String>,
}

/// node_event payload. `data` is name-based (camelCase) or Null.
/// timestamp_type: 1 = epoch, 0 = system, 2 = POSIX fallback (Node behavior).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatterNodeEvent {
    pub node_id: u64,
    pub endpoint_id: u16,
    pub cluster_id: u32,
    pub event_id: u32,
    pub event_number: u64,
    pub priority: u8,
    pub timestamp: i64,
    pub timestamp_type: u8,
    pub data: Value,
}

/// discover / discover_commissionable_nodes entry. Field defaults mirror the
/// Node server (host_name hardcoded, product_id -1 when unknown).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommissionableNodeData {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instance_name: Option<String>,
    pub host_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub long_discriminator: Option<u16>,
    pub vendor_id: i32,
    pub product_id: i32,
    pub commissioning_mode: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device_type: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pairing_instruction: Option<String>,
    pub pairing_hint: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mrp_retry_interval_idle: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mrp_retry_interval_active: Option<u32>,
    pub supports_tcp: bool,
    pub addresses: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rotating_id: Option<String>,
}

/// get_matter_fabrics entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatterFabricData {
    pub fabric_id: u64,
    pub vendor_id: u16,
    pub fabric_index: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fabric_label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vendor_name: Option<String>,
}

/// get_icd_state / register_icd / unregister_icd result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcdState {
    pub supported: bool,
    pub lit_supported: bool,
    pub registered: bool,
    pub operating_mode: Option<String>,
    pub awake: Option<bool>,
    pub available: Option<bool>,
    pub next_expected_checkin: Option<i64>,
}

impl IcdState {
    /// The honest-stub "not registered / not supported" shape.
    pub fn not_registered() -> Self {
        Self { supported: false, lit_supported: false, registered: false,
               operating_mode: None, awake: None, available: None, next_expected_checkin: None }
    }
}
```

Note `IcdState`'s `Option`s serialize as explicit `null` (no skip) — the Node server emits them as null.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p matter-rs-wire`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/wire/src
git commit -m "feat(wire): MatterNodeData/NodeEvent/Commissionable/Fabric/Icd wire models"
```

---

### Task 3: `controller` — storage (atomic JSON, identity/config/nodes, node-id allocation)

**Files:**
- Create: `crates/controller/src/storage.rs`
- Modify: `crates/controller/src/lib.rs` (add `pub mod storage;`), `crates/controller/Cargo.toml`

**Interfaces:**
- Consumes: serde/serde_json, std fs.
- Produces (Tasks 6, 8–11, 13, 17 consume):

```rust
pub struct Storage { /* root: PathBuf */ }
impl Storage {
    pub fn open(root: &Path) -> std::io::Result<Storage>;      // creates root, nodes/, sessions/
    pub fn root(&self) -> &Path;
    pub fn load_identity(&self) -> Option<ServerIdentity>;      // server.json
    pub fn save_identity(&self, id: &ServerIdentity) -> std::io::Result<()>;  // 0600
    pub fn load_config(&self) -> ConfigData;                    // config.json or defaults
    pub fn save_config(&self, cfg: &ConfigData) -> std::io::Result<()>;       // 0600
    pub fn load_nodes(&self) -> Vec<NodeRecord>;                // nodes/*.json (skip+warn on parse errors)
    pub fn save_node(&self, rec: &NodeRecord) -> std::io::Result<()>;
    pub fn delete_node(&self, node_id: u64) -> std::io::Result<()>;
}
pub struct ServerIdentity { pub fabric_id: u64, pub vendor_id: u16, pub controller_node_id: u64,
    pub compressed_fabric_id: u64,
    #[serde(with = "b64")] pub ca_private_key: Vec<u8>,      // RCAC signing key (RCAC-direct mode)
    #[serde(with = "b64")] pub rcac_tlv: Vec<u8>,
    #[serde(with = "b64")] pub controller_private_key: Vec<u8>,
    #[serde(with = "b64")] pub controller_noc_tlv: Vec<u8>,
    #[serde(with = "b64")] pub ipk: Vec<u8> }
pub struct WifiCredential { pub ssid: String, pub password: String }
pub struct ConfigData { pub fabric_label: String /* default "HomeAssistant" */,
    pub next_node_id: u64 /* default 1 */,
    pub wifi_credentials: BTreeMap<String, WifiCredential>,
    pub thread_datasets: BTreeMap<String, String> }
pub struct NodeRecord { pub node_id: u64, pub date_commissioned: String, pub last_interview: String,
    pub device_fabric_index: u8, pub addresses: Vec<String>,
    pub attributes: serde_json::Map<String, serde_json::Value> }
pub fn normalize_fabric_label(label: Option<&str>) -> String;   // trim; empty/None -> "HomeAssistant"; hard-truncate 32
pub fn allocate_node_id(cfg: &mut ConfigData, is_in_use: impl Fn(u64) -> bool) -> u64;
pub fn validate_credential_id(id: &str, existing: impl Iterator<Item = String>) -> Result<(), String>;
pub fn validate_thread_dataset(hex: &str) -> Result<(), String>;
pub fn format_node_date(t: std::time::SystemTime) -> String;    // local "YYYY-MM-DDTHH:mm:ss.SSS000"
```

**Node-compat semantics to implement exactly:**
- `allocate_node_id`: start at `next_node_id`, skip while `is_in_use(candidate)`, set `next_node_id = candidate + 1` (caller persists config BEFORE using the id). Serialization of concurrent calls is the caller's job (MatterController holds a tokio `Mutex` around it).
- `normalize_fabric_label(Some("  x  "))` → `"x"`; `None`/`""`/whitespace → `"HomeAssistant"`; result hard-truncated to 32 chars.
- `validate_credential_id` errors (exact strings): empty → `"invalid-credential-id: id must be non-empty"`; `default`/`delete` (case-insensitive) → `"invalid-credential-id: '<id>' is reserved"` — EXCEPT literal `"default"` itself is allowed (it's the implicit slot); duplicate (case-insensitive) of a *different* existing id → `"invalid-credential-id: '<id>' duplicates existing '<other>'"`.
- `validate_thread_dataset`: non-empty, even length, all hex → else `"Invalid Thread operational dataset: must be a non-empty hex string with even length (each byte is two hex characters)"`.
- Atomic write: write to `<path>.tmp-<pid>` in the same directory, `fs::rename` over the target; set mode 0600 (via `OpenOptions::mode`) before writing content for `server.json`/`config.json`.

- [ ] **Step 1: Add dependencies**

`crates/controller/Cargo.toml` `[dependencies]`: add `serde.workspace = true`, `chrono = "0.4"`, `base64 = "0.22"`, `tracing.workspace = true`. Move `tokio` `macros` feature usage to `[dev-dependencies] tokio = { version = "1", features = ["sync", "rt", "macros"] }` and keep `[dependencies] tokio = { version = "1", features = ["sync", "rt", "time"] }` (carryover item). Remove `thiserror` (unused, carryover). Also remove `thiserror` from `crates/wire/Cargo.toml`.

- [ ] **Step 2: Write the failing tests** (bottom of `storage.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn tmp() -> tempfile::TempDir { tempfile::tempdir().unwrap() }

    #[test]
    fn identity_roundtrip_and_0600() {
        let d = tmp();
        let s = Storage::open(d.path()).unwrap();
        assert!(s.load_identity().is_none());
        let id = ServerIdentity {
            fabric_id: 1, vendor_id: 0xFFF1, controller_node_id: 112233,
            compressed_fabric_id: 0xDEADBEEF,
            ca_private_key: vec![1; 32], rcac_tlv: vec![2; 40],
            controller_private_key: vec![3; 32], controller_noc_tlv: vec![4; 40],
            ipk: vec![5; 16],
        };
        s.save_identity(&id).unwrap();
        let back = s.load_identity().unwrap();
        assert_eq!(back.compressed_fabric_id, 0xDEADBEEF);
        assert_eq!(back.ipk, vec![5; 16]);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(d.path().join("server.json")).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        // key material is base64 in the file, not JSON arrays
        let raw = std::fs::read_to_string(d.path().join("server.json")).unwrap();
        assert!(!raw.contains("[1,1,1"));
    }

    #[test]
    fn node_records_roundtrip_and_delete() {
        let d = tmp();
        let s = Storage::open(d.path()).unwrap();
        let mut attributes = serde_json::Map::new();
        attributes.insert("1/6/0".into(), serde_json::json!(true));
        let rec = NodeRecord { node_id: 5, date_commissioned: "x".into(), last_interview: "y".into(),
                               device_fabric_index: 3, addresses: vec!["fe80::1%2".into()], attributes };
        s.save_node(&rec).unwrap();
        let all = s.load_nodes();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].node_id, 5);
        assert_eq!(all[0].device_fabric_index, 3);
        s.delete_node(5).unwrap();
        assert!(s.load_nodes().is_empty());
    }

    #[test]
    fn allocate_skips_in_use_and_advances() {
        let mut cfg = ConfigData::default();
        assert_eq!(cfg.next_node_id, 1);
        let id = allocate_node_id(&mut cfg, |n| n < 3); // 1,2 in use
        assert_eq!(id, 3);
        assert_eq!(cfg.next_node_id, 4);
        let id2 = allocate_node_id(&mut cfg, |_| false);
        assert_eq!(id2, 4);
    }

    #[test]
    fn fabric_label_normalization() {
        assert_eq!(normalize_fabric_label(None), "HomeAssistant");
        assert_eq!(normalize_fabric_label(Some("")), "HomeAssistant");
        assert_eq!(normalize_fabric_label(Some("   ")), "HomeAssistant");
        assert_eq!(normalize_fabric_label(Some("  Casa  ")), "Casa");
        assert_eq!(normalize_fabric_label(Some(&"x".repeat(40))), "x".repeat(32));
    }

    #[test]
    fn credential_id_validation_strings() {
        let existing = || ["Default".to_string(), "garage".to_string()].into_iter();
        assert!(validate_credential_id("default", existing()).is_ok()); // implicit slot always ok
        assert_eq!(validate_credential_id("", existing()).unwrap_err(),
                   "invalid-credential-id: id must be non-empty");
        assert_eq!(validate_credential_id("delete", existing()).unwrap_err(),
                   "invalid-credential-id: 'delete' is reserved");
        assert_eq!(validate_credential_id("GARAGE", existing()).unwrap_err(),
                   "invalid-credential-id: 'GARAGE' duplicates existing 'garage'");
        assert!(validate_credential_id("shed", existing()).is_ok());
    }

    #[test]
    fn thread_dataset_validation() {
        assert!(validate_thread_dataset("0e080000000000010000").is_ok());
        let err = validate_thread_dataset("0e0").unwrap_err();
        assert!(err.starts_with("Invalid Thread operational dataset"));
        assert!(validate_thread_dataset("").is_err());
        assert!(validate_thread_dataset("zz").is_err());
    }

    #[test]
    fn node_date_format() {
        let s = format_node_date(std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_millis(1_755_081_342_123));
        // local time, so only assert the shape: 19 chars datetime + ".SSS000", no Z/offset
        assert_eq!(s.len(), "2026-08-13T10:15:42.123000".len());
        assert!(s.ends_with("000"));
        assert!(!s.contains('Z') && !s.contains('+'));
        let dot = s.rfind('.').unwrap();
        assert_eq!(s.len() - dot, 7); // ".SSS000"
    }
}
```

Add `[dev-dependencies] tempfile = "3"` to `crates/controller/Cargo.toml`.

- [ ] **Step 3: Run tests to verify they fail**

Run: `cargo test -p matter-rs-controller`
Expected: FAIL — module missing.

- [ ] **Step 4: Implement `storage.rs`**

```rust
//! Plain-JSON storage under --storage-path. All writes atomic (tmp + rename,
//! same directory). server.json / config.json carry key material -> 0600.
//! nodes/<id>.json holds the served attribute cache verbatim.

use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

mod b64 {
    use base64::Engine as _;
    use serde::{Deserialize, Deserializer, Serializer};
    pub fn serialize<S: Serializer>(v: &Vec<u8>, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&base64::engine::general_purpose::STANDARD.encode(v))
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let s = String::deserialize(d)?;
        base64::engine::general_purpose::STANDARD.decode(s).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerIdentity {
    pub fabric_id: u64,
    pub vendor_id: u16,
    pub controller_node_id: u64,
    pub compressed_fabric_id: u64,
    #[serde(with = "b64")] pub ca_private_key: Vec<u8>,
    #[serde(with = "b64")] pub rcac_tlv: Vec<u8>,
    #[serde(with = "b64")] pub controller_private_key: Vec<u8>,
    #[serde(with = "b64")] pub controller_noc_tlv: Vec<u8>,
    #[serde(with = "b64")] pub ipk: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WifiCredential { pub ssid: String, pub password: String }

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ConfigData {
    pub fabric_label: String,
    pub next_node_id: u64,
    pub wifi_credentials: BTreeMap<String, WifiCredential>,
    pub thread_datasets: BTreeMap<String, String>,
}

impl Default for ConfigData {
    fn default() -> Self {
        Self { fabric_label: "HomeAssistant".into(), next_node_id: 1,
               wifi_credentials: BTreeMap::new(), thread_datasets: BTreeMap::new() }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeRecord {
    pub node_id: u64,
    pub date_commissioned: String,
    pub last_interview: String,
    pub device_fabric_index: u8,
    #[serde(default)]
    pub addresses: Vec<String>,
    #[serde(default)]
    pub attributes: serde_json::Map<String, Value>,
}

pub struct Storage { root: PathBuf }

impl Storage {
    pub fn open(root: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(root.join("nodes"))?;
        std::fs::create_dir_all(root.join("sessions"))?;
        Ok(Self { root: root.to_path_buf() })
    }

    pub fn root(&self) -> &Path { &self.root }

    pub fn load_identity(&self) -> Option<ServerIdentity> {
        read_json(&self.root.join("server.json"))
    }
    pub fn save_identity(&self, id: &ServerIdentity) -> io::Result<()> {
        write_json_atomic(&self.root.join("server.json"), id, true)
    }
    pub fn load_config(&self) -> ConfigData {
        read_json(&self.root.join("config.json")).unwrap_or_default()
    }
    pub fn save_config(&self, cfg: &ConfigData) -> io::Result<()> {
        write_json_atomic(&self.root.join("config.json"), cfg, true)
    }

    pub fn load_nodes(&self) -> Vec<NodeRecord> {
        let mut out = Vec::new();
        let Ok(entries) = std::fs::read_dir(self.root.join("nodes")) else { return out };
        for e in entries.flatten() {
            if e.path().extension().is_some_and(|x| x == "json") {
                match read_json::<NodeRecord>(&e.path()) {
                    Some(rec) => out.push(rec),
                    None => tracing::warn!("skipping unparseable node file {:?}", e.path()),
                }
            }
        }
        out.sort_by_key(|r| r.node_id);
        out
    }
    pub fn save_node(&self, rec: &NodeRecord) -> io::Result<()> {
        write_json_atomic(&self.root.join("nodes").join(format!("{}.json", rec.node_id)), rec, false)
    }
    pub fn delete_node(&self, node_id: u64) -> io::Result<()> {
        let p = self.root.join("nodes").join(format!("{node_id}.json"));
        if p.exists() { std::fs::remove_file(p) } else { Ok(()) }
    }
}

fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Option<T> {
    let data = std::fs::read_to_string(path).ok()?;
    match serde_json::from_str(&data) {
        Ok(v) => Some(v),
        Err(e) => { tracing::warn!("failed to parse {path:?}: {e}"); None }
    }
}

fn write_json_atomic<T: Serialize>(path: &Path, value: &T, secret: bool) -> io::Result<()> {
    let dir = path.parent().unwrap();
    let tmp = dir.join(format!(
        ".{}.tmp-{}", path.file_name().unwrap().to_string_lossy(), std::process::id()));
    {
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create(true).truncate(true);
        #[cfg(unix)]
        if secret { use std::os::unix::fs::OpenOptionsExt; opts.mode(0o600); }
        let file = opts.open(&tmp)?;
        serde_json::to_writer_pretty(&file, value)?;
        file.sync_all()?;
    }
    #[cfg(unix)]
    if secret {
        use std::os::unix::fs::PermissionsExt;
        // OpenOptions mode only applies on create; enforce on every write.
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))?;
    }
    std::fs::rename(&tmp, path)
}

/// Node-server compatible: trim; None/empty -> "HomeAssistant"; hard substring(0,32).
pub fn normalize_fabric_label(label: Option<&str>) -> String {
    let trimmed = label.map(str::trim).unwrap_or("");
    let effective = if trimmed.is_empty() { "HomeAssistant" } else { trimmed };
    effective.chars().take(32).collect()
}

/// ConfigStorage.allocateNodeId: start at next_node_id, skip in-use ids,
/// persist next = candidate + 1 (caller saves config before using the id).
pub fn allocate_node_id(cfg: &mut ConfigData, is_in_use: impl Fn(u64) -> bool) -> u64 {
    let mut candidate = cfg.next_node_id.max(1);
    let start = candidate;
    while is_in_use(candidate) { candidate += 1; }
    if candidate != start {
        tracing::info!("Skipped {} node id(s) from {start} already in use on the fabric, allocated {candidate} instead",
                       candidate - start);
    }
    cfg.next_node_id = candidate + 1;
    candidate
}

/// ConfigStorage credential-id rules ("default" itself is the implicit slot and allowed).
pub fn validate_credential_id(id: &str, existing: impl Iterator<Item = String>) -> Result<(), String> {
    if id.is_empty() {
        return Err("invalid-credential-id: id must be non-empty".into());
    }
    let lower = id.to_ascii_lowercase();
    if lower == "delete" || (lower == "default" && id != "default") {
        return Err(format!("invalid-credential-id: '{id}' is reserved"));
    }
    if lower == "default" { return Ok(()); }
    for other in existing {
        if other.to_ascii_lowercase() == lower && other != id {
            return Err(format!("invalid-credential-id: '{id}' duplicates existing '{other}'"));
        }
    }
    Ok(())
}

pub fn validate_thread_dataset(hex: &str) -> Result<(), String> {
    if hex.is_empty() || hex.len() % 2 != 0 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err("Invalid Thread operational dataset: must be a non-empty hex string with even length (each byte is two hex characters)".into());
    }
    Ok(())
}

/// Node getDateAsString(): LOCAL time, millis + literal "000", no timezone.
pub fn format_node_date(t: std::time::SystemTime) -> String {
    let dt: chrono::DateTime<chrono::Local> = t.into();
    dt.format("%Y-%m-%dT%H:%M:%S%.3f000").to_string()
}
```

- [ ] **Step 5: Run tests until they pass**

Run: `cargo test -p matter-rs-controller`
Expected: PASS. (If the "reserved" test disagrees with the implementation on `default`-vs-`Default`, the tests are the contract: literal lowercase `"default"` is allowed, any other casing of it is reserved.)

- [ ] **Step 6: Commit**

```bash
git add crates/controller crates/wire/Cargo.toml Cargo.lock
git commit -m "feat(controller): atomic JSON storage, node-id allocation, credential/label rules"
```

---

### Task 4: `controller` — `stack_api` (the Stack trait boundary)

**Files:**
- Create: `crates/controller/src/stack_api.rs`
- Modify: `crates/controller/src/lib.rs` (add `pub mod stack_api;`)

**Interfaces:**
- Consumes: serde_json, async-trait, tokio sync.
- Produces — the ONLY surface the controller uses to reach rs-matter; `stack::StackHandle` implements it in Task 16; controller tests use `FakeStack`:

```rust
// crates/controller/src/stack_api.rs — write exactly this file (plus tests below):

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
```

- [ ] **Step 1: Write the file above**, plus a `FakeStack` test helper at the bottom (compiled for tests AND exported so command tests in Tasks 8–11 can use it):

```rust
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
```

- [ ] **Step 2: Write a smoke test** (bottom of `stack_api.rs`):

```rust
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
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p matter-rs-controller`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/controller/src
git commit -m "feat(controller): Stack trait boundary with scriptable FakeStack"
```

---

### Task 5: `controller` — registry + MatterNodeData building

**Files:**
- Create: `crates/controller/src/registry.rs`
- Modify: `crates/controller/src/lib.rs` (add `pub mod registry;`)

**Interfaces:**
- Consumes: `storage::NodeRecord`, `matter_rs_wire::node::MatterNodeData`.
- Produces (NodeManager and command handlers consume):

```rust
pub struct Registry { /* Mutex<BTreeMap<u64, NodeEntry>> */ }
pub struct NodeEntry { pub record: NodeRecord, pub available: bool }
impl Registry {
    pub fn new(records: Vec<NodeRecord>) -> Self;           // all start available=false
    pub fn contains(&self, node_id: u64) -> bool;
    pub fn node_ids(&self) -> Vec<u64>;
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;
    pub fn insert(&self, record: NodeRecord);
    pub fn remove(&self, node_id: u64) -> bool;
    pub fn with_entry<R>(&self, node_id: u64, f: impl FnOnce(&mut NodeEntry) -> R) -> Option<R>;
    pub fn set_available(&self, node_id: u64, available: bool) -> Option<bool>; // Some(changed)
    pub fn node_data(&self, node_id: u64) -> Option<MatterNodeData>;
    pub fn all_node_data(&self, only_available: bool) -> Vec<MatterNodeData>;
    pub fn snapshot_record(&self, node_id: u64) -> Option<NodeRecord>;  // for persistence
}
pub fn build_node_data(record: &NodeRecord, available: bool) -> MatterNodeData;
```

**Building rules (Node-server exact):**
- `interview_version: 6`, `attribute_subscriptions: []` always.
- `is_bridge`: `attributes["1/29/0"]` (endpoint **1**, Node quirk) is an array containing an object whose key `"0"` equals `14`.
- `matter_version`: from `attributes["0/40/21"]` (SpecificationVersion u32) as `format!("{}.{}.{}", (v>>24)&0xFF, (v>>16)&0xFF, (v>>8)&0xFF)`; else from `attributes["0/40/0"]` (DataModelRevision): `<=16` → `"<1.2.0"`, `==17` → `"1.2.0"`, else `None`.

- [ ] **Step 1: Write the failing tests** (bottom of `registry.rs`)

```rust
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p matter-rs-controller registry`
Expected: FAIL.

- [ ] **Step 3: Implement `registry.rs`**

```rust
//! In-memory node registry: the attribute cache + availability, mirroring
//! nodes/<id>.json. Availability is CACHED here (never recomputed) so the
//! serialized `available` and the event stream can never disagree.

use std::collections::BTreeMap;
use std::sync::Mutex;

use matter_rs_wire::node::MatterNodeData;
use serde_json::Value;

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

    pub fn contains(&self, node_id: u64) -> bool { self.inner.lock().unwrap().contains_key(&node_id) }
    pub fn node_ids(&self) -> Vec<u64> { self.inner.lock().unwrap().keys().copied().collect() }
    pub fn len(&self) -> usize { self.inner.lock().unwrap().len() }
    pub fn is_empty(&self) -> bool { self.len() == 0 }

    pub fn insert(&self, record: NodeRecord) {
        self.inner.lock().unwrap().insert(record.node_id, NodeEntry { record, available: false });
    }
    pub fn remove(&self, node_id: u64) -> bool {
        self.inner.lock().unwrap().remove(&node_id).is_some()
    }

    pub fn with_entry<R>(&self, node_id: u64, f: impl FnOnce(&mut NodeEntry) -> R) -> Option<R> {
        self.inner.lock().unwrap().get_mut(&node_id).map(f)
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
        self.inner.lock().unwrap().get(&node_id).map(|e| build_node_data(&e.record, e.available))
    }

    pub fn all_node_data(&self, only_available: bool) -> Vec<MatterNodeData> {
        self.inner.lock().unwrap().values()
            .filter(|e| !only_available || e.available)
            .map(|e| build_node_data(&e.record, e.available))
            .collect()
    }

    pub fn snapshot_record(&self, node_id: u64) -> Option<NodeRecord> {
        self.inner.lock().unwrap().get(&node_id).map(|e| e.record.clone())
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p matter-rs-controller registry`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/controller/src
git commit -m "feat(controller): node registry with Node-compatible MatterNodeData building"
```

---

### Task 6: `controller` — NodeManager (StackEvent consumer, availability grace, event fan-out)

**Files:**
- Create: `crates/controller/src/node_manager.rs`
- Modify: `crates/controller/src/lib.rs` (add `pub mod node_manager;`)

**Interfaces:**
- Consumes: `Registry`, `Storage`, `stack_api::{StackEvent, NodeConnState, NodeEventData}`, `matter_rs_wire::envelope::EventMessage`, `matter_rs_wire::node::MatterNodeEvent`.
- Produces:

```rust
pub const RECONNECT_GRACE: std::time::Duration = std::time::Duration::from_secs(180);
pub const EVENT_HISTORY_SIZE: usize = 25;
pub struct NodeManager;
impl NodeManager {
    /// Spawns the consumer task. `events` is the broadcast sender OWNED BY
    /// MatterController for its whole life (carryover: never rotate it).
    pub fn spawn(
        registry: Arc<Registry>,
        storage: Arc<Storage>,
        events: tokio::sync::broadcast::Sender<EventMessage>,
        history: Arc<std::sync::Mutex<VecDeque<serde_json::Value>>>,
        rx: tokio::sync::mpsc::UnboundedReceiver<StackEvent>,
    ) -> tokio::task::JoinHandle<()>;
}
```

**Semantics (Node-server availability state machine, Nodes.ts):**
- `NodeState { Connected }`: cancel any grace timer; `available = true`; if that CHANGED → emit `node_updated` (full `MatterNodeData`) and log at info `Node <id> availability changed to true`.
- `NodeState { Reconnecting }`: if currently available and no grace timer → arm a `RECONNECT_GRACE` timer. On expiry: `available = false`, emit `node_updated`, log warn `Node <id> offline grace period expired, marking unavailable`. While the timer runs the node stays `available: true` (grace semantics).
- `PrimingSnapshot`: replace the whole attribute map, set `last_interview = format_node_date(now)`, persist the node file, emit `node_updated`.
- `AttributesChanged`: apply each `(path, value)` to the cache; emit one `attribute_updated` event per change with data `[node_id, path, value]` (3-element array); persist the node file (write-through — the atomic write is cheap at homelab scale; note: Node coalesces per-connection, we defer that to plan 3).
- `NodeEvent`: build a `MatterNodeEvent` (adding `node_id`), emit `node_event`, push its JSON into `history` (drop oldest past `EVENT_HISTORY_SIZE`). Single shared ring — deliberately NOT Node's per-connection-duplicated buffer quirk.
- Events for unknown node ids are dropped with a debug log (post-removal stragglers).
- Broadcast `send` errors (no receivers) are ignored.

- [ ] **Step 1: Write the failing tests** (bottom of `node_manager.rs`; note `#[tokio::test(start_paused = true)]` for the grace tests)

```rust
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p matter-rs-controller node_manager`
Expected: FAIL.

- [ ] **Step 3: Implement `node_manager.rs`**

```rust
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

struct Inner {
    registry: Arc<Registry>,
    storage: Arc<Storage>,
    events: broadcast::Sender<EventMessage>,
    history: Arc<Mutex<VecDeque<Value>>>,
    grace_timers: HashMap<u64, tokio::task::JoinHandle<()>>,
    self_tx: mpsc::UnboundedSender<StackEvent>,
}

/// Internal marker delivered when a grace timer fires. Uses Reconnecting's
/// slot: timers send a synthetic event back into the queue so all state
/// changes are serialized through one consumer.
fn grace_expired(node_id: u64) -> StackEvent {
    StackEvent::NodeState { node_id, state: NodeConnState::Reconnecting } // repurposed via timer map check
}

impl NodeManager {
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
    // (node_id -> deadline task). Timer tasks send GraceExpired over this channel.
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
            let updated = registry.with_entry(node_id, |e| {
                e.record.attributes = attributes.into_iter().collect();
                e.record.last_interview = format_node_date(std::time::SystemTime::now());
            }).is_some();
            if updated {
                persist(registry, storage, node_id);
                emit_node_updated(registry, events, node_id);
            }
        }
        StackEvent::AttributesChanged { node_id, changes } => {
            if !registry.contains(node_id) { return; }
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
            if !registry.contains(node_id) { return; }
            let payload = MatterNodeEvent {
                node_id,
                endpoint_id: event.endpoint_id, cluster_id: event.cluster_id,
                event_id: event.event_id, event_number: event.event_number,
                priority: event.priority, timestamp: event.timestamp,
                timestamp_type: event.timestamp_type, data: event.data,
            };
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
```

Delete the unused `grace_expired` helper if the select-loop version above is used as-is (it is — timers signal over `grace_rx`).

- [ ] **Step 4: Run tests until they pass**

Run: `cargo test -p matter-rs-controller node_manager`
Expected: PASS (5 tests). The paused-time tests must not flake: `advance` + `yield_now` drive the timers deterministically.

- [ ] **Step 5: Commit**

```bash
git add crates/controller/src
git commit -m "feat(controller): NodeManager with 3-minute availability grace and event fan-out"
```

---

### Task 7: `Controller` trait gains connection identity (fabric-label ownership needs it)

**Files:**
- Modify: `crates/controller/src/api.rs`, `crates/controller/src/stub.rs`, `crates/server/src/ws.rs`, `crates/server/tests/ws_protocol.rs` (only if compilation requires)

**Interfaces:**
- Produces (breaking change to the plan-1 trait — every implementor updates):

```rust
/// Identifies one WS connection for the lifetime of the process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnId(pub u64);

#[async_trait::async_trait]
pub trait Controller: Send + Sync + 'static {
    fn server_info(&self) -> ServerInfoMessage;
    fn node_count(&self) -> usize;
    async fn handle_command(&self, conn: ConnId, cmd: &CommandMessage) -> Result<serde_json::Value, CommandError>;
    /// Called exactly once when a connection closes (any reason). Default no-op.
    fn connection_closed(&self, _conn: ConnId) {}
    fn subscribe_events(&self) -> tokio::sync::broadcast::Receiver<EventMessage>;
}
```

**Why:** the Node server's `set_default_fabric_label` is owned by the first connection that calls it, released when that connection closes. Task 10 implements that; this task adds the plumbing.

- [ ] **Step 1: Update the trait** in `api.rs` as above (keep the existing `subscribe_events` doc comment).

- [ ] **Step 2: Update `stub.rs`**: signature `async fn handle_command(&self, _conn: ConnId, cmd: &CommandMessage)`; import `ConnId`; update its tests to pass `ConnId(1)`.

- [ ] **Step 3: Update `ws.rs`**: allocate ids and guarantee `connection_closed`:

```rust
use std::sync::atomic::{AtomicU64, Ordering};
static NEXT_CONN_ID: AtomicU64 = AtomicU64::new(1);
```

In `handle_connection`, first lines:
```rust
let conn = matter_rs_controller::api::ConnId(NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed));
// Guard: connection_closed fires on every exit path, including panics.
struct CloseGuard { controller: std::sync::Arc<dyn matter_rs_controller::api::Controller>, conn: matter_rs_controller::api::ConnId }
impl Drop for CloseGuard {
    fn drop(&mut self) { self.controller.connection_closed(self.conn); }
}
let _close_guard = CloseGuard { controller: state.controller.clone(), conn };
```
and thread `conn` through `handle_text_frame(state, conn, &text, &mut listening)` into `state.controller.handle_command(conn, &cmd)`.

- [ ] **Step 4: Run the whole suite**

Run: `cargo test --workspace`
Expected: PASS (existing ws_protocol/health/smoke tests unchanged in behavior).

- [ ] **Step 5: Commit**

```bash
git add crates/controller/src crates/server/src
git commit -m "feat(controller): ConnId + connection_closed on the Controller trait"
```

---

### Task 8: `MatterController` — core struct, dispatch, session & node commands

**Files:**
- Create: `crates/controller/src/real.rs`, `crates/controller/src/commands/mod.rs`, `crates/controller/src/commands/nodes.rs`
- Modify: `crates/controller/src/lib.rs` (add `pub mod real; pub mod commands;`)

**Interfaces:**
- Consumes: everything from Tasks 3–7.
- Produces:

```rust
/// Log-level access for get/set_loglevel; server::logging implements it (Task 17).
pub trait LogLevels: Send + Sync + 'static {
    fn get(&self) -> (String, Option<String>);              // (console, file-or-None)
    fn set(&self, console: Option<&str>, file: Option<&str>);
}

pub struct MatterController { /* fields below */ }
impl MatterController {
    /// Loads identity/config/nodes from storage, spawns NodeManager,
    /// starts supervisors for every known node.
    pub fn new(
        stack: Arc<dyn Stack>,
        storage: Arc<Storage>,
        identity: ServerIdentity,
        fabric_index: u8,                 // our controller's fabric index on its own Matter instance
        sdk_version: String,
        label_locked: bool,               // --default-fabric-label given
        log: Arc<dyn LogLevels>,
        stack_events: mpsc::UnboundedReceiver<StackEvent>,
    ) -> Arc<MatterController>;
}
impl Controller for MatterController { ... }
```

Internal fields (later command tasks use them — keep these names):

```rust
pub(crate) stack: Arc<dyn Stack>,
pub(crate) storage: Arc<Storage>,
pub(crate) registry: Arc<Registry>,
pub(crate) identity: ServerIdentity,
pub(crate) fabric_index: u8,
pub(crate) sdk_version: String,
pub(crate) config: std::sync::Mutex<ConfigData>,
pub(crate) alloc_lock: tokio::sync::Mutex<()>,        // serializes node-id allocation + commissioning
pub(crate) events: broadcast::Sender<EventMessage>,   // OWNED here for the controller's lifetime
pub(crate) history: Arc<std::sync::Mutex<VecDeque<Value>>>,
pub(crate) label_locked: bool,
pub(crate) label_owner: std::sync::Mutex<Option<ConnId>>,
pub(crate) log: Arc<dyn LogLevels>,
```

**Command semantics implemented in this task** (Node-exact result shapes; `err(code, msg)` = `CommandError::new`):

| command | behavior |
|---|---|
| `server_info` | build `ServerInfoMessage`: `fabric_id`/`compressed_fabric_id`/`controller_node_id` from identity, `fabric_index: Some(self.fabric_index)`, schema consts, `sdk_version`, `wifi_credentials_set` = default wifi credential exists with non-empty password, `wifi_ssid` = that credential's ssid (only when set), `thread_credentials_set` = default thread dataset exists, `bluetooth_enabled: false`, `ble_proxy_enabled: Some(false)` |
| `start_listening` | `Vec<MatterNodeData>` — `registry.all_node_data(false)` (ws.rs flips listening) |
| `get_nodes` | optional `only_available: bool` (default false) → filtered array |
| `get_node` | required `node_id`; unknown → code 5, details `Node <id> does not exist` |
| `diagnostics` | `{ "info": <server_info>, "nodes": <all>, "events": <history newest-last> }` |
| `interview_node` | `stack.interview(node_id)` → replace attributes + `last_interview` = now → persist → broadcast `node_updated` → result `null`. Stack error → code 2 (`NodeInterviewFailed`) with the stack message |
| `remove_node` | `stack.stop_supervisor`; best-effort `stack.remove_device_fabric(node_id, record.device_fabric_index)` (failure → warn log, continue); `registry.remove`; `storage.delete_node`; emit `node_removed` with data = bare node id; result `null` |
| `ping_node` | optional `attempts` (default 1); addresses = `stack.node_addresses` ∪ record.addresses (dedup, keep order); no addresses → `{}`; else system ping each concurrently → `{addr: bool}` keyed by UNMODIFIED address |
| `get_node_ip_addresses` | optional `prefer_cache` (unused v1 — we always merge live+cached), `scoped` (default false → strip `%iface`); same address sources; result `Vec<String>` |

**Arg-parsing helpers** (`commands/mod.rs`) — used by ALL later command tasks:

```rust
use serde_json::{Map, Value};
use matter_rs_wire::error::ServerErrorCode;
use crate::api::CommandError;

pub fn err(code: ServerErrorCode, msg: impl Into<String>) -> CommandError { CommandError::new(code, msg) }
pub fn invalid(msg: impl Into<String>) -> CommandError { err(ServerErrorCode::InvalidArguments, msg) }

pub fn require_u64(args: &Map<String, Value>, key: &str) -> Result<u64, CommandError> {
    args.get(key).and_then(Value::as_u64)
        .ok_or_else(|| invalid(format!("missing or invalid required argument: {key}")))
}
pub fn opt_u64(args: &Map<String, Value>, key: &str) -> Option<u64> { args.get(key).and_then(Value::as_u64) }
pub fn opt_bool(args: &Map<String, Value>, key: &str) -> Option<bool> { args.get(key).and_then(Value::as_bool) }
pub fn opt_str<'a>(args: &'a Map<String, Value>, key: &str) -> Option<&'a str> { args.get(key).and_then(Value::as_str) }
pub fn require_str<'a>(args: &'a Map<String, Value>, key: &str) -> Result<&'a str, CommandError> {
    opt_str(args, key).ok_or_else(|| invalid(format!("missing or invalid required argument: {key}")))
}

/// StackError -> wire error. `default_code` lets commissioning map to 1, interview to 2, etc.
pub fn stack_err(default_code: ServerErrorCode, e: crate::stack_api::StackError) -> CommandError {
    use crate::stack_api::StackErrorKind::*;
    let code = match e.kind {
        InvalidArguments => ServerErrorCode::InvalidArguments,
        NodeUnreachable => ServerErrorCode::NodeNotResolving,
        Busy | Timeout | Sdk => default_code,
    };
    err(code, e.message)
}
```

(Node reports missing args as a raw TypeError with code 0; we use code 8 with a clear message — accepted deviation #8, add it to the header list.)

- [ ] **Step 1: Write the failing tests** (`crates/controller/src/real.rs` bottom — the test rig here is reused by Tasks 9–11, keep it `pub(crate)` in a `#[cfg(test)] pub(crate) mod test_rig`)

```rust
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
        pub dir: tempfile::TempDir,
        pub stack_tx: tokio::sync::mpsc::UnboundedSender<crate::stack_api::StackEvent>,
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
        Rig { ctrl, stack, dir, stack_tx }
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

    #[tokio::test]
    async fn supervisors_started_for_known_nodes() {
        let r = rig_with_nodes(vec![node_record(5), node_record(6)]);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let calls = r.stack.calls();
        assert!(calls.contains(&"start_supervisor 5".to_string()));
        assert!(calls.contains(&"start_supervisor 6".to_string()));
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p matter-rs-controller real`
Expected: FAIL.

- [ ] **Step 3: Implement** `commands/mod.rs` (helpers above verbatim, plus `pub mod nodes;`), `real.rs`:

```rust
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
            config: Mutex::new(config), alloc_lock: tokio::sync::Mutex::new(()),
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

    pub(crate) fn config_snapshot(&self) -> ConfigData { self.config.lock().unwrap().clone() }

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
            // Tasks 9-11 extend this match. The catch-all stays last.
            other => Err(CommandError::new(
                ServerErrorCode::InvalidCommand, format!("Unknown command: {other}"))),
        }
        // `conn` is used by set_default_fabric_label (Task 10).
        .map_err(|e| { let _ = conn; e })
    }

    fn connection_closed(&self, conn: ConnId) {
        let mut owner = self.label_owner.lock().unwrap();
        if *owner == Some(conn) { *owner = None; }
    }

    fn subscribe_events(&self) -> broadcast::Receiver<EventMessage> { self.events.subscribe() }
}
```

`commands/nodes.rs`:

```rust
use serde_json::{json, Map, Value};

use matter_rs_wire::error::ServerErrorCode;

use crate::api::CommandError;
use crate::commands::{opt_bool, opt_u64, require_u64, stack_err};
use crate::real::MatterController;
use crate::storage::format_node_date;

pub async fn get_nodes(c: &MatterController, args: &Map<String, Value>) -> Result<Value, CommandError> {
    let only_available = opt_bool(args, "only_available").unwrap_or(false);
    Ok(serde_json::to_value(c.registry.all_node_data(only_available)).unwrap())
}

pub async fn get_node(c: &MatterController, args: &Map<String, Value>) -> Result<Value, CommandError> {
    let node_id = require_u64(args, "node_id")?;
    c.registry.node_data(node_id)
        .map(|n| serde_json::to_value(n).unwrap())
        .ok_or_else(|| CommandError::new(ServerErrorCode::NodeNotExists, format!("Node {node_id} does not exist")))
}

pub async fn diagnostics(c: &MatterController, args: &Map<String, Value>) -> Result<Value, CommandError> {
    let nodes = get_nodes(c, args).await?;
    let events: Vec<Value> = c.history.lock().unwrap().iter().cloned().collect();
    Ok(json!({ "info": c.build_server_info(), "nodes": nodes, "events": events }))
}

pub async fn interview_node(c: &MatterController, args: &Map<String, Value>) -> Result<Value, CommandError> {
    let node_id = require_u64(args, "node_id")?;
    c.ensure_node(node_id)?;
    let attributes = c.stack.interview(node_id).await
        .map_err(|e| stack_err(ServerErrorCode::NodeInterviewFailed, e))?;
    c.registry.with_entry(node_id, |e| {
        e.record.attributes = attributes.into_iter().collect();
        e.record.last_interview = format_node_date(std::time::SystemTime::now());
    });
    if let Some(rec) = c.registry.snapshot_record(node_id) {
        if let Err(e) = c.storage.save_node(&rec) { tracing::error!("persist node {node_id}: {e}"); }
    }
    if let Some(nd) = c.registry.node_data(node_id) {
        let _ = c.events.send(matter_rs_wire::envelope::EventMessage {
            event: "node_updated".into(), data: serde_json::to_value(nd).unwrap() });
    }
    Ok(Value::Null)
}

pub async fn remove_node(c: &MatterController, args: &Map<String, Value>) -> Result<Value, CommandError> {
    let node_id = require_u64(args, "node_id")?;
    c.ensure_node(node_id)?;
    let device_fab_idx = c.registry.snapshot_record(node_id).map(|r| r.device_fabric_index).unwrap_or(0);
    c.stack.stop_supervisor(node_id).await;
    if let Err(e) = c.stack.remove_device_fabric(node_id, device_fab_idx).await {
        tracing::warn!("RemoveFabric on node {node_id} failed ({}); removing locally anyway", e.message);
    }
    c.registry.remove(node_id);
    if let Err(e) = c.storage.delete_node(node_id) { tracing::error!("delete node file {node_id}: {e}"); }
    let _ = c.events.send(matter_rs_wire::envelope::EventMessage {
        event: "node_removed".into(), data: json!(node_id) });
    Ok(Value::Null)
}

/// Live (stack) addresses first, then cached record addresses; dedup preserving order.
async fn merged_addresses(c: &MatterController, node_id: u64) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    if let Ok(live) = c.stack.node_addresses(node_id).await {
        out.extend(live);
    }
    if let Some(rec) = c.registry.snapshot_record(node_id) {
        out.extend(rec.addresses);
    }
    let mut seen = std::collections::HashSet::new();
    out.retain(|a| seen.insert(a.clone()));
    out
}

pub async fn ping_node(c: &MatterController, args: &Map<String, Value>) -> Result<Value, CommandError> {
    let node_id = require_u64(args, "node_id")?;
    c.ensure_node(node_id)?;
    let attempts = opt_u64(args, "attempts").unwrap_or(1).max(1);
    let addrs = merged_addresses(c, node_id).await;
    let mut results = Map::new();
    let futures: Vec<_> = addrs.iter().map(|a| ping_one(a.clone(), attempts)).collect();
    for (addr, ok) in futures_join_all(futures).await {
        results.insert(addr, Value::Bool(ok));
    }
    Ok(Value::Object(results))
}

// Small local join_all to avoid a futures dependency.
async fn futures_join_all<T>(futs: Vec<impl std::future::Future<Output = T>>) -> Vec<T> {
    let mut out = Vec::with_capacity(futs.len());
    for f in futs { out.push(f.await); } // sequential is fine at homelab scale
    out
}

/// System ping (iputils on the Debian target; ping6 fallback for macOS dev).
async fn ping_one(addr: String, attempts: u64) -> (String, bool) {
    let bare = addr.split('%').next().unwrap_or(&addr).to_string();
    let is_v6 = bare.contains(':');
    let (bin, timeout_flag) = if cfg!(target_os = "macos") {
        (if is_v6 { "ping6" } else { "ping" }, "-t")
    } else {
        ("ping", "-W")
    };
    let mut cmd = tokio::process::Command::new(bin);
    if !cfg!(target_os = "macos") && is_v6 { cmd.arg("-6"); }
    cmd.arg("-c").arg(attempts.to_string()).arg(timeout_flag).arg("10").arg(&bare);
    cmd.stdout(std::process::Stdio::null()).stderr(std::process::Stdio::null());
    let ok = matches!(cmd.status().await.map(|s| s.success()), Ok(true));
    (addr, ok)
}

pub async fn get_node_ip_addresses(c: &MatterController, args: &Map<String, Value>) -> Result<Value, CommandError> {
    let node_id = require_u64(args, "node_id")?;
    c.ensure_node(node_id)?;
    let scoped = opt_bool(args, "scoped").unwrap_or(false);
    let addrs: Vec<String> = merged_addresses(c, node_id).await.into_iter()
        .map(|a| if scoped { a } else { a.split('%').next().unwrap_or(&a).to_string() })
        .collect();
    let mut seen = std::collections::HashSet::new();
    let addrs: Vec<String> = addrs.into_iter().filter(|a| seen.insert(a.clone())).collect();
    Ok(serde_json::to_value(addrs).unwrap())
}
```

(`tokio` needs the `process` feature in `[dependencies]` of the controller crate: `tokio = { version = "1", features = ["sync", "rt", "time", "process"] }`.)

- [ ] **Step 4: Run tests until they pass**

Run: `cargo test -p matter-rs-controller`
Expected: PASS (all module tests incl. earlier tasks').

- [ ] **Step 5: Commit**

```bash
git add crates/controller
git commit -m "feat(controller): MatterController with session and node commands"
```

---

### Task 9: `MatterController` — interaction commands (read/write/device_command)

**Files:**
- Create: `crates/controller/src/commands/interaction.rs`
- Modify: `crates/controller/src/commands/mod.rs` (add `pub mod interaction;`), `crates/controller/src/real.rs` (extend dispatch)

**Interfaces:**
- Consumes: `Stack::{read_attributes, write_attribute, invoke}`, `AttributePathSpec`, Task 8 helpers/test rig.
- Produces: dispatch arms `read_attribute`, `write_attribute`, `device_command`; helper `parse_attribute_path(&str) -> Result<AttributePathSpec, CommandError>`.

**Node-exact semantics:**
- `read_attribute`: `node_id`, `attribute_path` as string OR array of strings, `fabric_filtered` default false. Path segments: decimal numbers; anything non-numeric (`*`) is a wildcard; sentinels `0xffff` (endpoint) / `0xffffffff` (cluster, attribute) are also wildcards. Malformed path (not 3 segments) → code 8 `Invalid attribute path: <path>`. Empty read result → code 7 `Failed to read attribute: no values returned`. Result: `{ "<e>/<c>/<a>": <tag-based value>, ... }`.
- `write_attribute`: `node_id`, `attribute_path` (single, concrete), `value`. Any wildcard → code 8 `write_attribute does not support wildcards in attribute path`. Result (PascalCase, 1-element array): `[{ "Path": { "EndpointId": e, "ClusterId": c, "AttributeId": a }, "Status": <im status> }]`.
- `device_command`: `node_id`, `endpoint_id`, `cluster_id`, `command_name`, `payload` (default `{}`), `timed_request_timeout_ms?`. Empty payload object stays `{}` (stack encodes an empty struct). Result: the stack's name-based response JSON (Null for DefaultSuccess). Stack `InvalidArguments` (unknown cluster/command/field) passes through as code 8 with the stack's message.

- [ ] **Step 1: Write the failing tests** (bottom of `interaction.rs`)

```rust
#[cfg(test)]
mod tests {
    use crate::real::test_rig::*;
    use serde_json::json;

    #[tokio::test]
    async fn read_attribute_single_and_wildcard_paths() {
        let r = rig_with_nodes(vec![node_record(5)]);
        *r.stack.read_response.lock().unwrap() =
            Some(Ok(vec![("1/6/0".into(), json!(true)), ("2/6/0".into(), json!(false))]));
        let v = call(&r, "read_attribute", json!({"node_id": 5, "attribute_path": "*/6/0"})).await.unwrap();
        assert_eq!(v, json!({"1/6/0": true, "2/6/0": false}));
        assert!(r.stack.calls().iter().any(|c| c == "read node=5 paths=1 ff=false"));
    }

    #[tokio::test]
    async fn read_attribute_accepts_path_list_and_sentinels() {
        let r = rig_with_nodes(vec![node_record(5)]);
        *r.stack.read_response.lock().unwrap() = Some(Ok(vec![("1/6/0".into(), json!(true))]));
        let v = call(&r, "read_attribute",
            json!({"node_id": 5, "attribute_path": ["1/6/0", "65535/4294967295/4294967295"]})).await.unwrap();
        assert_eq!(v["1/6/0"], true);
        assert!(r.stack.calls().iter().any(|c| c == "read node=5 paths=2 ff=false"));
    }

    #[tokio::test]
    async fn read_attribute_empty_result_is_sdk_error() {
        let r = rig_with_nodes(vec![node_record(5)]);
        *r.stack.read_response.lock().unwrap() = Some(Ok(vec![]));
        let e = call(&r, "read_attribute", json!({"node_id": 5, "attribute_path": "1/6/0"})).await.unwrap_err();
        assert_eq!(e.code.code(), 7);
        assert_eq!(e.details, "Failed to read attribute: no values returned");
    }

    #[tokio::test]
    async fn write_attribute_rejects_wildcards_and_returns_pascal_case() {
        let r = rig_with_nodes(vec![node_record(5)]);
        let e = call(&r, "write_attribute",
            json!({"node_id": 5, "attribute_path": "*/6/0", "value": true})).await.unwrap_err();
        assert_eq!(e.code.code(), 8);
        assert_eq!(e.details, "write_attribute does not support wildcards in attribute path");

        let v = call(&r, "write_attribute",
            json!({"node_id": 5, "attribute_path": "1/8/16385", "value": 100})).await.unwrap();
        assert_eq!(v, json!([{"Path": {"EndpointId": 1, "ClusterId": 8, "AttributeId": 16385}, "Status": 0}]));
    }

    #[tokio::test]
    async fn device_command_passes_through() {
        let r = rig_with_nodes(vec![node_record(5)]);
        *r.stack.invoke_response.lock().unwrap() = Some(Ok(serde_json::Value::Null));
        let v = call(&r, "device_command", json!({
            "node_id": 5, "endpoint_id": 1, "cluster_id": 6,
            "command_name": "toggle", "payload": {}})).await.unwrap();
        assert_eq!(v, serde_json::Value::Null);
        assert!(r.stack.calls().iter().any(|c| c == "invoke node=5 1/6 toggle timed=None"));
    }

    #[tokio::test]
    async fn device_command_unknown_node() {
        let r = rig();
        let e = call(&r, "device_command", json!({
            "node_id": 9, "endpoint_id": 1, "cluster_id": 6, "command_name": "toggle", "payload": {}})).await.unwrap_err();
        assert_eq!(e.code.code(), 5);
        assert_eq!(e.details, "Node 9 does not exist");
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p matter-rs-controller interaction`
Expected: FAIL (commands route to "Unknown command").

- [ ] **Step 3: Implement `interaction.rs` and extend the dispatch match**

```rust
use serde_json::{json, Map, Value};

use matter_rs_wire::error::ServerErrorCode;

use crate::api::CommandError;
use crate::commands::{err, invalid, opt_bool, opt_u64, require_str, require_u64, stack_err};
use crate::real::MatterController;
use crate::stack_api::AttributePathSpec;

/// Node splitAttributePath: decimal segments; non-numeric OR the sentinels
/// 0xffff (endpoint) / 0xffffffff (cluster, attribute) mean wildcard.
pub fn parse_attribute_path(path: &str) -> Result<AttributePathSpec, CommandError> {
    let parts: Vec<&str> = path.split('/').collect();
    if parts.len() != 3 {
        return Err(invalid(format!("Invalid attribute path: {path}")));
    }
    let seg = |s: &str, sentinel: u64| -> Option<u64> {
        match s.parse::<u64>() {
            Ok(n) if n == sentinel => None,
            Ok(n) => Some(n),
            Err(_) => None, // '*' or anything non-numeric
        }
    };
    Ok(AttributePathSpec {
        endpoint: seg(parts[0], 0xFFFF).map(|n| n as u16),
        cluster: seg(parts[1], 0xFFFF_FFFF).map(|n| n as u32),
        attribute: seg(parts[2], 0xFFFF_FFFF).map(|n| n as u32),
    })
}

pub async fn read_attribute(c: &MatterController, args: &Map<String, Value>) -> Result<Value, CommandError> {
    let node_id = require_u64(args, "node_id")?;
    c.ensure_node(node_id)?;
    let fabric_filtered = opt_bool(args, "fabric_filtered").unwrap_or(false);
    let raw = args.get("attribute_path")
        .ok_or_else(|| invalid("missing or invalid required argument: attribute_path"))?;
    let path_strings: Vec<String> = match raw {
        Value::String(s) => vec![s.clone()],
        Value::Array(a) => a.iter()
            .map(|v| v.as_str().map(String::from)
                .ok_or_else(|| invalid("attribute_path entries must be strings")))
            .collect::<Result<_, _>>()?,
        _ => return Err(invalid("attribute_path must be a string or list of strings")),
    };
    let paths = path_strings.iter().map(|p| parse_attribute_path(p)).collect::<Result<Vec<_>, _>>()?;
    let values = c.stack.read_attributes(node_id, paths, fabric_filtered).await
        .map_err(|e| stack_err(ServerErrorCode::SdkStackError, e))?;
    if values.is_empty() {
        return Err(err(ServerErrorCode::SdkStackError, "Failed to read attribute: no values returned"));
    }
    Ok(Value::Object(values.into_iter().collect()))
}

pub async fn write_attribute(c: &MatterController, args: &Map<String, Value>) -> Result<Value, CommandError> {
    let node_id = require_u64(args, "node_id")?;
    c.ensure_node(node_id)?;
    let path = require_str(args, "attribute_path")?;
    let spec = parse_attribute_path(path)?;
    let (Some(endpoint), Some(cluster), Some(attribute)) = (spec.endpoint, spec.cluster, spec.attribute) else {
        return Err(invalid("write_attribute does not support wildcards in attribute path"));
    };
    let value = args.get("value").cloned().unwrap_or(Value::Null);
    let status = c.stack.write_attribute(node_id, endpoint, cluster, attribute, value).await
        .map_err(|e| stack_err(ServerErrorCode::SdkStackError, e))?;
    Ok(json!([{
        "Path": { "EndpointId": endpoint, "ClusterId": cluster, "AttributeId": attribute },
        "Status": status
    }]))
}

pub async fn device_command(c: &MatterController, args: &Map<String, Value>) -> Result<Value, CommandError> {
    let node_id = require_u64(args, "node_id")?;
    c.ensure_node(node_id)?;
    let endpoint = require_u64(args, "endpoint_id")? as u16;
    let cluster = require_u64(args, "cluster_id")? as u32;
    let command_name = require_str(args, "command_name")?.to_string();
    let payload = args.get("payload").cloned().unwrap_or_else(|| json!({}));
    let timed_ms = opt_u64(args, "timed_request_timeout_ms").map(|v| v as u16);
    c.stack.invoke(node_id, endpoint, cluster, command_name, payload, timed_ms).await
        .map_err(|e| stack_err(ServerErrorCode::SdkStackError, e))
}
```

Dispatch arms to add in `real.rs`:
```rust
"read_attribute" => commands::interaction::read_attribute(self, args).await,
"write_attribute" => commands::interaction::write_attribute(self, args).await,
"device_command" => commands::interaction::device_command(self, args).await,
```

- [ ] **Step 4: Run tests until they pass**

Run: `cargo test -p matter-rs-controller`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/controller
git commit -m "feat(controller): read/write/device_command with Node-compatible paths and errors"
```

---

### Task 10: `MatterController` — commissioning, discovery, credentials, fabric commands

**Files:**
- Create: `crates/controller/src/commands/commissioning.rs`, `crates/controller/src/commands/credentials.rs`, `crates/controller/src/commands/fabrics.rs`
- Modify: `crates/controller/src/commands/mod.rs`, `crates/controller/src/real.rs` (dispatch)

**Interfaces:**
- Consumes: `Stack::{commission, open_commissioning_window, browse_commissionable, device_fabrics, remove_device_fabric, update_fabric_label, interview, write_attribute, start_supervisor}`, storage helpers, `alloc_lock`, `label_owner`/`label_locked`, ConnId (dispatch passes it to `set_default_fabric_label` ONLY).
- Produces: dispatch arms for `commission_with_code`, `commission_on_network`, `open_commissioning_window`, `discover`, `discover_commissionable_nodes`, `set_wifi_credentials`, `set_thread_dataset`, `remove_wifi_credentials`, `remove_thread_dataset`, `get_all_credentials`, `set_default_fabric_label`, `get_fabric_label`, `get_matter_fabrics`, `remove_matter_fabric`, `set_acl_entry`, `set_node_binding`.

**`commissioning.rs` semantics:**

Shared flow `commission(c, target) -> Result<Value /* MatterNodeData */, CommandError>`:
1. Take `alloc_lock` for the WHOLE flow (serializes id allocation like Node's mutex, and PASE anyway serializes upstream).
2. `let mut cfg = c.config.lock().unwrap().clone();` → `let node_id = allocate_node_id(&mut cfg, |id| c.registry.contains(id) || id == c.identity.controller_node_id);` → persist config BEFORE commissioning (`save_config` + write back into `c.config`).
3. `c.stack.commission(CommissionRequest { node_id, target, fabric_label: cfg.fabric_label.clone() })`. Error → code 1, details `Commission failed: <stack message>` (StackErrorKind::Busy keeps its message, which carries the PASE-lockout hint from the stack).
4. `c.stack.interview(node_id)`. Error → best-effort `c.stack.remove_device_fabric(node_id, outcome.device_fabric_index)`, then code 1 `Commission failed: <message>`.
5. Build `NodeRecord` (`date_commissioned` = `last_interview` = `format_node_date(now)`, `device_fabric_index`, `addresses: vec![ip_of(outcome.address)]` — strip `:port`), insert into registry, `save_node`.
6. Emit `node_added` with the full `MatterNodeData`; `c.stack.start_supervisor(node_id)`; return the `MatterNodeData`.

- `commission_with_code`: `code` required, non-empty → else code 8 `No pairing code provided`; `network_only` accepted-and-ignored (no BLE, always on-network). Target `PaseTarget::Code { code }`.
- `commission_on_network`: `setup_pin_code` required → else code 8 `No passcode provided`. `filter_type` 1/2/3 require `filter` → exact errors `filter required for filter_type 1 (short discriminator)`, `... 2 (long discriminator)`, `... 3 (vendor ID)`. `ip_addr` (unless it starts with `fe80`) → `PaseTarget::Address { addr: format!("{ip_addr}:5540") }`, else `PaseTarget::OnNetwork { passcode, long_discriminator (type 2), short_discriminator (type 1), vendor_id (type 3) }`.
- `open_commissioning_window`: `node_id` required, `timeout` default 300 (`iteration`/`option`/`discriminator` accepted-and-ignored, Node behavior); result `{"setup_pin_code": ..., "setup_manual_code": ..., "setup_qr_code": ...}`.
- `discover` / `discover_commissionable_nodes`: `stack.browse_commissionable(3000)` → `Vec<CommissionableNodeData>` with per-entry defaults `host_name: "000000000000"`, `vendor_id: -1`, `product_id: -1`, `commissioning_mode: 1`, `pairing_hint: 0`, `supports_tcp: false`, `addresses: [ip]`, `instance_name: Some(...)`, `port: Some(port)`; everything else None (accepted deviation #5). **[README gap list renumbered 2026-08-15 after #2 was retired; this item is now #4 there. #2's retirement was itself reverted later the same day, in the plan 3 final-review wave, once the Node wire direction was settled (matter.js's WS wire is Matter-epoch at every depth) — #2 is back, appended as #7; this item's number, #4, is unaffected and still correct.]**

**`credentials.rs` semantics** (all mutations end with `save_config` + broadcast `server_info_updated` carrying fresh `server_info`; failures of the event send ignored):
- `set_wifi_credentials`: `ssid` required, `credentials` optional-empty, `id` default `"default"`. `validate_credential_id(id, existing wifi ids)` → code 8 on Err. Empty `credentials`: allowed only when an existing entry with this id has the SAME ssid (password kept) else code 8 `WiFi password is required (omit it only to keep the existing password for an unchanged SSID)`. Result `{}`.
- `set_thread_dataset`: `dataset` required; `validate_thread_dataset` → code 8; `validate_credential_id` against thread ids; store hex as-given. Result `{}`.
- `remove_wifi_credentials` / `remove_thread_dataset`: `id` default `"default"`; remove if present (no error when absent). Result `{}`.
- `get_all_credentials`: `{"wifi": [{"id", "ssid"}...], "thread": [{"id", "networkName"?, "extPanId"?}...]}` — force-prepend `{"id": "default", "ssid": ""}` / `{"id": "default"}` when no default entry exists. Thread `networkName`/`extPanId` from a tiny Thread-TLV walk of the hex dataset: iterate `(type: u8, len: u8, value)`; type `0x03` → networkName utf8, type `0x02` → extPanId uppercase hex; parse failure → only `{"id"}`.

**`fabrics.rs` semantics:**
- `set_default_fabric_label(label: string|null)` — takes `conn: ConnId`: if `label_locked` → log `Ignoring set_default_fabric_label ... (pinned via --default-fabric-label)`, return Null. Ownership: lock `label_owner`; `None` → claim for `conn`; `Some(other)` where `other != conn` → log-and-ignore, return Null (success). Owner (or fresh claim): `let label = normalize_fabric_label(label_arg)`; `stack.update_fabric_label(label)`; on success persist `config.fabric_label`; on stack error release ownership IF this call claimed it fresh, and map to code 7. Return Null.
- `get_fabric_label` → `{"fabric_label": <config.fabric_label>}`.
- `get_matter_fabrics(node_id)` → `stack.device_fabrics` mapped to `MatterFabricData` with `vendor_name: crate::vendors::name(vendor_id)` (Task 11 provides `vendors`; for THIS task stub the module with `pub fn name(_: u16) -> Option<String> { None }` and let Task 11 fill the table). Stack error → code 7 `No or invalid response received while querying fabrics`.
- `remove_matter_fabric(node_id, fabric_index)` → `stack.remove_device_fabric` → `{}`.
- `set_acl_entry(node_id, entry: [...])`: map each entry from snake_case JSON to TAG-BASED value: `{"1": privilege, "2": auth_mode, "3": subjects (list of u64 or null), "4": targets (list of {"0": cluster, "1": endpoint, "2": device_type} with nulls preserved) }`. Node quirk kept: drop subjects equal to the target node's own id; drop entries whose subjects list becomes empty (but keep null-subject entries); still report success. Write the resulting array to `0/31/0` via `stack.write_attribute`. Result `[{"path": {"endpoint_id": 0, "cluster_id": 31, "attribute_id": 0}, "status": <status>}]` (snake_case — different from write_attribute, Node quirk).
- `set_node_binding(node_id, endpoint, bindings: [...])`: map `{node→"1", group→"2", endpoint→"3", cluster→"4"}` omitting nulls; write to `<endpoint>/30/0`. Result `[{"path": {"endpoint_id": <endpoint>, "cluster_id": 30, "attribute_id": 0}, "status": <status>}]`.

- [ ] **Step 1: Write the failing tests** (one representative excerpt per module — write ALL of these):

`commissioning.rs` tests:
```rust
#[cfg(test)]
mod tests {
    use crate::real::test_rig::*;
    use crate::stack_api::{CommissionOutcome, WindowInfo};
    use serde_json::json;

    #[tokio::test]
    async fn commission_with_code_full_flow() {
        use crate::api::Controller;
        let r = rig();
        *r.stack.commission_response.lock().unwrap() =
            Some(Ok(CommissionOutcome { device_fabric_index: 3, address: "192.168.1.60:5540".into() }));
        *r.stack.interview_response.lock().unwrap() =
            Some(Ok([("0/40/2".to_string(), json!(65521))].into()));
        let mut events = r.ctrl.subscribe_events();
        let v = call(&r, "commission_with_code", json!({"code": "MT:TEST"})).await.unwrap();
        assert_eq!(v["node_id"], 1);
        assert_eq!(v["available"], false); // available flips when the supervisor connects
        assert_eq!(v["attributes"]["0/40/2"], 65521);
        let ev = events.recv().await.unwrap();
        assert_eq!(ev.event, "node_added");
        assert_eq!(ev.data["node_id"], 1);
        assert!(r.stack.calls().contains(&"start_supervisor 1".to_string()));
        // node id advanced + persisted
        let e = call(&r, "get_node", json!({"node_id": 1})).await;
        assert!(e.is_ok());
    }

    #[tokio::test]
    async fn commission_with_code_empty_code() {
        let r = rig();
        let e = call(&r, "commission_with_code", json!({"code": ""})).await.unwrap_err();
        assert_eq!(e.code.code(), 8);
        assert_eq!(e.details, "No pairing code provided");
    }

    #[tokio::test]
    async fn commission_failure_maps_to_code_1() {
        let r = rig();
        *r.stack.commission_response.lock().unwrap() = Some(Err(crate::stack_api::StackError::new(
            crate::stack_api::StackErrorKind::Busy,
            "device is busy (previous commissioning attempt may hold its failsafe for ~60s)")));
        let e = call(&r, "commission_with_code", json!({"code": "MT:TEST"})).await.unwrap_err();
        assert_eq!(e.code.code(), 1);
        assert!(e.details.starts_with("Commission failed: "));
        assert!(e.details.contains("busy"));
    }

    #[tokio::test]
    async fn commission_on_network_filter_validation() {
        let r = rig();
        let e = call(&r, "commission_on_network", json!({"setup_pin_code": 20202021, "filter_type": 2})).await.unwrap_err();
        assert_eq!(e.details, "filter required for filter_type 2 (long discriminator)");
        let e = call(&r, "commission_on_network", json!({})).await.unwrap_err();
        assert_eq!(e.details, "No passcode provided");
    }

    #[tokio::test]
    async fn open_commissioning_window_shape() {
        let r = rig_with_nodes(vec![node_record(5)]);
        *r.stack.window_response.lock().unwrap() = Some(Ok(WindowInfo {
            setup_pin_code: 12345678, setup_manual_code: "36296231493".into(),
            setup_qr_code: "MT:ABC".into() }));
        let v = call(&r, "open_commissioning_window", json!({"node_id": 5})).await.unwrap();
        assert_eq!(v, json!({"setup_pin_code": 12345678, "setup_manual_code": "36296231493", "setup_qr_code": "MT:ABC"}));
        assert!(r.stack.calls().iter().any(|c| c == "ocw node=5 timeout=300"));
    }

    #[tokio::test]
    async fn discover_maps_defaults() {
        let r = rig();
        *r.stack.browse_response.lock().unwrap() = Some(Ok(vec![crate::stack_api::DiscoveredDevice {
            instance_name: "A5F15790B69D73D9".into(), address: "192.168.1.61:5540".into() }]));
        let v = call(&r, "discover_commissionable_nodes", json!({})).await.unwrap();
        assert_eq!(v[0]["host_name"], "000000000000");
        assert_eq!(v[0]["vendor_id"], -1);
        assert_eq!(v[0]["addresses"], json!(["192.168.1.61"]));
    }
}
```

`credentials.rs` tests:
```rust
#[cfg(test)]
mod tests {
    use crate::real::test_rig::*;
    use serde_json::json;

    #[tokio::test]
    async fn wifi_credentials_set_get_remove_and_server_info() {
        use crate::api::Controller;
        let r = rig();
        let mut events = r.ctrl.subscribe_events();
        let v = call(&r, "set_wifi_credentials", json!({"ssid": "iot", "credentials": "hunter2"})).await.unwrap();
        assert_eq!(v, json!({}));
        assert_eq!(events.recv().await.unwrap().event, "server_info_updated");
        let si = call(&r, "server_info", json!({})).await.unwrap();
        assert_eq!(si["wifi_credentials_set"], true);
        assert_eq!(si["wifi_ssid"], "iot");
        let v = call(&r, "get_all_credentials", json!({})).await.unwrap();
        assert_eq!(v["wifi"], json!([{"id": "default", "ssid": "iot"}]));
        // secrets are write-only: password never appears
        assert!(!v.to_string().contains("hunter2"));
        call(&r, "remove_wifi_credentials", json!({})).await.unwrap();
        let si = call(&r, "server_info", json!({})).await.unwrap();
        assert_eq!(si["wifi_credentials_set"], false);
    }

    #[tokio::test]
    async fn wifi_password_required_unless_same_ssid() {
        let r = rig();
        let e = call(&r, "set_wifi_credentials", json!({"ssid": "iot", "credentials": ""})).await.unwrap_err();
        assert_eq!(e.details, "WiFi password is required (omit it only to keep the existing password for an unchanged SSID)");
        call(&r, "set_wifi_credentials", json!({"ssid": "iot", "credentials": "pw"})).await.unwrap();
        // same ssid, empty password -> keeps old
        call(&r, "set_wifi_credentials", json!({"ssid": "iot", "credentials": ""})).await.unwrap();
        let si = call(&r, "server_info", json!({})).await.unwrap();
        assert_eq!(si["wifi_credentials_set"], true);
    }

    #[tokio::test]
    async fn thread_dataset_validation_and_decode() {
        let r = rig();
        let e = call(&r, "set_thread_dataset", json!({"dataset": "xyz"})).await.unwrap_err();
        assert_eq!(e.code.code(), 8);
        // TLVs: 0x02 (ExtPanId) len 8; 0x03 (NetworkName) len 4 "test"
        let ds = "0208deadbeefcafe0001030474657374";
        call(&r, "set_thread_dataset", json!({"dataset": ds})).await.unwrap();
        let v = call(&r, "get_all_credentials", json!({})).await.unwrap();
        assert_eq!(v["thread"][0]["id"], "default");
        assert_eq!(v["thread"][0]["networkName"], "test");
        assert_eq!(v["thread"][0]["extPanId"], "DEADBEEFCAFE0001");
    }

    #[tokio::test]
    async fn named_credentials_and_reserved_ids() {
        let r = rig();
        call(&r, "set_wifi_credentials", json!({"ssid": "a", "credentials": "b", "id": "garage"})).await.unwrap();
        let e = call(&r, "set_wifi_credentials", json!({"ssid": "a", "credentials": "b", "id": "GARAGE"})).await.unwrap_err();
        assert_eq!(e.details, "invalid-credential-id: 'GARAGE' duplicates existing 'garage'");
        let e = call(&r, "set_wifi_credentials", json!({"ssid": "a", "credentials": "b", "id": "delete"})).await.unwrap_err();
        assert_eq!(e.details, "invalid-credential-id: 'delete' is reserved");
        let v = call(&r, "get_all_credentials", json!({})).await.unwrap();
        // default force-prepended even though only "garage" exists
        assert_eq!(v["wifi"][0]["id"], "default");
        assert_eq!(v["wifi"][1]["id"], "garage");
    }
}
```

`fabrics.rs` tests:
```rust
#[cfg(test)]
mod tests {
    use crate::api::{ConnId, Controller};
    use crate::real::test_rig::*;
    use serde_json::json;

    #[tokio::test]
    async fn fabric_label_ownership_per_connection() {
        let r = rig();
        // conn 1 claims
        let v = r.ctrl.handle_command(ConnId(1), &cmd("set_default_fabric_label", json!({"label": "Casa"}))).await.unwrap();
        assert_eq!(v, serde_json::Value::Null);
        assert!(r.stack.calls().contains(&"update_fabric_label Casa".to_string()));
        // conn 2 is ignored but still succeeds
        r.ctrl.handle_command(ConnId(2), &cmd("set_default_fabric_label", json!({"label": "Nope"}))).await.unwrap();
        let v = call(&r, "get_fabric_label", json!({})).await.unwrap();
        assert_eq!(v, json!({"fabric_label": "Casa"}));
        // conn 1 closing releases ownership; conn 2 can now set
        r.ctrl.connection_closed(ConnId(1));
        r.ctrl.handle_command(ConnId(2), &cmd("set_default_fabric_label", json!({"label": "Second"}))).await.unwrap();
        let v = call(&r, "get_fabric_label", json!({})).await.unwrap();
        assert_eq!(v, json!({"fabric_label": "Second"}));
    }

    #[tokio::test]
    async fn empty_label_resets_to_homeassistant() {
        let r = rig();
        call(&r, "set_default_fabric_label", json!({"label": ""})).await.unwrap();
        let v = call(&r, "get_fabric_label", json!({})).await.unwrap();
        assert_eq!(v, json!({"fabric_label": "HomeAssistant"}));
    }

    #[tokio::test]
    async fn get_matter_fabrics_maps_device_list() {
        let r = rig_with_nodes(vec![node_record(5)]);
        *r.stack.fabrics_response.lock().unwrap() = Some(Ok(vec![crate::stack_api::DeviceFabric {
            fabric_id: 1, vendor_id: 0xFFF1, fabric_index: 3, fabric_label: "HomeAssistant".into() }]));
        let v = call(&r, "get_matter_fabrics", json!({"node_id": 5})).await.unwrap();
        assert_eq!(v[0]["fabric_index"], 3);
        assert_eq!(v[0]["fabric_label"], "HomeAssistant");
        let v = call(&r, "remove_matter_fabric", json!({"node_id": 5, "fabric_index": 3})).await.unwrap();
        assert_eq!(v, json!({}));
    }

    #[tokio::test]
    async fn set_acl_entry_strips_self_subjects_and_writes_tag_based() {
        let r = rig_with_nodes(vec![node_record(5)]);
        let v = call(&r, "set_acl_entry", json!({"node_id": 5, "entry": [
            {"privilege": 5, "auth_mode": 2, "subjects": [112233, 5], "targets": null},
            {"privilege": 3, "auth_mode": 2, "subjects": [5], "targets": null}
        ]})).await.unwrap();
        // second entry lost its only subject (self) and was dropped; still success
        assert_eq!(v, json!([{"path": {"endpoint_id": 0, "cluster_id": 31, "attribute_id": 0}, "status": 0}]));
        assert!(r.stack.calls().iter().any(|c| c == "write node=5 0/31/0"));
    }

    #[tokio::test]
    async fn set_node_binding_writes_binding_cluster() {
        let r = rig_with_nodes(vec![node_record(5)]);
        let v = call(&r, "set_node_binding", json!({"node_id": 5, "endpoint": 1,
            "bindings": [{"node": 2, "group": null, "endpoint": 1, "cluster": 6}]})).await.unwrap();
        assert_eq!(v, json!([{"path": {"endpoint_id": 1, "cluster_id": 30, "attribute_id": 0}, "status": 0}]));
        assert!(r.stack.calls().iter().any(|c| c == "write node=5 1/30/0"));
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p matter-rs-controller commissioning credentials fabrics` (or the whole crate)
Expected: FAIL.

- [ ] **Step 3: Implement the three modules per the semantics blocks above.** Implementation notes that need to be exact:

- `commission_with_code` / `commission_on_network` share `async fn do_commission(c, target)`; hold `c.alloc_lock` across steps 2–6.
- Dispatch: `set_default_fabric_label` is the ONLY handler that receives `conn`:
```rust
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
```
(remove the `let _ = conn;` shim from Task 8's dispatch now that `conn` is genuinely used).
- `server_info_updated` broadcast helper on MatterController:
```rust
pub(crate) fn broadcast_server_info_updated(&self) {
    let _ = self.events.send(matter_rs_wire::envelope::EventMessage {
        event: "server_info_updated".into(),
        data: serde_json::to_value(self.build_server_info()).unwrap(),
    });
}
```
- Thread dataset TLV walk (in `credentials.rs`):
```rust
fn thread_dataset_info(hex: &str) -> (Option<String>, Option<String>) {
    let Ok(bytes) = (0..hex.len()).step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16))
        .collect::<Result<Vec<u8>, _>>() else { return (None, None) };
    let (mut name, mut xpan) = (None, None);
    let mut i = 0usize;
    while i + 2 <= bytes.len() {
        let (t, l) = (bytes[i], bytes[i + 1] as usize);
        let Some(v) = bytes.get(i + 2..i + 2 + l) else { break };
        match t {
            0x03 => name = std::str::from_utf8(v).ok().map(String::from),
            0x02 => xpan = Some(v.iter().map(|b| format!("{b:02X}")).collect()),
            _ => {}
        }
        i += 2 + l;
    }
    (name, xpan)
}
```
- `set_acl_entry` tag mapping (AccessControlEntryStruct context tags: privilege=1, authMode=2, subjects=3, targets=4; target struct: cluster=0, endpoint=1, deviceType=2). `set_node_binding` TargetStruct tags: node=1, group=2, endpoint=3, cluster=4.

- [ ] **Step 4: Run tests until they pass**

Run: `cargo test -p matter-rs-controller`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/controller
git commit -m "feat(controller): commissioning, discovery, credentials and fabric commands"
```

---

### Task 11: `MatterController` — vendors, loglevel, honest stubs; full command-surface test

**Files:**
- Create: `crates/controller/src/commands/misc.rs`
- Modify: `crates/controller/src/vendors.rs` (created as a `None` stub in Task 10 — replace with the real table)
- Modify: `crates/controller/src/commands/mod.rs`, `crates/controller/src/real.rs` (dispatch), `crates/controller/src/lib.rs`

**Interfaces:**
- Produces: `vendors::name(vendor_id: u16) -> Option<&'static str>` and `vendors::all() -> &'static [(u16, &'static str)]`; dispatch arms for `get_vendor_names`, `get_loglevel`, `set_loglevel`, `get_icd_state`, `register_icd`, `unregister_icd`, `resync_icd`, `check_node_update`, `update_node`.

**Semantics:**
- `vendors.rs`: extract the vendor table from the Node clone — run `grep -rn "VendorIds" matterjs-server/packages/ws-controller/src --include="*.ts" -l` and port the id→name entries into `pub static VENDORS: &[(u16, &str)]` (sorted by id, binary-search lookup). If the file has hundreds of entries, port them all mechanically (regex-replace in the editor); if extraction proves impractical within the task, fall back to this minimal seed (and leave a `// TODO(plan3): full table` note): `4874 "Eve Systems"`, `4447 "Nanoleaf"`, `4476 "IKEA of Sweden"`, `4488 "Yeelight"`, `4489 "Innr"`, `4610 "Aqara"`, `4631 "TP-Link"`, `4874`, `4919 "Tuya"`, `4937 "Apple Home"`, `4938 "Apple"`, `4996 "Signify Netherlands B.V."`, `24582 "Google LLC"`, `65521 "Test Vendor"`.
- `get_vendor_names(filter_vendors?: number[])`: result object keyed by DECIMAL-STRING vendor id → name; no/empty filter → whole table; unknown filtered ids silently omitted.
- `get_loglevel` → `{"console_loglevel": <level>, "file_loglevel": <level or null>}` from `self.log.get()`. `set_loglevel(console_loglevel?, file_loglevel?)` → `self.log.set(...)` then same result as get. Accepted level names: `fatal|critical|error|warning|warn|notice|info|debug|verbose` (unknown → treated as `info`, matching logging.rs map_level fallback).
- ICD stubs (design: honest "not registered"): all four require a known `node_id` (else code 5 exact string). `get_icd_state`/`register_icd`/`unregister_icd` → `IcdState::not_registered()` JSON; `resync_icd` → `null`.
- `check_node_update(node_id)` → `null` (no update available). `update_node(node_id, software_version)` → code 11 `OTA is disabled`. (`initiate_ota_upload` is not in the 31-command scope; it falls through to Unknown command like `import_test_node`, `send_webrtc_provider_command`, `subscribe_attribute`, `get_thread_diagnostics`, `get_thread_border_routers`, `get_network_topology` — all code 9 `Unknown command: <cmd>`, which is spec-conformant gating for out-of-scope features.)

- [ ] **Step 1: Write the failing tests** (bottom of `misc.rs`)

```rust
#[cfg(test)]
mod tests {
    use crate::real::test_rig::*;
    use serde_json::json;

    #[tokio::test]
    async fn vendor_names_full_and_filtered() {
        let r = rig();
        let v = call(&r, "get_vendor_names", json!({})).await.unwrap();
        assert_eq!(v["4476"], "IKEA of Sweden");
        let v = call(&r, "get_vendor_names", json!({"filter_vendors": [4476, 1]})).await.unwrap();
        assert_eq!(v.as_object().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn loglevel_get_set() {
        let r = rig();
        let v = call(&r, "get_loglevel", json!({})).await.unwrap();
        assert_eq!(v, json!({"console_loglevel": "info", "file_loglevel": null}));
        let v = call(&r, "set_loglevel", json!({"console_loglevel": "debug"})).await.unwrap();
        // NopLog ignores sets; shape is what matters here
        assert!(v.get("console_loglevel").is_some());
    }

    #[tokio::test]
    async fn icd_stubs() {
        let r = rig_with_nodes(vec![node_record(5)]);
        let v = call(&r, "get_icd_state", json!({"node_id": 5})).await.unwrap();
        assert_eq!(v["supported"], false);
        assert_eq!(v["registered"], false);
        assert_eq!(v["operating_mode"], serde_json::Value::Null);
        let v = call(&r, "resync_icd", json!({"node_id": 5})).await.unwrap();
        assert_eq!(v, serde_json::Value::Null);
        let e = call(&r, "get_icd_state", json!({"node_id": 9})).await.unwrap_err();
        assert_eq!(e.details, "Node 9 does not exist");
    }

    #[tokio::test]
    async fn ota_stubs() {
        let r = rig_with_nodes(vec![node_record(5)]);
        let v = call(&r, "check_node_update", json!({"node_id": 5})).await.unwrap();
        assert_eq!(v, serde_json::Value::Null);
        let e = call(&r, "update_node", json!({"node_id": 5, "software_version": 2})).await.unwrap_err();
        assert_eq!(e.code.code(), 11);
        assert_eq!(e.details, "OTA is disabled");
    }

    #[tokio::test]
    async fn all_31_commands_are_dispatched() {
        // The full v1 surface: every command must be routed (i.e. NOT hit the
        // "Unknown command" fallback), whatever its result is.
        let r = rig();
        let all = [
            "server_info", "start_listening", "diagnostics", "ping_node", "get_node_ip_addresses",
            "get_nodes", "get_node", "interview_node", "remove_node",
            "device_command", "read_attribute", "write_attribute",
            "commission_with_code", "commission_on_network", "open_commissioning_window",
            "discover_commissionable_nodes", "discover",
            "set_wifi_credentials", "set_thread_dataset", "remove_wifi_credentials",
            "remove_thread_dataset", "get_all_credentials",
            "set_default_fabric_label", "get_fabric_label", "get_matter_fabrics",
            "remove_matter_fabric", "set_acl_entry", "set_node_binding",
            "get_vendor_names", "get_loglevel", "set_loglevel",
        ];
        assert_eq!(all.len(), 31);
        for name in all {
            match call(&r, name, json!({})).await {
                Ok(_) => {}
                Err(e) => assert!(
                    !e.details.starts_with("Unknown command"),
                    "{name} hit the Unknown command fallback"),
            }
        }
        // and the honest-stub / gated set:
        for name in ["get_icd_state", "register_icd", "unregister_icd", "resync_icd",
                     "check_node_update", "update_node"] {
            let e = call(&r, name, json!({"node_id": 1})).await.unwrap_err();
            assert!(!e.details.starts_with("Unknown command"), "{name}");
        }
        for name in ["import_test_node", "send_webrtc_provider_command", "subscribe_attribute",
                     "get_thread_diagnostics", "get_thread_border_routers", "get_network_topology"] {
            let e = call(&r, name, json!({})).await.unwrap_err();
            assert_eq!(e.code.code(), 9, "{name} must be Unknown command");
        }
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p matter-rs-controller misc`
Expected: FAIL.

- [ ] **Step 3: Implement `vendors.rs` + `misc.rs` + dispatch arms.** `misc.rs` sketch:

```rust
use serde_json::{json, Map, Value};

use matter_rs_wire::error::ServerErrorCode;
use matter_rs_wire::node::IcdState;

use crate::api::CommandError;
use crate::commands::{err, require_u64};
use crate::real::MatterController;

pub async fn get_vendor_names(_c: &MatterController, args: &Map<String, Value>) -> Result<Value, CommandError> {
    let filter: Option<Vec<u64>> = args.get("filter_vendors")
        .and_then(Value::as_array)
        .map(|a| a.iter().filter_map(Value::as_u64).collect());
    let mut out = Map::new();
    for (id, name) in crate::vendors::all() {
        if filter.as_ref().is_none_or(|f| f.contains(&(*id as u64))) {
            out.insert(id.to_string(), json!(name));
        }
    }
    Ok(Value::Object(out))
}

pub async fn get_loglevel(c: &MatterController, _args: &Map<String, Value>) -> Result<Value, CommandError> {
    let (console, file) = c.log.get();
    Ok(json!({"console_loglevel": console, "file_loglevel": file}))
}

pub async fn set_loglevel(c: &MatterController, args: &Map<String, Value>) -> Result<Value, CommandError> {
    c.log.set(args.get("console_loglevel").and_then(Value::as_str),
              args.get("file_loglevel").and_then(Value::as_str));
    get_loglevel(c, args).await
}

pub async fn icd_state(c: &MatterController, args: &Map<String, Value>) -> Result<Value, CommandError> {
    let node_id = require_u64(args, "node_id")?;
    c.ensure_node(node_id)?;
    Ok(serde_json::to_value(IcdState::not_registered()).unwrap())
}

pub async fn resync_icd(c: &MatterController, args: &Map<String, Value>) -> Result<Value, CommandError> {
    let node_id = require_u64(args, "node_id")?;
    c.ensure_node(node_id)?;
    Ok(Value::Null)
}

pub async fn check_node_update(c: &MatterController, args: &Map<String, Value>) -> Result<Value, CommandError> {
    let node_id = require_u64(args, "node_id")?;
    c.ensure_node(node_id)?;
    Ok(Value::Null)
}

pub async fn update_node(c: &MatterController, args: &Map<String, Value>) -> Result<Value, CommandError> {
    let node_id = require_u64(args, "node_id")?;
    c.ensure_node(node_id)?;
    Err(err(ServerErrorCode::UpdateError, "OTA is disabled"))
}
```

Dispatch arms: `"get_vendor_names"`, `"get_loglevel"`, `"set_loglevel"`, `"get_icd_state" | "register_icd" | "unregister_icd" => misc::icd_state`, `"resync_icd"`, `"check_node_update"`, `"update_node"`.

- [ ] **Step 4: Run tests until they pass**

Run: `cargo test -p matter-rs-controller`
Expected: PASS — the controller crate's command surface is complete against FakeStack.

- [ ] **Step 5: Commit**

```bash
git add crates/controller
git commit -m "feat(controller): vendor table, loglevel, ICD/OTA stubs — full 31-command surface"
```

---

### Task 12: `stack` crate scaffold + TLV↔JSON codec

**Files:**
- Create: `crates/stack/Cargo.toml`, `crates/stack/src/lib.rs`, `crates/stack/src/tlv_json.rs`
- Modify: root `Cargo.toml` (add `crates/stack` to members)

**Interfaces:**
- Consumes: rs-matter TLV APIs (`TLVElement`, `TLVValue`, `TLVTag`, `TLVWrite` — see `rs-matter-ref/rs-matter/src/tlv/read.rs`, `tlv/write.rs`), `matter_rs_gen`.
- Produces (Tasks 15/16 consume):

```rust
pub const MATTER_EPOCH_OFFSET_S: u64 = 946_684_800;      // 2000-01-01 - 1970-01-01
pub const MATTER_EPOCH_OFFSET_US: u64 = 946_684_800_000_000;

/// TLV -> tag-based JSON (numeric-string keys for struct fields).
pub fn tlv_to_json(elem: &TLVElement) -> Result<serde_json::Value, rs_matter::error::Error>;
/// Attribute value -> JSON, applying epoch_s/epoch_us -> Unix at top level
/// when `gen` knows the attribute's type (accepted deviation #2 for nesting).
/// **[RESOLVED 2026-08-15, plan 3 Task 5: epoch conversion briefly ran at
/// every depth of the tag-based walk (`typed_to_json`), and accepted
/// deviation #2 was retired on that basis. REVERTED same day, in the plan 3
/// final-review wave, once the Node wire direction was actually settled:
/// matter.js's WS wire carries Matter-epoch values at every depth (the JS
/// layer's own values are Unix, but `Converters.ts` subtracts the offset
/// converting matter -> WS, re-encoding back to Matter epoch before the wire).
/// `typed_to_json` was deleted and `attr_value_to_json` reverted to
/// top-level-only conversion; deviation #2 is back in the README "Accepted
/// parity gaps" list (now item #7 there, appended rather than reusing #2's
/// old slot).]**
pub fn attr_value_to_json(cluster: u32, attr: u32, elem: &TLVElement) -> Result<Value, Error>;
/// TLV -> name-based JSON using a gen struct/event field list (camelCase keys);
/// unknown field ids fall back to the numeric key. Applies base64/epoch by field type.
pub fn tlv_to_json_named(elem: &TLVElement, fields: &[matter_rs_gen::Field],
                         cluster: &matter_rs_gen::Cluster) -> Result<Value, Error>;
/// Tag-based JSON -> TLV under `tag`. `hint` (type name + is_list + cluster for
/// struct resolution) drives width/base64/epoch; None -> heuristics.
pub fn write_json<W: TLVWrite>(w: &mut W, tag: &TLVTag, v: &Value,
                               hint: Option<TypeHint<'_>>) -> Result<(), Error>;
/// Name-based JSON object (command payload) -> TLV struct under `tag`,
/// resolving field names case-insensitively against `fields`.
/// Unknown key -> Err (maps to InvalidArguments upstream).
pub fn write_json_named<W: TLVWrite>(w: &mut W, tag: &TLVTag, obj: &Map<String, Value>,
                                     fields: &[matter_rs_gen::Field],
                                     cluster: &matter_rs_gen::Cluster) -> Result<(), Error>;
pub struct TypeHint<'a> { pub ty: &'a str, pub is_list: bool, pub cluster: Option<&'static matter_rs_gen::Cluster> }
```

**Conversion rules (Node Converters.ts semantics):**
- Reading (TLV→JSON): unsigned → u64 number, signed → i64, bool, f32/f64, utf8 → string, octets → base64 string (STANDARD engine), Null → null, Array/List → JSON array, Struct → object keyed by decimal context-tag; anonymous/other tags inside structs are skipped with a debug log.
- Type-driven extras when a `gen` type is known: `epoch_s` → `unix = matter + MATTER_EPOCH_OFFSET_S`; `epoch_us` → `unix_us = matter + MATTER_EPOCH_OFFSET_US`; struct-typed fields recurse with the named/nested field list (named mode) or plain tag-based (tag mode).
- Writing (JSON→TLV): with a hint — `boolean`→bool; `int8u/int16u/int24u/int32u/enum8/enum16/bitmap8/16/32`→u8/u16/u32 minimal fit; `int64u/bitmap64/epoch_us/fabric_id/node_id/...`→u64; signed `int8s..int64s`→i64 minimal; `single`→f32, `double`→f64; `octet_string/long_octet_string`→base64-decode→octets; `char_string/long_char_string`→utf8; `epoch_s`→`matter = unix - OFFSET` as u32; struct type names resolve via `cluster.find_struct` (tag-based objects use numeric keys→field codes for nested hints). `is_list` wraps in start_array/end_container with anonymous element tags. Without a hint — heuristics: Bool→bool, u64→minimal-width unsigned, i64(neg)→minimal signed, f64→f64, String→utf8, Null→null, Array→array of anonymous, Object with all-numeric keys→struct with context tags **sorted ascending numerically** (TLV struct canonical order).
- The writer closures rs-matter hands out implement `TLVWrite` (`ReadSenderSlot` etc.), so these functions are generic over `W: TLVWrite`.

- [ ] **Step 1: Create the crate**

`crates/stack/Cargo.toml`:
```toml
[package]
name = "matter-rs-stack"
version.workspace = true
edition.workspace = true
license.workspace = true

[dependencies]
matter-rs-controller = { path = "../controller" }
matter-rs-gen = { path = "../gen" }
rs-matter = { git = "https://github.com/project-chip/rs-matter", rev = "03bc8f2aeb7765a93e7863e2263f73c7bbc3d401", features = ["async-io", "max-sessions-32", "case-resumption"] }
embassy-futures = "0.1"
embassy-time = { version = "0.5", features = ["std"] }
embassy-time-queue-utils = { version = "0.3", features = ["generic-queue-64"] }
async-executor = "1"
async-io = "2"
futures-lite = "2"
static_cell = "1"
socket2 = { version = "0.5", features = ["all"] }
if-addrs = "0.15"
rand = { version = "0.8", features = ["std", "std_rng"] }
base64 = "0.22"
serde_json.workspace = true
async-trait.workspace = true
tracing.workspace = true
tokio = { version = "1", features = ["sync", "rt"] }
```
(If `case-resumption` turns out not to be a feature name at this rev, check `rs-matter-ref/rs-matter/Cargo.toml` `[features]` and use the exact name; drop it only if resumption is unconditional.)

`crates/stack/src/lib.rs` (skeleton for this task):
```rust
//! The ONLY crate that imports rs-matter. Everything runs on one dedicated
//! OS thread (rs-matter futures are !Send); the outside world talks to it
//! through `StackHandle` (Task 16) which implements
//! `matter_rs_controller::stack_api::Stack`.

pub mod tlv_json;
```

- [ ] **Step 2: Write the failing tests** (bottom of `tlv_json.rs`). Test vectors are built with rs-matter's own writer (`WriteBuf` implements `TLVWrite`):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use rs_matter::tlv::{TLVElement, TLVTag, TLVWrite};
    use rs_matter::utils::storage::WriteBuf;
    use serde_json::json;

    fn build(f: impl FnOnce(&mut WriteBuf<'_>)) -> Vec<u8> {
        let mut buf = [0u8; 256];
        let mut wb = WriteBuf::new(&mut buf);
        f(&mut wb);
        wb.as_slice().to_vec()
    }

    #[test]
    fn struct_to_tag_based_object() {
        let bytes = build(|w| {
            w.start_struct(&TLVTag::Anonymous).unwrap();
            w.u16(&TLVTag::Context(0), 22).unwrap();
            w.utf8(&TLVTag::Context(1), "hi").unwrap();
            w.bool(&TLVTag::Context(2), true).unwrap();
            w.null(&TLVTag::Context(3)).unwrap();
            w.end_container().unwrap();
        });
        let v = tlv_to_json(&TLVElement::new(&bytes)).unwrap();
        assert_eq!(v, json!({"0": 22, "1": "hi", "2": true, "3": null}));
    }

    #[test]
    fn array_of_structs_and_octets() {
        let bytes = build(|w| {
            w.start_array(&TLVTag::Anonymous).unwrap();
            w.start_struct(&TLVTag::Anonymous).unwrap();
            w.u8(&TLVTag::Context(0), 14).unwrap();
            w.end_container().unwrap();
            w.str(&TLVTag::Anonymous, &[0xDE, 0xAD]).unwrap();
            w.end_container().unwrap();
        });
        let v = tlv_to_json(&TLVElement::new(&bytes)).unwrap();
        assert_eq!(v, json!([{"0": 14}, "3q0="])); // base64(0xDE 0xAD)
    }

    #[test]
    fn signed_and_large_unsigned() {
        let bytes = build(|w| {
            w.start_struct(&TLVTag::Anonymous).unwrap();
            w.i32(&TLVTag::Context(0), -5).unwrap();
            w.u64(&TLVTag::Context(1), u64::MAX).unwrap();
            w.end_container().unwrap();
        });
        let v = tlv_to_json(&TLVElement::new(&bytes)).unwrap();
        assert_eq!(v["0"], json!(-5));
        assert_eq!(v["1"], json!(u64::MAX)); // stays a full-precision number
    }

    #[test]
    fn named_conversion_uses_gen_fields() {
        // OperationalCredentials NOCResponse: statusCode=0, fabricIndex=1
        let cluster = matter_rs_gen::cluster(62).unwrap();
        let resp = cluster.find_struct("NOCResponse").unwrap();
        let bytes = build(|w| {
            w.start_struct(&TLVTag::Anonymous).unwrap();
            w.u8(&TLVTag::Context(0), 0).unwrap();
            w.u8(&TLVTag::Context(1), 3).unwrap();
            w.end_container().unwrap();
        });
        let v = tlv_to_json_named(&TLVElement::new(&bytes), resp.fields, cluster).unwrap();
        assert_eq!(v, json!({"statusCode": 0, "fabricIndex": 3}));
    }

    #[test]
    fn write_named_payload_roundtrip() {
        // LevelControl MoveToLevelRequest { level: int8u = 0, transitionTime = 1, ... }
        let cluster = matter_rs_gen::cluster(8).unwrap();
        let input = cluster.find_struct("MoveToLevelRequest").unwrap();
        let payload = json!({"level": 100, "transitionTime": null, "optionsMask": 0, "optionsOverride": 0});
        let mut buf = [0u8; 128];
        let mut wb = rs_matter::utils::storage::WriteBuf::new(&mut buf);
        write_json_named(&mut wb, &TLVTag::Anonymous,
                         payload.as_object().unwrap(), input.fields, cluster).unwrap();
        let back = tlv_to_json(&TLVElement::new(wb.as_slice())).unwrap();
        assert_eq!(back["0"], 100);
        assert_eq!(back["1"], serde_json::Value::Null);
    }

    #[test]
    fn write_named_unknown_field_errors() {
        let cluster = matter_rs_gen::cluster(8).unwrap();
        let input = cluster.find_struct("MoveToLevelRequest").unwrap();
        let payload = json!({"nope": 1});
        let mut buf = [0u8; 64];
        let mut wb = rs_matter::utils::storage::WriteBuf::new(&mut buf);
        assert!(write_json_named(&mut wb, &TLVTag::Anonymous,
                                 payload.as_object().unwrap(), input.fields, cluster).is_err());
    }

    #[test]
    fn write_tag_based_with_octet_hint_roundtrip() {
        let mut buf = [0u8; 64];
        let mut wb = rs_matter::utils::storage::WriteBuf::new(&mut buf);
        write_json(&mut wb, &TLVTag::Anonymous, &json!("3q0="),
                   Some(TypeHint { ty: "octet_string", is_list: false, cluster: None })).unwrap();
        let elem = TLVElement::new(wb.as_slice());
        assert_eq!(elem.octets().unwrap(), &[0xDE, 0xAD]);
    }

    #[test]
    fn tag_based_object_sorts_context_tags() {
        let mut buf = [0u8; 64];
        let mut wb = rs_matter::utils::storage::WriteBuf::new(&mut buf);
        write_json(&mut wb, &TLVTag::Anonymous, &json!({"2": 2, "0": 0, "10": 10}), None).unwrap();
        let back = tlv_to_json(&TLVElement::new(wb.as_slice())).unwrap();
        // JSON object key order isn't asserted; the TLV encodes 0,2,10 in order
        // and parses back completely.
        assert_eq!(back, json!({"0": 0, "2": 2, "10": 10}));
    }

    #[test]
    fn epoch_seconds_attribute_converts_to_unix() {
        // Any cluster attr typed epoch_s works; find one in gen (e.g. TimeSynchronization)
        // — if lookup is brittle, test the helper directly:
        let bytes = build(|w| { w.u32(&TLVTag::Anonymous, 100).unwrap(); });
        let v = apply_epoch("epoch_s", tlv_to_json(&TLVElement::new(&bytes)).unwrap());
        assert_eq!(v, json!(100u64 + MATTER_EPOCH_OFFSET_S));
    }
}
```

(`apply_epoch(ty, value)` is the small internal helper both converters share; expose it `pub(crate)`.)

- [ ] **Step 3: Verify the crate compiles and tests fail meaningfully**

Run: `cargo test -p matter-rs-stack`
Expected: compile errors first (functions missing), then failing tests. NOTE: the first build compiles rs-matter (~2–4 min warm).

- [ ] **Step 4: Implement `tlv_json.rs`**

Core walker (complete; helpers around it as needed):

```rust
use base64::Engine as _;
use rs_matter::error::{Error, ErrorCode};
use rs_matter::tlv::{TLVElement, TLVTag, TLVValue, TLVWrite};
use serde_json::{Map, Value};

pub const MATTER_EPOCH_OFFSET_S: u64 = 946_684_800;
pub const MATTER_EPOCH_OFFSET_US: u64 = 946_684_800_000_000;

fn b64() -> base64::engine::general_purpose::GeneralPurpose {
    base64::engine::general_purpose::STANDARD
}

pub fn tlv_to_json(elem: &TLVElement) -> Result<Value, Error> {
    Ok(match elem.value()? {
        TLVValue::S8(v) => Value::from(v), TLVValue::S16(v) => Value::from(v),
        TLVValue::S32(v) => Value::from(v), TLVValue::S64(v) => Value::from(v),
        TLVValue::U8(v) => Value::from(v), TLVValue::U16(v) => Value::from(v),
        TLVValue::U32(v) => Value::from(v), TLVValue::U64(v) => Value::from(v),
        TLVValue::False => Value::from(false), TLVValue::True => Value::from(true),
        TLVValue::F32(v) => Value::from(v), TLVValue::F64(v) => Value::from(v),
        TLVValue::Utf8l(s) | TLVValue::Utf16l(s) | TLVValue::Utf32l(s) | TLVValue::Utf64l(s) => Value::from(s),
        TLVValue::Str8l(b) | TLVValue::Str16l(b) | TLVValue::Str32l(b) | TLVValue::Str64l(b) =>
            Value::from(b64().encode(b)),
        TLVValue::Null => Value::Null,
        TLVValue::Struct => {
            let mut obj = Map::new();
            for child in elem.container()?.iter() {
                let child = child?;
                match child.tag()? {
                    TLVTag::Context(n) => { obj.insert(n.to_string(), tlv_to_json(&child)?); }
                    other => tracing::debug!("skipping non-context struct member tag {other:?}"),
                }
            }
            Value::Object(obj)
        }
        TLVValue::Array | TLVValue::List => {
            let mut arr = Vec::new();
            for child in elem.container()?.iter() {
                arr.push(tlv_to_json(&child?)?);
            }
            Value::Array(arr)
        }
        TLVValue::EndCnt => return Err(ErrorCode::InvalidData.into()),
    })
}
```

- `attr_value_to_json`: `tlv_to_json` then `apply_epoch(gen_attr_ty_or_empty, value)`.
- `apply_epoch`: for `"epoch_s"` add `MATTER_EPOCH_OFFSET_S` to a `u64` value; `"epoch_us"` add `MATTER_EPOCH_OFFSET_US`; anything else pass through.
- `tlv_to_json_named`: like the Struct arm, but map `TLVTag::Context(n)` through `fields.iter().find(|f| f.code == n)`; when found use `f.name` as the key and: octet types stay base64 (already), `epoch_*` adjusted, struct-typed fields recurse with `cluster.find_struct(f.ty)`'s fields (fall back to tag-based when the struct is unknown); when not found keep the numeric key.
- `write_json` / `write_json_named` per the rules table above. Signed/unsigned width pick: `if v <= u8::MAX as u64 { w.u8(...) } else if ... u16 ... u32 ... u64`. `is_list` hints wrap the JSON array with `start_array/end_container` and write each element with the scalar part of the hint. `write_json_named` errors with `ErrorCode::InvalidData` on unknown keys (the ops layer maps it to `InvalidArguments` with a `Command field "<key>" unknown`-style message — Task 15).
- Struct writes emit fields in ascending context-tag order (`BTreeMap<u8, &Value>` intermediate).

- [ ] **Step 5: Run tests until they pass**

Run: `cargo test -p matter-rs-stack`
Expected: PASS. Exact `TLVValue` variant names/shape may differ slightly at this rev — `rs-matter-ref/rs-matter/src/tlv.rs:761` is the source of truth; adjust mechanically.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml Cargo.lock crates/stack
git commit -m "feat(stack): crate scaffold + TLV<->JSON codec (tag-based and name-based)"
```

---

### Task 13: `stack` — fabric identity bootstrap (RCAC-direct)

**Files:**
- Create: `crates/stack/src/identity.rs`
- Modify: `crates/stack/src/lib.rs` (add `pub mod identity;`)

**Interfaces:**
- Consumes: `matter_rs_controller::storage::{Storage, ServerIdentity}`, rs-matter cert/crypto/fabric APIs (exactly the ones the spike used — `spike/src/main.rs:229-303` is the reference implementation of the generate path).
- Produces:

```rust
/// Load-or-generate the controller identity and install it as a fabric on
/// the Matter instance. Returns the identity (persisted to server.json) and
/// the local fabric index.
pub fn ensure_identity<C: rs_matter::crypto::Crypto>(
    matter: &rs_matter::Matter<'_>,
    crypto: &C,
    storage: &matter_rs_controller::storage::Storage,
    fabric_id: u64,
    vendor_id: u16,
    fabric_label: &str,
) -> Result<(matter_rs_controller::storage::ServerIdentity, core::num::NonZeroU8), rs_matter::error::Error>;

pub const CONTROLLER_NODE_ID: u64 = 112233;
```

**Behavior:**
1. `storage.load_identity()`:
   - `Some(id)`: if `id.fabric_id != fabric_id` or `id.vendor_id != vendor_id`, log a warn that the STORED identity wins (an existing fabric must never be regenerated because a CLI flag changed). Rebuild the canon key types from the stored bytes and `matter.with_state(|s| s.fabrics.add(crypto, controller_key.reference(), &id.rcac_tlv, &id.controller_noc_tlv, &[], Some(ipk.reference()), id.vendor_id, id.controller_node_id))`.
   - `None`: generate exactly like the spike but RCAC-direct only:
     `RcacGenerator::new(&mut rcac_buf).generate(crypto, fabric_id, VALID_FOREVER)` → `(rcac_priv, rcac)`; controller secret key + CSR; `NocGenerator::create(rcac_priv.reference(), rcac, &[], &mut noc_buf)` → `generate(crypto, csr, CONTROLLER_NODE_ID, &[], VALID_FOREVER)` → controller NOC; random 16-byte IPK; `fabrics.add(...)` with `icac = &[]`.
     Persist every byte-blob canon form to `ServerIdentity` (`CanonPkcSecretKey::write_canon` for the keys; the rcac/noc are already TLV byte slices) plus `compressed_fabric_id` read from the added `Fabric`.
2. In both paths: `s.fabrics.update_label(fab_idx, fabric_label)` (ignore error with a warn), and return `(identity, fab_idx)`.
3. Canon-type reconstruction helper (needed here and by ops/commission):
```rust
pub(crate) fn canon_secret_key(bytes: &[u8]) -> Result<rs_matter::crypto::CanonPkcSecretKey, Error> {
    let mut k = rs_matter::crypto::CanonPkcSecretKey::new();
    if bytes.len() != k.access().len() { return Err(ErrorCode::InvalidData.into()); }
    k.access_mut().copy_from_slice(bytes);
    Ok(k)
}
```
(same pattern for `CanonAeadKey` / the IPK; the spike's `ipk.access_mut()` shows the accessor shape — if `access()/access_mut()` differ at this rev, mirror whatever `spike/src/main.rs:277-279` compiles against.)

- [ ] **Step 1: Write the failing test** (bottom of `identity.rs`; no network needed — `Matter::init` + `with_state` work without running transport)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use matter_rs_controller::storage::Storage;
    use rs_matter::crypto::default_crypto;
    use rs_matter::dm::devices::test::{DAC_PRIVKEY, TEST_DEV_ATT, TEST_DEV_COMM, TEST_DEV_DET};
    use rs_matter::utils::init::InitMaybeUninit;
    use rs_matter::Matter;
    use static_cell::StaticCell;

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
        assert!(storage.load_identity().is_some());

        // Fresh Matter instance (new "process"): identity must LOAD, not regenerate.
        let m2 = M2.uninit().init_with(Matter::init(&TEST_DEV_DET, TEST_DEV_COMM, &TEST_DEV_ATT, 0));
        let (id2, idx2) = ensure_identity(m2, &crypto, &storage, 1, 0xFFF1, "HomeAssistant").unwrap();
        assert_eq!(id1.rcac_tlv, id2.rcac_tlv);
        assert_eq!(id1.compressed_fabric_id, id2.compressed_fabric_id);
        assert_eq!(idx1, idx2);
    }
}
```

Add `[dev-dependencies] tempfile = "3"` to the stack crate.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p matter-rs-stack identity`
Expected: FAIL (module missing).

- [ ] **Step 3: Implement per the behavior block.** The generate path is a line-for-line adaptation of `spike/src/main.rs:236-295` with `no_icac` hardwired true and the ephemeral values captured into `ServerIdentity` instead of dropped. Buffer sizes: `MAX_CERT_TLV_AND_ASN1_LEN` for rcac/noc buffers, `[0u8; 256]` for the CSR.

- [ ] **Step 4: Run test until it passes**

Run: `cargo test -p matter-rs-stack`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/stack
git commit -m "feat(stack): RCAC-direct fabric identity bootstrap with persistence"
```

---

### Task 14: `stack` — shared context, generic IM operations, report sink

**Files:**
- Create: `crates/stack/src/ctx.rs`, `crates/stack/src/ops/mod.rs`, `crates/stack/src/ops/interact.rs`, `crates/stack/src/reports.rs`
- Modify: `crates/stack/src/lib.rs` (add `pub(crate) mod ctx; pub(crate) mod ops; pub(crate) mod reports;`)

**Interfaces:**
- Consumes: `ImClient` trait methods (`rs-matter-ref/rs-matter/src/im/client.rs` — canonical loops quoted below), `tlv_json`, `matter_rs_gen`, `stack_api` types.
- Produces:

```rust
// ctx.rs — shared, single-threaded state for everything on the stack thread.
pub(crate) struct StackCtx<C: rs_matter::crypto::Crypto> {
    pub matter: &'static rs_matter::Matter<'static>,
    pub crypto: C,
    pub fab_idx: core::num::NonZeroU8,
    pub identity: matter_rs_controller::storage::ServerIdentity,
    pub events: tokio::sync::mpsc::UnboundedSender<StackEvent>,
    /// subscription_id -> node_id (report routing)
    pub subs: core::cell::RefCell<std::collections::HashMap<u32, u64>>,
    /// node_id -> last report instant (liveness)
    pub liveness: core::cell::RefCell<std::collections::HashMap<u64, embassy_time::Instant>>,
    /// node_id -> last seen event_number (dedupe across resubscribes)
    pub last_event: core::cell::RefCell<std::collections::HashMap<u64, u64>>,
    /// node_id -> last known addresses ("ip" strings, most recent first)
    pub addrs: core::cell::RefCell<std::collections::HashMap<u64, Vec<String>>>,
    /// node_id -> supervisor task (dropping cancels)
    pub supervisors: core::cell::RefCell<std::collections::HashMap<u64, async_executor::Task<()>>>,
}
pub(crate) fn map_err(e: rs_matter::error::Error) -> StackError;   // ErrorCode-driven kind mapping
pub(crate) async fn with_timeout<T>(secs: u64, fut: impl Future<Output = Result<T, rs_matter::error::Error>>) -> Result<T, StackError>;

// ops/interact.rs
pub(crate) async fn read_attributes<C: Crypto>(ctx: &StackCtx<C>, node_id: u64,
    paths: &[AttributePathSpec], fabric_filtered: bool) -> Result<Vec<(String, Value)>, StackError>;
pub(crate) async fn write_attribute<C: Crypto>(ctx: &StackCtx<C>, node_id: u64,
    endpoint: u16, cluster: u32, attribute: u32, value: &Value) -> Result<u8, StackError>;
pub(crate) async fn invoke<C: Crypto>(ctx: &StackCtx<C>, node_id: u64, endpoint: u16, cluster: u32,
    command_name: &str, payload: &Value, timed_ms: Option<u16>) -> Result<Value, StackError>;
pub(crate) async fn interview<C: Crypto>(ctx: &StackCtx<C>, node_id: u64)
    -> Result<std::collections::BTreeMap<String, Value>, StackError>;

// reports.rs
pub(crate) struct ReportSink<C: Crypto>(pub Rc<StackCtx<C>>);      // ReportDataHandler impl
```

**Error mapping (`ctx.rs`):** rs-matter `ErrorCode::NotFound` → `NodeUnreachable` ("could not resolve node via mDNS"); `RxTimeout`/`TxTimeout` → `Timeout`; `Busy` → `Busy` with message `device is busy (a previous commissioning attempt may still hold its failsafe for ~60s)`; `InvalidData` from payload encoding → `InvalidArguments`; everything else `Sdk` with `format!("{e:?}")`. `with_timeout` = `embassy_futures::select` of the future and `embassy_time::Timer` (the spike's `with_timeout`, `spike/src/main.rs:404-416`), timeout → `StackError { Timeout, "IM operation timed out after <secs>s" }`. IM timeout 30s, commissioning 60s.

- [ ] **Step 1: Implement `ctx.rs`** with the struct, `map_err`, `with_timeout` and a unit test for `map_err`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use matter_rs_controller::stack_api::StackErrorKind;
    use rs_matter::error::ErrorCode;

    #[test]
    fn error_kinds() {
        assert_eq!(map_err(ErrorCode::NotFound.into()).kind, StackErrorKind::NodeUnreachable);
        assert_eq!(map_err(ErrorCode::RxTimeout.into()).kind, StackErrorKind::Timeout);
        assert_eq!(map_err(ErrorCode::Busy.into()).kind, StackErrorKind::Busy);
        assert_eq!(map_err(ErrorCode::Invalid.into()).kind, StackErrorKind::Sdk);
    }
}
```

- [ ] **Step 2: Implement `ops/interact.rs`.** These cannot be unit-tested without a device (verified e2e in Task 19); the deliverable here is compiling, review-clean code following the canonical loops. Full implementation:

```rust
//! Generic IM operations. Each function initiates a CASE exchange (cheap on
//! a warm session; internally mDNS-resolves on a cold one) and drives the
//! rs-matter sender state machines. Responses are converted to owned JSON
//! inside the exchange borrow (the RX buffer cannot escape).

use std::collections::BTreeMap;

use matter_rs_controller::stack_api::{AttributePathSpec, StackError, StackErrorKind};
use rs_matter::crypto::Crypto;
use rs_matter::im::client::{ImClient, SubscribeOutcome, TxOutcome};
use rs_matter::im::encoding::attr::{AttrDataTag, AttrPath, AttrResp};
use rs_matter::im::encoding::invoke::{CmdDataTag, CmdResp};
use rs_matter::im::encoding::GenericPath;
use rs_matter::tlv::{TLVTag, TLVWrite};
use rs_matter::transport::exchange::Exchange;
use serde_json::Value;

use crate::ctx::{map_err, with_timeout, StackCtx};
use crate::tlv_json;

const IM_TIMEOUT_SECS: u64 = 30;

fn to_attr_path(p: &AttributePathSpec) -> AttrPath {
    AttrPath::from_gp(&GenericPath::new(
        p.endpoint.map(Into::into),
        p.cluster.map(Into::into),
        p.attribute,
    ))
}

pub(crate) async fn read_attributes<C: Crypto>(
    ctx: &StackCtx<C>, node_id: u64, paths: &[AttributePathSpec], fabric_filtered: bool,
) -> Result<Vec<(String, Value)>, StackError> {
    with_timeout(IM_TIMEOUT_SECS, async {
        let exchange = Exchange::initiate(ctx.matter, &ctx.crypto, ctx.fab_idx, node_id).await?;
        let attr_paths: Vec<AttrPath> = paths.iter().map(to_attr_path).collect();
        let mut sender = exchange.read_sender().await?;
        let mut chunk = loop {
            match sender.tx().await? {
                TxOutcome::BuildRequest(builder) => {
                    sender = builder
                        .attr_requests_from(&attr_paths)?
                        .fabric_filtered(fabric_filtered)?
                        .end()?;
                }
                TxOutcome::GotResponse(c) => break c,
            }
        };
        let mut out = Vec::new();
        loop {
            {
                let resp = chunk.response()?;
                if let Some(reports) = &resp.attr_reports {
                    for report in reports.iter() {
                        match report? {
                            AttrResp::Data(data) => {
                                let gp = data.path.to_gp();
                                let (Some(e), Some(cl), Some(a)) = (gp.endpoint, gp.cluster, gp.leaf) else { continue };
                                let json = tlv_json::attr_value_to_json(cl, a, &data.data)?;
                                out.push((format!("{e}/{cl}/{a}"), json));
                            }
                            AttrResp::Status(s) => {
                                tracing::debug!("read: path status {:?}", s.status);
                            }
                        }
                    }
                }
            }
            match chunk.complete().await? {
                Some(next) => chunk = next,
                None => break,
            }
        }
        Ok(out)
    }).await
}

pub(crate) async fn interview<C: Crypto>(
    ctx: &StackCtx<C>, node_id: u64,
) -> Result<BTreeMap<String, Value>, StackError> {
    // Full wildcard, fabric-filtered (Node interview behavior). Bigger budget:
    // a full read of a bridge can take a while.
    let all = [AttributePathSpec { endpoint: None, cluster: None, attribute: None }];
    read_attributes_inner(ctx, node_id, &all, true, 120).await
        .map(|v| v.into_iter().collect())
}
```

Structure note: implement the body shown under `read_attributes` as
`read_attributes_inner(ctx, node_id, paths, fabric_filtered, timeout_secs)`;
`read_attributes` calls it with `IM_TIMEOUT_SECS`, `interview` with `120`.

```rust
pub(crate) async fn write_attribute<C: Crypto>(
    ctx: &StackCtx<C>, node_id: u64, endpoint: u16, cluster: u32, attribute: u32, value: &Value,
) -> Result<u8, StackError> {
    let hint = matter_rs_gen::cluster(cluster).and_then(|cl| cl.attr(attribute).map(|a|
        crate::tlv_json::TypeHint { ty: a.ty, is_list: a.is_list, cluster: matter_rs_gen::cluster(cluster) }));
    with_timeout(IM_TIMEOUT_SECS, async {
        let exchange = Exchange::initiate(ctx.matter, &ctx.crypto, ctx.fab_idx, node_id).await?;
        let mut sender = exchange.write_sender(None).await?;
        let handle = loop {
            match sender.tx().await? {
                TxOutcome::BuildRequest(builder) => {
                    let entry = builder.write_requests()?
                        .push()?
                        .path(endpoint.into(), cluster, attribute)?
                        .data(|w| crate::tlv_json::write_json(
                            w, &TLVTag::Context(AttrDataTag::Data as u8), value, hint.clone()))?
                        .end()?;
                    sender = entry.end()?.end()?;
                }
                TxOutcome::GotResponse(h) => break h,
            }
        };
        let resp = handle.response()?;
        let mut status = 0u8;
        for s in resp.write_responses.iter() {
            let s = s?;
            status = s.status.status as u8; // numeric IM status of the (single) write
        }
        Ok(status)
    }).await
}

pub(crate) async fn invoke<C: Crypto>(
    ctx: &StackCtx<C>, node_id: u64, endpoint: u16, cluster: u32,
    command_name: &str, payload: &Value, timed_ms: Option<u16>,
) -> Result<Value, StackError> {
    let meta = matter_rs_gen::cluster(cluster).ok_or_else(|| StackError::new(
        StackErrorKind::InvalidArguments, format!("Cluster Id \"{cluster}\" unknown")))?;
    let cmd = meta.find_command_ci(command_name).ok_or_else(|| StackError::new(
        StackErrorKind::InvalidArguments,
        format!("Command \"{command_name}\" does not exist on cluster \"{}\"", meta.name)))?;
    // Devices reject spec-timed commands sent untimed; default a timed budget in.
    let timed_ms = timed_ms.or(if cmd.is_timed { Some(10_000) } else { None });
    let input_fields = cmd.input.and_then(|s| meta.find_struct(s)).map(|s| s.fields);
    let payload_obj = payload.as_object().cloned().unwrap_or_default();

    with_timeout(IM_TIMEOUT_SECS, async {
        let exchange = Exchange::initiate(ctx.matter, &ctx.crypto, ctx.fab_idx, node_id).await?;
        let mut sender = exchange.invoke_sender(timed_ms).await?;
        let mut chunk = loop {
            match sender.tx().await? {
                TxOutcome::BuildRequest(builder) => {
                    let entry = builder
                        .suppress_response(false)?
                        .timed_request(timed_ms.is_some())?
                        .invoke_requests()?
                        .push()?
                        .path(endpoint.into(), cluster, cmd.code)?
                        .data(|w| {
                            let tag = TLVTag::Context(CmdDataTag::Data as u8);
                            match input_fields {
                                Some(fields) => crate::tlv_json::write_json_named(w, &tag, &payload_obj, fields, meta),
                                None => { w.start_struct(&tag)?; w.end_container() } // empty args struct
                            }
                        })?
                        .end()?;
                    sender = entry.end()?.end()?;
                }
                TxOutcome::GotResponse(c) => break c,
            }
        };
        let mut result = Value::Null;
        loop {
            if chunk.is_status_only() {
                // DefaultSuccess as bare StatusResponse
            } else {
                let resp = chunk.response()?;
                if let Some(resp) = resp {
                    if let Some(invoke_responses) = &resp.invoke_responses {
                        for r in invoke_responses.iter() {
                            match r? {
                                CmdResp::Cmd(data) => {
                                    result = match cmd.output.and_then(|o| meta.find_struct(o)) {
                                        Some(out) => crate::tlv_json::tlv_to_json_named(&data.data, out.fields, meta)?,
                                        None => crate::tlv_json::tlv_to_json(&data.data)?,
                                    };
                                }
                                CmdResp::Status(s) => {
                                    if s.status.status as u8 != 0 {
                                        return Err(rs_matter::error::ErrorCode::Invalid.into());
                                    }
                                }
                            }
                        }
                    }
                }
            }
            match chunk.complete().await? {
                Some(next) => chunk = next,
                None => break,
            }
        }
        Ok(result)
    }).await
    .map_err(|e| {
        // encoding errors from write_json_named surface as InvalidData
        if e.kind == StackErrorKind::Sdk && e.message.contains("InvalidData") {
            StackError::new(StackErrorKind::InvalidArguments,
                format!("Invalid payload for command \"{command_name}\": {}", e.message))
        } else { e }
    })
}
```

Exact builder/response idioms (`.status.status`, `CmdStatus` field names, `data.path.to_gp()`) may need mechanical adjustment against `rs-matter-ref/rs-matter/tests/im/client_reads.rs`, `client_writes.rs:44-67`, `client_invokes.rs:42-64` — those three test files are the ground truth for this rev.

- [ ] **Step 3: Implement `reports.rs`** — the `ReportDataHandler` that routes post-priming subscription reports:

```rust
//! Routes unsolicited ReportData (subscription reports) into StackEvents.
//! Registered via InteractionModel::new_with_reports (Task 16). Reports for
//! unknown subscription ids answer InvalidSubscription, which makes devices
//! drop stale subscriptions from before a restart — intended.

use std::rc::Rc;

use matter_rs_controller::stack_api::{NodeEventData, StackEvent};
use rs_matter::crypto::Crypto;
use rs_matter::dm::types::handler::{ReportContext, ReportDataHandler};
use rs_matter::im::encoding::attr::{AttrResp, ReportDataResp};
use rs_matter::im::encoding::event::EventResp;
use rs_matter::im::encoding::status::IMStatusCode; // adjust path to the actual IMStatusCode location
use serde_json::Value;

use crate::ctx::StackCtx;
use crate::tlv_json;

pub(crate) struct ReportSink<C: Crypto>(pub Rc<StackCtx<C>>);

impl<C: Crypto> ReportDataHandler for ReportSink<C> {
    async fn handle_report(&self, rctx: impl ReportContext, report: &ReportDataResp<'_>)
        -> Result<(), IMStatusCode> {
        let ctx = &self.0;
        let Some(sub_id) = rctx.subscription().subscription_id else {
            return Err(IMStatusCode::InvalidSubscription);
        };
        let Some(node_id) = ctx.subs.borrow().get(&sub_id).copied() else {
            tracing::debug!("report for unknown subscription {sub_id}");
            return Err(IMStatusCode::InvalidSubscription);
        };
        ctx.liveness.borrow_mut().insert(node_id, embassy_time::Instant::now());

        let mut changes: Vec<(String, Value)> = Vec::new();
        if let Some(reports) = &report.attr_reports {
            for r in reports.iter() {
                let Ok(r) = r else { continue };
                if let AttrResp::Data(data) = r {
                    let gp = data.path.to_gp();
                    if let (Some(e), Some(cl), Some(a)) = (gp.endpoint, gp.cluster, gp.leaf) {
                        if let Ok(json) = tlv_json::attr_value_to_json(cl, a, &data.data) {
                            changes.push((format!("{e}/{cl}/{a}"), json));
                        }
                    }
                }
            }
        }
        if !changes.is_empty() {
            let _ = ctx.events.send(StackEvent::AttributesChanged { node_id, changes });
        }

        if let Some(events) = &report.event_reports {
            for r in events.iter() {
                let Ok(EventResp::Data(data)) = r else { continue };
                let event_number = data.event_number.into();
                {
                    let mut last = ctx.last_event.borrow_mut();
                    let seen = last.entry(node_id).or_insert(0);
                    if event_number <= *seen && *seen != 0 { continue; }
                    *seen = event_number;
                }
                let (timestamp, timestamp_type) = convert_timestamp(&data.timestamp);
                let json = event_json(ctx, &data);
                let _ = ctx.events.send(StackEvent::NodeEvent { node_id, event: NodeEventData {
                    endpoint_id: data.path.endpoint.unwrap_or(0).into(),
                    cluster_id: data.path.cluster.unwrap_or(0),
                    event_id: data.path.event.unwrap_or(0),
                    event_number,
                    priority: data.priority as u8,
                    timestamp, timestamp_type,
                    data: json,
                }});
            }
        }
        Ok(())
    }
}
```

with two helpers: `convert_timestamp(&EventDataTimestamp) -> (i64, u8)` — epoch-us variant → unix millis (`us / 1000 + MATTER_EPOCH_OFFSET_US / 1000`) with type 1; system variant → millis as-is with type 0; delta/other → `(unix now millis, 2)` (check the actual `EventDataTimestamp` variants at `rs-matter-ref/rs-matter/src/im/encoding/event.rs`); and `event_json` — look up `matter_rs_gen::cluster(cluster_id).and_then(|c| c.event(event_id))` → `tlv_to_json_named(&data.data, event.fields, cluster)`, fallback `tlv_to_json`, empty/absent payload → `Value::Null`.

- [ ] **Step 4: Build + test**

Run: `cargo test -p matter-rs-stack`
Expected: compiles clean (this is the gate — the IM plumbing has no host-only test harness), `ctx` and `tlv_json` tests PASS. `cargo clippy -p matter-rs-stack` clean.

- [ ] **Step 5: Commit**

```bash
git add crates/stack
git commit -m "feat(stack): generic IM read/write/invoke/interview and subscription report sink"
```

---

### Task 15: `stack` — node supervisor + commissioning/window/fabrics/discovery ops

**Files:**
- Create: `crates/stack/src/supervisor.rs`, `crates/stack/src/ops/commission.rs`, `crates/stack/src/ops/window.rs`, `crates/stack/src/ops/fabrics.rs`, `crates/stack/src/ops/discovery.rs`
- Modify: `crates/stack/src/lib.rs`, `crates/stack/src/ops/mod.rs`

**Interfaces:**
- Consumes: Task 14's `StackCtx` + `ops::interact`, spike commissioning flow (`spike/src/main.rs:229-350` — line-for-line reference), rs-matter `Commissioner`/`NocGenerator`/`QrPayload`/`BasicCommData`/SPAKE2+ crypto.
- Produces:

```rust
// supervisor.rs
pub(crate) async fn supervise<C: Crypto>(ctx: Rc<StackCtx<C>>, node_id: u64);  // runs until cancelled

// ops/commission.rs
pub(crate) async fn commission<C: Crypto>(ctx: &StackCtx<C>, req: CommissionRequest)
    -> Result<CommissionOutcome, StackError>;

// ops/window.rs
pub(crate) async fn open_window<C: Crypto>(ctx: &StackCtx<C>, node_id: u64, timeout_secs: u16)
    -> Result<WindowInfo, StackError>;

// ops/fabrics.rs
pub(crate) async fn device_fabrics<C: Crypto>(ctx: &StackCtx<C>, node_id: u64)
    -> Result<Vec<DeviceFabric>, StackError>;
pub(crate) async fn remove_device_fabric<C: Crypto>(ctx: &StackCtx<C>, node_id: u64, fabric_index: u8)
    -> Result<(), StackError>;
pub(crate) async fn update_fabric_label<C: Crypto>(ctx: &StackCtx<C>, label: &str)
    -> Result<(), StackError>;

// ops/discovery.rs
pub(crate) async fn browse<C: Crypto>(ctx: &StackCtx<C>, timeout_ms: u32)
    -> Result<Vec<DiscoveredDevice>, StackError>;
```

**`supervisor.rs` — the availability engine.** Full logic:

```rust
//! One task per commissioned node: establish a single wildcard subscription
//! (attributes + events, like matter.js), feed the priming report to the
//! controller, then watch liveness and resubscribe with backoff. The
//! ReportSink (Task 14) handles the post-priming reports; this task owns
//! establishment, liveness timeout, and the Connected/Reconnecting signals.

use std::rc::Rc;

use embassy_time::{Duration, Instant, Timer};
use matter_rs_controller::stack_api::{NodeConnState, StackEvent};
use rs_matter::crypto::Crypto;
use rs_matter::im::client::{ImClient, SubscribeOutcome, TxOutcome};
use rs_matter::im::encoding::attr::{AttrPath, AttrResp};
use rs_matter::im.encoding::event::EventPath; // fix path syntax when writing
use rs_matter::im::encoding::GenericPath;
use rs_matter::transport::exchange::Exchange;

use crate::ctx::StackCtx;
use crate::tlv_json;

const MIN_INTERVAL_FLOOR_SECS: u16 = 0;
const MAX_INTERVAL_CEIL_SECS: u16 = 60;
const LIVENESS_SLACK_SECS: u64 = 15;
const BACKOFF_SCHEDULE_SECS: [u64; 5] = [2, 5, 10, 30, 60];

pub(crate) async fn supervise<C: Crypto>(ctx: Rc<StackCtx<C>>, node_id: u64) {
    let mut backoff_idx = 0usize;
    loop {
        match establish(&ctx, node_id).await {
            Ok((sub_id, max_int)) => {
                backoff_idx = 0;
                let _ = ctx.events.send(StackEvent::NodeState {
                    node_id, state: NodeConnState::Connected { max_interval_secs: max_int } });
                // Liveness watch: device must report at least every max_int.
                let deadline = Duration::from_secs(max_int as u64 + LIVENESS_SLACK_SECS);
                loop {
                    Timer::after(Duration::from_secs(5)).await;
                    let last = ctx.liveness.borrow().get(&node_id).copied();
                    match last {
                        Some(t) if Instant::now().duration_since(t) < deadline => continue,
                        _ => break,
                    }
                }
                ctx.subs.borrow_mut().remove(&sub_id);
                tracing::warn!("node {node_id}: subscription went silent, resubscribing");
                let _ = ctx.events.send(StackEvent::NodeState { node_id, state: NodeConnState::Reconnecting });
            }
            Err(e) => {
                tracing::debug!("node {node_id}: subscribe attempt failed: {e:?}");
                let _ = ctx.events.send(StackEvent::NodeState { node_id, state: NodeConnState::Reconnecting });
                let delay = BACKOFF_SCHEDULE_SECS[backoff_idx.min(BACKOFF_SCHEDULE_SECS.len() - 1)];
                backoff_idx += 1;
                Timer::after(Duration::from_secs(delay)).await;
            }
        }
    }
}

/// CASE + wildcard subscribe; sends PrimingSnapshot; returns (sub_id, max_int).
async fn establish<C: Crypto>(ctx: &Rc<StackCtx<C>>, node_id: u64)
    -> Result<(u32, u16), rs_matter::error::Error> {
    let exchange = Exchange::initiate(ctx.matter, &ctx.crypto, ctx.fab_idx, node_id).await?;
    let attr_paths = [AttrPath::from_gp(&GenericPath::new(None, None, None))];
    let event_paths = [EventPath { node: None, endpoint: None, cluster: None, event: None, is_urgent: None }];

    let mut sender = exchange.subscribe_sender().await?;
    let mut chunk = loop {
        match sender.tx().await? {
            TxOutcome::BuildRequest(builder) => {
                sender = builder
                    .keep_subs(false)?                     // clean slate on (re)connect
                    .min_int_floor(MIN_INTERVAL_FLOOR_SECS)?
                    .max_int_ceil(MAX_INTERVAL_CEIL_SECS)?
                    .attr_requests_from(&attr_paths)?
                    .event_requests_from(&event_paths)?
                    .fabric_filtered(false)?
                    .end()?;
            }
            TxOutcome::GotResponse(c) => break c,
        }
    };

    let mut snapshot = std::collections::BTreeMap::new();
    let established = loop {
        {
            let resp = chunk.response()?;
            if let Some(reports) = &resp.attr_reports {
                for r in reports.iter() {
                    if let AttrResp::Data(data) = r? {
                        let gp = data.path.to_gp();
                        if let (Some(e), Some(cl), Some(a)) = (gp.endpoint, gp.cluster, gp.leaf) {
                            snapshot.insert(format!("{e}/{cl}/{a}"),
                                            tlv_json::attr_value_to_json(cl, a, &data.data)?);
                        }
                    }
                }
            }
            // priming event reports: feed the dedupe watermark only
            if let Some(events) = &resp.event_reports { /* update ctx.last_event like reports.rs */ }
        }
        match chunk.complete().await? {
            SubscribeOutcome::NextChunk(next) => chunk = next,
            SubscribeOutcome::Established(est) => break est,
        }
    };

    ctx.subs.borrow_mut().insert(established.subscription_id, node_id);
    ctx.liveness.borrow_mut().insert(node_id, Instant::now());
    let _ = ctx.events.send(StackEvent::PrimingSnapshot { node_id, attributes: snapshot });
    Ok((established.subscription_id, established.max_int))
}
```

**`ops/commission.rs`** — the spike flow parameterized (RCAC-direct hardwired):

```rust
use core::num::NonZeroU8;
use core::pin::pin;

use embassy_futures::select::{select, Either};
use embassy_time::{Duration, Timer};
use matter_rs_controller::stack_api::{CommissionOutcome, CommissionRequest, PaseTarget, StackError, StackErrorKind};
use rs_matter::cert::gen::VALID_FOREVER;
use rs_matter::cert::{MAX_CERT_TLV_AND_ASN1_LEN, MAX_CERT_TLV_LEN};
use rs_matter::crypto::Crypto;
use rs_matter::onboard::noc::NocGenerator;
use rs_matter::onboard::{CommissionOptions, Commissioner};
use rs_matter::pairing::qr::QrPayload;
use rs_matter::transport::network::mdns::CommissionableFilter;
use rs_matter::transport::network::Address;

use crate::ctx::{map_err, StackCtx};

const COMMISSION_TIMEOUT_SECS: u64 = 60;
const BROWSE_TIMEOUT_MS: u32 = 30_000;

pub(crate) async fn commission<C: Crypto>(
    ctx: &StackCtx<C>, req: CommissionRequest,
) -> Result<CommissionOutcome, StackError> {
    // 1. Resolve passcode + peer address.
    let (passcode, addr) = match &req.target {
        PaseTarget::Code { code } => {
            let mut qr_buf = [0u8; 512];
            let code = code.trim();
            let (passcode, filter) = if code.starts_with("MT:") {
                let qr = QrPayload::parse(code, &mut qr_buf).map_err(map_err)?;
                (qr.passcode(), qr.commissionable_filter())
            } else {
                let manual = QrPayload::parse_pairing_code(code).map_err(map_err)?;
                (manual.passcode(), manual.commissionable_filter())
            };
            let (addr, instance) = ctx.matter.transport()
                .browse_commissionable(&filter, &[], BROWSE_TIMEOUT_MS).await.map_err(map_err)?;
            tracing::info!("discovered commissionable {instance:016x} at {addr}");
            (passcode, addr)
        }
        PaseTarget::OnNetwork { passcode, long_discriminator, short_discriminator, vendor_id } => {
            let filter = CommissionableFilter {
                discriminator: *long_discriminator,
                short_discriminator: *short_discriminator,
                vendor_id: *vendor_id,
                product_id: None, device_type: None,
                commissioning_mode_only: long_discriminator.is_none()
                    && short_discriminator.is_none() && vendor_id.is_none(),
            };
            let (addr, _) = ctx.matter.transport()
                .browse_commissionable(&filter, &[], BROWSE_TIMEOUT_MS).await.map_err(map_err)?;
            (*passcode, addr)
        }
        PaseTarget::Address { passcode, addr } => {
            let sa: std::net::SocketAddr = addr.parse().map_err(|e| StackError::new(
                StackErrorKind::InvalidArguments, format!("invalid ip_addr: {e}")))?;
            (*passcode, Address::Udp(sa))
        }
    };

    // 2. NocGenerator from the persisted CA (RCAC-direct: signer = RCAC key).
    let ca_key = crate::identity::canon_secret_key(&ctx.identity.ca_private_key).map_err(map_err)?;
    let mut noc_buf = [0u8; MAX_CERT_TLV_AND_ASN1_LEN];
    let mut noc_generator = NocGenerator::create(
        ca_key.reference(), &ctx.identity.rcac_tlv, &[], &mut noc_buf).map_err(map_err)?;
    let mut commissioner_buf = [0u8; MAX_CERT_TLV_LEN];
    let mut commissioner = Commissioner::new(
        ctx.matter, &ctx.crypto, ctx.fab_idx, &mut noc_generator, &mut commissioner_buf);
    let opts = CommissionOptions { allow_test_attestation: true, ..CommissionOptions::new() };

    // 3. Phase 1 (PASE + over-PASE config), with timeout. Busy here usually
    //    means a previous attempt's failsafe is still held (spike finding 2).
    let phase1 = {
        let mut fut = pin!(commissioner.commission(addr, passcode, &opts, req.node_id, VALID_FOREVER));
        let mut timeout = pin!(Timer::after(Duration::from_secs(COMMISSION_TIMEOUT_SECS)));
        match select(&mut fut, &mut timeout).await {
            Either::First(r) => r.map_err(map_err)?,
            Either::Second(_) => return Err(StackError::new(StackErrorKind::Timeout,
                format!("commissioning timed out after {COMMISSION_TIMEOUT_SECS}s (a previous failed attempt may hold the device's PASE session for ~60s)"))),
        }
    };

    // 4. Phase 2 (CASE + CommissioningComplete), with timeout.
    {
        let mut fut = pin!(commissioner.complete_via_case(addr, &phase1));
        let mut timeout = pin!(Timer::after(Duration::from_secs(COMMISSION_TIMEOUT_SECS)));
        match select(&mut fut, &mut timeout).await {
            Either::First(r) => r.map_err(map_err)?,
            Either::Second(_) => return Err(StackError::new(StackErrorKind::Timeout,
                "CASE completion timed out".into())),
        }
    }

    // 5. Best-effort fabric label on the fresh node.
    if let Err(e) = crate::ops::interact::invoke(ctx, req.node_id, 0, 62, "updateFabricLabel",
        &serde_json::json!({"label": req.fabric_label}), None).await {
        tracing::warn!("UpdateFabricLabel on node {} failed: {}", req.node_id, e.message);
    }

    let address = match addr { Address::Udp(sa) => sa.to_string(), other => format!("{other}") };
    ctx.addrs.borrow_mut().insert(req.node_id, vec![ip_of(&address)]);
    Ok(CommissionOutcome { device_fabric_index: phase1.fabric_index.get(), address })
}

fn ip_of(addr: &str) -> String {
    // "[fe80::1%2]:5540" -> "fe80::1%2"; "192.168.1.50:5540" -> "192.168.1.50"
    if let Some(rest) = addr.strip_prefix('[') {
        rest.split(']').next().unwrap_or(rest).to_string()
    } else {
        addr.rsplit_once(':').map(|(ip, _)| ip.to_string()).unwrap_or_else(|| addr.to_string())
    }
}
```

**`ops/window.rs`** — enhanced commissioning window with a self-computed PAKE verifier:

```rust
//! open_commissioning_window: generate passcode/discriminator/salt, compute
//! the SPAKE2+ verifier (w0 || L, 97 bytes) exactly like the password branch
//! of Spake2P::setup_verifier (rs-matter/src/sc/pase/spake2p.rs:288-307),
//! send the (timed) OpenCommissioningWindow command, and hand back the
//! matching manual pairing code + QR string.

use matter_rs_controller::stack_api::{StackError, StackErrorKind, WindowInfo};
use rs_matter::crypto::{Crypto, RngCore as _};

const PAKE_ITERATIONS: u32 = 2_000; // rs-matter SPAKE2P_ITERATION_COUNT
const INVALID_PASSCODES: &[u32] = &[
    0, 11111111, 22222222, 33333333, 44444444, 55555555, 66666666, 77777777,
    88888888, 99999999, 12345678, 87654321,
];

pub(crate) async fn open_window<C: Crypto>(
    ctx: &crate::ctx::StackCtx<C>, node_id: u64, timeout_secs: u16,
) -> Result<WindowInfo, StackError> {
    use crate::ctx::map_err;
    // Randomness via the crypto backend's RNG (same accessor the spike used
    // for the IPK: `crypto.rand()?.fill_bytes(...)`, spike/src/main.rs:279).
    let mut rng = ctx.crypto.rand().map_err(map_err)?;
    let passcode = loop {
        let mut b = [0u8; 4];
        rng.fill_bytes(&mut b);
        let p = u32::from_le_bytes(b) % 99_999_998 + 1; // 1..=99999998
        if !INVALID_PASSCODES.contains(&p) { break p; }
    };
    let mut d = [0u8; 2];
    rng.fill_bytes(&mut d);
    let discriminator: u16 = u16::from_le_bytes(d) % 4096;
    let mut salt = [0u8; 32];
    rng.fill_bytes(&mut salt);

    let verifier = compute_pase_verifier(&ctx.crypto, passcode, PAKE_ITERATIONS, &salt)?; // 97 bytes

    let payload = serde_json::json!({
        "commissioningTimeout": timeout_secs,
        "PAKEPasscodeVerifier": base64_std(&verifier),
        "discriminator": discriminator,
        "iterations": PAKE_ITERATIONS,
        "salt": base64_std(&salt),
    });
    crate::ops::interact::invoke(ctx, node_id, 0, 60, "openCommissioningWindow",
                                 &payload, Some(10_000)).await?;

    // Manual code + QR from rs-matter's own pairing helpers. BasicCommData
    // (rs-matter/src/lib.rs:126) holds the password as a canon LE-u32 blob:
    let mut password = rs_matter::crypto::Spake2pVerifierPassword::new();
    password.access_mut().copy_from_slice(&passcode.to_le_bytes());
    let comm_data = rs_matter::BasicCommData { password, discriminator };
    let setup_manual_code = comm_data.compute_pairing_code().to_string();
    let (vid, pid) = basic_info_vid_pid(ctx, node_id).await; // reads 0/40/2 and 0/40/4, defaults 0
    let setup_qr_code = build_qr(&comm_data, vid, pid)?;     // QrPayload::new + as_str -> "MT:..."
    Ok(WindowInfo { setup_pin_code: passcode, setup_manual_code, setup_qr_code })
}
```

`compute_pase_verifier` (the load-bearing crypto — write exactly this math): PBKDF2 the passcode (4-byte LE) with `(iterations, salt)` into `Spake2pW` (`crypto.pbkdf()?.derive(pw_ref, iterations as usize, salt, &mut w0w1s)`); split into `w0s`/`w1s` halves; `w0 = crypto.ec_scalar_mod_p(w0s)`; `w1 = crypto.ec_scalar_mod_p(w1s)`; `l_pt = crypto.ec_generator_point()?.mul(&w1)?`; output = `w0` canonical scalar bytes (32) ++ `l_pt` canonical point bytes (65). The field names/types are in `rs-matter-ref/rs-matter/src/sc/pase/spake2p.rs:288-307` and the canon lengths in the same file's header. The exact `BasicCommData`/`QrPayload::new` construction is at `rs-matter-ref/rs-matter/src/lib.rs:126-132` and `rs-matter/src/pairing/qr.rs:153-175` (`DiscoveryCapabilities` on-IP-network flag, `CommFlowType::Standard`, empty serial, `no_optional_data()` from `pairing/qr.rs:83`).

**`ops/fabrics.rs`:**
- `device_fabrics`: `interact::read_attributes(ctx, node_id, &[path 0/62/1], fabric_filtered=false)` → the value is a JSON array of tag-based `FabricDescriptorStruct`s — map tags `"2"`→vendor_id, `"3"`→fabric_id, `"4"`→(device's view of) node id (ignored), `"5"`→label, `"254"`→fabric_index. Missing/empty read → `StackError::new(Sdk, "No or invalid response received while querying fabrics")`.
- `remove_device_fabric`: `interact::invoke(ctx, node_id, 0, 62, "removeFabric", &json!({"fabricIndex": fabric_index}), None)` → check the NOCResponse JSON: `statusCode` must be 0, else `Err(Sdk, format!("RemoveFabric failed with status {status}"))`.
- `update_fabric_label`: `ctx.matter.with_state(|s| s.fabrics.update_label(ctx.fab_idx, label))` (map error), then for every currently-supervised node fire `interact::invoke(..., "updateFabricLabel", ...)` best-effort (warn on failure, don't abort).

**`ops/discovery.rs`:**
```rust
pub(crate) async fn browse<C: Crypto>(ctx: &StackCtx<C>, timeout_ms: u32)
    -> Result<Vec<DiscoveredDevice>, StackError> {
    // rs-matter's browse is single-result; loop with an exclude list (cap 6).
    let filter = CommissionableFilter { commissioning_mode_only: true, ..Default::default() };
    let mut exclude: Vec<u64> = Vec::new();
    let mut out = Vec::new();
    while exclude.len() < 6 {
        match ctx.matter.transport().browse_commissionable(&filter, &exclude, timeout_ms).await {
            Ok((addr, instance)) => {
                out.push(DiscoveredDevice {
                    instance_name: format!("{instance:016X}"),
                    address: match addr { Address::Udp(sa) => sa.to_string(), o => format!("{o}") },
                });
                exclude.push(instance);
            }
            Err(_) => break, // NotFound/timeout: sweep done
        }
    }
    Ok(out)
}
```
(`CommissionableFilter` may not implement `Default` — construct all-`None` fields explicitly if so.)

- [ ] **Step 1: Implement all five modules** per the code above (fixing the deliberately-flagged syntax sketch spots: the `use rs_matter::im.encoding` typo, the rng helpers via `ctx.crypto.rand()` as the spike does at `spike/src/main.rs:279`, `BasicCommData` construction).

- [ ] **Step 2: Build gate**

Run: `cargo build -p matter-rs-stack && cargo clippy -p matter-rs-stack && cargo test -p matter-rs-stack`
Expected: clean build, existing tests still PASS. Where the sketch and the rev's real API disagree, the referenced rs-matter files win — keep the BEHAVIOR (timeouts, error mapping, event flow) exactly as specified.

- [ ] **Step 3: Commit**

```bash
git add crates/stack
git commit -m "feat(stack): node supervisor, commissioning, OCW, device fabrics, discovery"
```

---

### Task 16: `stack` — runtime thread, request loop, `StackHandle` (impl `Stack`)

**Files:**
- Create: `crates/stack/src/runtime.rs`, `crates/stack/src/mdns.rs`
- Modify: `crates/stack/src/lib.rs` (public API: `spawn`, `StackConfig`, `StackHandle`, `ReadyInfo`)

**Interfaces:**
- Consumes: everything in the stack crate; `spike/src/mdns.rs` (port it: `SPIKE_IFACE` env → `primary_interface` parameter, hostname `"matter-rs-server"`); IM responder wiring pattern from `rs-matter-ref/rs-matter/tests/im/subscription_reboot.rs:277-297`; `Matter::startup` (`rs-matter/src/lib.rs:653`) + `run_persist_resumption` (`lib.rs:712`) with `DirKvBlobStore` (`rs-matter/src/persist.rs:457`) rooted at `<storage>/sessions/`.
- Produces (the server binary consumes in Task 17):

```rust
pub struct StackConfig {
    pub storage: std::sync::Arc<matter_rs_controller::storage::Storage>,
    pub fabric_id: u64,
    pub vendor_id: u16,
    pub fabric_label: String,
    pub primary_interface: Option<String>,
}
pub struct ReadyInfo {
    pub identity: matter_rs_controller::storage::ServerIdentity,
    pub fabric_index: u8,
}
#[derive(Clone)]
pub struct StackHandle { /* mpsc::Sender<StackRequest> + thread JoinHandle share */ }
impl matter_rs_controller::stack_api::Stack for StackHandle { ... }

/// Spawns the dedicated rs-matter thread. Await `ready` before serving.
pub fn spawn(config: StackConfig) -> (
    StackHandle,
    tokio::sync::mpsc::UnboundedReceiver<StackEvent>,      // -> NodeManager
    tokio::sync::oneshot::Receiver<Result<ReadyInfo, String>>,
);
```

**Runtime structure** (`runtime.rs`):

```rust
pub(crate) enum StackRequest {
    Commission { req: CommissionRequest, reply: Reply<CommissionOutcome> },
    Read { node_id: u64, paths: Vec<AttributePathSpec>, fabric_filtered: bool, reply: Reply<Vec<(String, Value)>> },
    Write { node_id: u64, endpoint: u16, cluster: u32, attribute: u32, value: Value, reply: Reply<u8> },
    Invoke { node_id: u64, endpoint: u16, cluster: u32, command_name: String, payload: Value,
             timed_ms: Option<u16>, reply: Reply<Value> },
    Interview { node_id: u64, reply: Reply<BTreeMap<String, Value>> },
    OpenWindow { node_id: u64, timeout_secs: u16, reply: Reply<WindowInfo> },
    DeviceFabrics { node_id: u64, reply: Reply<Vec<DeviceFabric>> },
    RemoveDeviceFabric { node_id: u64, fabric_index: u8, reply: Reply<()> },
    UpdateFabricLabel { label: String, reply: Reply<()> },
    StartSupervisor { node_id: u64 },
    StopSupervisor { node_id: u64 },
    NodeAddresses { node_id: u64, reply: Reply<Vec<String>> },
    Browse { timeout_ms: u32, reply: Reply<Vec<DiscoveredDevice>> },
    Shutdown { done: tokio::sync::oneshot::Sender<()> },
}
type Reply<T> = tokio::sync::oneshot::Sender<Result<T, StackError>>;
```

Thread body (spawned by `lib.rs::spawn` via `std::thread::Builder::new().name("matter-stack")`):
1. Statics via `StaticCell` (one stack per process): `Matter`, IM buffers, `InteractionModelState`.
2. `Matter::init(&TEST_DEV_DET, TEST_DEV_COMM, &TEST_DEV_ATT, 0)` (controller never gets commissioned; test dev-att constants are fine, exactly like the spike), `default_crypto(rand::thread_rng(), DAC_PRIVKEY)`.
3. Dual-stack ephemeral UDP socket — port `create_dual_stack_socket` from `spike/src/main.rs:420-435` verbatim.
4. `DirKvBlobStore` at `storage.root().join("sessions")`; `matter.startup(matter.kv(&kv))` (loads CASE-resumption records; fabric slots are empty in KV by design — identity is server.json-owned). A startup error is a warn, not fatal.
5. `identity::ensure_identity(...)` → send `Ok(ReadyInfo{..})` (or `Err(msg)` and return) over the ready oneshot.
6. Build `Rc<StackCtx>` and run, on `async_executor::LocalExecutor` + `futures_lite::future::block_on(ex.run(main_fut))`, the select of:
   - `matter.run(&crypto, &socket, &socket, NoNetwork)` — exit = fatal, log error, stop.
   - `mdns::run_builtin_mdns(matter, &crypto, primary_interface)` — **exit = WARN and keep going** (spike finding 3: discovery/commissioning degrade, live nodes keep working). Wrap: `async { if let Err(e) = mdns_fut.await { tracing::warn!("mDNS runner exited: {e:?}; discovery and cold-resolve degraded"); } core::future::pending::<()>().await }`.
   - the IM responder: `InteractionModel::new_with_reports(matter, &crypto, &buffers, (Node::new(&[]), EmptyHandler), &kv2, NoopWirelessNetCtl::new(NetworkType::Ethernet), ReportSink(ctx.clone()), &state)` + `Responder::new_default(&im).run::<4>()` (exact construction: `subscription_reboot.rs:277-297`).
   - `matter.run_persist_resumption(matter.kv(&kv3), <500ms min interval>)` — flushes CASE resumption to disk.
   - the request loop:
```rust
async {
    while let Some(req) = rx.recv().await {
        match req {
            StackRequest::Shutdown { done } => {
                ctx.supervisors.borrow_mut().clear();   // cancels all supervisor tasks
                let _ = done.send(());
                break;
            }
            StackRequest::StartSupervisor { node_id } => {
                let ctx2 = ctx.clone();
                let task = ex.spawn(crate::supervisor::supervise(ctx2, node_id));
                ctx.supervisors.borrow_mut().insert(node_id, task);
            }
            StackRequest::StopSupervisor { node_id } => {
                ctx.supervisors.borrow_mut().remove(&node_id); // drop cancels
                let node_subs: Vec<u32> = ctx.subs.borrow().iter()
                    .filter(|(_, n)| **n == node_id).map(|(s, _)| *s).collect();
                for s in node_subs { ctx.subs.borrow_mut().remove(&s); }
            }
            // Every other variant: spawn a detached task so a slow op (60s
            // commissioning) never blocks the loop.
            other => { let ctx2 = ctx.clone(); ex.spawn(handle_request(ctx2, other)).detach(); }
        }
    }
}
```
   `handle_request` matches the remaining variants onto `ops::*` and sends the result into `reply` (ignore send errors — caller gone). `NodeAddresses` answers from `ctx.addrs` merged with the peer addresses of the node's live sessions if accessible via `matter.with_state(|s| ...)` (see `Session::get_peer_addr`, `rs-matter/src/transport/session.rs:245`; if the sessions collection isn't iterable per-node at this rev, the addr cache alone is acceptable — the controller merges in cached record addresses anyway).
7. `StackHandle` methods: build the request, `self.tx.send(...)` (an `unbounded_send`; a closed channel maps to `StackError::new(Sdk, "stack thread is down")`), await the oneshot. `shutdown()` sends `Shutdown` and then `spawn_blocking`-joins the thread handle with a 5s cap.

**mdns.rs:** port `spike/src/mdns.rs` with three changes: `SPIKE_IFACE` env → `iface: Option<&str>` parameter (from `--primary-interface`); hostname `"matter-rs-server"`; keep the multicast-join-failure-is-a-warning behavior.

- [ ] **Step 1: Implement `mdns.rs`, `runtime.rs`, and the `lib.rs` public API** per the structure above.

- [ ] **Step 2: Write the boot smoke test** (`crates/stack/tests/boot.rs`):

```rust
use std::sync::Arc;

use matter_rs_controller::stack_api::Stack;
use matter_rs_controller::storage::Storage;

#[tokio::test]
async fn stack_boots_persists_identity_and_shuts_down() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(dir.path()).unwrap());
    let (handle, _events, ready) = matter_rs_stack::spawn(matter_rs_stack::StackConfig {
        storage: storage.clone(), fabric_id: 1, vendor_id: 0xFFF1,
        fabric_label: "HomeAssistant".into(), primary_interface: None,
    });
    let ready = tokio::time::timeout(std::time::Duration::from_secs(30), ready)
        .await.expect("ready in time").expect("channel").expect("stack up");
    assert_eq!(ready.identity.controller_node_id, 112233);
    assert_ne!(ready.identity.compressed_fabric_id, 0);
    assert!(storage.load_identity().is_some());

    // An operation against a nonexistent node fails cleanly (mDNS resolve miss),
    // proving the request loop dispatches and replies.
    let err = handle.read_attributes(999, vec![
        matter_rs_controller::stack_api::AttributePathSpec { endpoint: Some(0), cluster: Some(40), attribute: Some(2) }
    ], false).await.unwrap_err();
    assert!(!err.message.is_empty());

    handle.shutdown().await; // must return (thread joined), not hang
}
```

- [ ] **Step 3: Run it**

Run: `cargo test -p matter-rs-stack --test boot -- --nocapture`
Expected: PASS. Watch for: the ready handshake racing mDNS init (ensure ready is sent before/independently of the mdns future's first poll), and shutdown hanging (the executor must get a chance to observe the loop break — keep `Shutdown` handled inline in the loop as written).

- [ ] **Step 4: Full workspace check + commit**

```bash
cargo test --workspace
git add crates/stack
git commit -m "feat(stack): runtime thread, request loop, StackHandle implementing Stack"
```

---

### Task 17: `server` — wire the real controller (replace the stub)

**Files:**
- Modify: `crates/server/src/main.rs`, `crates/server/src/logging.rs`, `crates/server/Cargo.toml` (add `matter-rs-stack` path dep), `crates/server/tests/smoke.rs`

**Interfaces:**
- Consumes: `matter_rs_stack::{spawn, StackConfig}`, `MatterController::new`, `LogLevels`.
- Produces: the deployed binary runs the real stack. `StubController` stays in the tree (integration tests use it).

- [ ] **Step 1: `logging.rs` — reload handle.** `init(&Config) -> LogControl` where `LogControl` stores the `tracing_subscriber::reload::Handle` for the `EnvFilter` layer plus the current level names:

```rust
pub struct LogControl {
    handle: tracing_subscriber::reload::Handle<tracing_subscriber::EnvFilter, tracing_subscriber::Registry>,
    console: std::sync::Mutex<String>,
    has_file: bool,
}
impl matter_rs_controller::real::LogLevels for LogControl {
    fn get(&self) -> (String, Option<String>) {
        let c = self.console.lock().unwrap().clone();
        (c.clone(), self.has_file.then_some(c)) // one filter drives both (deviation #4)
    }
    fn set(&self, console: Option<&str>, _file: Option<&str>) {
        if let Some(level) = console {
            let mapped = crate::logging::map_level(level).unwrap_or(tracing::Level::INFO);
            let _ = self.handle.reload(tracing_subscriber::EnvFilter::builder()
                .with_default_directive(mapped.into()).parse_lossy(""));
            *self.console.lock().unwrap() = level.to_string();
        }
    }
}
```
(Build `init` with `tracing_subscriber::reload::Layer::new(filter)`; keep `map_level` and the file layer as-is. The registry type parameter must match the actual layered stack — adjust the `Handle<_, S>` type accordingly; compile errors here are type-plumbing only.)

- [ ] **Step 2: `main.rs`** — replace the stub block:

```rust
let storage = Arc::new(matter_rs_controller::storage::Storage::open(&config.storage_path)
    .expect("cannot open --storage-path"));
let (stack, stack_events, ready) = matter_rs_stack::spawn(matter_rs_stack::StackConfig {
    storage: storage.clone(),
    fabric_id: config.fabric_id,
    vendor_id: config.vendor_id,
    fabric_label: matter_rs_controller::storage::normalize_fabric_label(config.default_fabric_label.as_deref()),
    primary_interface: config.primary_interface.clone(),
});
let ready = tokio::time::timeout(std::time::Duration::from_secs(60), ready).await
    .expect("stack start timed out").expect("stack thread died")
    .unwrap_or_else(|e| { eprintln!("fatal: matter stack failed to start: {e}"); std::process::exit(1); });
let sdk_version = format!("matter-rs-server/{} (rs-matter/03bc8f2)", env!("CARGO_PKG_VERSION"));
let controller = matter_rs_controller::real::MatterController::new(
    Arc::new(stack.clone()), storage, ready.identity, ready.fabric_index,
    sdk_version, config.default_fabric_label.is_some(), Arc::new(log_control), stack_events);
```
Keep the existing storage-dir chmod logic BEFORE `Storage::open` (open creates subdirs). If `--default-fabric-label` is set, also persist it into config.json at startup (locked label wins over a stale stored one).

Shutdown path: after the axum servers drain, `stack.shutdown().await` (bounded) — the WS `server_shutdown` frames already went out via the watch channel.

- [ ] **Step 3: smoke test update** (`tests/smoke.rs`): the binary now starts a real stack. Point `--storage-path` at the temp dir, keep the rest; after SIGTERM assert exit 0 AND that `<dir>/server.json` exists (identity persisted). Use a NEW temp dir per run and remove it at the end (carryover: the old test leaked its dir).

- [ ] **Step 4: Run everything**

Run: `cargo test --workspace`
Expected: PASS. The smoke test now exercises: boot → identity generation → serving → clean SIGTERM.

- [ ] **Step 5: Commit**

```bash
git add crates/server Cargo.lock
git commit -m "feat(server): wire the rs-matter controller (stub retired from main)"
```

---

### Task 18: Carryover hygiene batch (plan-1 review leftovers)

**Files:**
- Modify: `crates/server/src/ws.rs`, `crates/server/src/config.rs`, `crates/server/tests/ws_protocol.rs`, `crates/server/tests/smoke.rs`, `crates/wire/Cargo.toml`, `crates/controller/Cargo.toml`, `README.md`

Work through `docs/superpowers/plans/2026-08-13-plan2-carryover.md` top to bottom:

- [ ] **Step 1: ws.rs — warn on broadcast Lagged.** In the event arm: `Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => tracing::warn!("connection dropped {n} events (slow consumer)")`, keep serving. `Err(Closed)` keeps the current silent behavior (controller owns the sender for its lifetime; Closed means shutdown).
- [ ] **Step 2: config.rs — env isolation + multi-address env.** Add `value_delimiter = ','` to `listen_address` so `LISTEN_ADDRESS=127.0.0.1,::1` works; add a test. Make the three existing config tests immune to ambient `PORT`/`STORAGE_PATH`/etc. by clearing the relevant vars via a `#[test]`-local guard (`std::env::remove_var` in a mutex-serialized helper — combine all env-sensitive tests into one `#[test]` fn if simpler).
- [ ] **Step 3: dual-stack note.** `main.rs` binds `[::]` when no `--listen-address`: set `socket2`-level `set_only_v6(false)` before bind (via `socket2::Socket` → `TcpListener::from_std`) so IPv4 works on `IPV6_V6ONLY=1` hosts; if that's disproportionate, document the limitation in README under deployment. Prefer the socket option (it's ~6 lines).
- [ ] **Step 4: test gaps.** ws_protocol: add (a) shutdown event reaches a connection that never sent `start_listening`; (b) a two-listener `spawn_server` variant proving both addresses serve `/health`. smoke.rs: temp dir cleanup (Step 3 of Task 17 may already have done it — verify).
- [ ] **Step 5: dependency tidy.** `thiserror` gone from wire+controller (done in Task 3 — verify); tokio `macros` dev-only in controller (done — verify); note the duplicate tokio-tungstenite (0.24 dev vs axum's 0.29) in a `Cargo.toml` comment as accepted (test-only).
- [ ] **Step 6: Run + commit**

```bash
cargo test --workspace
git add -A
git commit -m "chore: plan-1 carryover hygiene (Lagged warn, env-safe config tests, dual-stack bind, test gaps)"
```

---

### Task 19: E2E acceptance against a virtual matter.js device + README

**Files:**
- Create: `scripts/e2e-virtual-device.md` (runbook), `crates/server/tests/e2e_virtual.rs` (`#[ignore]`d)
- Modify: `README.md`

This is the plan's real gate: the full loop against the strictest peer (matter.js), on this machine — the same setup that passed spike leg 1.

- [ ] **Step 1: Runbook** (`scripts/e2e-virtual-device.md`): document the exact procedure:

```markdown
# E2E: matter-rs-server vs virtual matter.js device

1. Start the virtual device (separate terminal, leave running):
   npx -y @matter/examples matter-device
   -> note the QR code line "MT:..." and/or the manual pairing code.

2. Start the server (fresh storage):
   cargo run -p matter-rs-server -- --storage-path /tmp/mrs-e2e --listen-address 127.0.0.1 --primary-interface <lan-if>

3. Drive it over WS (websocat or the test below):
   {"message_id":"1","command":"commission_with_code","args":{"code":"<MT:...>"}}
     -> result is a MatterNodeData with node_id 1, attributes populated
   {"message_id":"2","command":"start_listening"} -> [node]
   {"message_id":"3","command":"device_command","args":{"node_id":1,"endpoint_id":1,"cluster_id":6,"command_name":"toggle","payload":{}}}
     -> null; an attribute_updated event [1,"1/6/0",...] follows via the subscription
   {"message_id":"4","command":"read_attribute","args":{"node_id":1,"attribute_path":"1/6/0"}}

4. Restart the server; verify get_nodes still returns the node (storage),
   and that it becomes available again (re-subscription) within ~30s.

5. Kill the device; verify available flips to false after the 3-minute grace.
```

- [ ] **Step 2: Automated (ignored) test** (`e2e_virtual.rs`): automate steps 1–3 when `MRS_E2E=1`: spawn `npx -y @matter/examples matter-device` with `kill_on_drop`, scrape the pairing code from stdout, spawn the server binary, drive the WS flow with tokio-tungstenite, assert: commission result shape, `attribute_updated` after `toggle`, `read_attribute` roundtrip. Skip cleanly (early return) unless `MRS_E2E=1`. This is best-effort automation of the runbook — flakiness in device startup is acceptable, the runbook is authoritative.

- [ ] **Step 3: RUN the e2e** (runbook or `MRS_E2E=1 cargo test -p matter-rs-server --test e2e_virtual -- --ignored --nocapture`) and fix what it finds. This step is not done until commissioning + toggle + attribute_updated + restart-persistence all pass against the real matter.js device. Budget for iteration here — this is where the rs-matter API-usage bugs surface.

- [ ] **Step 4: README** — update status (plan 2: real controller), document: storage layout (server.json/config.json/nodes//sessions/), RCAC-direct mode + why (spike finding 1), `--primary-interface` guidance, the e2e runbook pointer, and the deployment notes (Thread RA sysctl from spike finding 4, conntrack timeout).

- [ ] **Step 5: Commit**

```bash
git add scripts README.md crates/server/tests
git commit -m "test: e2e acceptance vs virtual matter.js device + deployment docs"
```

---

### Task 20: Finish the branch

- [ ] **Step 1:** `cargo test --workspace && cargo build --release` — all green.
- [ ] **Step 2:** Update the roadmap artifact if asked; update project memory (plan 2 status).
- [ ] **Step 3:** Use superpowers:finishing-a-development-branch (merge `plan2-rs-matter-core` to master, push).

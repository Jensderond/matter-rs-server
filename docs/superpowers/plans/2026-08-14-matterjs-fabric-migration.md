# matter.js → matter-rs-server Fabric Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A one-shot CLI tool (`matter-rs-migrate`) that reads an existing matterjs-server storage directory and writes an equivalent matter-rs-server storage directory, so the Rust server serves the same fabric and no device is re-commissioned.

**Architecture:** New workspace crate `crates/migrate` with four units — `jsdb` (matter.js WAL KV reader), `decode` (tagged-value codec), `convert` (field mapping), and a checks/report/CLI layer — plus one new public module `migration` in `crates/stack`, which is the tool's only door into rs-matter (minting the controller NOC against the preserved CA, deriving the compressed fabric id and operational IPK). The tool writes destination files through `matter_rs_controller::storage::Storage`, the same code the server reads them with. Dry-run is the default; the source store is never opened for writing.

**Tech Stack:** Rust (workspace edition 2021), serde_json, flate2 (gzip), hex, clap 4 (derive — same as `crates/server`), thiserror (workspace dep), tempfile (dev). rs-matter only inside `crates/stack`.

**Spec:** `docs/superpowers/specs/2026-08-14-matterjs-fabric-migration-design.md` — the plan argues from the spec; executors read both.

## Research findings baked into this plan

These were open questions in the spec; they were settled by reading matter.js **v0.17.9** source (the exact version matterjs-server 0.17.9 pins) before writing this plan. Cite paths are in the matter.js repo, `packages/general/src/storage/`.

1. **`del` op semantics** (spec said "must confirm from the data"; confirmed from code instead — `wal/WalTransaction.ts`, `applyCommit`). Three shapes:
   - `{"op":"del","key":K,"values":["f1","f2"]}` → delete the listed **fields** from `data[K]`.
   - `{"op":"del","key":""}` (no `values`) → clear the **entire store** (`clearAll`).
   - `{"op":"del","key":K}` (no `values`) → delete `data[K]` **and every context whose key starts with `K.`** (a subtree delete — the spec did not mention this shape; missing it silently resurrects deleted subtrees).
2. **WAL reader semantics** (`wal/WalReader.ts`): segment files are `wal/XXXXXXXX.jsonl` (8 hex digits, matched case-insensitively) with an optional `.gz` variant that is **preferred when both exist**; segments replay in ascending numeric order; line offsets are **0-based and count every line including blank ones** (blank lines are then skipped); a commit replays only if `(segment, offset) > snapshot.commitId`; a bare-array line is a legacy commit equal to `{ts:0, ops:<array>}`. matter.js *skips* malformed lines with a warning — the tool instead **hard-errors naming the file, line, segment and offset**, per the spec's error-handling section.
3. **Snapshot** (`wal/WalSnapshot.ts`): `snapshot.json.gz` or `snapshot.json`; when both exist the **newer mtime wins** (tie → the .gz). Shape: `{"commitId":{"segment":N,"offset":N},"ts":N,"data":{...}}`, `data` a flat map of dotted context keys → field maps.
4. **Tagged-value codec** (`StringifyTools.ts`): a value is tagged iff it is a **string** that starts with `{"__object__":"` and ends with `}`. Tags: `BigInt` (decimal string), `Uint8Array` (hex via `Bytes.toHex` — emit lowercase, accept either case), `Map` (**double-encoded**: `__value__` is a JSON *string* containing a `toJson`'d array of `[key, value]` pairs whose members carry their own tags), `Undefined`, and legacy tags — `EventNumber`/`FabricId`/`NodeId` carry decimal strings like `BigInt`; `AttributeId`/`CaseAuthenticatedTag`/`ClusterId`/`CommandId`/`DataVersion`/`DeviceTypeId`/`EndpointNumber`/`EntryIndex`/`EventId`/`FabricIndex`/`FieldId`/`GroupId`/`VendorId` carry plain numbers. Anything else (including `Interval`, which we never expect in the fields we read) → **error**, per spec.
5. **rs-matter surface for the stack helper** (verified against the pinned rev's source in `rs-matter-ref/`): `rs_matter::fabric::Fabrics::new()` is public and standalone — `fabrics.add(...)` then `fabric.compressed_fabric_id()` derives the compressed fabric id from the blobs with **no `Matter` instance**; `rs_matter::group_keys::KeySet` has public fields and a public `update(crypto, epoch_key, &compressed_fabric_id)` that is exactly the operational-IPK KDF (`fabric.rs:696` calls it); `CertRef::pubkey()` extracts the root public key for fabric-index matching. Chain verification (`verify_chain_start`) checks validity windows against a **Matter-epoch-microseconds** `UtcTime` — the helper must verify at *the current time*, not at `VALID_FOREVER.not_before` (= year 2000), because the preserved matter.js RCAC has a real notBefore that a year-2000 instant predates. `MATTER_EPOCH_OFFSET_S` already exists in `crates/stack/src/tlv_json.rs:17`.

**One deliberate deviation from the spec's Testing section.** The spec asks for "a redacted copy of the CT 109 store … committed as the integration fixture". Redacting key material while keeping the self-checks meaningful is impossible by substitution alone: replacing the root key invalidates the stored `operationalId`, `operationalIdentityProtectionKey`, and every certificate, so a faithful redactor must *re-mint and re-derive all of them* — at which point it is a fixture **generator**. This plan therefore builds the integration fixture programmatically at test time (Task 6): the same structure, the same WAL scale (tens of thousands of lines), stale snapshot + mandatory replay, multi-fabric device caches, and self-consistent generated key material — and **nothing committed to the repo at all**, which is strictly stronger than "no production key material in the repo". The real CT 109 clone is still exercised, as the spec's manual acceptance step (dry-run first), listed at the end of this plan.

## Global Constraints

- rs-matter is imported **only** by `crates/stack` (`crates/stack/src/lib.rs:1`). `crates/migrate` links `matter-rs-controller` and `matter-rs-stack`, never rs-matter.
- The tool is **read-only on the matter.js store under all circumstances**: source files are opened only for reading; the WAL is replayed in memory.
- Dry-run is the default; writing requires `--write`, and any failed self-check aborts before the first write.
- Destination writes go through `matter_rs_controller::storage::Storage` (atomic tmp+rename, 0600 for `server.json`/`config.json`), and the first identity write uses `Storage::create_identity`, which refuses to overwrite an existing `server.json`.
- `device_fabric_index` is matched by root public key or written as `0`. Never inferred, defaulted, or copied.
- Node files are written with `attributes: {}` — the server's per-node supervisor repopulates the cache on first boot.
- `sessions/` is not migrated; `wifi_credentials`/`thread_datasets` start empty (the source store has none — do not go looking).
- No production key material in the repo (this plan commits none at all).
- CLI: `matter-rs-migrate --from <matter.js store root> --to <matter-rs-server storage path> [--write]`. Exit code 0 only when every self-check passes (and, with `--write`, every write succeeds).
- Workspace conventions: thiserror for module errors, clap 4 derive for the CLI, `tempfile` in dev-deps, tests colocated in `#[cfg(test)] mod tests`.

## File structure

| File | Responsibility |
|---|---|
| `Cargo.toml` (workspace root) | add `"crates/migrate"` to `members` |
| `crates/migrate/Cargo.toml` | crate manifest, `[[bin]] matter-rs-migrate` + lib |
| `crates/migrate/src/lib.rs` | module decls; `Options`, `Report`, `MigrateError`, `run()` |
| `crates/migrate/src/decode.rs` | tagged-value codec; knows nothing about our storage |
| `crates/migrate/src/jsdb.rs` | matter.js KV reader (namespace, snapshot, WAL replay); knows nothing about Matter |
| `crates/migrate/src/convert.rs` | store → `SourceFabric` / `NodePlan` / `ConfigData` mapping |
| `crates/migrate/src/checks.rs` | the five self-checks over converted data |
| `crates/migrate/src/main.rs` | thin clap wrapper over `run()` |
| `crates/migrate/tests/migrate_store.rs` | fixture generator + end-to-end integration tests |
| `crates/stack/src/migration.rs` | **new pub module**: `identity_from_preserved_ca`, `verify_identity`, `derive_operational_ipk`, `rcac_public_key`, `rcac_serial_is_der_canonical`, `generate_ca` |
| `crates/stack/src/identity.rs` | visibility only: `generate_usable_rcac` and `serial_is_der_canonical` become `pub(crate)` |
| `crates/stack/src/lib.rs` | add `pub mod migration;` |

Task order: 1 (scaffold+decode) → 2 (jsdb) → 3 (stack migration module) → 4 (convert) → 5 (checks+run+CLI) → 6 (integration fixture+tests) → 7 (workspace verification, docs, wrap-up). Tasks 1–3 are mutually independent; 4 needs 1–3; 5 needs 4; 6 needs 5.

---

### Task 1: Crate scaffold + `decode` (tagged-value codec)

**Files:**
- Modify: `Cargo.toml` (workspace root, `members` line)
- Create: `crates/migrate/Cargo.toml`
- Create: `crates/migrate/src/lib.rs` (module decls only at this point)
- Create: `crates/migrate/src/main.rs` (compiling stub)
- Create: `crates/migrate/src/decode.rs` (+ its `#[cfg(test)]` tests)

**Interfaces:**
- Consumes: nothing from other tasks.
- Produces (used by Tasks 2, 4, 6):
  - `decode::DecodeError` — `UnknownTag(String)`, `Malformed(String)`, `WrongType { expected: &'static str, got: String }`; derives `Debug, PartialEq`, implements `std::error::Error` via thiserror.
  - `pub fn as_u64(v: &serde_json::Value) -> Result<u64, DecodeError>` — plain JSON number, or `BigInt`/`EventNumber`/`FabricId`/`NodeId` tag (decimal string), or a legacy numeric tag (plain number payload).
  - `pub fn as_bytes(v: &Value) -> Result<Vec<u8>, DecodeError>` — `Uint8Array` tag, hex payload, either case.
  - `pub fn as_str(v: &Value) -> Result<&str, DecodeError>` — plain string that is **not** a tagged document (tagged → `WrongType`).
  - `pub fn as_map_entries(v: &Value) -> Result<Vec<(Value, Value)>, DecodeError>` — `Map` tag (double-encoded) **or** a plain JSON array of 2-element arrays; entries returned raw (keys/values keep their own tags).
  - `pub fn is_undefined(v: &Value) -> bool` — the `Undefined` tag.

- [ ] **Step 1: Scaffold the crate**

Workspace root `Cargo.toml`, `members`:

```toml
members = ["crates/gen", "crates/wire", "crates/controller", "crates/stack", "crates/server", "crates/migrate"]
```

`crates/migrate/Cargo.toml`:

```toml
[package]
name = "matter-rs-migrate"
version.workspace = true
edition.workspace = true
license.workspace = true

[lib]
name = "matter_rs_migrate"
path = "src/lib.rs"

[[bin]]
name = "matter-rs-migrate"
path = "src/main.rs"

[dependencies]
matter-rs-controller = { path = "../controller" }
matter-rs-stack = { path = "../stack" }
serde_json.workspace = true
thiserror.workspace = true
# gzip for snapshot.json.gz and *.jsonl.gz WAL segments (read), and for the
# integration fixture generator (write).
flate2 = "1"
hex = "0.4"
clap = { version = "4", features = ["derive"] }

[dev-dependencies]
tempfile = "3"
```

`crates/migrate/src/lib.rs` (grows in Task 5; for now just):

```rust
//! One-shot matter.js -> matter-rs-server fabric migration. See
//! docs/superpowers/specs/2026-08-14-matterjs-fabric-migration-design.md.

pub mod decode;
```

`crates/migrate/src/main.rs` stub:

```rust
fn main() {}
```

Run: `cargo build -p matter-rs-migrate`
Expected: compiles (with a missing `decode.rs` error first — create an empty `decode.rs`, then it compiles).

- [ ] **Step 2: Write the failing decode tests**

In `crates/migrate/src/decode.rs`, the tests first (the implementation below them starts as `todo!()`-free stubs that return `Err(DecodeError::Malformed("unimplemented".into()))` so the file compiles):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    // Emit tags exactly as matter.js's toJson does (StringifyTools.ts).
    fn bigint(n: u64) -> Value {
        Value::String(format!("{{\"__object__\":\"BigInt\",\"__value__\":\"{n}\"}}"))
    }
    fn uint8array(hex_str: &str) -> Value {
        Value::String(format!("{{\"__object__\":\"Uint8Array\",\"__value__\":\"{hex_str}\"}}"))
    }

    #[test]
    fn u64_from_plain_number_and_every_bigint_shaped_tag() {
        assert_eq!(as_u64(&json!(65521)), Ok(65521));
        assert_eq!(as_u64(&bigint(112233)), Ok(112233));
        // Legacy tags fromJson maps through BigInt(...): decimal-string payload.
        for tag in ["NodeId", "FabricId", "EventNumber"] {
            let v = Value::String(format!("{{\"__object__\":\"{tag}\",\"__value__\":\"23\"}}"));
            assert_eq!(as_u64(&v), Ok(23), "for tag {tag}");
        }
        // Legacy tags fromJson passes through verbatim: numeric payload.
        let v = Value::String("{\"__object__\":\"VendorId\",\"__value__\":65521}".to_string());
        assert_eq!(as_u64(&v), Ok(65521));
    }

    #[test]
    fn u64_rejects_negatives_floats_and_overflow() {
        assert!(matches!(as_u64(&json!(-1)), Err(DecodeError::WrongType { .. })));
        assert!(matches!(as_u64(&json!(1.5)), Err(DecodeError::WrongType { .. })));
        let v = Value::String(
            "{\"__object__\":\"BigInt\",\"__value__\":\"99999999999999999999\"}".to_string(),
        );
        assert!(matches!(as_u64(&v), Err(DecodeError::Malformed(_))));
        assert!(matches!(as_u64(&json!("112233")), Err(DecodeError::WrongType { .. })));
    }

    #[test]
    fn bytes_accept_both_hex_cases_and_reject_odd_length() {
        assert_eq!(as_bytes(&uint8array("0424fed0b3")), Ok(vec![0x04, 0x24, 0xfe, 0xd0, 0xb3]));
        assert_eq!(as_bytes(&uint8array("ABCD")), Ok(vec![0xab, 0xcd]));
        assert_eq!(as_bytes(&uint8array("")), Ok(vec![]));
        assert!(matches!(as_bytes(&uint8array("abc")), Err(DecodeError::Malformed(_))));
        assert!(matches!(as_bytes(&json!("abcd")), Err(DecodeError::WrongType { .. })));
        assert!(matches!(as_bytes(&bigint(1)), Err(DecodeError::WrongType { .. })));
    }

    #[test]
    fn map_entries_unwrap_the_double_encoding() {
        // matter.js: toJson(map) emits {"__object__":"Map","__value__":<JSON-
        // stringified STRING containing toJson(entries)>}. Build it the same way.
        let inner = serde_json::to_string(&json!([
            [bigint(10), {"discoveryData": {"discoveredAt": 1699999999999u64}}],
            [bigint(23), {"discoveryData": {"discoveredAt": 1700000000000u64}}],
        ]))
        .unwrap();
        let outer = Value::String(format!(
            "{{\"__object__\":\"Map\",\"__value__\":{}}}",
            serde_json::to_string(&inner).unwrap()
        ));
        let entries = as_map_entries(&outer).unwrap();
        assert_eq!(entries.len(), 2);
        // Entries come back RAW: the keys are still tagged, decoded by the caller.
        assert_eq!(as_u64(&entries[0].0), Ok(10));
        assert_eq!(as_u64(&entries[1].0), Ok(23));
        assert_eq!(entries[1].1["discoveryData"]["discoveredAt"], json!(1700000000000u64));

        // A plain array of pairs is also accepted (post-decode shape).
        let plain = json!([[bigint(5), {"x": 1}]]);
        assert_eq!(as_map_entries(&plain).unwrap().len(), 1);
        assert!(matches!(as_map_entries(&json!([[1, 2, 3]])), Err(DecodeError::Malformed(_))));
        assert!(matches!(as_map_entries(&json!(7)), Err(DecodeError::WrongType { .. })));
    }

    #[test]
    fn unknown_tags_are_a_hard_error_never_a_passthrough() {
        // The spec's rule: a silently-unconverted value is a boot failure at
        // best and a wrong fabric at worst. Interval is real-but-unexpected;
        // Foo is hypothetical; both must refuse.
        for tag in ["Interval", "Foo"] {
            let v = Value::String(format!("{{\"__object__\":\"{tag}\",\"__value__\":\"1\"}}"));
            assert_eq!(as_u64(&v), Err(DecodeError::UnknownTag(tag.to_string())), "for {tag}");
            assert_eq!(as_bytes(&v), Err(DecodeError::UnknownTag(tag.to_string())), "for {tag}");
        }
    }

    #[test]
    fn a_string_that_starts_like_a_tag_but_is_not_json_is_malformed() {
        // fromJson would throw on JSON.parse here; mirroring that beats
        // treating it as an ordinary string.
        let v = Value::String("{\"__object__\":\"BigInt\", broken}".to_string());
        assert!(matches!(as_u64(&v), Err(DecodeError::Malformed(_))));
        assert!(matches!(as_str(&v), Err(DecodeError::Malformed(_))));
    }

    #[test]
    fn as_str_takes_plain_strings_only() {
        assert_eq!(as_str(&json!("HomeAssistant")), Ok("HomeAssistant"));
        assert_eq!(as_str(&json!("")), Ok(""));
        assert!(matches!(as_str(&bigint(1)), Err(DecodeError::WrongType { .. })));
        assert!(matches!(as_str(&json!(1)), Err(DecodeError::WrongType { .. })));
    }

    #[test]
    fn undefined_tag_is_recognised() {
        let v = Value::String("{\"__object__\":\"Undefined\"}".to_string());
        assert!(is_undefined(&v));
        assert!(!is_undefined(&json!("x")));
        assert!(!is_undefined(&json!(null)));
        // ...and the typed getters refuse it rather than inventing a value.
        assert!(matches!(as_u64(&v), Err(DecodeError::WrongType { .. })));
    }
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cargo test -p matter-rs-migrate decode`
Expected: FAIL (stub implementations).

- [ ] **Step 4: Implement the codec**

```rust
//! matter.js tagged-value codec (StringifyTools.ts, matter.js v0.17.9).
//!
//! Detection is byte-for-byte matter.js's own rule: a value is a tagged
//! document iff it is a STRING that starts with `{"__object__":"` and ends
//! with `}`. An unknown tag is a hard error — a silently-unconverted key is a
//! boot failure at best and a wrong fabric at worst (spec, "Value encoding").

use serde_json::Value;

#[derive(Debug, PartialEq, thiserror::Error)]
pub enum DecodeError {
    #[error("unknown __object__ tag {0:?}")]
    UnknownTag(String),
    #[error("malformed tagged value: {0}")]
    Malformed(String),
    #[error("expected {expected}, got {got}")]
    WrongType { expected: &'static str, got: String },
}

const TAG_PREFIX: &str = "{\"__object__\":\"";

/// Tags whose `__value__` is a decimal string (matter.js maps them via `BigInt(...)`).
const BIGINT_TAGS: &[&str] = &["BigInt", "EventNumber", "FabricId", "NodeId"];
/// Legacy tags whose `__value__` is already a plain JSON number.
const NUMBER_TAGS: &[&str] = &[
    "AttributeId", "CaseAuthenticatedTag", "ClusterId", "CommandId", "DataVersion",
    "DeviceTypeId", "EndpointNumber", "EntryIndex", "EventId", "FabricIndex",
    "FieldId", "GroupId", "VendorId",
];

enum Tagged {
    U64(u64),
    Bytes(Vec<u8>),
    MapEntries(Vec<(Value, Value)>),
    Undefined,
}

/// `None` = not a tagged document (an ordinary value).
fn parse_tagged(v: &Value) -> Option<Result<Tagged, DecodeError>> {
    let s = v.as_str()?;
    if !(s.starts_with(TAG_PREFIX) && s.ends_with('}')) {
        return None;
    }
    Some(parse_tagged_inner(s))
}

fn parse_tagged_inner(s: &str) -> Result<Tagged, DecodeError> {
    let doc: Value = serde_json::from_str(s).map_err(|e| DecodeError::Malformed(e.to_string()))?;
    let tag = doc
        .get("__object__")
        .and_then(Value::as_str)
        .ok_or_else(|| DecodeError::Malformed("__object__ is not a string".into()))?;
    let value = doc.get("__value__");
    if tag == "Undefined" {
        return Ok(Tagged::Undefined);
    }
    if BIGINT_TAGS.contains(&tag) {
        let payload = value
            .and_then(Value::as_str)
            .ok_or_else(|| DecodeError::Malformed(format!("{tag} without a string __value__")))?;
        let n: u64 = payload
            .parse()
            .map_err(|e| DecodeError::Malformed(format!("{tag} {payload:?}: {e}")))?;
        return Ok(Tagged::U64(n));
    }
    if NUMBER_TAGS.contains(&tag) {
        let n = value
            .and_then(Value::as_u64)
            .ok_or_else(|| DecodeError::Malformed(format!("{tag} without a u64 __value__")))?;
        return Ok(Tagged::U64(n));
    }
    if tag == "Uint8Array" {
        let payload = value
            .and_then(Value::as_str)
            .ok_or_else(|| DecodeError::Malformed("Uint8Array without a string __value__".into()))?;
        let bytes = hex::decode(payload)
            .map_err(|e| DecodeError::Malformed(format!("Uint8Array hex: {e}")))?;
        return Ok(Tagged::Bytes(bytes));
    }
    if tag == "Map" {
        // Double-encoded: __value__ is a JSON STRING holding a toJson'd array
        // of [key, value] pairs; the pair members keep their own tags.
        let payload = value
            .and_then(Value::as_str)
            .ok_or_else(|| DecodeError::Malformed("Map without a string __value__".into()))?;
        let inner: Value =
            serde_json::from_str(payload).map_err(|e| DecodeError::Malformed(format!("Map: {e}")))?;
        return entries_from_array(&inner);
    }
    Err(DecodeError::UnknownTag(tag.to_string()))
}

fn entries_from_array(v: &Value) -> Result<Tagged, DecodeError> {
    let Value::Array(items) = v else {
        return Err(DecodeError::Malformed("Map __value__ is not an array".into()));
    };
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        match item.as_array().map(Vec::as_slice) {
            Some([k, val]) => out.push((k.clone(), val.clone())),
            _ => return Err(DecodeError::Malformed("Map entry is not a [key, value] pair".into())),
        }
    }
    Ok(Tagged::MapEntries(out))
}

fn wrong_type(expected: &'static str, v: &Value) -> DecodeError {
    DecodeError::WrongType { expected, got: short_debug(v) }
}

/// A bounded rendering for error messages (values can be huge blobs).
fn short_debug(v: &Value) -> String {
    let s = v.to_string();
    if s.chars().count() > 80 {
        let head: String = s.chars().take(80).collect();
        format!("{head}…")
    } else {
        s
    }
}

pub fn as_u64(v: &Value) -> Result<u64, DecodeError> {
    match parse_tagged(v) {
        Some(Ok(Tagged::U64(n))) => Ok(n),
        Some(Ok(_)) => Err(wrong_type("u64", v)),
        Some(Err(e)) => Err(e),
        None => v.as_u64().ok_or_else(|| wrong_type("u64", v)),
    }
}

pub fn as_bytes(v: &Value) -> Result<Vec<u8>, DecodeError> {
    match parse_tagged(v) {
        Some(Ok(Tagged::Bytes(b))) => Ok(b),
        Some(Ok(_)) => Err(wrong_type("Uint8Array", v)),
        Some(Err(e)) => Err(e),
        None => Err(wrong_type("Uint8Array", v)),
    }
}

pub fn as_str(v: &Value) -> Result<&str, DecodeError> {
    match parse_tagged(v) {
        Some(Ok(_)) => Err(wrong_type("plain string", v)),
        Some(Err(e)) => Err(e),
        None => v.as_str().ok_or_else(|| wrong_type("plain string", v)),
    }
}

pub fn as_map_entries(v: &Value) -> Result<Vec<(Value, Value)>, DecodeError> {
    match parse_tagged(v) {
        Some(Ok(Tagged::MapEntries(e))) => Ok(e),
        Some(Ok(_)) => Err(wrong_type("Map", v)),
        Some(Err(e)) => Err(e),
        None => match entries_from_array(v) {
            Ok(Tagged::MapEntries(e)) => Ok(e),
            Ok(_) => unreachable!("entries_from_array only builds MapEntries"),
            Err(_) if !v.is_array() => Err(wrong_type("Map", v)),
            Err(e) => Err(e),
        },
    }
}

pub fn is_undefined(v: &Value) -> bool {
    matches!(parse_tagged(v), Some(Ok(Tagged::Undefined)))
}
```

Note: `str::floor_char_boundary` is nightly — replace `short_debug`'s truncation with `s.chars().take(80).collect::<String>()` if it does not compile on stable.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p matter-rs-migrate decode`
Expected: PASS (all 8 tests).

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml crates/migrate
git commit -m "feat(migrate): crate scaffold + matter.js tagged-value codec"
```

---

### Task 2: `jsdb` — the matter.js WAL KV reader

**Files:**
- Create: `crates/migrate/src/jsdb.rs` (+ its `#[cfg(test)]` tests)
- Modify: `crates/migrate/src/lib.rs` (add `pub mod jsdb;`)

**Interfaces:**
- Consumes: nothing from other tasks (raw `serde_json::Value` throughout — tagged strings stay strings; decoding is the caller's business).
- Produces (used by Tasks 4, 5, 6):
  - `pub struct JsDb` — the replayed store. Constructors:
    - `pub fn open_store(root: &Path) -> Result<(JsDb, String), JsdbError>` — locates the single `server-*` namespace directory under a matter.js store root, loads it, returns the namespace directory name for the report.
    - `pub fn open_namespace(dir: &Path) -> Result<JsDb, JsdbError>` — loads one namespace directory (driver check → snapshot → WAL replay).
    - `pub fn from_data(data: BTreeMap<String, serde_json::Map<String, Value>>) -> JsDb` — test/back-door constructor, used by `convert`'s unit tests.
  - Accessors:
    - `pub fn get(&self, context: &str) -> Option<&serde_json::Map<String, Value>>`
    - `pub fn field(&self, context: &str, field: &str) -> Option<&Value>`
    - `pub fn context_keys(&self) -> impl Iterator<Item = &str>`
  - `pub enum JsdbError` (thiserror) — variants exactly:
    ```rust
    #[derive(Debug, thiserror::Error)]
    pub enum JsdbError {
        #[error("cannot read {path}: {source}")]
        Io { path: std::path::PathBuf, #[source] source: std::io::Error },
        #[error("no server-* namespace under {root} (found: {found:?}); is this a matter.js store?")]
        NoNamespace { root: std::path::PathBuf, found: Vec<String> },
        #[error("{count} server-* namespaces under {root}: {found:?}; expected exactly one")]
        MultipleNamespaces { root: std::path::PathBuf, count: usize, found: Vec<String> },
        #[error("{path} is not a WAL KV namespace: driver.json says {found}")]
        NotWalKv { path: std::path::PathBuf, found: String },
        #[error("bad snapshot {path}: {reason}")]
        BadSnapshot { path: std::path::PathBuf, reason: String },
        #[error("bad WAL line {path}:{line_number}: {reason}")]
        BadWalLine { path: std::path::PathBuf, line_number: usize, reason: String },
    }
    ```
    (`line_number` is 1-based for human consumption; the WAL *offset* is the 0-based line index used for commitId comparison — same number ± 1, both derived from one counter.)

- [ ] **Step 1: Write the failing tests**

The tests write real namespace directories into a tempdir. Shared helpers at the top of the test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};
    use std::io::Write as _;
    use std::path::Path;

    fn write_driver(ns: &Path) {
        std::fs::create_dir_all(ns).unwrap();
        std::fs::write(ns.join("driver.json"), br#"{"kind":"wal","type":"kv"}"#).unwrap();
    }

    /// Write `snapshot.json.gz` with the given commitId and data.
    fn write_snapshot(ns: &Path, segment: u64, offset: u64, data: Value) {
        let doc = json!({"commitId": {"segment": segment, "offset": offset}, "ts": 1700000000000u64, "data": data});
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        gz.write_all(doc.to_string().as_bytes()).unwrap();
        std::fs::write(ns.join("snapshot.json.gz"), gz.finish().unwrap()).unwrap();
    }

    /// Write one WAL segment file from raw lines (joined with \n).
    fn write_wal(ns: &Path, filename: &str, lines: &[&str]) {
        let wal = ns.join("wal");
        std::fs::create_dir_all(&wal).unwrap();
        let body = lines.join("\n");
        if filename.ends_with(".gz") {
            let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
            gz.write_all(body.as_bytes()).unwrap();
            std::fs::write(wal.join(filename), gz.finish().unwrap()).unwrap();
        } else {
            std::fs::write(wal.join(filename), body).unwrap();
        }
    }

    fn upd(key: &str, values: Value) -> String {
        json!({"ts": 1700000000001u64, "ops": [{"op": "upd", "key": key, "values": values}]}).to_string()
    }

    #[test]
    fn snapshot_alone_loads_when_there_is_no_wal() {
        let d = tempfile::tempdir().unwrap();
        write_driver(d.path());
        write_snapshot(d.path(), 1, 0, json!({"credentials": {"fabric": {"label": "x"}}}));
        let db = JsDb::open_namespace(d.path()).unwrap();
        assert_eq!(db.field("credentials", "fabric").unwrap()["label"], json!("x"));
        assert!(db.get("nope").is_none());
    }

    #[test]
    fn wal_replay_merges_updates_and_creates_new_contexts() {
        let d = tempfile::tempdir().unwrap();
        write_driver(d.path());
        write_snapshot(d.path(), 0, 0, json!({"a": {"kept": 1, "overwritten": 1}}));
        write_wal(d.path(), "00000001.jsonl", &[
            &upd("a", json!({"overwritten": 2, "added": 3})),
            &upd("brand.new", json!({"x": true})),
        ]);
        let db = JsDb::open_namespace(d.path()).unwrap();
        let a = db.get("a").unwrap();
        assert_eq!(a["kept"], json!(1));
        assert_eq!(a["overwritten"], json!(2));
        assert_eq!(a["added"], json!(3));
        assert_eq!(db.field("brand.new", "x"), Some(&json!(true)));
    }

    /// The commitId boundary: lines at or before the snapshot's commitId are
    /// already IN the snapshot; re-applying them must not resurrect state.
    /// Blank lines still consume an offset (WalReader.ts counts, then skips).
    #[test]
    fn replay_starts_strictly_after_the_snapshot_commit_id() {
        let d = tempfile::tempdir().unwrap();
        write_driver(d.path());
        write_snapshot(d.path(), 1, 1, json!({"a": {}}));
        write_wal(d.path(), "00000001.jsonl", &[
            &upd("a", json!({"resurrected": true})), // offset 0: <= (1,1), skip
            "",                                       // offset 1: blank, but COUNTS
            &upd("a", json!({"fresh": true})),        // offset 2: > (1,1), apply
        ]);
        let db = JsDb::open_namespace(d.path()).unwrap();
        let a = db.get("a").unwrap();
        assert!(a.get("resurrected").is_none(), "re-applied a pre-snapshot commit");
        assert_eq!(a["fresh"], json!(true), "blank lines must still count toward offsets");
    }

    #[test]
    fn all_three_del_shapes_apply_exactly_as_matter_js_does() {
        let d = tempfile::tempdir().unwrap();
        write_driver(d.path());
        write_snapshot(d.path(), 0, 0, json!({
            "a": {"f1": 1, "f2": 2},
            "nodes": {"commissionedNodes": 1},
            "nodes.peer1": {"x": 1},
            "nodes.peer1.endpoints.0.62": {"1": 1},
            "nodesibling": {"survives": true},
        }));
        write_wal(d.path(), "00000001.jsonl", &[
            // field delete
            &json!({"ts": 1, "ops": [{"op": "del", "key": "a", "values": ["f1"]}]}).to_string(),
            // subtree delete: "nodes" AND every "nodes.*" context — but NOT "nodesibling"
            &json!({"ts": 2, "ops": [{"op": "del", "key": "nodes"}]}).to_string(),
        ]);
        let db = JsDb::open_namespace(d.path()).unwrap();
        assert!(db.field("a", "f1").is_none());
        assert_eq!(db.field("a", "f2"), Some(&json!(2)));
        assert!(db.get("nodes").is_none());
        assert!(db.get("nodes.peer1").is_none());
        assert!(db.get("nodes.peer1.endpoints.0.62").is_none());
        assert_eq!(db.field("nodesibling", "survives"), Some(&json!(true)));
    }

    #[test]
    fn del_with_empty_key_and_no_values_clears_everything() {
        let d = tempfile::tempdir().unwrap();
        write_driver(d.path());
        write_snapshot(d.path(), 0, 0, json!({"a": {"x": 1}, "b": {"y": 2}}));
        write_wal(d.path(), "00000001.jsonl", &[
            &json!({"ts": 1, "ops": [{"op": "del", "key": ""}]}).to_string(),
            &upd("after", json!({"z": 3})),
        ]);
        let db = JsDb::open_namespace(d.path()).unwrap();
        assert!(db.get("a").is_none());
        assert!(db.get("b").is_none());
        assert_eq!(db.field("after", "z"), Some(&json!(3)));
    }

    #[test]
    fn segments_replay_in_numeric_order_and_gz_is_preferred() {
        let d = tempfile::tempdir().unwrap();
        write_driver(d.path());
        write_snapshot(d.path(), 0, 0, json!({}));
        write_wal(d.path(), "00000002.jsonl", &[&upd("a", json!({"v": "seg2"}))]);
        write_wal(d.path(), "00000001.jsonl", &[&upd("a", json!({"v": "seg1"}))]);
        // A .gz twin of segment 1 with different content: the .gz must win.
        write_wal(d.path(), "00000001.jsonl.gz", &[&upd("a", json!({"v": "seg1-gz", "gz": true}))]);
        let db = JsDb::open_namespace(d.path()).unwrap();
        assert_eq!(db.field("a", "v"), Some(&json!("seg2")), "segment 2 must replay after 1");
        assert_eq!(db.field("a", "gz"), Some(&json!(true)), "the .gz segment variant must be preferred");
    }

    #[test]
    fn a_legacy_bare_array_line_is_a_commit() {
        let d = tempfile::tempdir().unwrap();
        write_driver(d.path());
        write_snapshot(d.path(), 0, 0, json!({}));
        write_wal(d.path(), "00000001.jsonl", &[
            &json!([{"op": "upd", "key": "a", "values": {"legacy": true}}]).to_string(),
        ]);
        let db = JsDb::open_namespace(d.path()).unwrap();
        assert_eq!(db.field("a", "legacy"), Some(&json!(true)));
    }

    /// The spec's rule, deliberately stricter than matter.js (which warns and
    /// skips): a corrupt or truncated WAL is a hard error naming the line.
    #[test]
    fn a_malformed_wal_line_is_a_hard_error_naming_the_line() {
        let d = tempfile::tempdir().unwrap();
        write_driver(d.path());
        write_snapshot(d.path(), 0, 0, json!({}));
        write_wal(d.path(), "00000001.jsonl", &[
            &upd("a", json!({"x": 1})),
            "{ truncated",
        ]);
        let err = JsDb::open_namespace(d.path()).unwrap_err();
        match err {
            JsdbError::BadWalLine { line_number, ref path, .. } => {
                assert_eq!(line_number, 2);
                assert!(path.ends_with("wal/00000001.jsonl"));
            }
            other => panic!("expected BadWalLine, got {other}"),
        }
        // Unknown op kinds and non-object lines are the same hard error.
        write_wal(d.path(), "00000001.jsonl", &[
            &json!({"ts": 1, "ops": [{"op": "zap", "key": "a"}]}).to_string(),
        ]);
        assert!(matches!(JsDb::open_namespace(d.path()), Err(JsdbError::BadWalLine { line_number: 1, .. })));
    }

    #[test]
    fn missing_snapshot_replays_the_whole_wal_and_missing_wal_is_fine() {
        let d = tempfile::tempdir().unwrap();
        write_driver(d.path());
        write_wal(d.path(), "00000001.jsonl", &[&upd("a", json!({"x": 1}))]);
        let db = JsDb::open_namespace(d.path()).unwrap();
        assert_eq!(db.field("a", "x"), Some(&json!(1)));

        let d2 = tempfile::tempdir().unwrap();
        write_driver(d2.path());
        write_snapshot(d2.path(), 0, 0, json!({"only": {"snap": 1}}));
        let db2 = JsDb::open_namespace(d2.path()).unwrap();
        assert_eq!(db2.field("only", "snap"), Some(&json!(1)));
    }

    #[test]
    fn driver_json_must_declare_a_wal_kv_store() {
        let d = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(d.path()).unwrap();
        std::fs::write(d.path().join("driver.json"), br#"{"kind":"jsonfile","type":"kv"}"#).unwrap();
        assert!(matches!(JsDb::open_namespace(d.path()), Err(JsdbError::NotWalKv { .. })));
    }

    /// Store-root discovery: exactly one server-* namespace; python-migration
    /// leftovers (chip_*.ini, certificates/, credentials/) must not confuse it.
    #[test]
    fn open_store_finds_exactly_one_server_namespace() {
        let root = tempfile::tempdir().unwrap();
        let ns = root.path().join("server-1-fff1");
        write_driver(&ns);
        write_snapshot(&ns, 0, 0, json!({"a": {"x": 1}}));
        // Distractors: other namespaces and python leftovers.
        std::fs::create_dir_all(root.path().join("client")).unwrap();
        std::fs::create_dir_all(root.path().join("certificates")).unwrap();
        std::fs::write(root.path().join("chip_config.ini"), b"").unwrap();

        let (db, name) = JsDb::open_store(root.path()).unwrap();
        assert_eq!(name, "server-1-fff1");
        assert_eq!(db.field("a", "x"), Some(&json!(1)));

        // None found → NoNamespace listing what IS there.
        let empty = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(empty.path().join("client")).unwrap();
        match JsDb::open_store(empty.path()).unwrap_err() {
            JsdbError::NoNamespace { found, .. } => assert!(found.contains(&"client".to_string())),
            other => panic!("expected NoNamespace, got {other}"),
        }

        // Two found → refuse loudly rather than pick one.
        let ns2 = root.path().join("server-2-1234");
        write_driver(&ns2);
        assert!(matches!(JsDb::open_store(root.path()), Err(JsdbError::MultipleNamespaces { count: 2, .. })));
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p matter-rs-migrate jsdb`
Expected: FAIL (nothing implemented).

- [ ] **Step 3: Implement the reader**

Key implementation points (mirroring `WalReader.ts` / `WalSnapshot.ts` / `applyCommit` — cited in the module doc):

```rust
//! Read-only reader for matter.js's WAL KV store format (matter.js v0.17.9,
//! packages/general/src/storage/wal/). Snapshot-then-replay: the snapshot's
//! `commitId` marks the last (segment, offset) it already includes; only
//! strictly later WAL commits apply. Values stay raw serde_json::Value —
//! tagged strings are `decode`'s business, not ours.
//!
//! One deliberate deviation: matter.js skips malformed WAL lines with a
//! warning; we hard-error naming the line, because a migration that silently
//! drops commits is the failure mode most likely to look like success.

use std::collections::BTreeMap;
use std::io::Read as _;
use std::path::{Path, PathBuf};

use serde_json::Value;
```

- `open_store`: `read_dir(root)`, collect directory names; namespaces = names starting with `"server-"`. 0 → `NoNamespace { found: <all entry names, sorted> }`; ≥2 → `MultipleNamespaces`; 1 → `open_namespace(root.join(name))`.
- `open_namespace`:
  1. Read `driver.json`; parse `{kind, type}`; require `kind == "wal" && type == "kv"`, else `NotWalKv { found: <raw driver.json content, trimmed> }`.
  2. Snapshot: if both `snapshot.json.gz` and `snapshot.json` exist, pick the newer mtime (tie → the .gz); decompress `.gz` via `flate2::read::GzDecoder`; parse into a private struct:
     ```rust
     #[derive(serde::Deserialize)]
     struct SnapshotFile { #[serde(rename = "commitId")] commit_id: CommitId, data: BTreeMap<String, serde_json::Map<String, Value>> }
     #[derive(serde::Deserialize, Clone, Copy, PartialEq, PartialOrd, Eq, Ord)]
     struct CommitId { segment: u64, offset: u64 }
     ```
     (Field-order derive of `Ord` on `(segment, offset)` is exactly `compareCommitIds`.) Neither file → empty data, `after = None`. Parse failure → `BadSnapshot`. **Add `serde = { workspace = true }` to `crates/migrate/Cargo.toml` dependencies for the derive.**
  3. WAL: list `wal/` (absent → done). A file is a segment iff its name matches `^[0-9a-f]{8}\.jsonl(\.gz)?$` case-insensitively — parse the 8 hex digits into a segment number, collect into a `BTreeMap<u64, PathBuf>` where a `.gz` path **overwrites** a plain one for the same segment (gz preferred). Iterate ascending; skip segments `< after.segment`.
  4. Per segment: read all lines (gz-decompress first if needed; split on `'\n'`). For each line, 0-based `offset` counts every line; skip lines that are empty after `trim()`; skip when `Some(CommitId { segment, offset }) <= after`; otherwise parse and apply.
  5. Parse a line: `serde_json::from_str::<Value>` → error = `BadWalLine`. An array = legacy commit (its elements are the ops); an object = `{ts, ops}` where `ops` must be an array — anything else is `BadWalLine`.
  6. Apply an op (this is `applyCommit` verbatim):
     ```rust
     match op kind {
         "upd" => { values must be an object; data.entry(key).or_default() then insert each field }
         "del" if values is an array of strings => { if let Some(ctx) = data.get_mut(key) { for f in values { ctx.remove(f); } } }
         "del" if key.is_empty() => data.clear(),
         "del" => { data.remove(key); let prefix = format!("{key}."); data.retain(|k, _| !k.starts_with(&prefix)); }
         _ => return BadWalLine ("unknown op kind"),
     }
     ```
     A `del` whose `values` is present but not an array of strings → `BadWalLine` (matter.js validates the same in `deserializeCommit`).
- All file reads map IO errors to `JsdbError::Io { path }`. **No file is ever opened for writing.**

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p matter-rs-migrate jsdb`
Expected: PASS (all 11 tests).

- [ ] **Step 5: Commit**

```bash
git add crates/migrate
git commit -m "feat(migrate): matter.js WAL KV reader (snapshot + replay, hard-error on corruption)"
```

---

### Task 3: `stack::migration` — the rs-matter helpers

**Files:**
- Create: `crates/stack/src/migration.rs` (+ its `#[cfg(test)]` tests)
- Modify: `crates/stack/src/lib.rs:6-13` (add `pub mod migration;` to the module list)
- Modify: `crates/stack/src/identity.rs` (visibility only: `fn generate_usable_rcac` → `pub(crate) fn`, `fn serial_is_der_canonical` → `pub(crate) fn`; no behaviour change)

**Interfaces:**
- Consumes: `matter_rs_controller::stack_api::{StackError, StackErrorKind}`, `matter_rs_controller::storage::{ServerIdentity, IDENTITY_VERSION}`, `crate::identity::{generate_usable_rcac, serial_is_der_canonical}`, `crate::tlv_json::MATTER_EPOCH_OFFSET_S`.
- Produces (used by Tasks 4, 5, 6 — **all errors are `StackError`, never rs-matter types**, so the tool never needs rs-matter in scope):
  ```rust
  pub fn identity_from_preserved_ca(
      ca_private_key: &[u8], rcac_tlv: &[u8],
      fabric_id: u64, vendor_id: u16, node_id: u64, ipk_epoch_key: &[u8],
  ) -> Result<ServerIdentity, StackError>;
  pub fn verify_identity(id: &ServerIdentity) -> Result<(), StackError>;
  pub fn derive_operational_ipk(ipk_epoch_key: &[u8], compressed_fabric_id: u64) -> Result<Vec<u8>, StackError>;
  pub fn rcac_public_key(rcac_tlv: &[u8]) -> Result<Vec<u8>, StackError>;
  pub fn rcac_serial_is_der_canonical(rcac_tlv: &[u8]) -> Result<bool, StackError>;
  pub fn generate_ca(fabric_id: u64) -> Result<(Vec<u8>, Vec<u8>), StackError>; // (ca_private_key, rcac_tlv) — fixture/test helper
  ```

- [ ] **Step 1: Write the failing tests**

In `crates/stack/src/migration.rs`, tests first (stub the functions with `Err(StackError::new(StackErrorKind::Sdk, "unimplemented".into()))` bodies so it compiles — check `StackError::new`'s message parameter type in `crates/controller/src/stack_api.rs` and adjust `into()`s accordingly):

```rust
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p matter-rs-stack migration`
Expected: FAIL (stubs).

- [ ] **Step 3: Implement the module**

```rust
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
    let rcac_fabric_id = CertRef::new(TLVElement::new(rcac_tlv))
        .get_fabric_id()
        .map_err(|e| sdk_err("rcac_tlv does not parse as a Matter TLV certificate", e))?;
    if rcac_fabric_id != fabric_id {
        return Err(StackError::new(
            StackErrorKind::Sdk,
            format!("the preserved RCAC carries fabric id {rcac_fabric_id}, not the requested {fabric_id}"),
        ));
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
```

Adjustment notes for the implementer:
- Check `StackError::new`'s exact signature in `crates/controller/src/stack_api.rs` (it may take `impl Into<String>` — adapt the `format!` calls if needed).
- `CanonAeadKey`/`CanonPkcSecretKey` accessors: `access()` exists on the owned canon types (see `crates/stack/src/identity.rs:259-263` for prior art). `CanonPkcSecretKey::try_from(&[u8])` is the constructor `crates/stack/src/identity.rs:385-387` (`canon_secret_key`, already `pub(crate)`) uses — call either form.
- If `verify_chain_start`'s builder API differs from the sketch, the working pattern is in this repo at `crates/stack/src/identity.rs:552-562` (`persisted_ca_key_still_signs_a_device_noc`).
- In `identity.rs`, change exactly two `fn` items to `pub(crate) fn`: `generate_usable_rcac` (`crates/stack/src/identity.rs:290`) and `serial_is_der_canonical` (`crates/stack/src/identity.rs:368`). Touch nothing else there.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p matter-rs-stack migration`
Expected: PASS (8 tests).

- [ ] **Step 5: Make sure nothing else in the stack regressed**

Run: `cargo test -p matter-rs-stack`
Expected: PASS (the existing identity/ops/tlv_json suites are untouched).

- [ ] **Step 6: Commit**

```bash
git add crates/stack
git commit -m "feat(stack): migration helpers — mint identity from a preserved CA, IPK/compressed-id KDFs"
```

---

### Task 4: `convert` — the mapping table

**Files:**
- Create: `crates/migrate/src/convert.rs` (+ its `#[cfg(test)]` tests)
- Modify: `crates/migrate/src/lib.rs` (add `pub mod convert;`)

**Interfaces:**
- Consumes: `crate::jsdb::JsDb` (incl. `from_data` in tests), `crate::decode`, `matter_rs_controller::storage::{ConfigData, NodeRecord, normalize_fabric_label, format_node_date}`, `matter_rs_controller::addr::ip_of`.
- Produces (used by Task 5):
  ```rust
  pub struct SourceFabric {
      pub fabric_id: u64,
      pub controller_node_id: u64,   // fabric.nodeId (112233 on the reference install)
      pub vendor_id: u16,            // fabric.rootVendorId
      pub label: String,             // fabric.label ("" on the reference install)
      pub ipk_epoch_key: Vec<u8>,    // fabric.identityProtectionKey (16 bytes)
      pub operational_ipk: Vec<u8>,  // fabric.operationalIdentityProtectionKey (16 bytes; check-3 oracle)
      pub operational_id: Vec<u8>,   // fabric.operationalId (8 bytes; check-1 oracle)
      pub ca_private_key: Vec<u8>,   // certificates.rootKeyPair.privateKey (32 bytes)
      pub rcac_tlv: Vec<u8>,         // certificates.rootCertBytes
  }
  pub fn read_source_fabric(db: &JsDb) -> Result<SourceFabric, ConvertError>;

  #[derive(Debug, Clone, PartialEq)]
  pub enum FabricIndexSource {
      MatchedByRootPublicKey,
      FallbackZero(String), // the reason, verbatim in the report
  }
  pub struct NodePlan { pub record: NodeRecord, pub fabric_index: FabricIndexSource }
  pub fn plan_nodes(db: &JsDb, root_public_key: &[u8]) -> Result<Vec<NodePlan>, ConvertError>;

  pub fn config_from(source: &SourceFabric, nodes: &[NodePlan]) -> ConfigData;

  #[derive(Debug, thiserror::Error)]
  pub enum ConvertError {
      #[error("missing {0} in the matter.js store")]
      Missing(&'static str),
      #[error("{context}: {source}")]
      Decode { context: String, #[source] source: crate::decode::DecodeError },
      #[error("{0}")]
      Invalid(String),
  }
  ```

**Mapping rules (from the spec's table, made exact):**
- `SourceFabric` reads context `"credentials"` field `"fabric"` (an object) and context `"certificates"` fields `"rootKeyPair"` (object with `"privateKey"`) and `"rootCertBytes"`. Every missing context/field is `Missing("credentials.fabric.fabricId")`-style, naming the full dotted path. Length validation: `ipk_epoch_key` and `operational_ipk` exactly 16 bytes, `operational_id` exactly 8, `ca_private_key` exactly 32 — anything else is `Invalid` naming the field and both lengths. `vendor_id` must fit `u16`. `label` may be absent → `""`.
- Node records come from context `"nodes"` field `"commissionedNodes"` (a tagged Map; keys are tagged node ids). An absent field or context → `Ok(vec![])` (a fabric with zero commissioned nodes is legal; check 4 still balances because both sides count the same source).
- Per node: `date_commissioned` = `format_node_date(UNIX_EPOCH + Duration::from_millis(discoveryData.discoveredAt))`; `last_interview` = same string. A missing `discoveredAt` falls back to `format_node_date(SystemTime::now())` — the field is cosmetic, and inventing "now" is honest ("we first saw it at migration time") where failing the whole migration for it would not be.
- `addresses` = `vec![ip_of(operationalServerAddress.ip)]` when present (bracket-free per `controller::addr`), else `vec![]`.
- `attributes` = `serde_json::Map::new()` — always empty, per spec.
- `device_fabric_index`: matched from context `"nodes.peer{node_id}.endpoints.0.62"` field `"1"` (the cached Operational Credentials `fabrics` attribute — an array of objects `{rootPublicKey, vendorId, fabricId, nodeId, label, fabricIndex}`). Take the entries whose `rootPublicKey` bytes equal `root_public_key`; **exactly one** match with `1 <= fabricIndex <= 254` → that index, `MatchedByRootPublicKey`. Zero matches, multiple matches, a missing/unparseable cache, or an out-of-range index → index `0`, `FallbackZero(reason)`. **A matching failure is never an abort and never a guess** — `0` is invalid in Matter, so `RemoveFabric(0)` is rejected by the device and the code at `crates/controller/src/commands/nodes.rs:68-70` already degrades to local-only removal. Decode errors *inside the cache lookup* are folded into the `FallbackZero` reason rather than propagated (the cache is best-effort; the fabric identity fields above are not).
- `config_from`: `fabric_label = normalize_fabric_label(Some(&source.label))` (empty → `"HomeAssistant"`, trim, 32 chars); `next_node_id = max(node ids) + 1`, or `1` with no nodes; `wifi_credentials`/`thread_datasets` empty.

- [ ] **Step 1: Write the failing tests**

```rust
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
```

Also add to `crates/migrate/src/jsdb.rs` (if not already done in Task 2):

```rust
/// Test/support constructor: a store from already-replayed data.
pub fn from_data(data: BTreeMap<String, serde_json::Map<String, Value>>) -> JsDb {
    JsDb { data }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p matter-rs-migrate convert`
Expected: FAIL.

- [ ] **Step 3: Implement the mapping**

Implementation notes beyond the rules above:

```rust
//! The spec's mapping table, executable. Reads replayed matter.js state and
//! produces the inputs for `server.json` / `config.json` / `nodes/<id>.json`.
//! Identity fields are strict (a broken fabric must not migrate); the
//! per-device fabric-index match is best-effort with a loud fallback to 0
//! (invalid in Matter, so RemoveFabric(0) is rejected instead of evicting
//! someone else's admin — fail safe, never plausible).
```

- A small private helper keeps the `Missing`/`Decode` plumbing flat:
  ```rust
  fn need<'a>(m: &'a serde_json::Map<String, Value>, ctx: &'static str, field: &'static str, full: &'static str) -> Result<&'a Value, ConvertError>
  ```
  (or inline `ok_or(ConvertError::Missing("credentials.fabric.fabricId"))` per field — either is fine; every `Missing` carries the full dotted path as a `&'static str`).
- `plan_nodes` sorts by `node_id` ascending (deterministic reports and files).
- `match_fabric_index(db, node_id, root_public_key) -> (u8, FabricIndexSource)` is a private fn; its reasons are full sentences, e.g. `"no cached fabrics attribute at nodes.peer23.endpoints.0.62 — removing this device will leave our fabric behind on it"`, `"no fabric in the cached table carries our root public key"`, `"2 fabrics in the cached table carry our root public key; refusing to pick"`.
- `dates`: `std::time::UNIX_EPOCH + std::time::Duration::from_millis(ms)` into `format_node_date`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p matter-rs-migrate convert`
Expected: PASS (7 tests).

- [ ] **Step 5: Commit**

```bash
git add crates/migrate
git commit -m "feat(migrate): matter.js -> matter-rs storage mapping with fail-safe fabric-index matching"
```

---

### Task 5: Self-checks, report, `run()`, and the CLI

**Files:**
- Create: `crates/migrate/src/checks.rs` (+ its `#[cfg(test)]` tests)
- Modify: `crates/migrate/src/lib.rs` (add `pub mod checks;` plus `Options`/`Report`/`MigrateError`/`run()` and their `#[cfg(test)]` tests)
- Modify: `crates/migrate/src/main.rs` (real CLI)

**Interfaces:**
- Consumes: everything from Tasks 1–4 plus `matter_rs_stack::migration::{identity_from_preserved_ca, verify_identity, derive_operational_ipk, rcac_public_key, rcac_serial_is_der_canonical}` and `matter_rs_controller::storage::Storage`.
- Produces (used by Task 6 and the binary):
  ```rust
  // checks.rs
  pub struct CheckOutcome { pub name: &'static str, pub passed: bool, pub detail: String }
  pub fn run_all(
      identity: &ServerIdentity, source: &SourceFabric,
      config: &ConfigData, nodes: &[NodePlan],
  ) -> Vec<CheckOutcome>;

  // lib.rs
  pub struct Options { pub from: PathBuf, pub to: PathBuf, pub write: bool }
  pub struct Report {
      pub namespace: String,
      pub fabric_id: u64,
      pub compressed_fabric_id: u64,
      pub vendor_id: u16,
      pub controller_node_id: u64,
      pub fabric_label: String,
      pub next_node_id: u64,
      pub nodes: Vec<(u64, u8, FabricIndexSource)>, // (node id, resolved index, how)
      pub checks: Vec<CheckOutcome>,
      pub rcac_serial_note: Option<String>,
      pub ignored_python_leftovers: Vec<String>,
      pub wrote: Option<PathBuf>, // Some(to) only after a successful --write
  }
  impl Report { pub fn ok(&self) -> bool /* every check passed */ }
  impl std::fmt::Display for Report { /* the human report, one screen */ }
  pub fn run(opts: &Options) -> Result<Report, MigrateError>;

  #[derive(Debug, thiserror::Error)]
  pub enum MigrateError {
      #[error(transparent)] Jsdb(#[from] crate::jsdb::JsdbError),
      #[error(transparent)] Convert(#[from] crate::convert::ConvertError),
      #[error("{0}")] Stack(String), // StackError flattened: "<kind>: <message>"
      #[error("self-checks failed; nothing was written")] ChecksFailed,
      #[error("--from and --to are the same directory")] SamePath,
      #[error("writing {path}: {source}")] Write { path: PathBuf, #[source] source: std::io::Error },
  }
  ```

**The five self-checks (spec, "Self-checks") as `run_all` implements them:**
1. `fabric-identity` — `identity.compressed_fabric_id.to_be_bytes() == source.operational_id[..]`. Detail carries both as hex (`{:016x}` vs `hex::encode`) so a failure is diagnosable from the report alone.
2. `admin-identity` — `matter_rs_stack::migration::verify_identity(identity)` passes **and** `identity.controller_node_id == source.controller_node_id`. Detail names the node id.
3. `ipk-choice` — `derive_operational_ipk(&identity.ipk, identity.compressed_fabric_id)? == source.operational_ipk`. This is the spec's proof that `identityProtectionKey` is the epoch key; a derivation *error* is a failed check, not a crash.
4. `node-accounting` — `config.next_node_id > n.record.node_id` for every node (vacuously true when empty, with `next_node_id == 1`), and the planned file count equals the commissioned-node count (same length by construction here; the write path re-verifies against `Storage::load_nodes().len()` after writing — see below).
5. `fabric-index-sanity` — every node is `MatchedByRootPublicKey` or literally `0`+`FallbackZero`; the detail **lists every fallback node with its reason**, because that list is the operator's warning that removing those devices leaves our fabric behind on them. A node with a non-zero index and a `FallbackZero` source (impossible by construction) fails the check — the check exists to catch a future refactor breaking the invariant.

**`run()` flow (order matters — every step before `--write` is read-only):**
1. Guard: canonicalize `from`; if `to` exists, canonicalize and compare — equal → `SamePath` (also `SamePath` when the raw paths are equal).
2. `JsDb::open_store(&opts.from)` → `(db, namespace)`.
3. `convert::read_source_fabric(&db)`.
4. `rcac_public_key(&source.rcac_tlv)` — doubles as the "rootCertBytes really is Matter TLV" validation; its error is decisive and early, per the spec's open risk.
5. `identity_from_preserved_ca(&source.ca_private_key, &source.rcac_tlv, source.fabric_id, source.vendor_id, source.controller_node_id, &source.ipk_epoch_key)`.
6. `plan_nodes(&db, &root_pk)`, `config_from(&source, &nodes)`.
7. `checks::run_all(...)`.
8. `rcac_serial_note`: when `rcac_serial_is_der_canonical(&source.rcac_tlv)? == false`, set the note (verbatim): `"the migrated RCAC's serial number is not canonical DER; the server will warn about it at every boot. For a migrated fabric this is EXPECTED and HARMLESS — every commissioned device already trusts this exact root. It limits nothing except commissioning new matter.js-based test devices."`
9. `ignored_python_leftovers`: scan the top level of `--from` for `chip_*.ini` files, `certificates/` and `credentials/` directories, and 16-lowercase-hex `*.json` files; list names only. (The spec: they will mislead a reader into thinking the CHIP files are the source of truth — the tool ignores them and says so.)
10. If any check failed: return the report with `wrote: None`; `run` still returns `Ok(report)` — the caller decides the exit code from `report.ok()`. **`--write` with failed checks writes nothing** (guard before step 11).
11. `--write` and all checks passed: `Storage::open(&opts.to)`; `storage.create_identity(&identity)` **first** (its refusal to overwrite an existing `server.json` is the only overwrite guard, per spec — surface its error as `MigrateError::Write` and write nothing else); then `storage.save_config(&config)`; then `storage.save_node(&plan.record)` for each. Finally re-verify: `storage.load_nodes().len() == nodes.len()` (the write half of check 4) — mismatch is `MigrateError::Write`-worthy.
12. Return the report with `wrote: Some(to)`.

Note dry-run must **not** call `Storage::open(&opts.to)` at all — `open` creates `nodes/` and `sessions/` directories, and a dry run must leave the destination untouched.

**`Display for Report`** prints, in order: the namespace and source path header; the identity block (fabric id, compressed fabric id as 16-hex, vendor id, controller node id, label, next node id); a node table — one line per node: id, resolved fabric index, `matched by root public key` or `FALLBACK 0 — <reason>`; each check as `ok fabric-identity — <detail>` / `FAILED ipk-choice — <detail>`; the RCAC note if set; the python-leftovers line if non-empty; and the final line `dry run — nothing written (pass --write to migrate)` or `wrote <path>`.

**`main.rs`:**

```rust
use clap::Parser;
use std::process::ExitCode;

/// One-shot matter.js -> matter-rs-server fabric migration. Reads a
/// matterjs-server storage directory, self-checks the fabric identity
/// offline, and (with --write) creates a matter-rs-server storage directory
/// serving the same fabric. The source store is never modified.
#[derive(Parser)]
#[command(name = "matter-rs-migrate", version)]
struct Cli {
    /// matter.js store root (e.g. /var/lib/matterjs-server). Opened read-only.
    #[arg(long)]
    from: std::path::PathBuf,
    /// matter-rs-server storage path to create. Refuses to overwrite an
    /// existing fabric (server.json).
    #[arg(long)]
    to: std::path::PathBuf,
    /// Actually write. Without this flag the tool reads, runs every
    /// self-check, prints what it would create, and exits non-zero on any
    /// failed check.
    #[arg(long, default_value_t = false)]
    write: bool,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let opts = matter_rs_migrate::Options { from: cli.from, to: cli.to, write: cli.write };
    match matter_rs_migrate::run(&opts) {
        Ok(report) => {
            println!("{report}");
            if report.ok() { ExitCode::SUCCESS } else { ExitCode::FAILURE }
        }
        Err(e) => {
            eprintln!("error: {e}");
            let mut source = std::error::Error::source(&e);
            while let Some(s) = source {
                eprintln!("  caused by: {s}");
                source = s.source();
            }
            ExitCode::FAILURE
        }
    }
}
```

- [ ] **Step 1: Write the failing checks tests**

In `crates/migrate/src/checks.rs`. Real key material via `matter_rs_stack::migration` so the checks are exercised against genuine derivations:

```rust
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
```

- [ ] **Step 2: Write the failing lib-level tests** (in `crates/migrate/src/lib.rs`'s `#[cfg(test)]`; the full end-to-end path is Task 6's integration suite — here only what does not need a store on disk)

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identical_from_and_to_are_refused() {
        let d = tempfile::tempdir().unwrap();
        let opts = Options { from: d.path().to_path_buf(), to: d.path().to_path_buf(), write: false };
        assert!(matches!(run(&opts), Err(MigrateError::SamePath)));
    }

    #[test]
    fn a_missing_source_store_is_a_named_error_not_a_panic() {
        let d = tempfile::tempdir().unwrap();
        let opts = Options {
            from: d.path().join("does-not-exist"),
            to: d.path().join("out"),
            write: false,
        };
        let err = run(&opts).unwrap_err();
        assert!(err.to_string().contains("does-not-exist") || matches!(err, MigrateError::Jsdb(_)), "{err}");
        assert!(!d.path().join("out").exists(), "dry-run pathing must not create the destination");
    }
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cargo test -p matter-rs-migrate checks && cargo test -p matter-rs-migrate --lib`
Expected: FAIL.

- [ ] **Step 4: Implement checks.rs, lib.rs (`run`, `Report`, `Display`), main.rs**

Per the interface block and flow above. `checks.rs` header comment:

```rust
//! The five offline self-checks (spec, "Self-checks"). All run in both modes;
//! any failure aborts before the first write. Checks 1-3 are what make booting
//! the migrated server against live devices a low-risk cutover step instead of
//! an experiment.
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p matter-rs-migrate`
Expected: PASS (decode + jsdb + convert + checks + lib).

- [ ] **Step 6: Smoke the binary**

Run: `cargo run -p matter-rs-migrate -- --help`
Expected: usage text showing `--from`, `--to`, `--write` with the doc comments above.

Run: `cargo run -p matter-rs-migrate -- --from /nonexistent --to /tmp/nope; echo "exit: $?"`
Expected: `error: ...` on stderr, `exit: 1`, and `/tmp/nope` not created.

- [ ] **Step 7: Commit**

```bash
git add crates/migrate
git commit -m "feat(migrate): five offline self-checks, dry-run report, guarded write, CLI"
```

---

### Task 6: Integration fixture generator + end-to-end tests

**Files:**
- Create: `crates/migrate/tests/migrate_store.rs` (fixture generator + integration tests in one file)

**Interfaces:**
- Consumes: `matter_rs_migrate::{run, Options}`, `matter_rs_migrate::convert::FabricIndexSource`, `matter_rs_stack::migration::{generate_ca, identity_from_preserved_ca, derive_operational_ipk, rcac_public_key}`, `matter_rs_controller::storage::Storage`, flate2 (gz writing), tempfile.
- Produces: nothing for later tasks — this is the gate that the tool and the server agree.

**What the fixture is.** A programmatically generated matter.js store with the reference install's shape and scale, written fresh into a tempdir by each test (nothing committed — see the header's deviation note):
- `driver.json` = `{"kind":"wal","type":"kv"}` in namespace `server-1-fff1`; python-migration leftovers at the store root (`chip_config.ini`, `certificates/`, `credentials/`, `ca88e679a3505b0a.json`) that the tool must ignore-and-report.
- **Self-consistent key material**: `generate_ca(1)` → mint once via `identity_from_preserved_ca` to learn the `compressed_fabric_id` → the store's `operationalId` = its big-endian bytes, `operationalIdentityProtectionKey` = `derive_operational_ipk(epoch, compressed)`, `identityProtectionKey` = the epoch key `[0x5a; 16]`. So checks 1–3 pass for the *right* reasons.
- **Stale snapshot + mandatory WAL replay** (the spec's "five nodes, ids up to 23"): the snapshot (`commitId {segment:1, offset:2}`) knows only nodes 10, 12, 21 and carries label `"stale"`; WAL offsets 0–2 are poison pills that would set label `"resurrected"` (replaying them = commitId-boundary bug); offsets 3+ hold ~40,000 filler attribute-cache `upd` lines (scale realism: the reference WAL is 38,723 lines), then the real mutations: the full 5-node `commissionedNodes` Map (10, 12, 21, 22, 23), label set to `""`, one field-`del`, one subtree-`del` of a decoy context, and the per-device Operational Credentials caches. A **second segment `00000002.jsonl.gz`** carries the last few commits (gz + multi-segment coverage).
- **Device fabric tables**: node 10 caches three fabrics with ours at **index 3** behind two foreign ones (the spec's `peer1` trap); nodes 12, 21, 22 cache a single matching entry (indices 2, 2, 4); node 23 (the Thread node, address `fd6a::1`) has **no cache** → fallback 0.
- **Addresses**: node 10 `192.168.1.60` (plain IPv4), node 22 `[fe80::9%eth1]` (bracketed — must come out bracket-free), node 23 `fd6a::1`, node 12 no `operationalServerAddress` at all.

- [ ] **Step 1: Write the fixture generator + failing tests**

```rust
//! End-to-end: generate a matter.js store with the reference install's shape
//! (stale snapshot, ~40k-line WAL, multi-admin device caches), migrate it, and
//! read the result back through the server's own Storage. This is the gate
//! that the tool and the server agree on the format.

use std::collections::BTreeMap;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use matter_rs_controller::storage::Storage;
use matter_rs_migrate::convert::FabricIndexSource;
use matter_rs_migrate::{run, Options};
use matter_rs_stack::migration::{
    derive_operational_ipk, generate_ca, identity_from_preserved_ca, rcac_public_key,
};
use serde_json::{json, Value};

const EPOCH_KEY: [u8; 16] = [0x5a; 16];

// ---- tagged-value emitters (matter.js toJson, StringifyTools.ts) ----

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

struct Fixture {
    store_root: PathBuf,
    ca_private_key: Vec<u8>,
    rcac_tlv: Vec<u8>,
    compressed_fabric_id: u64,
    root_public_key: Vec<u8>,
}

fn foreign_root_pk(fill: u8) -> Vec<u8> {
    let mut pk = vec![0x04];
    pk.extend_from_slice(&[fill; 64]);
    pk
}

fn fabric_entry(pk: &[u8], index: u64, label: &str) -> Value {
    json!({
        "rootPublicKey": bytes(pk), "vendorId": 4996, "fabricId": bigint(1),
        "nodeId": bigint(112233), "label": label, "fabricIndex": index,
    })
}

fn commissioned_node(id: u64, addr: Option<&str>, discovered_ms: u64) -> (Value, Value) {
    let mut v = json!({
        "discoveryData": {"discoveredAt": discovered_ms},
        "deviceData": {"basicInformation": {"vendorId": 4996, "productId": 1}},
    });
    if let Some(ip) = addr {
        v["operationalServerAddress"] = json!({"type": "udp", "ip": ip, "port": 5540});
    }
    (bigint(id), v)
}

/// The credentials.fabric object, parameterised by label so both the stale
/// snapshot and the poison-pill WAL lines can reuse it.
fn fabric_field(f: &Fixture, label: &str) -> Value {
    json!({
        "fabricId": bigint(1),
        "nodeId": bigint(112233),
        "rootNodeId": bigint(112233),
        "rootVendorId": 65521,
        "identityProtectionKey": bytes(&EPOCH_KEY),
        "operationalIdentityProtectionKey": bytes(&derive_operational_ipk(&EPOCH_KEY, f.compressed_fabric_id).unwrap()),
        "operationalId": bytes(&f.compressed_fabric_id.to_be_bytes()),
        "label": label,
    })
}

fn gz(data: &[u8]) -> Vec<u8> {
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    enc.write_all(data).unwrap();
    enc.finish().unwrap()
}

fn upd_line(key: &str, values: Value) -> String {
    json!({"ts": 1700000000000u64, "ops": [{"op": "upd", "key": key, "values": values}]}).to_string()
}

/// Build the whole store under `root`. ~40k WAL lines; runs in well under a
/// second in release-test, a few seconds in debug — acceptable for the gate.
fn build_fixture(root: &Path) -> Fixture {
    let (ca_private_key, rcac_tlv) = generate_ca(1).unwrap();
    let minted =
        identity_from_preserved_ca(&ca_private_key, &rcac_tlv, 1, 0xFFF1, 112233, &EPOCH_KEY).unwrap();
    let f = Fixture {
        store_root: root.to_path_buf(),
        root_public_key: rcac_public_key(&rcac_tlv).unwrap(),
        compressed_fabric_id: minted.compressed_fabric_id,
        ca_private_key,
        rcac_tlv,
    };

    // Python-migration leftovers the tool must ignore-and-report.
    std::fs::create_dir_all(root.join("certificates")).unwrap();
    std::fs::create_dir_all(root.join("credentials")).unwrap();
    std::fs::write(root.join("chip_config.ini"), b"[legacy]\n").unwrap();
    std::fs::write(root.join(format!("{:016x}.json", f.compressed_fabric_id)), b"{}").unwrap();

    let ns = root.join("server-1-fff1");
    let wal = ns.join("wal");
    std::fs::create_dir_all(&wal).unwrap();
    std::fs::write(ns.join("driver.json"), br#"{"kind":"wal","type":"kv"}"#).unwrap();

    // Stale snapshot: 3 of 5 nodes, wrong label, commitId (1, 2).
    let snapshot = json!({
        "commitId": {"segment": 1, "offset": 2},
        "ts": 1699000000000u64,
        "data": {
            "credentials": {"fabric": fabric_field(&f, "stale")},
            "certificates": {
                "rootKeyPair": {"privateKey": bytes(&f.ca_private_key), "publicKey": bytes(&f.root_public_key)},
                "rootCertBytes": bytes(&f.rcac_tlv),
            },
            "nodes": {"commissionedNodes": map_tag(vec![
                commissioned_node(10, Some("192.168.1.60"), 1_690_000_000_000),
                commissioned_node(12, None, 1_690_000_100_000),
                commissioned_node(21, Some("192.168.1.61"), 1_690_000_200_000),
            ])},
            "decoy": {"gone": true},
            "decoy.child": {"gone": true},
        },
    });
    std::fs::write(ns.join("snapshot.json.gz"), gz(snapshot.to_string().as_bytes())).unwrap();

    // Segment 1: offsets 0-2 are poison pills already contained in the
    // snapshot — replaying them would set the label to "resurrected".
    let mut seg1: Vec<String> = vec![
        upd_line("credentials", json!({"fabric": fabric_field(&f, "resurrected")})),
        upd_line("credentials", json!({"fabric": fabric_field(&f, "resurrected")})),
        upd_line("credentials", json!({"fabric": fabric_field(&f, "resurrected")})),
    ];
    // Scale realism: ~40k filler attribute-cache updates (reference: 38,723).
    for i in 0..40_000u64 {
        seg1.push(upd_line(
            &format!("nodes.peer{}.endpoints.1.6", 10 + (i % 3)),
            json!({"0": i % 2 == 0}),
        ));
    }
    // The real mutations the snapshot is missing:
    seg1.push(upd_line("nodes", json!({"commissionedNodes": map_tag(vec![
        commissioned_node(10, Some("192.168.1.60"), 1_690_000_000_000),
        commissioned_node(12, None, 1_690_000_100_000),
        commissioned_node(21, Some("192.168.1.61"), 1_690_000_200_000),
        commissioned_node(22, Some("[fe80::9%eth1]"), 1_695_000_000_000),
        commissioned_node(23, Some("fd6a::1"), 1_699_999_999_999),
    ])})));
    seg1.push(upd_line("credentials", json!({"fabric": fabric_field(&f, "")})));
    // A field-del and a subtree-del, so replay exercises every op shape.
    seg1.push(json!({"ts": 1, "ops": [{"op": "del", "key": "decoy", "values": ["gone"]}]}).to_string());
    seg1.push(json!({"ts": 2, "ops": [{"op": "del", "key": "decoy"}]}).to_string());
    // Device fabric tables: node 10 is the spec's multi-admin trap.
    seg1.push(upd_line("nodes.peer10.endpoints.0.62", json!({"1": [
        fabric_entry(&foreign_root_pk(0xBB), 1, "Mijn huis"),
        fabric_entry(&foreign_root_pk(0xCC), 2, ""),
        fabric_entry(&f.root_public_key, 3, "HomeAssistant"),
    ]})));
    seg1.push(upd_line("nodes.peer12.endpoints.0.62", json!({"1": [fabric_entry(&f.root_public_key, 2, "HomeAssistant")]})));
    std::fs::write(wal.join("00000001.jsonl"), seg1.join("\n")).unwrap();

    // Segment 2, gzipped: the remaining device caches. Node 23 gets none.
    let seg2 = [
        upd_line("nodes.peer21.endpoints.0.62", json!({"1": [fabric_entry(&f.root_public_key, 2, "HomeAssistant")]})),
        upd_line("nodes.peer22.endpoints.0.62", json!({"1": [fabric_entry(&f.root_public_key, 4, "HomeAssistant")]})),
    ]
    .join("\n");
    std::fs::write(wal.join("00000002.jsonl.gz"), gz(seg2.as_bytes())).unwrap();

    f
}

/// Recursive (path, bytes) inventory, for proving the source was not touched.
fn dir_inventory(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut out = BTreeMap::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for e in std::fs::read_dir(&dir).unwrap().flatten() {
            let p = e.path();
            if p.is_dir() { stack.push(p); } else { out.insert(p.clone(), std::fs::read(&p).unwrap()); }
        }
    }
    out
}

#[test]
fn dry_run_checks_everything_and_writes_nothing() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let to = dst.path().join("matter-rs");
    build_fixture(src.path());
    let before = dir_inventory(src.path());

    let report = run(&Options { from: src.path().into(), to: to.clone(), write: false }).unwrap();

    assert!(report.ok(), "checks failed:\n{report}");
    assert_eq!(report.checks.len(), 5);
    assert_eq!(report.namespace, "server-1-fff1");
    assert!(report.wrote.is_none());
    assert!(!to.exists(), "dry run created the destination");
    assert_eq!(dir_inventory(src.path()), before, "the source store was modified");
    // Python leftovers ignored, and said so.
    assert!(report.ignored_python_leftovers.iter().any(|n| n == "chip_config.ini"), "{:?}", report.ignored_python_leftovers);
    // The report text lists the fallback node loudly.
    let text = report.to_string();
    assert!(text.contains("23") && text.to_uppercase().contains("FALLBACK"), "{text}");
}

#[test]
fn write_produces_a_store_the_server_code_reads_back() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let to = dst.path().join("matter-rs");
    let f = build_fixture(src.path());
    let before = dir_inventory(src.path());

    let report = run(&Options { from: src.path().into(), to: to.clone(), write: true }).unwrap();
    assert!(report.ok(), "{report}");
    assert_eq!(report.wrote.as_deref(), Some(to.as_path()));
    assert_eq!(dir_inventory(src.path()), before, "the source store was modified");

    // Read back through the server's own strict loader.
    let storage = Storage::open(&to).unwrap();
    let id = storage.load_identity().unwrap().expect("server.json must exist");
    assert_eq!(id.fabric_id, 1);
    assert_eq!(id.vendor_id, 0xFFF1);
    assert_eq!(id.controller_node_id, 112233);
    assert_eq!(id.compressed_fabric_id, f.compressed_fabric_id);
    assert_eq!(id.ca_private_key, f.ca_private_key);
    assert_eq!(id.rcac_tlv, f.rcac_tlv);
    assert_eq!(id.ipk, EPOCH_KEY.to_vec());
    matter_rs_stack::migration::verify_identity(&id).unwrap();

    let cfg = storage.load_config();
    assert_eq!(cfg.fabric_label, "HomeAssistant"); // empty source label -> default
    assert_eq!(cfg.next_node_id, 24);
    assert!(cfg.wifi_credentials.is_empty() && cfg.thread_datasets.is_empty());

    // WAL replay was mandatory: 22 and 23 exist only in the WAL.
    let nodes = storage.load_nodes();
    assert_eq!(nodes.iter().map(|n| n.node_id).collect::<Vec<_>>(), vec![10, 12, 21, 22, 23]);
    let by_id = |id: u64| nodes.iter().find(|n| n.node_id == id).unwrap();
    assert_eq!(by_id(10).device_fabric_index, 3, "the multi-admin trap");
    assert_eq!(by_id(12).device_fabric_index, 2);
    assert_eq!(by_id(21).device_fabric_index, 2);
    assert_eq!(by_id(22).device_fabric_index, 4);
    assert_eq!(by_id(23).device_fabric_index, 0, "no cache must mean 0, never a guess");
    assert_eq!(by_id(22).addresses, vec!["fe80::9%eth1".to_string()], "brackets must not survive");
    assert_eq!(by_id(23).addresses, vec!["fd6a::1".to_string()]);
    assert!(by_id(12).addresses.is_empty());
    for n in &nodes {
        assert!(n.attributes.is_empty(), "attribute caches must start empty");
        assert!(!n.date_commissioned.is_empty());
        assert_eq!(n.date_commissioned, n.last_interview);
    }
    // The commitId boundary held: the poison-pill label never surfaced.
    assert_ne!(cfg.fabric_label, "resurrected");
}

#[test]
fn a_second_write_refuses_to_overwrite_the_new_fabric() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let to = dst.path().join("matter-rs");
    build_fixture(src.path());

    run(&Options { from: src.path().into(), to: to.clone(), write: true }).unwrap();
    let server_json_before = std::fs::read(to.join("server.json")).unwrap();

    let err = run(&Options { from: src.path().into(), to: to.clone(), write: true }).unwrap_err();
    assert!(err.to_string().contains("server.json"), "{err}");
    assert_eq!(std::fs::read(to.join("server.json")).unwrap(), server_json_before);
}

#[test]
fn a_corrupt_wal_aborts_before_anything_is_written() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let to = dst.path().join("matter-rs");
    build_fixture(src.path());
    // Truncate the last WAL segment mid-line.
    let seg = src.path().join("server-1-fff1/wal/00000001.jsonl");
    let mut bytes = std::fs::read(&seg).unwrap();
    bytes.truncate(bytes.len() - 40);
    std::fs::write(&seg, bytes).unwrap();

    let err = run(&Options { from: src.path().into(), to: to.clone(), write: true }).unwrap_err();
    assert!(err.to_string().contains("00000001.jsonl"), "{err}");
    assert!(!to.exists(), "a failed migration left a partial destination");
}

/// The wrong-IPK store: identityProtectionKey and its operational twin
/// swapped. Everything ELSE is consistent — exactly the "looks correct, fails
/// subtly" fabric check 3 exists to catch. --write must write nothing.
#[test]
fn a_wrong_ipk_choice_fails_check_3_and_writes_nothing() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let to = dst.path().join("matter-rs");
    build_fixture(src.path());

    // Swap the two IPKs in the final WAL state by appending one more commit
    // (segment 3 replays last, so it wins). Read the current fabric object
    // back through the tool's own reader, swap the keys, re-emit.
    let (db, _) = matter_rs_migrate::jsdb::JsDb::open_store(src.path()).unwrap();
    let fabric = db.field("credentials", "fabric").unwrap().clone();
    let epoch = matter_rs_migrate::decode::as_bytes(&fabric["identityProtectionKey"]).unwrap();
    let op = matter_rs_migrate::decode::as_bytes(&fabric["operationalIdentityProtectionKey"]).unwrap();
    let mut swapped = fabric;
    swapped["identityProtectionKey"] = bytes(&op);
    swapped["operationalIdentityProtectionKey"] = bytes(&epoch);
    std::fs::write(
        src.path().join("server-1-fff1/wal/00000003.jsonl"),
        upd_line("credentials", json!({"fabric": swapped})),
    )
    .unwrap();

    let report = run(&Options { from: src.path().into(), to: to.clone(), write: true }).unwrap();
    assert!(!report.ok());
    let ipk = report.checks.iter().find(|c| c.name == "ipk-choice").unwrap();
    assert!(!ipk.passed, "{report}");
    assert!(report.wrote.is_none());
    assert!(!to.exists(), "failed checks must abort before the first write");
}
```

Implementation notes:
- The wrong-IPK test leans on `jsdb` and `decode` being `pub mod`s (they are, from Tasks 1–2). Its substance: append a WAL commit that swaps the two keys, then assert check 3 fails and nothing is written.
- Integration tests build inside the same package, so the crate's regular `[dependencies]` (`serde_json`, `flate2`, `hex`, `matter-rs-stack`, `matter-rs-controller`) are importable from `tests/` directly — no `[dev-dependencies]` additions should be needed beyond the existing `tempfile`. If the build says otherwise, add the named crate to `[dev-dependencies]` and move on.

- [ ] **Step 2: Run the tests to verify current state**

Run: `cargo test -p matter-rs-migrate --test migrate_store`
Expected: everything compiles and — because Tasks 1–5 are complete — these should largely PASS on the first run. Any failure here is a real integration bug (the units disagree about the format); debug it with superpowers:systematic-debugging, not by loosening the assertion.

- [ ] **Step 3: Run the full crate suite**

Run: `cargo test -p matter-rs-migrate`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/migrate
git commit -m "test(migrate): reference-shaped fixture generator + end-to-end migration gate"
```

---

### Task 7: Workspace verification, docs, wrap-up

**Files:**
- Modify: `docs/superpowers/specs/2026-08-14-matterjs-fabric-migration-design.md:3` (status line)

**Interfaces:** none — this is the finishing gate.

- [ ] **Step 1: Full workspace verification**

Run: `cargo test --workspace`
Expected: PASS — including the untouched server/controller/stack suites (269+ tests pre-existing).

Run: `cargo build --release -p matter-rs-migrate && ls -la target/release/matter-rs-migrate`
Expected: the release binary exists (it is what actually runs on CT 110 at cutover).

Run: `cargo clippy -p matter-rs-migrate -p matter-rs-stack 2>&1 | tail -5`
Expected: no new warnings in the touched crates (pre-existing warnings elsewhere are out of scope).

- [ ] **Step 2: Update the spec status line**

In `docs/superpowers/specs/2026-08-14-matterjs-fabric-migration-design.md`, change:

```markdown
**Status:** design approved 2026-08-14, plan not yet written.
```

to:

```markdown
**Status:** design approved 2026-08-14; plan written 2026-08-14
(`docs/superpowers/plans/2026-08-14-matterjs-fabric-migration.md`). The plan
settled two open questions from matter.js v0.17.9 source: `del` carries an
optional field list (fields-only delete when present; whole-key **plus
`key.`-prefixed subtree** delete when absent; clear-all when the key is empty),
and blank WAL lines still consume a commit offset.
```

- [ ] **Step 3: Commit**

```bash
git add docs/superpowers/specs/2026-08-14-matterjs-fabric-migration-design.md
git commit -m "docs: migration plan written; record confirmed WAL del semantics in the spec"
```

---

## Out of plan: manual acceptance (cutover) — for Jens, not for an executor

Recorded so the plan's end state is unambiguous; none of this runs in CI and none of it blocks the tasks above.

1. Copy the CT 109 store clone to the dev machine and run a **dry-run** first: `matter-rs-migrate --from <clone> --to /tmp/migrated`. Expected: all five checks pass; the report shows five nodes; `operationalId` check pins `ca88e679a3505b0a`; the RCAC-serial note likely appears (expected, harmless).
2. `--write`, then boot `matter-rs-server --storage-path /tmp/migrated` **with the Node server stopped** — never both: two controllers sharing node id 112233 evict each other's subscriptions in a loop (observed on the CT 109 clone; spec, "Online verification").
3. Confirm one real device responds to a command with no re-commissioning.
4. Before real cutover, answer the spec's open deployment question: `--primary-interface` is single-valued but the fabric spans `eth0`, `eth1`, and a Thread ULA — a wrong choice makes migration *look* failed.

## Self-review notes (performed while writing)

- **Spec coverage:** source format incl. `del` semantics → Task 2; tagged values incl. unknown-tag rejection → Task 1; the "What migrates" table → Task 4 (fields), Task 5 (write path), Task 3 (minted NOC, IPK, compressed id); the new stack helper → Task 3; all five self-checks → Task 5 (each pinned by a dedicated failing-input test); error handling (read-only source, atomic writes, `create_identity` refusal, abort-before-write) → Tasks 5 & 6; testing section → unit tests per task + Task 6 integration gate; risks: `rootCertBytes` encoding → hard early error via `rcac_public_key` (Tasks 3/5), RCAC serial warn → report note (Task 5), multi-admin fabric-index stakes → Tasks 4/6 trap tests, WAL-replay-mandatory → Task 6 stale-snapshot fixture, python leftovers → Tasks 2/5/6.
- **Known deviation** (flagged in the header): generated fixture instead of a committed redacted store copy.
- **Type consistency spot-checks:** `FabricIndexSource` spelled identically in Tasks 4/5/6; `SourceFabric` field list identical in Tasks 4/5; `Report.checks: Vec<CheckOutcome>` matches `checks::run_all`'s return; `JsDb::from_data` introduced in Task 2's interface and used in Task 4's tests; `generate_ca`/`identity_from_preserved_ca`/`derive_operational_ipk`/`rcac_public_key` signatures identical in Tasks 3/5/6; `StackError::new(kind, impl Into<String>)` verified against `crates/controller/src/stack_api.rs:31`.


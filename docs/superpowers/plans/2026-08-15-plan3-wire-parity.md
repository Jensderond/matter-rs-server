# Plan 3: Wire Parity — Converter Tightening + Report Merging Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the four wire-behaviour gaps left open at the plan-2 handoff — invalid-UTF-8 attribute strings (the live 0/52/0 skip), chunked lists split across subscription `ReportData` messages, sequential `ping_node`, and the `diagnostics` args question — and tighten the one converter deviation that shows on the wire (nested epoch fields), pinned by a golden fixture corpus.

**Architecture:** All changes live in `crates/stack` (TLV→JSON conversion, report accumulation) and `crates/controller` (the `ping_node` command). Node parity is argued from the matterjs-server sources checked out at `matterjs-server/` (read-only reference); rs-matter ground truth from `rs-matter-ref/` (pinned clone, gitignored — recreate at rev `03bc8f2` if absent; the fork commit `405064b` does not touch any file cited here).

**Tech Stack:** Rust workspace (`cargo test --workspace` must stay green), rs-matter via the fork pin, serde_json, tokio (controller side only — the stack crate is single-threaded, `RefCell`-based, no locks).

**Spec:** No single spec document. The requirements are: the "Still open" items in `docs/superpowers/plans/2026-08-13-plan2-execution-notes.md` (items 1, 2 of the numbered list), the "Known limitations (v1)" and "Accepted parity gaps" sections of `README.md`, and the Node-behaviour citations embedded in each task below (all verified 2026-08-15 against the local `matterjs-server/` checkout).

## Global Constraints

- `cargo test --workspace` green at every task boundary; compare `cargo clippy --workspace --all-targets` **by warning location, never by count** (known pre-existing: `storage.rs:313` is_multiple_of, `stack_api.rs:169` + `runtime.rs:649` type_complexity, 5 in `gen`'s build script).
- The stack crate runs on one thread; rs-matter futures are `!Send`. Never hold a `RefCell` borrow across an `.await`.
- Hand-formatted ~100 columns; the repo is deliberately not `cargo fmt`-clean — match surrounding style, never reformat.
- Error policy for report conversion is log-and-skip (one bad attribute must not discard an interview); do not change it.
- Node parity claims in code comments cite the matterjs-server source file and line the way existing comments cite `rs-matter-ref` paths.
- Update `README.md`'s limitation/gap lists in the same task that removes the limitation — the lists exist "so nobody fixes them silently", and the reverse also holds: nothing fixed may stay listed.

## File Structure

- `crates/stack/src/tlv_json.rs` — Tasks 1 (lossy UTF-8 leaf) and 5 (typed tag-based walk). Grows two functions; no split needed.
- `crates/stack/src/reports.rs` — Task 2: new `PendingReports` struct next to `AttrAccumulator`; `ReportSink::handle_report` shrinks to a call into it.
- `crates/stack/src/ctx.rs` — Task 2: one new `StackCtx` field.
- `crates/stack/src/supervisor.rs`, `crates/stack/src/runtime.rs` — Task 2: clear the new field at (re)subscribe and node removal.
- `crates/controller/src/commands/nodes.rs` — Task 3: concurrent ping.
- `crates/stack/tests/wire_fixtures.rs` + `crates/stack/tests/fixtures/attr_values.json` — Task 6 (new files).
- `README.md` — Tasks 2, 3, 5 each retire their bullet.

---

### Task 1: Lossy UTF-8 decode for attribute strings (the 0/52/0 skip)

**Why.** During the live migration acceptance, node 12's priming skipped attribute `0/52/0` with `TLVTypeMismatch`. `0/52/0` is SoftwareDiagnostics(52) `threadMetrics` (IDL: `crates/gen/idl/controller-clusters-V1.6.0.0.matter:2131`), a list of `ThreadMetricsStruct` whose field 1 `name` is `char_string<8>` (line 2119) — a thread-name buffer that real firmware fills with non-UTF-8 bytes. rs-matter's TLV reader hard-fails any invalid UTF-8 with `TLVTypeMismatch` (`rs-matter-ref/rs-matter/src/tlv/read.rs:229-240`), so `AttrAccumulator::absorb` logs "cannot convert 0/52/0" and drops the whole attribute. matter.js has no such failure mode: JavaScript string decoding replaces invalid sequences with U+FFFD, so the Node server reported this attribute fine. JSON cannot carry invalid UTF-8 either way, so lossy replacement is the only wire-compatible behaviour.

**Mechanism.** `TLVElement::value()` is what throws, so the fix must run *before* it: check `control().value_type.is_utf8()` (`rs-matter-ref/rs-matter/src/tlv.rs:186-191`) and take the raw payload via `octets()`, which accepts any variable-size element including UTF-8 strings (`rs-matter-ref/rs-matter/src/tlv/read.rs:516-524`) — note `str()` does NOT work here, `is_str()` matches octet strings only (`tlv.rs:178-183`).

**Files:**
- Modify: `crates/stack/src/tlv_json.rs` (`tlv_to_json_at`, `tlv_to_json_named_at`)
- Test: same file, `mod tests`

**Interfaces:**
- Produces: `fn lossy_utf8(elem: &TLVElement) -> Result<Option<Value>, Error>` (private to `tlv_json`), also called by Task 5's `typed_to_json`.

- [ ] **Step 1: Write the failing test**

In `tlv_json.rs`'s `mod tests` (a raw TLV element is control byte `0x0C` = UTF-8 string with 1-octet length, anonymous tag, then length, then payload):

```rust
/// Node 12's live 0/52/0 skip: SoftwareDiagnostics.threadMetrics carries a
/// char_string<8> thread name that real firmware fills with non-UTF-8 bytes.
/// rs-matter's TLVValue hard-fails those with TLVTypeMismatch
/// (rs-matter-ref/rs-matter/src/tlv/read.rs:229-240); matter.js decodes
/// lossily (JS string semantics), so Node reported the attribute fine. JSON
/// cannot carry invalid UTF-8 either way: replace, like Node, never drop.
#[test]
fn invalid_utf8_string_converts_lossily_instead_of_failing() {
    // Anonymous Utf8l(1-byte len): 0xFF is not valid UTF-8, 'b' is.
    let raw = [0x0C, 0x02, 0xFF, b'b'];
    let v = tlv_to_json(&TLVElement::new(&raw)).expect("lossy, not an error");
    assert_eq!(v, json!("\u{FFFD}b"));

    // Valid UTF-8 is byte-identical to before.
    let ok = [0x0C, 0x02, b'h', b'i'];
    assert_eq!(tlv_to_json(&TLVElement::new(&ok)).unwrap(), json!("hi"));

    // Octet strings (0x10 = 1-octet length) still go out as base64, not text.
    let oct = [0x10, 0x02, 0xFF, 0x00];
    assert_eq!(tlv_to_json(&TLVElement::new(&oct)).unwrap(), json!("/wA="));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p matter-rs-stack invalid_utf8_string_converts -- --nocapture`
Expected: FAIL — the first assertion errors with `TLVTypeMismatch` (the `expect` panics).

- [ ] **Step 3: Implement the lossy leaf**

In `tlv_json.rs`, above `tlv_to_json_at`:

```rust
/// A UTF-8 string leaf, decoded lossily (invalid sequences become U+FFFD).
///
/// `Ok(None)` when the element is not a UTF-8 string at all. Must run before
/// `TLVElement::value()`, which hard-fails invalid UTF-8 with TLVTypeMismatch
/// (`rs-matter-ref/rs-matter/src/tlv/read.rs:229-240`) — the failure that cost
/// node 12 its 0/52/0 report. matter.js decodes lossily, and JSON cannot carry
/// the raw bytes regardless, so replacement is the only wire-compatible shape.
/// `octets()` (not `str()`, which is octet-strings-only per `is_str`) returns
/// the raw payload of any variable-size element.
fn lossy_utf8(elem: &TLVElement) -> Result<Option<Value>, Error> {
    Ok(if elem.control()?.value_type.is_utf8() {
        Some(Value::from(String::from_utf8_lossy(elem.octets()?).into_owned()))
    } else {
        None
    })
}
```

Then in `tlv_to_json_at`, first line of the body after the depth check:

```rust
    if let Some(s) = lossy_utf8(elem)? {
        return Ok(s);
    }
```

The `TLVValue::Utf8l(..) => Value::from(s)` match arm below becomes unreachable for the utf8 case but stays (it costs nothing and documents the shape); do not delete it.

Also in `tlv_to_json_named_at`, before its `if !matches!(elem.value()?, TLVValue::Struct)` line, add the same two-line early return — a name-based payload that is a bare string must get the same treatment.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p matter-rs-stack --lib`
Expected: PASS, no other test broken.

- [ ] **Step 5: Commit**

```bash
git add crates/stack/src/tlv_json.rs
git commit -m "fix(stack): decode invalid UTF-8 attribute strings lossily like Node (live 0/52/0 skip)"
```

---

### Task 2: Merge chunked lists across subscription ReportData messages

**Why.** `ReportSink::handle_report` (`crates/stack/src/reports.rs:307-367`) builds a fresh `AttrAccumulator` per `ReportData` *message*. A subscription update whose chunked list spans messages (a bridge's `PartsList`) therefore loses the elements from earlier messages — `append` files them as `orphan_appends` and warns "reported incomplete" (`reports.rs:170-185`). The read/priming paths already merge across messages because they hold one accumulator across the whole exchange (pinned by the existing test `a_list_opened_in_one_message_and_appended_to_in_the_next_still_merges`); only the sink is per-message. `ReportDataResp.more_chunks` says whether more messages follow (already read at `reports.rs:346`).

**Design.** A `PendingReports` map (node_id → accumulator-in-progress) owned by `StackCtx`, so state survives across `handle_report` calls. Buffer *all* attribute changes until the final message — not just chunked lists — so a multi-message report reaches the WS client as one `attribute_updated` batch (last-wins per path is the accumulator's existing `put` semantics). Events and liveness stay per-message: they are not chunk-split and delaying liveness would starve the watchdog on a slow multi-message report. Stale-entry hygiene: a device that dies mid-report leaves a pending entry, so the entry is dropped wherever the subscription or node lifecycle resets — `supervisor::establish` (next to `reset_event_high_water`) and `runtime.rs`'s `forget_node`.

**Files:**
- Modify: `crates/stack/src/reports.rs` (new struct + `handle_report`), `crates/stack/src/ctx.rs:63-78` (field + doc-contract), `crates/stack/src/supervisor.rs` (~line 137, next to `reset_event_high_water`), `crates/stack/src/runtime.rs:495-501` (`forget_node`)
- Modify: `README.md` (retire the "Lists split across ReportData messages" limitation)
- Test: `crates/stack/src/reports.rs` `mod tests`

**Interfaces:**
- Produces: `pub(crate) struct PendingReports` with `fn absorb_message(&mut self, node_id: u64, report: &ReportDataResp<'_>, who: &str) -> Option<Vec<(String, Value)>>` (returns `Some(changes)` only when the report is complete) and `fn forget(&mut self, node_id: u64)`.
- `StackCtx` gains `pub pending_reports: RefCell<PendingReports>`.

- [ ] **Step 1: Write the failing test**

In `reports.rs` `mod tests`, reusing the existing `report_bytes`/`path`/`build` helpers:

```rust
/// The ReportSink used to build one accumulator per ReportData *message*, so a
/// chunked list spanning messages of one subscription report lost its earlier
/// elements (the append warned "reported incomplete"). PendingReports carries
/// the accumulator across messages, keyed by node, and only releases the
/// changes when more_chunks stops.
#[test]
fn a_subscription_list_split_across_messages_merges_before_release() {
    let empty = build(|w| {
        w.start_array(&TLVTag::Anonymous).unwrap();
        w.end_container().unwrap();
    });
    let e0 = build(|w| w.u16(&TLVTag::Anonymous, 11).unwrap());
    let e1 = build(|w| w.u16(&TLVTag::Anonymous, 22).unwrap());

    let msg1 = report_bytes(
        &[
            AttrResp::Data(AttrData::new(None, path(0, 29, 3, None), TLVElement::new(&empty))),
            AttrResp::Data(AttrData::new(None, path(0, 29, 3, Some(None)), TLVElement::new(&e0))),
        ],
        true, // MoreChunkedMsgs
    );
    let msg2 = report_bytes(
        &[AttrResp::Data(AttrData::new(None, path(0, 29, 3, Some(None)), TLVElement::new(&e1)))],
        false,
    );

    let mut pending = PendingReports::default();

    let elem = TLVElement::new(&msg1);
    let report1 = ReportDataResp::from_tlv(&elem).unwrap();
    assert_eq!(pending.absorb_message(9, &report1, "test"), None, "held until final message");

    let elem = TLVElement::new(&msg2);
    let report2 = ReportDataResp::from_tlv(&elem).unwrap();
    let changes = pending.absorb_message(9, &report2, "test").expect("final message releases");
    assert_eq!(changes, vec![("0/29/3".to_string(), json!([11, 22]))]);

    // Nothing left behind for the node.
    let elem = TLVElement::new(&msg2);
    let report2 = ReportDataResp::from_tlv(&elem).unwrap();
    assert_eq!(
        pending.absorb_message(9, &report2, "test"),
        Some(vec![("0/29/3".to_string(), json!([22]))]),
        "a later report starts from a clean accumulator (this one is a lone orphan append)"
    );
}

/// Interleaving: two nodes mid-report must not share an accumulator.
#[test]
fn pending_reports_are_per_node() {
    let a = build(|w| w.u16(&TLVTag::Anonymous, 1).unwrap());
    let msg_more = report_bytes(
        &[AttrResp::Data(AttrData::new(None, path(1, 6, 0, None), TLVElement::new(&a)))],
        true,
    );
    let msg_final = report_bytes(
        &[AttrResp::Data(AttrData::new(None, path(1, 6, 1, None), TLVElement::new(&a)))],
        false,
    );

    let mut pending = PendingReports::default();
    let elem = TLVElement::new(&msg_more);
    let r = ReportDataResp::from_tlv(&elem).unwrap();
    assert_eq!(pending.absorb_message(1, &r, "test"), None);

    // Node 2 completes in one message; node 1's pending state is untouched.
    let elem = TLVElement::new(&msg_final);
    let r = ReportDataResp::from_tlv(&elem).unwrap();
    assert_eq!(pending.absorb_message(2, &r, "test"), Some(vec![("1/6/1".to_string(), json!(1))]));

    let elem = TLVElement::new(&msg_final);
    let r = ReportDataResp::from_tlv(&elem).unwrap();
    let merged = pending.absorb_message(1, &r, "test").unwrap();
    assert_eq!(merged, vec![("1/6/0".to_string(), json!(1)), ("1/6/1".to_string(), json!(1))]);

    // forget() drops half-done state (resubscribe / node removal).
    let elem = TLVElement::new(&msg_more);
    let r = ReportDataResp::from_tlv(&elem).unwrap();
    assert_eq!(pending.absorb_message(1, &r, "test"), None);
    pending.forget(1);
    let elem = TLVElement::new(&msg_final);
    let r = ReportDataResp::from_tlv(&elem).unwrap();
    assert_eq!(pending.absorb_message(1, &r, "test"), Some(vec![("1/6/1".to_string(), json!(1))]));
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p matter-rs-stack pending_reports a_subscription_list_split`
Expected: FAIL to compile — `PendingReports` does not exist.

- [ ] **Step 3: Implement `PendingReports`**

In `reports.rs`, after `AttrAccumulator`'s impl block:

```rust
/// Accumulators-in-progress for multi-message subscription reports, keyed by
/// node id.
///
/// `ReportSink::handle_report` is called once per `ReportData` *message*, but a
/// report (and a chunked list inside it) can span several messages, signalled
/// by `MoreChunkedMsgs`. The read/priming paths hold one accumulator across
/// their whole exchange and merge naturally; the sink holds it here instead,
/// releasing the merged changes only on the final message so the WS client
/// sees one complete `attribute_updated` batch.
///
/// Keyed by node id alone: the supervisor maintains exactly one subscription
/// per node, and messages of one report arrive in order on one exchange, so
/// there is nothing finer to key on. An entry whose final message never
/// arrives (device died mid-report) is dropped by [`Self::forget`], called on
/// resubscribe (`supervisor::establish`) and node removal (`runtime::forget_node`).
#[derive(Default)]
pub(crate) struct PendingReports(HashMap<u64, AttrAccumulator>);

impl PendingReports {
    /// Absorb one message. `Some(changes)` when this message completes the
    /// report; `None` while more messages are pending.
    pub fn absorb_message(
        &mut self,
        node_id: u64,
        report: &ReportDataResp<'_>,
        who: &str,
    ) -> Option<Vec<(String, Value)>> {
        let mut acc = self.0.remove(&node_id).unwrap_or_default();
        acc.absorb(report, who);
        if report.more_chunks == Some(true) {
            self.0.insert(node_id, acc);
            return None;
        }
        if acc.failures() > 0 {
            tracing::warn!("{who}: {} attribute report(s) skipped", acc.failures());
        }
        Some(acc.into_pairs())
    }

    /// Drop half-done state for a node — resubscribe or removal makes it stale.
    pub fn forget(&mut self, node_id: u64) {
        self.0.remove(&node_id);
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p matter-rs-stack --lib`
Expected: PASS.

- [ ] **Step 5: Wire it into `StackCtx` and the sink**

`ctx.rs`: add to `StackCtx` (after `last_event`, with the field doc) and to `StackCtx::new`'s initializer (`pending_reports: RefCell::new(Default::default())`):

```rust
    /// node_id -> subscription report still awaiting its final ReportData
    /// message (see `reports::PendingReports`). Subscription-lifetime, like
    /// `subs`: cleared on resubscribe and on node removal.
    pub pending_reports: RefCell<crate::reports::PendingReports>,
```

`reports.rs` `handle_report`: replace the per-message accumulator block (lines 343-356, from `let who = ...` through the `AttributesChanged` send) with:

```rust
        let who = format!("node {node_id}");
        // Borrow is dropped before the send; never held across an await.
        let changes = ctx.pending_reports.borrow_mut().absorb_message(node_id, report, &who);
        if let Some(changes) = changes {
            if !changes.is_empty() {
                // A closed receiver means the controller is shutting down.
                let _ = ctx.events.send(StackEvent::AttributesChanged { node_id, changes });
            }
        }
```

Delete the now-superseded `saw_list_chunks() && more_chunks` warning block. If that leaves `saw_list_chunks` without callers, delete the method and its `saw_list_chunks` field too (check `supervisor.rs` and `ops/interact.rs` for other callers first; `cargo build` will name them).

`supervisor.rs` `establish` (~line 137): directly under `ctx.reset_event_high_water(node_id);` add:

```rust
    // A resubscribe orphans any report the old subscription left half-sent.
    ctx.pending_reports.borrow_mut().forget(node_id);
```

`runtime.rs` `forget_node` (line 495): add a `pending: &mut PendingReports` parameter cleared alongside `last_event`/`addrs` (`pending.forget(node_id);`), update its call site (~line 434) to pass `&mut ctx.pending_reports.borrow_mut()`, and extend the existing `forget_node` unit test (~line 615) to assert a pending entry is gone too. Update the cleanup-contract doc on `StackCtx::supervisors` (`ctx.rs:66-78`) to list `pending_reports` among the caches `stop_supervisor`'s path must clear.

- [ ] **Step 6: Run the workspace suite**

Run: `cargo test --workspace`
Expected: PASS. If the existing sink-adjacent tests assert the old per-message flush, update them to the new hold-until-final contract — the new contract is the specified one.

- [ ] **Step 7: Retire the README limitation**

In `README.md`, delete the bullet "**Lists split across `ReportData` *messages* are not merged.**" from Known limitations.

- [ ] **Step 8: Commit**

```bash
git add crates/stack/src/reports.rs crates/stack/src/ctx.rs crates/stack/src/supervisor.rs crates/stack/src/runtime.rs README.md
git commit -m "fix(stack): merge subscription reports across ReportData messages before forwarding"
```

---

### Task 3: `ping_node` pings addresses concurrently (Node parity)

**Why.** Node pings all of a node's addresses in parallel — `ControllerCommandHandler.ts:1410-1419`: `ipAddresses.map(async ip => ... pingIp(ip, 10, attempts))` under `Promise.all`. Ours walks them sequentially (`futures_join_all`, `commands/nodes.rs:106-110`, "sequential is fine at homelab scale"), so worst case is `attempts × 10s × N_addresses` — the plan-2 handoff flagged this **Important** (execution-notes item 1). The result shape (`{addr: bool}`) is unchanged.

**Files:**
- Modify: `crates/controller/src/commands/nodes.rs` (replace `futures_join_all`)
- Modify: `README.md` (drop "pings ... sequentially rather than concurrently" from the out-of-scope note)
- Test: same file, `mod tests`

**Interfaces:**
- Produces: `async fn join_all_concurrent<T: Send + 'static>(futs: Vec<impl Future<Output = T> + Send + 'static>) -> Vec<T>` — results in **input order** (callers zip them against the address list).

- [ ] **Step 1: Write the failing test**

```rust
    /// Node pings every address in parallel (ControllerCommandHandler.ts:1410-1419,
    /// Promise.all); sequential was a plan-2 shortcut with a worst case of
    /// attempts x 10s x N_addresses. Two 300ms sleeps finishing well under
    /// 600ms proves concurrency; exact timing is deliberately slack.
    #[tokio::test]
    async fn pings_run_concurrently_and_keep_input_order() {
        let start = std::time::Instant::now();
        let out = join_all_concurrent(vec![
            Box::pin(async {
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                1u8
            }) as std::pin::Pin<Box<dyn std::future::Future<Output = u8> + Send>>,
            Box::pin(async { 2u8 }),
        ])
        .await;
        assert_eq!(out, vec![1, 2], "input order, not completion order");
        assert!(start.elapsed() < std::time::Duration::from_millis(550),
                "sequential execution would take >=600ms");
    }
```

(Add `use super::join_all_concurrent;` to the test module's imports.)

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p matter-rs-controller pings_run_concurrently`
Expected: FAIL to compile — `join_all_concurrent` does not exist.

- [ ] **Step 3: Implement**

Replace `futures_join_all` (nodes.rs:106-110) with:

```rust
/// Concurrent join preserving input order — Node pings every address in
/// parallel (`ControllerCommandHandler.ts:1410-1419`), and `attempts * 10s`
/// per address is too slow to serialize. Spawned tasks rather than a `futures`
/// dependency; a panicked ping task poisons only its own slot's `expect`,
/// which matches the old behaviour of a panic in a sequential await.
async fn join_all_concurrent<T: Send + 'static>(
    futs: Vec<impl std::future::Future<Output = T> + Send + 'static>,
) -> Vec<T> {
    let handles: Vec<_> = futs.into_iter().map(tokio::spawn).collect();
    let mut out = Vec::with_capacity(handles.len());
    for h in handles {
        out.push(h.await.expect("ping task panicked"));
    }
    out
}
```

Update the call site in `ping_node` from `futures_join_all(futures)` to `join_all_concurrent(futures)`. If the compiler objects that `ping_one`'s future is not `Send + 'static` as collected, build the vec as `addrs.iter().map(|a| ping_one(a.clone(), attempts)).collect()` — `ping_one` takes owned `String` and is `Send`.

The generic test futures in Step 1 are boxed; make the parameter `impl Future` bound work for both by accepting `Pin<Box<dyn Future<Output = T> + Send>>` **only if** the generic version fails to unify in the test — prefer the generic signature and un-box the test if it compiles either way.

- [ ] **Step 4: Run tests**

Run: `cargo test -p matter-rs-controller`
Expected: PASS, including the existing `ping_attempts` tests.

- [ ] **Step 5: Update README and commit**

In `README.md`'s "Out of scope in v1, not deviations" bullet, delete the sentence "`ping_node` pings a node's addresses sequentially rather than concurrently."

```bash
git add crates/controller/src/commands/nodes.rs README.md
git commit -m "feat(controller): ping node addresses concurrently, matching Node's Promise.all"
```

---

### Task 4: `diagnostics` args — already Node parity; pin it and correct the record

**Why.** Plan-2 execution-notes item 2 flagged "`diagnostics` forwards args to get_nodes ... contradicting the spec's 'all'". Verified 2026-08-15 against the Node source: Node does exactly the same — `WebSocketControllerHandler.ts:748-753` builds `nodes: this.#handleGetNodes(args)`, and `#handleGetNodes` (line 1097) honours `only_available`. The plan-2 command table was wrong, not the code. Deliverable: a test that pins the passthrough (so nobody "fixes" it toward the wrong spec), a comment citing the Node lines, and a resolution note in the execution notes.

**Files:**
- Modify: `crates/controller/src/commands/nodes.rs` (doc comment on `diagnostics` + test)
- Modify: `docs/superpowers/plans/2026-08-13-plan2-execution-notes.md` (annotate item 2 resolved)

**Interfaces:** none new.

- [ ] **Step 1: Write the pinning test**

In `nodes.rs` `mod tests` (the rig helpers live in `crate::real::test_rig`; see `commands/commissioning.rs`'s test module for the import pattern — `use crate::real::test_rig::*;` plus `serde_json::json`):

```rust
    /// Verified against the Node source 2026-08-15: diagnostics builds its
    /// nodes array via getNodes WITH the caller's args
    /// (WebSocketControllerHandler.ts:748-753), and getNodes honours
    /// only_available (line 1097). The plan-2 spec table said "all nodes";
    /// the reference implementation disagrees, and the reference wins. This
    /// test exists so nobody "fixes" the passthrough toward the wrong spec.
    #[tokio::test]
    async fn diagnostics_forwards_only_available_like_node() {
        let r = rig_with_nodes(vec![node_record(1), node_record(2)]);
        // Neither node has a live supervisor, so both are unavailable.
        let v = call(&r, "diagnostics", json!({})).await.unwrap();
        assert_eq!(v["nodes"].as_array().unwrap().len(), 2);
        let v = call(&r, "diagnostics", json!({"only_available": true})).await.unwrap();
        assert_eq!(v["nodes"].as_array().unwrap().len(), 0);
    }
```

- [ ] **Step 2: Run it — it should PASS immediately**

Run: `cargo test -p matter-rs-controller diagnostics_forwards`
Expected: PASS (the behaviour already exists — this is a pinning test, the one TDD exception, because the production change it guards against is a *future deletion*). If it FAILS, stop and re-read `get_nodes`/`diagnostics` — the passthrough regressed and this task just caught it.

- [ ] **Step 3: Document at both ends**

Doc comment on `pub async fn diagnostics` in `nodes.rs`:

```rust
/// Args are forwarded to `get_nodes` on purpose: Node builds `nodes` via
/// getNodes(args) (`WebSocketControllerHandler.ts:748-753`), so
/// `only_available` filters here exactly as it does there. The plan-2 command
/// table's "all nodes" was wrong — see the pinning test.
```

In `docs/superpowers/plans/2026-08-13-plan2-execution-notes.md`, append to the item-2 line:

```markdown
   **[RESOLVED 2026-08-15, plan 3: Node forwards args too
   (`WebSocketControllerHandler.ts:748-753`); behaviour already matched. The
   spec table was wrong. Pinned by `diagnostics_forwards_only_available_like_node`.]**
```

- [ ] **Step 4: Commit**

```bash
git add crates/controller/src/commands/nodes.rs docs/superpowers/plans/2026-08-13-plan2-execution-notes.md
git commit -m "test(controller): pin diagnostics arg passthrough as verified Node parity"
```

---

### Task 5: Nested epoch conversion in tag-based attribute JSON

**Why.** Accepted deviation #2 (README): epoch conversion is top-level only in the tag-based attribute path — `attr_value_to_json` (`tlv_json.rs:143-148`) applies `apply_epoch` to the attribute's own type and never descends. Node converts model-driven at *every* depth: `Converters.ts` classifies each member model (`classifyModel`, lines 207-233) and applies EpochS/EpochUS wherever they appear in the walk (`convertMatterToWebSocket`, lines 394-398), including struct members and list elements. Concrete wire difference: TimeSynchronization(56) `timeZone[]` (attr 5, IDL line 2524) is a list of `TimeZoneStruct` whose field 1 `validAt` is `epoch_us` (IDL lines 2489-2493) — we emit Matter-epoch micros where Node emits Unix. The *named* path already recurses correctly (`named_field_to_json`, `tlv_json.rs:179-194` applies `apply_epoch` per field); this task brings the tag-based path level with it. Keys stay numeric tags; only leaf values are type-driven.

**Files:**
- Modify: `crates/stack/src/tlv_json.rs` (`attr_value_to_json` + new `typed_to_json`)
- Modify: `README.md` (retire accepted gap #2)
- Test: same file, `mod tests`

**Interfaces:**
- `attr_value_to_json(cluster: u32, attr: u32, elem: &TLVElement) -> Result<Value, Error>` — signature unchanged (callers in `reports.rs` untouched).
- Produces: `fn typed_to_json(elem: &TLVElement, ty: &str, cluster: &'static Cluster, depth: u8) -> Result<Value, Error>` (private).

- [ ] **Step 1: Write the failing test**

```rust
    /// Accepted deviation #2 retired: Node converts epoch fields at every
    /// depth of the model walk (Converters.ts classifyModel lines 207-233 +
    /// convertMatterToWebSocket lines 394-398), not just the top level.
    /// TimeSynchronization.timeZone (56/5) is a list of TimeZoneStruct whose
    /// field 1 validAt is epoch_us: the nested value must come out Unix.
    #[test]
    fn epoch_fields_convert_inside_structs_and_lists() {
        // [ { 0: offset=3600, 1: validAt=0 (Matter epoch), 2: "CET" } ]
        let bytes = {
            let mut buf = [0u8; 128];
            let mut wb = WriteBuf::new(&mut buf);
            wb.start_array(&TLVTag::Anonymous).unwrap();
            wb.start_struct(&TLVTag::Anonymous).unwrap();
            wb.i32(&TLVTag::Context(0), 3600).unwrap();
            wb.u64(&TLVTag::Context(1), 0).unwrap();
            wb.utf8(&TLVTag::Context(2), "CET").unwrap();
            wb.end_container().unwrap();
            wb.end_container().unwrap();
            wb.as_slice().to_vec()
        };
        let v = attr_value_to_json(56, 5, &TLVElement::new(&bytes)).unwrap();
        // Matter epoch 0 == 2000-01-01T00:00:00Z == 946684800 Unix seconds.
        assert_eq!(
            v,
            json!([{"0": 3600, "1": 946_684_800_000_000u64, "2": "CET"}]),
            "validAt must be shifted to Unix micros at depth 2"
        );

        // Top-level epoch attributes keep working: 56/0 UTCTime is epoch_us.
        let top = {
            let mut buf = [0u8; 16];
            let mut wb = WriteBuf::new(&mut buf);
            wb.u64(&TLVTag::Anonymous, 0).unwrap();
            wb.as_slice().to_vec()
        };
        assert_eq!(
            attr_value_to_json(56, 0, &TLVElement::new(&top)).unwrap(),
            json!(946_684_800_000_000u64)
        );

        // A struct field the IDL revision doesn't know passes through raw.
        let unknown_field = {
            let mut buf = [0u8; 64];
            let mut wb = WriteBuf::new(&mut buf);
            wb.start_array(&TLVTag::Anonymous).unwrap();
            wb.start_struct(&TLVTag::Anonymous).unwrap();
            wb.u64(&TLVTag::Context(200), 5).unwrap();
            wb.end_container().unwrap();
            wb.end_container().unwrap();
            wb.as_slice().to_vec()
        };
        assert_eq!(
            attr_value_to_json(56, 5, &TLVElement::new(&unknown_field)).unwrap(),
            json!([{"200": 5}])
        );
    }
```

(If `WriteBuf`/`TLVWrite`/`TLVTag` are not yet imported in `mod tests`, mirror the imports the existing write-side tests use.)

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p matter-rs-stack epoch_fields_convert_inside`
Expected: FAIL — the nested assertion sees the raw Matter-epoch `0`, because `apply_epoch` only ran against the top-level array.

- [ ] **Step 3: Implement the typed walk**

Replace `attr_value_to_json`'s body:

```rust
/// Attribute values stay tag-based on the wire; the type-driven part is the
/// epoch shift, applied at *every* depth of the walk like Node's model-driven
/// converter (`Converters.ts` classifyModel lines 207-233 /
/// convertMatterToWebSocket lines 394-398) — accepted deviation #2, retired.
pub fn attr_value_to_json(cluster: u32, attr: u32, elem: &TLVElement) -> Result<Value, Error> {
    let Some((meta, a)) = matter_rs_gen::cluster(cluster).and_then(|c| c.attr(attr).map(|a| (c, a)))
    else {
        // Unknown attribute: untyped fallback, exactly as before.
        return tlv_to_json(elem);
    };
    typed_to_json(elem, a.ty, meta, 0)
}

/// Tag-based conversion with the IDL type threaded through the walk. Keys are
/// numeric tags (unlike the named walk in `tlv_to_json_named_at`); the type
/// only steers leaf conversion. Arrays recurse with the same `ty` because the
/// IDL spells a list as `Type name[]` — element type == attribute type.
fn typed_to_json(
    elem: &TLVElement,
    ty: &str,
    cluster: &'static Cluster,
    depth: u8,
) -> Result<Value, Error> {
    if depth > MAX_DEPTH {
        return Err(invalid());
    }
    if let Some(s) = lossy_utf8(elem)? {
        return Ok(s);
    }
    match elem.value()? {
        TLVValue::Array | TLVValue::List => {
            let mut arr = Vec::new();
            for child in elem.container()?.iter() {
                arr.push(typed_to_json(&child?, ty, cluster, depth + 1)?);
            }
            Ok(Value::Array(arr))
        }
        TLVValue::Struct => match cluster.find_struct(ty) {
            Some(nested) => {
                let mut obj = Map::new();
                for child in elem.container()?.iter() {
                    let child = child?;
                    match child.tag()? {
                        TLVTag::Context(n) => {
                            let v = match nested.fields.iter().find(|f| f.code == n as u32) {
                                Some(f) => typed_to_json(&child, f.ty, cluster, depth + 1)?,
                                // Field ids this IDL revision doesn't know
                                // still have to reach the client.
                                None => tlv_to_json_at(&child, depth + 1)?,
                            };
                            obj.insert(n.to_string(), v);
                        }
                        other => tracing::debug!("skipping non-context struct member tag {other:?}"),
                    }
                }
                Ok(Value::Object(obj))
            }
            // A struct whose type the IDL doesn't name: untyped fallback.
            None => tlv_to_json_at(elem, depth),
        },
        // Leaf: convert, then shift if the type is an epoch.
        _ => apply_epoch(ty, tlv_to_json_at(elem, depth)?),
    }
}
```

Note `Cluster::attr` and `Cluster::find_struct` already exist (`crates/gen/src/lib.rs`); `Attr.is_list` is *not* needed — chunked-list element reports arrive as bare elements and recurse through the leaf/struct arms with the attribute's element type, which is exactly right.

- [ ] **Step 4: Run the crate suite**

Run: `cargo test -p matter-rs-stack --lib`
Expected: PASS. Watch specifically for the existing top-level epoch tests around `apply_epoch` (`tlv_json.rs` tests near line 832) — they must be untouched.

- [ ] **Step 5: Retire README gap #2 and commit**

In `README.md`'s "Accepted parity gaps" list, delete item 2 ("Epoch conversion is top-level only.") and renumber (or leave numbering gaps — match the list's existing editorial style; the intro says "seven", update it to "six").

```bash
git add crates/stack/src/tlv_json.rs README.md
git commit -m "feat(stack): convert epoch fields at every depth of tag-based attribute JSON (Node parity)"
```

---

### Task 6: Golden wire-fixture corpus for attribute conversion

**Why.** Tasks 1 and 5 change the attribute-JSON pipeline that every read, priming snapshot and subscription update flows through. A reviewable corpus of (TLV bytes → expected wire JSON) pairs pins the whole `attr_value_to_json` surface in one place, gives future regressions (like 0/52/0) a file to land in, and documents the Node-derived expectations with citations instead of scattering them across unit tests.

**Files:**
- Create: `crates/stack/tests/fixtures/attr_values.json`
- Create: `crates/stack/tests/wire_fixtures.rs`
- Modify: `crates/stack/src/lib.rs` — only if `tlv_json` is not already visible to integration tests; if it is `pub(crate)`, re-export the one entry point as `#[doc(hidden)] pub use tlv_json::attr_value_to_json;` (check first — `crates/stack/src/lib.rs` may already `pub mod tlv_json`).

**Interfaces:**
- Consumes: `attr_value_to_json(cluster, attr, &TLVElement)` from Task 5.

- [ ] **Step 1: Write the corpus**

`crates/stack/tests/fixtures/attr_values.json` — `tlv` is the hex of one anonymous-tagged TLV element (the attribute's `Data`), `expect` the wire JSON. Every entry names its provenance:

```json
[
  {
    "name": "0/52/0 regression: invalid UTF-8 thread name decodes lossily",
    "why": "node 12 live skip; ThreadMetricsStruct.name is char_string<8> (IDL:2119); Node decodes lossily",
    "cluster": 52, "attr": 0,
    "tlv": "16 15 26 00 2a00 0000 0c01 ff 18 18",
    "expect": [{"0": 42, "1": "�"}]
  },
  {
    "name": "56/5 nested epoch: TimeZoneStruct.validAt shifts to Unix micros",
    "why": "Converters.ts EpochUS at any depth (lines 394-398); Matter 0 == Unix 946684800s",
    "cluster": 56, "attr": 5,
    "tlv": "16 15 2500 100e 2601 00000000 0c02 434554 18 18",
    "expect": [{"0": 3600, "1": 946684800000000, "2": "CET"}]
  },
  {
    "name": "56/0 top-level epoch_us",
    "why": "pre-existing behaviour, must survive the typed-walk rewrite",
    "cluster": 56, "attr": 0,
    "tlv": "26 00000000",
    "expect": 946684800000000
  },
  {
    "name": "octet string emits base64",
    "why": "Converters.ts ConvKind.Bytes -> Bytes.toBase64 (line 403)",
    "cluster": 40, "attr": 18,
    "tlv": "10 04 deadbeef",
    "expect": "3q2+7w=="
  },
  {
    "name": "null attribute stays null",
    "why": "convertMatterToWebSocket returns null unchanged (line 372)",
    "cluster": 40, "attr": 5,
    "tlv": "14",
    "expect": null
  },
  {
    "name": "unknown cluster falls back to untyped conversion",
    "why": "Node: model undefined -> simple conversions only (lines 375-388)",
    "cluster": 64512, "attr": 0,
    "tlv": "04 2a",
    "expect": 42
  }
]
```

**The hex above is illustrative of the format, not authoritative** — the test in Step 2 is written so a wrong hand-encoding fails loudly, and the *authoritative* way to produce each `tlv` value is Step 3.

- [ ] **Step 2: Write the harness**

`crates/stack/tests/wire_fixtures.rs`:

```rust
//! Golden corpus: attribute TLV -> wire JSON, expectations derived from the
//! matterjs-server converter (Converters.ts) with per-entry citations. Add a
//! fixture here for every future wire regression before fixing it.

use rs_matter::tlv::TLVElement;
use serde::Deserialize;

#[derive(Deserialize)]
struct Fixture {
    name: String,
    #[allow(dead_code)]
    why: String,
    cluster: u32,
    attr: u32,
    tlv: String,
    expect: serde_json::Value,
}

fn unhex(s: &str) -> Vec<u8> {
    let compact: String = s.chars().filter(|c| !c.is_whitespace()).collect();
    (0..compact.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&compact[i..i + 2], 16).expect("fixture hex"))
        .collect()
}

#[test]
fn attribute_conversion_matches_the_golden_corpus() {
    let raw = include_str!("fixtures/attr_values.json");
    let fixtures: Vec<Fixture> = serde_json::from_str(raw).expect("fixture file parses");
    assert!(!fixtures.is_empty());

    let mut failures = Vec::new();
    for f in &fixtures {
        let bytes = unhex(&f.tlv);
        match matter_rs_stack::tlv_json::attr_value_to_json(f.cluster, f.attr, &TLVElement::new(&bytes)) {
            Ok(v) if v == f.expect => {}
            Ok(v) => failures.push(format!("{}: got {v}, expected {}", f.name, f.expect)),
            Err(e) => failures.push(format!("{}: conversion failed: {e}", f.name)),
        }
    }
    assert!(failures.is_empty(), "corpus mismatches:\n{}", failures.join("\n"));
}
```

Adjust the `matter_rs_stack::tlv_json::attr_value_to_json` path to however the crate actually exposes it (see Files note above).

- [ ] **Step 3: Make the corpus hex authoritative**

Run: `cargo test -p matter-rs-stack --test wire_fixtures -- --nocapture`

For every entry that fails on hex (not on semantics): regenerate the bytes with a scratch `#[test]` that builds the value with `WriteBuf`/`TLVWrite` exactly like `tlv_json.rs`'s own tests and prints `hex::encode`-style output (two-digit lowercase per byte; write the tiny formatter inline — the workspace has no hex dep), paste the printed hex into the fixture, delete the scratch test. Every entry that fails on *semantics* is a real finding: stop and reconcile against the cited Converters.ts lines before touching the expectation.

Expected end state: PASS with all entries.

- [ ] **Step 4: Run the workspace suite and commit**

Run: `cargo test --workspace`
Expected: PASS.

```bash
git add crates/stack/tests/wire_fixtures.rs crates/stack/tests/fixtures/attr_values.json crates/stack/src/lib.rs
git commit -m "test(stack): golden wire-fixture corpus for attribute TLV->JSON conversion"
```

---

## Self-Review (performed at write time)

1. **Coverage:** execution-notes item 1 (sequential ping) → Task 3; item 2 (diagnostics args) → Task 4; README limitation "lists split across messages" → Task 2; live 0/52/0 nit → Task 1; accepted gap #2 (top-level epoch) → Task 5; "plan 3 fixtures" mandate → Task 6. Gaps #1, #3-#7 of the README list stay accepted by decision (Jens, 2026-08-15) — no task, deliberately.
2. **Placeholders:** none; every step carries code or an exact edit location. Task 6 Step 1's hex is explicitly marked non-authoritative with a defined regeneration procedure.
3. **Type consistency:** `PendingReports::absorb_message` returns `Option<Vec<(String, Value)>>` in Task 2 Steps 1, 3 and the sink wiring; `lossy_utf8` defined in Task 1 and reused by name in Task 5's `typed_to_json`; `attr_value_to_json` signature unchanged across Tasks 5-6.

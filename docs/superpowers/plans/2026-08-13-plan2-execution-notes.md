# Plan 2 execution notes — handoff state (updated 2026-08-14)

Plan: `docs/superpowers/plans/2026-08-13-plan2-rs-matter-core.md`, executed via
subagent-driven development on branch `plan2-rs-matter-core`.
**Tasks 1–19 are COMPLETE.** Tasks 1–16 were per-task reviewed (each with a spec
review and a quality review, plus fix rounds where needed — all closed); Task 17
was implemented and reviewed by the controller directly rather than by subagents
(see "Task 17 as executed" below); Tasks 18–19 went back to the review pass.
Task 19 was the plan's acceptance gate and it passed against the virtual matter.js
device — see "Task 19 as executed". **Task 20 (whole-branch review) has now RUN,
its verdict was "ready to merge with fixes", and its one fix wave is applied** —
see "Task 20: whole-branch review and its fix wave" below. A scoped re-review of
that wave is the remaining step.

Moved machines twice at Jens's direction: tasks 1–11 on the first, 12–16 on
CT 110, and now again because rs-matter rebuilds were too slow there. The SDD
working ledger is gitignored/ephemeral and does NOT travel — **this file is the
handoff.** Everything a resuming controller needs is here plus git history.

## Completed tasks (commits)

| Task | What | Commit(s) |
|---|---|---|
| 1 | `gen` crate: vendored CSA IDL V1.6.0.0 + metadata tables | 4ddd8ae..a114120 |
| 2 | `wire` node/event/fabric/ICD models | ..f00b3b7 |
| 3 | controller storage (atomic JSON, alloc, credential/label rules) | ..062c877 |
| 4 | `stack_api` Stack trait + FakeStack | ..5d70b97 |
| 5 | registry + MatterNodeData building | ..bfb86e4 |
| 6 | NodeManager + 3-min availability grace | ..119158a |
| 7 | ConnId on Controller trait + ws.rs CloseGuard | ..cabb84e |
| 8 | MatterController core + session/node commands | ..6bd282f |
| 9 | interaction commands (read/write/device_command) | ..e6b5656 |
| 10 | commissioning/discovery/credentials/fabric commands | ..38f1b72 |
| 11 | full vendor table, loglevel, ICD/OTA stubs — 31-command surface | ..abb1f4e |
| 12 | `stack` crate scaffold + TLV↔JSON codec | `81a0df4` |
| 13 | RCAC-direct fabric identity bootstrap + persistence | `3d67013` |
| 14 | generic IM read/write/invoke/interview + report sink | `ae056ad` |
| 15 | node supervisor, commissioning, OCW, device fabrics, discovery | `e50547a` |
| 16 | runtime thread, request loop, `StackHandle` | `2674994` |
| 17 | `server` — real controller wired, stub retired from main | `511a93d` |
| 18 | carryover hygiene batch | `df09ceb`, `bc422b1` |
| 19 | e2e acceptance vs virtual matter.js device + README | `c5c609a`..`5075c70` |

`cargo test --workspace` green at every task boundary: 95 → 126 → 136 → 167 →
209 → 239 → 244 → 249 → **256** tests. At Task 19's tip: **256 passing, 0 failed,
1 ignored** (the `#[ignore]`d + `MRS_E2E=1`-gated e2e), and `cargo clippy
--workspace --all-targets` byte-identical to the Task 18 baseline (the three known
pre-existing warnings plus `gen`'s build script; see the Task 18 list).

> Task 16's fix round was verified on this machine after the move: `cargo test
> --workspace` ran to **239 passing, 0 failed**, closing the one gate the notes
> flagged as inspection-only. `rs-matter-ref/` was already present at the pinned
> rev `03bc8f2`, so neither prerequisite needed redoing.

## Task 17 as executed

Process deviation: implemented and reviewed by the controller directly, not via
the two parallel review subagents tasks 12–16 used (the session's operating
instructions bar dispatching agents unprompted). The review checklist was still
worked — it caught one real defect, in the new tests rather than the code: a
`reload::Handle` holds only a *weak* reference to the filter its `Layer` owns, so
the first test rig dropped the layer and every `reload` failed with
`SubscriberGone`. The production path is unaffected (`init` moves the layer into
the global subscriber, which lives for the process), and `set`'s `if let Err`
guard correctly declined to report a level change that had not happened. Both
facts are now pinned by `a_failed_reload_does_not_change_the_reported_level`.

**Tasks 18–20 should go back to the two-review pass** if subagents are available.

How each Task 16 must-handle item was closed:

1. **Dead stack now visible.** The `StackEvent` stream is relayed through `main`
   (`relay_tx`/`relay_rx`), and its end-of-stream fires a `died_tx` oneshot that
   sits in `main`'s shutdown `select!`. Verified the signal is real: `ctx` (which
   owns the events sender) is a thread-local `Rc` declared *before* `im` and the
   executor, so it drops last as `run_stack` returns — no clone outlives the
   thread. A dead stack now runs the normal shutdown (so WS clients get their
   `server_shutdown` frame and HA reconnects) and then `exit(1)`.
   `node_manager.rs:53` still breaks silently; that is now harmless because the
   process is going down with it.
2. **Closed ready channel = failed boot.** All three modes — timeout, `Ok(Err)`
   from the oneshot, `Err` from the boot — go through `fatal()`: log, plain
   `fatal:` line on stderr, `exit(1)`, no panic backtrace. Both channels on
   purpose: `RUST_LOG` can silence our target, and a silent fatal exit is worse
   than a duplicated line.
3. **10s shutdown budgeted.** `DRAIN_TIMEOUT` is 3s for the listeners, then
   `shutdown()`'s own ≤10s, so ~13s worst case — documented on the const, and
   `smoke.rs`'s `STOP_CAP` is 30s so it cannot be flaky by construction.
4/5. In-flight-work loss and log-only mDNS degradation are documented at the
   shutdown call site; they stay Task 19 README limitations.

**Deviation from the plan's Step 2 snippet, deliberate:** the plan passes
`normalize_fabric_label(config.default_fabric_label.as_deref())` as the boot
label, which is `"HomeAssistant"` whenever the flag is absent. That would revert
the fabric label on every boot while `config.json` — written by
`set_default_fabric_label`, and what `get_fabric_label` and `commission_with_code`
read — still reported the client's choice. Now: the CLI flag pins and is persisted
(so the pin beats a stale stored value, as the plan intends), and without the flag
the **stored** label is the truth.

Smaller things done along the way, all in `crates/server`: the `TcpListener` bind
failure and the `SIGTERM` handler install no longer `unwrap`/`panic`, and a bind
failure stops the already-running stack thread before exiting.

## Task 20: whole-branch review and its fix wave

The whole-branch review ran at `5a6923b` and found what the per-task reviews
structurally could not: the *interaction* between decisions made in different
tasks. **Verdict: ready to merge with fixes. No Critical findings.** Its triage
list lives in the (ephemeral) SDD ledger; everything it ruled in is applied in one
fix wave, `9726cc9` / `d75a380` / `e84dea3` plus this docs commit:

- **`config.json` read-modify-write was unserialized and all writers shared one
  temp path** — the one genuine concurrency bug on the branch, and the reason
  Task 3's carry-forward check ("confirm `MatterController`'s mutex serializes ALL
  config write paths") is now answered *no*. Two connections is the normal case, so
  a lost update dropped a credential from disk *and* memory, and two writers into
  `.config.json.tmp-<pid>` could leave invalid JSON that `load_config` silently
  answers with `ConfigData::default()`. Fixed with a dedicated `config_write` tokio
  mutex spanning the clone-mutate-save-writeback (the std `Mutex<ConfigData>` stays
  for cheap reads, never held across an await) and a process-wide counter in the
  temp name. Three regression tests, each verified to fail against the pre-fix code.
- **Client-supplied numbers were narrowed with `as`** instead of validated — the
  only class where a malformed request *succeeded against a different target*. See
  the corrected carryover entry below.
- **`InvalidArguments` was flattened to `NodeCommissionFailed`** in the
  commissioning family only; the stack's classification now survives (8 / 4, with 1
  as the default). The Node server's own code for a malformed pairing code is
  UNVERIFIED — `matterjs-server` is not cloned on this machine — and a comment at
  the call site says so, so a future reader with the Node source checks rather than
  "fixes" it blind.
- Hygiene: poison-tolerant `std::sync::Mutex` access everywhere in `controller`
  (a panic in a `Registry` closure used to poison `Registry::inner`, after which
  *every* command panics forever), `split_ip_port`/`ip_of` consolidated into
  `controller::addr` (two copies had cost two bugs fixed twice each), credential
  commands surfacing persistence failures, temp-file cleanup on a failed write,
  `ping_node`'s `attempts` clamped to 10, a `CLUSTERS` sortedness test, the two
  `{e:?}` slips in `identity.rs`, the redacting `Debug`, `version` on
  `ServerIdentity`, and the `fabric_id` warn.

Deliberately NOT done, on Jens's ruling: the `NotFound` (0x8b) carve-out (his
call, see the corrected limitation entry), any `interview`/`PrimingSnapshot`
attribute reordering (JSON key order is not semantic), a `cargo fmt` sweep, and the
~12 `serde_json::to_value(..).unwrap()` sites on types that provably serialize.

`cargo test --workspace` at the end of the wave: **269 passing, 0 failed,
1 ignored** (256 before it, plus 13 new tests). `cargo clippy --workspace
--all-targets` is back to exactly the known pre-existing set.

## Environment prerequisites

- **`rs-matter-ref/`** at the repo root: a clone at pinned rev
  `03bc8f2aeb7765a93e7863e2263f73c7bbc3d401`. Gitignored, does NOT travel —
  recreate it before Task 19. Every task cites paths into it as ground truth.
  A symlink into `~/.cargo/git/checkouts/rs-matter-*/03bc8f2` also works.
  *Present and verified at the pinned rev on the current machine.*
- **`matterjs-server/`** is NOT needed for tasks 12–20 (grepped; only the Node
  facts already embedded in each task are used). Clone it only if Task 20 wants to
  spot-check exact Node error strings.
- **node/npx** required for Task 19 (`npx -y @matter/examples matter-device`).
- **Disk**: needs headroom. `target/` reached 6.5G and a full disk crashed
  `rust-lld` with SIGBUS mid-link — a toolchain-looking failure that is really
  ENOSPC. **If a build dies with a linker signal, an LLVM stack dump, or
  "unexpected EOF", check `df -h` FIRST.**
- **Build cost**: changing any dependency's *feature set* invalidates the graph
  and forces a full rs-matter recompile (~10 min on CT 110), and dev-dependency
  features unify differently for the test profile, so `build` and `test` can each
  pay it. This killed two subagents whose no-output watchdogs fired at 600s while
  the build was fine. **Give subagents a 900000 ms Bash timeout, pre-run gates
  yourself, and check `git status` before assuming a "stalled" agent lost work.**

## Findings AWAITING JENS'S RULING (plan-mandated; do not "fix" silently)

1. **(Important) `ping_node` pings sequentially** though the spec table says
   concurrently — the plan's own sample code chose sequential ("fine at homelab
   scale"); worst case `attempts × 10s × N_addresses`. `commands/nodes.rs`.
2. **(Minor) `diagnostics` forwards args to get_nodes**, so a client sending
   `only_available` gets a filtered `nodes` array, contradicting the spec's "all".
3. **(Important, Task 19) File the upstream rs-matter serial-number bug.** Needs
   Jens's go-ahead to file from his account; see "Task 19 as executed" for the
   diagnosis and the exact source lines. This replaces finding 1's old "minimal
   repro + upstream issue about the ICAC" TODO — that characterisation was wrong.
4. **(Task 19, still open) `NotFound → NodeNotResolving` carve-out.** Task 19's live
   run never produced an IM `NotFound` (0x8b) on a `DefaultSuccess` command, so
   there is **no new evidence** either way and the question below stands unchanged.
   Task 20's whole-branch review established that it **is** fixable (see the
   limitation entry below, corrected); the ~10-line carve-out is deliberately left
   to Jens because it changes which wire code a device-reported status produces,
   which is a spec-level behaviour decision, not a bug fix.
5. **(Task 20) `ServerIdentity`'s `Vec<u8>` key copies still do not zeroize on
   drop** unlike rs-matter's `CryptoSensitive`, and `to_writer_pretty` leaves
   base64 key strings in freed heap. The third item of that trio — the derived
   `Debug` — is fixed (`e84dea3`). These two are unchanged and still deferred.

## Task 17 MUST HANDLE (from Task 16's reviews)

1. **A dead stack is invisible until someone asks.** If `matter.run` or the IM
   responder exits, the thread logs `error!` and stops — nothing lands on the
   `StackEvent` channel, so the WS server keeps answering `server_info` normally
   while every Matter command returns `Sdk: "stack thread is down"` forever. HA
   still shows the bridge online and **there is no non-zero exit for
   systemd/docker to restart on.** `node_manager.rs:53` does
   `let Some(ev) = ev else { break };`, exiting silently and aborting its grace
   timers. Needs a fatal `StackEvent` or a process exit. Cheapest existing signal:
   the `StackEvent` receiver ends when the thread's `ctx` drops, so
   `events_rx.recv() == None` is a usable "stack died" edge (undocumented today).
2. **Treat a CLOSED ready channel as a failed boot**, not "still starting":
   `ready_tx` moves into the thread closure, so if `Builder::spawn` itself fails
   there is no sender left and the caller sees `RecvError` (logged at `error!`).
3. **`shutdown()`'s worst case is 10s, not 5s** — up to 5s for the loop's `done`
   acknowledgement, then up to 5s for the join. Budget accordingly.
4. **Shutdown drops in-flight work**: detached ops (e.g. a 60s commissioning
   attempt) are abandoned and `run_persist_resumption` is cancelled without a
   final tick, losing ≤500ms of CASE resumption records (cost: one handshake).
   Don't expect a clean drain.
5. **mDNS degradation is log-only** — no event, no flag on `ReadyInfo`.

## Task 19 as executed — the acceptance gate PASSED

Run on a dev Mac against `@matter/examples@0.15.6`'s `matter-device` on `en0`
(node v24.19.0, rs-matter `03bc8f2`). Commissioning (2.24s), `start_listening`,
`toggle` → `attribute_updated`, `read_attribute`, restart persistence with CASE
resumption (boot → `available: true` in 2.1s), and the 3-minute offline grace
(180.004s from "subscription went silent" to `available: false`) all verified.
Runbook with the real transcripts: `scripts/e2e-virtual-device.md`. Automated
best-effort version: `crates/server/tests/e2e_virtual.rs`, `#[ignore]`d **and**
gated on `MRS_E2E=1` (`MRS_E2E_INTERFACE` overrides the interface, default `en0`).

### THE upstream rs-matter bug — file this, and note it supersedes finding 1

**rs-matter emits a certificate's serial number verbatim as the X.509
`serialNumber` INTEGER, so ~half of all generated RCACs and ICACs are negative DER
integers that any strict peer rejects.**

- `rs-matter/src/cert/asn1_writer.rs:183-185` — `fn integer(&mut self, _tag, i)` is
  `self.write_str(0x02, i)`: a straight copy, no sign handling.
- `rs-matter/src/onboard/cac.rs:100-106` — `RcacGenerator::generate` fills the
  serial with 8 **raw random bytes**. `IcacGenerator` does the same.
- `rs-matter/src/cert/gen.rs:385` — `validate_serial_number` rejects only the
  *redundant-leading-zero* form; its own test at `:704` happily accepts `[0xFF]`.
- `rs-matter/src/onboard/noc.rs:237-251` — `encode_serial_asn1` **does** pad
  correctly, so **NOCs were never affected**. Only the CA certs are.

Consequence: a peer that re-encodes the Matter TLV to DER itself inserts the `0x00`
sign pad DER requires, hashes a different TBS certificate, and rejects the cert.
matter.js does exactly that (`Rcac.verify`, and `DerCodec.#encodeInteger` in
`@matter/general/dist/esm/codec/DerCodec.js:219`), answering
`AddTrustedRootCertificate` with status 0x85 "Signature verification failed", after
which `AddNOC` fails "Root certificate not found". rs-matter signs its own
conversion, so it verifies its own certs and its CI never sees this.

**This is spike finding 1's real cause.** `spike/SPIKE-RESULTS.md` now carries an
amendment: the finding's "matter.js rejects rs-matter's ICAC TLV/DER encoding"
diagnosis was wrong, and its "OK in RCAC-direct mode" was luck — RCAC-direct only
halves the number of coin flips, and Task 19's first run hit the identical failure
on the **RCAC**. Any upstream issue should be about the serial encoding.

Our fix (`c5c609a`): `stack::identity::generate_usable_rcac` redraws up to 32 times
until `serial_is_der_canonical` accepts the serial (expected 2 draws). Redrawing is
the smallest fix a downstream caller has — `RcacGenerator` takes no serial, and
reaching past it to the public `CertGenerator` would mean owning the whole RCAC
subject/issuer/extension layout locally. **If the pin moves and upstream has fixed
this, the loop becomes a no-op that always succeeds on the first draw** — harmless,
and its doc comment says why to keep it until then.

### Other Task 19 fixes

- **`5590091` — IPv6 brackets were persisted into `NodeRecord::addresses`.**
  `split_ip_port` used a bare `rsplit_once(':')`, so rs-matter's
  `"[fe80::1%14]:5540"` stored as `"[fe80::1%14]"`; `get_node_ip_addresses` then cut
  the scope id at `%` and handed clients the unclosed literal `"[fe80::1"`, and
  `ping_node` handed `ping6` the same. `discover_commissionable_nodes` had it too.
- **`e08273d` — a browse that found nothing blamed slot contention.** `browse_one`
  armed its outer timer with the same budget it gave rs-matter, but rs-matter arms
  the browse's own timeout only *after* winning the rendezvous slot
  (`transport.rs:520-552`), so the outer always won and every empty-network browse
  reached the client as "another discovery or commissioning was holding the browse
  slot". The inner budget is now cut short by a 100ms margin (`inner_budget_ms`),
  with `budget_ms` flooring at 1ms because 0 is the one input with no
  strictly-shorter answer.

### Carryovers for Task 20 / future work

1. **A pre-fix `server.json` is never repaired.** `generate_usable_rcac` protects
   only freshly minted identities and `create_identity` refuses to overwrite, so an
   install predating `c5c609a` keeps a non-canonical RCAC forever and fails every
   matter.js commissioning. `ensure_identity` now `warn!`s on load naming the
   symptom and the (destructive, operator-only) recovery; it deliberately does not
   remint, because that discards the CA key every commissioned node trusts.
2. **Legacy bracketed addresses on disk are never repaired — DEFERRED by ruling.**
   Nothing migrates a `nodes/<id>.json` whose `addresses` still holds
   `"[fe80::1%14]"`, and the priming/interview path does not refresh `addresses`.
   The reader side is graceful (`commands/nodes.rs:66-77,100-126` treats them as
   opaque strings; `ping_one` just reports `false`, no panic, no dropped node) and
   no install exists that could hold one, so self-healing would mean touching the
   interview path for zero present benefit. Recorded, not fixed.
3. **`attribute_subscriptions` is always `[]`** in every `MatterNodeData` observed,
   including for a fully subscribed node. Not investigated — plausibly plan-3 parity
   work, but it is the one field that looked wrong in real output.
4. **Two operational traps worth knowing** (both cost debugging time and are now
   documented in the runbook): the matter.js device's stdout **must** be drained for
   its whole life or it fills the 64KB pipe, blocks in `write` and stops answering on
   the network — which presents as a browse timeout that looks like our bug; and
   `pkill -f matter-device` matches the npx wrapper while `pkill -f DeviceNode.js`
   matches nothing, so it is easy to end up with **two device instances sharing one
   storage dir**, whose CASE-resumption and MRP-retransmission noise also looks like
   a stack bug. Use `pkill -f '\.bin/matter-device'` and verify with `pgrep`.
5. **`attribute_updated` needs the subscription to already exist.**
   `commission_with_code` returns *before* the supervisor subscribes, so a command
   issued in that window changes the device but produces no `attribute_updated` — the
   new value is folded into the priming snapshot and arrives as another
   `node_updated`. A client (HA) can hit the same window. Documented in the runbook.

## Task 19 constraints and must-verifies (RESOLVED where marked)

> Task 19 is COMPLETE. Every constraint below was either confirmed by the
> live run or answered; see "Task 19 as executed". Kept because they are
> still true statements about the system, with the answers folded in.

- **One stack per process, and `shutdown()` does NOT reset it** (statics stay
  claimed for the process lifetime). So each e2e test needing a live stack must be
  **its own test binary** (separate file in `tests/`), and an in-process restart is
  impossible — the runbook's restart step must relaunch the binary. A second
  `spawn()` refuses cleanly via `Err` on the ready channel.
- **`EventDataTimestamp::EpochTimestamp` — SETTLED (Task 19): Posix milliseconds
  since 1970.** rs-matter's doc comment (`im/encoding/event.rs:349`) was right and
  **the plan was wrong**; Task 14 had implemented the plan's epoch-*micros*-since-
  the-*Matter*-epoch reading, which reported every event ~26 years too early. Fixed
  in `db81989`; `convert_timestamp` now forwards the raw `u64`. Evidence: matter.js
  types the field `TlvPosixMs` (a bare `TlvUInt64`) and writes Unix ms into it
  unconverted, CHIP stamps `Timestamp::Epoch` in ms too, and a real
  `BasicInformation.shutDown` logged by the device as `epochTimestamp: 1786698881562`
  now arrives on the WS as exactly `1786698881562`. `MATTER_EPOCH_OFFSET_US` still
  applies to `epoch-us` *attribute* fields in `tlv_json` — different spec type,
  never wrong. **How to reproduce an `EpochTimestamp` at all:** `im/events.rs:161`'s
  TODO holds (rs-matter devices only emit `SystemTimestamp`), *and* our own priming
  events are deliberately not forwarded (`supervisor::establish` rule 3), so a device
  restart shows nothing. Interrupt the matter.js device **while a subscription is
  live** so it emits `shutDown`; racy, ~2 tries. Runbook step 6 documents it.
- **IM status `NotFound` (0x8b) on a DefaultSuccess command is reported as "could
  not resolve node via mDNS".** rs-matter's `InvokeRespChunk::receive`
  (`im/client.rs:945-961`) converts a bare non-success `StatusResponse` via
  `to_error_code()` before our code sees a chunk, so 0x8b → `ErrorCode::NotFound`
  → `NodeUnreachable` → wire `NodeNotResolving`. **STILL OPEN after Task 19** —
  the live run never produced a 0x8b on a `DefaultSuccess` command (the matter.js
  OnOffLight answers `toggle` normally and there was no way to provoke one), so
  there is no new evidence. Documented in the README's limitations list as a known
  wrong-looking message. A device that rejects a command this way would settle it.

  **CORRECTION (Task 20's whole-branch review): this is NOT "unfixable in
  `ops/interact.rs`", as these notes said until now.** It is fixable there, by
  splitting the `NotFound` mapping *by phase*: on that path `ErrorCode::NotFound`
  can only originate in the mDNS resolve inside `Transport::initiate`, so once
  `Exchange::initiate` has returned `Ok`, a `NotFound` is a device-reported IM
  status and cannot be a resolve failure. A ~10-line carve-out — map `NotFound`
  to `NodeUnreachable` only while establishing the exchange, and to
  `Sdk`/`SdkStackError` afterwards — would do it. It is deliberately **deferred to
  Jens**, because changing which wire code a device-reported status produces is a
  spec-level behaviour decision, not a fix to apply on a reviewer's authority.
  This sentence exists so that a future reader does not skip the investigation on
  the strength of the word "unfixable".
- **Cross-`ReportData`-message list merging is not implemented.** Within one
  message, chunked lists merge correctly; a long `PartsList` in a *subscription*
  report split across messages is reported incomplete, with a runtime warning.
  A real bridge is where this shows. **Not exercised by Task 19** — the
  `matter-device` example is one endpoint with a 1-element `PartsList`, so nothing
  ever chunked across messages and the warning never fired. Use `matter-bridge`
  from the same `@matter/examples` package to reach it.
- **`node_addresses` is `[]` for any node this process run did not commission**
  (see the Task 16 note below). **Confirmed by Task 19**, and it is what exposed the
  bracketed-address bug (`5590091`): after a restart the stored record is the only
  answer `get_node_ip_addresses` / `ping_node` have.
- **mDNS degradation is log-only — confirmed on macOS.** `join_multicast_v4` fails
  with `Invalid argument (os error 22)` on the LAN address and rs-matter logs a
  recurring `Failed to send mDNS broadcast to 224.0.0.251:5353`. No event, no flag,
  and discovery works fine over IPv6 link-local throughout. Both are expected on
  macOS and absent on Linux; the runbook and README say so, so nobody re-debugs them.

## README known-limitations section (Task 19 step 4) — DONE

Written in `f774f2b`, amended in fix round 1. `README.md` now carries all of the
below plus the plan's seven accepted parity gaps as their own numbered sub-list
(under "Accepted parity gaps vs the Node server"), the `sessions/`-directory
deviation from the spec's `sessions.json`, the pre-fix-`server.json` RCAC warning,
and a clear separation between *deviations* and *scope exclusions* (no BLE/OTA/
dashboard/DCL, `allow_test_attestation`, sequential `ping_node` — those are
exclusions and gaps, NOT the plan's seven).

- Fixed mDNS hostname `matter-rs-server` (plan-specified). `BuiltinMdns` does no
  name-conflict resolution, so two instances on one LAN segment publish
  conflicting records for `matter-rs-server.local`. `compressed_fabric_id` (already
  on `ReadyInfo`) would make it unique.
- One stack per process, not reset by `shutdown()` — a restart means a new process.
- `node_addresses` empty for nodes not commissioned by this process run.
- Plus the plan's seven accepted v1 deviations (plan lines 27–36).

## Task 18 carryover list (current)

- Silence the `Rig.dir` / `stack_tx` dead-code warnings in the controller test rig.
- **Secret hygiene in `crates/controller/src/storage.rs`** — the trio was verified
  NOT reachable today, hence deferred. **The `Debug` half is now DONE** (`e84dea3`:
  hand-written redacting `Debug`, so no future `{:?}` on `ServerIdentity` or on the
  `pub` `ReadyInfo` that holds it can print the CA key). Still open and still
  deferred: those `Vec<u8>` copies do not zeroize on drop unlike rs-matter's
  `CryptoSensitive`; `to_writer_pretty` leaves base64 key strings in freed heap.
- ~~`crates/controller/src/commands/interaction.rs:78` does `.map(|v| v as u16)` on
  the timed-invoke timeout, so a client sending 65536 lands as `Some(0)` — a
  request already expired on arrival.~~ **DONE (`9726cc9`), and the description
  above was stale**: `ops::interact::normalize_timed` filters `Some(0)`, so a
  truncated 65536 degraded to the 10s default rather than arriving expired. It was
  still silently not the budget the client asked for, and it is now validated —
  along with every other client-supplied number that used to be narrowed with `as`
  (`commands::narrow`). That whole class mattered because a truncating cast made a
  malformed request succeed *against a different target*: `"70000/6/0"` read and
  wrote endpoint 4464, and `fabric_index: 256` meant index 0.
- ~~`fabric_id` has the same stored-scalar-vs-derived-truth exposure that
  `compressed_fabric_id` had.~~ **RESOLVED (`e84dea3`) — a warn, nothing more.**
  `install` compares the stored scalar against the RCAC's `get_fabric_id()` and
  warns only: the certificates are the operative truth (`NocGenerator::create`
  takes the fabric id from the RCAC, `Fabric::update` reads it back out of the NOC
  to derive the compressed id), so a mismatch is not fatal to CASE. Erroring would
  refuse to boot a working install and auto-correcting would contradict "a stored
  identity always wins over the CLI flags" — the two horns of the old dilemma.
  Warn-and-boot is asserted by
  `a_stored_fabric_id_that_disagrees_with_the_rcac_warns_but_still_boots`.
- Pre-existing clippy: `manual_is_multiple_of` (`storage.rs:221`),
  `type_complexity` (`stack_api.rs:169`), 5 warnings in `gen`'s build script.
- The repo is not `cargo fmt`-clean by convention (hand-formatted ~100 cols across
  all crates). If a fmt gate is wanted, do it as one deliberate sweep, never
  smuggled into a feature task.

## Deferred minors for Task 20's whole-branch review

- Task 3: fabric-label truncation counts chars not UTF-16 units (Node parity,
  astral-plane only); credential-id case-fold ASCII-only vs Node's Unicode
  `toLowerCase`; no parent-dir fsync after atomic rename; no test that 0600
  survives an overwrite.
- Task 6: grace-timer staleness keyed on map presence, not timer generation —
  theoretical compound race under rapid Connected/Reconnecting churn.
- Task 8: diagnostics event-ring ordering untested beyond the empty case.
- Task 15: `epoch_s` writes reject a fractional JSON number (e.g. a JS
  `Date.now()/1000`); unverified against the Node server.
- Task 16: statics are claimed before the fallible boot steps, so a retry `spawn`
  in-process reports "already running" instead of the real cause. Single-boot
  caller today.

## UPSTREAM rs-matter bugs found (worth reporting at Task 20)

1. `impl Display for TLVTag` (`rs-matter/src/tlv.rs:736`) has a `_ => self.fmt(f)`
   arm — **infinite recursion / stack overflow** for any non-`Anonymous`/`Context`
   tag. Never format a `TLVTag` with `{}`; use `{:?}`.
2. `QrPayload::is_valid()` is **inverted**: `check_payload_common_constraints`
   (`pairing/qr.rs:213`) does
   `if VendorId::is_valid_operationally(self.vid) && (self.vid != 0) { return false }`,
   so a *legitimate* operational vendor id makes the payload report itself invalid.
   We never call it, so it doesn't bite us.

## Conventions established across tasks 12–16 (keep following these)

- Commit trailer: `Claude-Session: https://claude.ai/code/session_01BxfHyF8XvzcwxUtWUcDuYM`
  on every commit, matching tasks 1–11 so the branch reads as one execution.
- **`format!("Error::{e}")`, never `format!("{e:?}")`**, for any rs-matter error
  reaching a client-visible message: rs-matter is built with `backtrace` via `os`,
  and `Debug for Error` writes the whole captured backtrace into the WS `details`
  field. `Error::{e}` is byte-identical to `Debug` with backtrace off.
- Never `Display` an rs-matter `Address` into a wire field — it prefixes the
  transport (`"UDP 1.2.3.4:5540"`). Use `ops::addr_to_string` / `ops::ip_of`.
- `embassy_time::Instant::duration_since` is an `unwrap!` that panics on a negative
  delta. Always `saturating_duration_since`.
- `crates/gen`'s `Field.ty`/`Attr.ty` are NOT case-normalized (the IDL has caps
  spellings like `EPOCH_US`, `OCTET_STRING`). Never compare a type name with `==`;
  `tlv_json` folds case centrally. Do NOT lowercase in `gen/build.rs` —
  `Cluster::find_struct` is case-sensitive and `Struct.name` keeps IDL casing.
- rs-matter imports: everything IM is the flat `rs_matter::im::*` re-export (NOT
  `im::encoding::*`); report traits are at `rs_matter::dm`. No `.into()` on id
  types — they're plain aliases and it trips `useless_conversion`.
- Bound recursion on anything walking device-supplied TLV (`tlv_json` caps at 32);
  a `TypeHint` with a struct-typed `ty` must carry `cluster: Some(..)`.
- No `unwrap`/`expect`/`unsafe`/panicking indexing in non-test code, anywhere.
- Every task: two parallel reviews (spec compliance + code quality) before commit.
  Every one of tasks 12–16 needed a fix round, and in each case the quality review
  found something neither the tests nor a clean build could: uppercase IDL type
  names mis-converting timestamps, unbounded recursion aborting the process,
  corrupt `server.json` minting a new fabric and clobbering the CA key,
  subscription reports cross-wired between devices, chunked lists collapsing to
  one element, and a supervisor cancellation leaking a device's subscription slot.
  **Keep the review pass.**

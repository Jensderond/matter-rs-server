# Plan 2 execution notes — handoff state (updated 2026-08-14)

Plan: `docs/superpowers/plans/2026-08-13-plan2-rs-matter-core.md`, executed via
subagent-driven development on branch `plan2-rs-matter-core`.
**Tasks 1–16 are COMPLETE and per-task reviewed** (each with a spec review and a
quality review, plus fix rounds where needed — all closed). **Resume at Task 17**
(`server` — wire the real controller, replacing the stub).

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

`cargo test --workspace` green at every task boundary: 95 → 126 → 136 → 167 →
209 → 239 tests. At Task 16's commit: **239 passing, 0 failed**, `cargo clippy -p
matter-rs-stack --all-targets` clean, `cargo check -p matter-rs-stack
--all-targets` clean in 57s.

> Task 16's fix round landed in full but its agent's completion record was lost;
> the code was verified by inspection (all 8 items present) plus a clean
> `cargo check`. **The one thing not re-run after that fix round is
> `cargo test --workspace`** — do that first on the new machine. It was 239 green
> before the fix round, and the round was comment/doc rewrites plus an AtomicBool
> gate and added tests.

## Environment prerequisites

- **`rs-matter-ref/`** at the repo root: a clone at pinned rev
  `03bc8f2aeb7765a93e7863e2263f73c7bbc3d401`. Gitignored, does NOT travel —
  recreate it before Tasks 17/19. Every task cites paths into it as ground truth.
  A symlink into `~/.cargo/git/checkouts/rs-matter-*/03bc8f2` also works.
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

## Task 19 constraints and must-verifies

- **One stack per process, and `shutdown()` does NOT reset it** (statics stay
  claimed for the process lifetime). So each e2e test needing a live stack must be
  **its own test binary** (separate file in `tests/`), and an in-process restart is
  impossible — the runbook's restart step must relaunch the binary. A second
  `spawn()` refuses cleanly via `Err` on the ready channel.
- **`EventDataTimestamp::EpochTimestamp` unit is unresolved.** rs-matter's doc
  (`im/encoding/event.rs:349`) says "Posix milliseconds since 1970"; the plan says
  epoch-*micros* since the *Matter* epoch — differing by ×1000 AND 30 years.
  rs-matter passes the raw u64 through, so it has no opinion. Task 14 implemented
  the plan's (matter.js-compatible) reading with a `NOTE:` in `convert_timestamp`.
  **Check a real event's `timestamp` against wall-clock.** Note
  `im/events.rs:161`'s TODO: rs-matter *devices* only emit `SystemTimestamp`, so
  an rs-matter-to-rs-matter test never exercises this — needs the matter.js device.
- **IM status `NotFound` (0x8b) on a DefaultSuccess command is reported as "could
  not resolve node via mDNS".** rs-matter's `InvokeRespChunk::receive`
  (`im/client.rs:945-961`) converts a bare non-success `StatusResponse` via
  `to_error_code()` before our code sees a chunk, so 0x8b → `ErrorCode::NotFound`
  → `NodeUnreachable` → wire `NodeNotResolving`. Unfixable in `ops/interact.rs`; a
  consequence of the plan's own `NotFound → NodeUnreachable` rule. Decide whether
  the rule needs a carve-out.
- **Cross-`ReportData`-message list merging is not implemented.** Within one
  message, chunked lists merge correctly; a long `PartsList` in a *subscription*
  report split across messages is reported incomplete, with a runtime warning.
  A real bridge is where this shows.
- **`node_addresses` is `[]` for any node this process run did not commission**
  (see the Task 16 note below).

## README known-limitations section (Task 19 step 4)

- Fixed mDNS hostname `matter-rs-server` (plan-specified). `BuiltinMdns` does no
  name-conflict resolution, so two instances on one LAN segment publish
  conflicting records for `matter-rs-server.local`. `compressed_fabric_id` (already
  on `ReadyInfo`) would make it unique.
- One stack per process, not reset by `shutdown()` — a restart means a new process.
- `node_addresses` empty for nodes not commissioned by this process run.
- Plus the plan's seven accepted v1 deviations (plan lines 27–36).

## Task 18 carryover list (current)

- Silence the `Rig.dir` / `stack_tx` dead-code warnings in the controller test rig.
- **Secret hygiene in `crates/controller/src/storage.rs`** — all three verified NOT
  reachable today, hence deferred: `ServerIdentity` derives `Debug` while holding
  `ca_private_key`/`controller_private_key`/`ipk` as raw `Vec<u8>` (any future
  `{:?}` dumps the fabric trust anchor into a log; wants a redacting `Debug`);
  those `Vec<u8>` copies do not zeroize on drop unlike rs-matter's
  `CryptoSensitive`; `to_writer_pretty` leaves base64 key strings in freed heap.
- `crates/controller/src/commands/interaction.rs:78` does `.map(|v| v as u16)` on
  the timed-invoke timeout, so a client sending 65536 lands as `Some(0)` — a
  request already expired on arrival. Validate the range instead of truncating.
- `fabric_id` has the same stored-scalar-vs-derived-truth exposure that
  `compressed_fabric_id` had (the RCAC carries the real one via
  `CertRef::get_fabric_id()`). Deliberately not auto-corrected: unlike the node id
  a mismatch is not fatal to CASE, so erroring would refuse to boot a working
  install, and silently correcting collides with the "stored wins over CLI flags"
  warn. Needs a decision.
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

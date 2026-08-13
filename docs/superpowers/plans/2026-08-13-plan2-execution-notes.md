# Plan 2 execution notes — handoff state (2026-08-13)

Plan: `docs/superpowers/plans/2026-08-13-plan2-rs-matter-core.md`, executed via
superpowers:subagent-driven-development on branch `plan2-rs-matter-core`.
Tasks 1–11 are COMPLETE and per-task reviewed (each with spec + quality review;
fix rounds where needed, all closed). Execution moved to another machine at
Jens's direction — **resume at Task 12** (`stack` crate scaffold + tlv_json).

The SDD workspace (`.superpowers/sdd/…`, ledger/briefs/reports) is gitignored
and did NOT travel; start a fresh ledger. Everything a resuming controller
needs is in this file + git history.

## Completed tasks (commit ranges)

| Task | What | Commits |
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
| 11 | full vendor table, loglevel, ICD/OTA stubs — 31-command surface complete | ..abb1f4e |

`cargo test --workspace` green at every task boundary (95 tests at handoff).

## Findings AWAITING JENS'S RULING (plan-mandated; do not "fix" silently)

1. **(Important) `ping_node` pings sequentially** though the spec table says
   concurrently — the plan's own sample code chose sequential ("fine at
   homelab scale"); worst case `attempts × 10s × N_addresses` latency.
   commands/nodes.rs `futures_join_all`.
2. **(Minor) `diagnostics` forwards args to get_nodes**, so a client sending
   `only_available` gets a filtered `nodes` array, contradicting the spec's
   "all". Plan sample-code artifact.

## Rulings already made by the executing controller

- **Task 11 vendor table:** plan-internal conflict (brief said "port ALL
  entries" but its pinned test used filter id 1 as an unknown — the full
  1245-entry table resolves 1 = Panasonic). Ruling: full table governs; the
  test's unknown id became 39321 (verified absent). Table extracted from
  `matterjs-server/packages/ws-controller/src/data/VendorIDs.ts`.
- **Worktree skipped** (branch in main checkout): plan tasks copy from the
  gitignored `rs-matter-ref/` and `matterjs-server/` clones at repo root,
  which a worktree would not contain. Same applies on the next machine —
  Tasks 12+ need `rs-matter-ref/` present for API reference (and Task 19
  needs npx/node for the virtual device).

## Deferred minors (feed these to the final whole-branch review, Task 20)

- Task 3: fabric-label truncation counts chars not UTF-16 units (Node parity,
  astral-plane only); credential-id case-fold ASCII-only vs Node's Unicode
  toLowerCase; no parent-dir fsync after atomic rename; no test that 0600
  survives an overwrite.
- Task 6: grace-timer staleness is keyed on map presence, not timer
  generation — theoretical compound race under rapid Connected/Reconnecting
  churn; consider a generation counter if reconnect churn matters.
- Task 8: diagnostics event-ring ordering untested beyond the empty case;
  `alloc_lock`/`log` fields were transitional dead code until Tasks 9–11
  (now consumed).

## Items routed to Task 18 (carryover hygiene batch)

- **tokio `macros` feature:** `node_manager.rs` uses `tokio::select!`, but the
  controller crate's `[dependencies]` tokio lacks `macros` (moved dev-only per
  the plan-1 carryover). `cargo build -p matter-rs-controller` standalone
  fails; masked in workspace builds by feature unification. Add `macros` back
  to `[dependencies]` — non-test code now needs it (supersedes the carryover's
  dev-only suggestion). A pre-existing `Rig.dir`/`stack_tx` dead-code warning
  in the test rig is worth silencing at the same time.

## Notes for Tasks 12–16 (stack crate)

- rs-matter pinned rev `03bc8f2aeb7765a93e7863e2263f73c7bbc3d401`; first build
  compiles rs-matter (~2–4 min warm, ~30 min cold on CT 110 per the spike).
- Verify the `case-resumption` feature name against
  `rs-matter-ref/rs-matter/Cargo.toml` before relying on it (plan Task 12
  flags this).
- The plan's rs-matter API sketches were verified against the checkout at
  planning time, but exact builder idioms may need mechanical adjustment —
  the plan names the ground-truth files per task (tests/im/client_*.rs,
  subscription_reboot.rs:277-297, spike/src/main.rs).

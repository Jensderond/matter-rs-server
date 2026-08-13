# Carryover notes for plan 2 (from plan 1 reviews)

Deferred minors triaged carry-forward by the plan-1 final whole-branch review:

- **Controller impls must own their broadcast `Sender` for their entire
  lifetime** (documented on `subscribe_events` in `crates/controller/src/api.rs`)
  — a rotated/replaced sender degrades every connection's event arm.
- **Drain-on-start_listening stops at `Err(Lagged)`** (`crates/server/src/ws.rs`
  try_recv loop): under broadcast-buffer overflow at the drain moment,
  pre-listening events behind the lag marker could still leak. Revisit when the
  real controller produces sustained event volume (consider re-subscribing
  instead of draining).
- **Broadcast `Lagged` is silently swallowed** in the event forward arm — add a
  `tracing::warn!` for plan-2 debuggability.
- **Config tests are process-env sensitive** (every flag has an `env` binding;
  `PORT` in CI would break `defaults_match_node_server`) — isolate env in tests.
- **`LISTEN_ADDRESS` env var can't supply multiple addresses** (no
  `value_delimiter`).
- **Default bind `[::]` relies on platform dual-stack** (`IPV6_V6ONLY` hosts
  won't serve IPv4) — consider explicit socket option or dual bind.
- **Dependency hygiene:** tokio `macros` feature could be dev-only in
  `crates/controller`; `thiserror` declared but unused in wire + controller;
  two `tokio-tungstenite` versions in the graph (dev-dep 0.24 vs axum's 0.29).
- **Test coverage gaps:** shutdown event untested for non-listening
  connections; multi-listen-address path untested; smoke test leaves its temp
  dir behind.
- **Wire details for plan 3 fixtures:** serde parse-error `Display` is
  currently sent as `details` on malformed JSON (verify wording against Node
  fixtures); ServerInfo test asserts skip-on-None for only 2 of 4 optionals.

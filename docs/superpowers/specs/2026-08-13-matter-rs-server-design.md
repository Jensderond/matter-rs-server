# matter-rs-server — Design

**Date:** 2026-08-13
**Status:** Approved design, pending implementation plan

## Goal

A Rust port of the Open Home Foundation matterjs-server: a Matter controller
daemon exposing the python-matter-server-compatible WebSocket API on port 5580,
usable as a **drop-in replacement** for the Node.js matter-server in a Home
Assistant homelab. Primary motivation is footprint: the Node server idles at
~400 MB RSS; this daemon targets an order-of-magnitude reduction.

### Success criteria

1. The existing Home Assistant Matter integration connects to
   `ws://host:5580/ws` and works unchanged (commissioning, control, live entity
   state) without noticing the swap.
2. Idle RSS < 50 MB with a typical homelab fabric (tens of nodes).
3. Single self-contained binary, systemd service on Debian 13 amd64
   (Proxmox LXC), same CLI/env conventions as the Node server:
   `matter-rs-server --storage-path /var/lib/matter-rs-server`.
4. Devices in scope: WiFi/Ethernet and Thread (behind an existing border
   router). **Out of scope for v1:** BLE commissioning (local and proxy), OTA,
   the web dashboard, WebRTC/cameras, Thread diagnostics/topology (phase 2),
   time sync, Python-storage migration.
5. Migration of the existing matter.js fabric is a **later, separate one-shot
   tool** — not part of the server. v1 starts a fresh fabric; storage is
   designed so the migration tool can write it directly.

## Foundation: rs-matter (pinned main)

Built on [project-chip/rs-matter](https://github.com/project-chip/rs-matter),
pinned to a specific `main` commit as a git dependency
(`rs-matter = { git = "...", rev = "<sha>" }`, locked via `Cargo.lock`,
bumped deliberately). Chosen over the alternative (`phunapps/matter-rust`,
controller-focused but single-maintainer with near-zero community) for
long-term sustainability; rs-matter is CSA/project-chip-affiliated and
multi-contributor. Its commissioner side (PASE/CASE initiators, IM client,
CA/NOC issuance via `onboard`, commissioner mDNS browse, CASE resumption) is
young and unreleased, so:

### Gaps we own on top of rs-matter

1. **Persistence** — rs-matter leaves fabric/CA/session/node storage entirely
   to the caller. We define our own storage (see Storage).
2. **Attestation** — rs-matter has no PAA-chain verification path yet
   (`allow_test_attestation` is effectively mandatory). v1 accepts device
   attestation without chain verification — practically equivalent to what
   `--enable-test-net-dcl` users run today, acceptable on a private LAN.
   Fast-follow: wire rs-matter's `AttestationTrustStore` to a real PAA root
   store once upstream lands verification.
3. **Subscription report loop** — rs-matter establishes subscriptions and
   delivers priming reports; the long-lived listen/auto-resubscribe loop that
   keeps HA entity state live is ours (see Data flow).
4. **Cluster metadata table** — the HA wire format needs field-id→name mapping
   for command responses and events (attribute values are numeric-tagged and
   need no metadata). We reuse rs-matter-codegen's `.matter` IDL parser at
   build time to generate a runtime lookup table (`gen/`).

**Risk posture:** everything rs-matter-specific is confined to one crate
(`stack/`). If upstream stalls or we must fork, the WS server, node registry,
and wire codec are untouched.

## Architecture

One Cargo workspace, one deployed binary, tokio async runtime:

```
matter-rs-server/
├── crates/
│   ├── server/      # binary: CLI+env config, HTTP server (:5580), /health,
│   │                #   /ws upgrade, logging (incl. rotating file logger),
│   │                #   signal handling / systemd integration
│   ├── wire/        # HA protocol: serde models for commands/events, the
│   │                #   TLV<->JSON codec (tag-based + name-based), bigint-safe
│   │                #   JSON, Python-compatible error codes
│   ├── controller/  # node registry, attribute cache, availability tracking,
│   │                #   subscription manager, commissioning orchestration,
│   │                #   credentials store, node-id allocation
│   └── stack/       # ONLY crate importing rs-matter: transports, mDNS,
│                    #   PASE/CASE, IM client, CA/NOC issuance, fabric table
└── gen/             # build-time cluster metadata from .matter IDL files
```

### CLI / deployment

Same flag/env conventions as the Node server where in scope: `--storage-path`
(`STORAGE_PATH`), `--port` (default 5580), `--listen-address` (repeatable),
`--vendorid` (default 0xFFF1), `--fabricid` (default 1), `--log-level`,
`--log-file`, `--primary-interface`, `--default-fabric-label`. Out-of-scope
flags (`--bluetooth-adapter`, `--ble-proxy`, OTA flags, `--disable-dashboard`)
are accepted but warn-and-ignore (the Node server's behavior for its own
deprecated flags), so an existing unit file never fails to start.

Deployment target: Debian 13 amd64 in a Proxmox LXC. Requirements carried over
from the Node server docs: bridged NIC on the same L2 as the Matter devices,
IPv6 link-local enabled, `nf_conntrack_udp_timeout_stream >= 1800` on the
conntrack path for sleepy Thread devices. No Bluetooth needed.

### Endpoints

| Path        | Kind      | Behavior |
|-------------|-----------|----------|
| `/ws`       | WebSocket | The HA-compatible API |
| `GET /health` | HTTP    | `{ version, node_count }`; 405 otherwise |
| anything else | HTTP    | 404 (no dashboard in v1) |

## WebSocket API (schema 13)

Reports `schema_version: 13`, `min_supported_schema_version: 11`. Request
`{message_id, command, args}`; success `{message_id, result}`; error
`{message_id, error_code, details}`. `server_info` is pushed unsolicited on
connect; events flow only after `start_listening`.

### Fully implemented (31 commands)

- Session: `server_info`, `start_listening`, `diagnostics`, `ping_node`,
  `get_node_ip_addresses`
- Nodes: `get_nodes`, `get_node`, `interview_node`, `remove_node`
- Interaction: `device_command`, `read_attribute` (with `*` wildcards),
  `write_attribute`
- Commissioning: `commission_with_code` (QR + manual code),
  `commission_on_network`, `open_commissioning_window`,
  `discover_commissionable_nodes`, `discover`
- Credentials: `set_wifi_credentials`, `set_thread_dataset`,
  `remove_wifi_credentials`, `remove_thread_dataset`, `get_all_credentials`
  (named lists per schema 12; secrets are write-only)
- Fabric: `set_default_fabric_label`, `get_fabric_label`,
  `get_matter_fabrics`, `remove_matter_fabric`, `set_acl_entry`,
  `set_node_binding`
- Misc: `get_vendor_names` (static table), `get_loglevel`, `set_loglevel`

Fabric-label semantics replicated: `--default-fabric-label` pins and locks;
otherwise first `set_default_fabric_label` connection owns the label for its
lifetime; empty → `"HomeAssistant"`; truncated to 32 chars.

### Honest stubs (correct wire shape, degraded behavior)

- `get_icd_state` / `register_icd` / `resync_icd` / `unregister_icd` — report
  "not registered"; real ICD support is a fast-follow if LIT devices appear.
- `check_node_update` — "no update available"; `update_node` /
  `initiate_ota_upload` — error 11 / 101 (OTA out of scope).
- `get_thread_diagnostics`, `get_thread_border_routers`,
  `get_network_topology` — error until phase 2 (HA Thread panel loses detail;
  device control unaffected).
- `import_test_node`, `send_webrtc_provider_command` — invalid command (9).

### Events

`node_added`, `node_updated`, `node_removed`, `node_event`,
`attribute_updated`, `server_shutdown`. (Thread/topology/WebRTC events return
with their phase-2 features; the Node server already gates them per-connection
so their absence is spec-conformant.)

### Wire-format invariants (compatibility-critical)

- Attribute values: **tag-based** JSON structs (numeric field-id keys).
- Command responses and `node_event` data: **name-based** camelCase structs
  (from the generated cluster metadata table).
- `node_id`, `fabric_id`, `compressed_fabric_id`, `event_number`, timestamps:
  emitted as **unquoted u64 JSON numbers**, parsed exactly.
- `attribute_updated` payload: 3-tuple `[node_id, "endpoint/cluster/attr", value]`.
- Attribute paths: decimal `endpoint/cluster/attribute` strings.
- Octet strings ↔ base64; epoch time conversions as in the Node converter.
- `MatterNodeData` shape: `node_id`, `date_commissioned`, `last_interview`,
  `interview_version: 6`, `available`, `is_bridge` (device type 14 in
  `1/29/0`), `attributes`, `attribute_subscriptions: []`.

Reference for all of the above: `matterjs-server/packages/ws-controller/src/server/Converters.ts`
and `packages/ws-client/src/models/`.

## Storage

Own format under `--storage-path`; all writes atomic (temp file + rename in
the same directory); key material files mode 0600. Plain JSON,
human-inspectable.

```
/var/lib/matter-rs-server/
├── server.json    # fabric identity: CA cert+key, operational keypair, NOC,
│                  #   IPK, fabric id / vendor id, controller node id
├── config.json    # fabric label, next node id, named wifi/thread credentials
├── nodes/
│   └── <node-id>.json  # date_commissioned, last_interview, full attribute
│                       #   cache in the exact tag-based JSON shape served to
│                       #   HA (get_nodes = file read)
└── sessions.json  # CASE resumption records (best-effort; loss = slower reconnect)
```

Deliberately not matter.js's WAL format. The future migration tool reads
matter.js storage and writes this layout (constraint from upstream: the CHIP/
matter.js operational key handling means migration re-issues a NOC from the
preserved CA — same approach matterjs-server itself uses for Python
migration).

## Data flow

Per commissioned node, one supervisor task owns the lifecycle:

1. Connect (CASE, resumption when possible).
2. Establish a single wildcard subscription (all attributes + events), like
   matter.js does.
3. Priming report → refresh attribute cache → coalesced `node_updated`.
4. Report loop: change → update cache → `attribute_updated` / `node_event`
   fan-out.
5. On failure: mark reconnecting, retry with backoff. **3-minute grace**
   before `available: false`; reconnect within grace skips the full cache
   rebuild.

WS connections are consumers. Commands go through a dispatcher to the
controller; identical concurrent invokes are deduplicated. Events fan out via
a broadcast channel to every listening connection. Per-connection send queue
with three classes (modeled on the Node server's backpressure design):
**reliable** (command responses — never dropped), **ordered** (events — FIFO,
droppable oldest-first past a cap), **coalescable** (`node_updated` — latest
wins, built lazily at send time). A slow client cannot stall other
connections; a stalled send beyond a watchdog timeout closes that connection.

Node-id allocation: monotonic `next_node_id` in `config.json`, skipping ids
in use, serialized by a mutex (mirrors `ConfigStorage.allocateNodeId`).

## Error handling

Single internal error enum, mapped at the WS boundary to python-matter-server
codes: 0 Unknown, 1 NodeCommissionFailed, 2 NodeInterviewFailed,
3 NodeNotReady, 4 NodeNotResolving, 5 NodeNotExists, 6 VersionMismatch,
7 SDKStackError (rs-matter error string in `details`), 8 InvalidArguments
(validated before any network I/O), 9 InvalidCommand, 10 UpdateCheckError,
11 UpdateError, plus 100/101 where their features exist.

Isolation: each node supervisor is an independent task; a panic is caught,
logged, the node marked unavailable, and the supervisor restarted. No node
can take down the daemon or other nodes. Storage write failures are logged
loudly and retried; the daemon keeps serving from memory.

Shutdown: SIGTERM → `server_shutdown` event to clients → close subscriptions
→ flush storage → exit. Ordered and bounded (matches systemd stop semantics).

## Testing

1. **Wire-codec fixtures (highest value):** run the Node matterjs-server
   locally, capture real WS request/response/event JSON (plus ws-client test
   fixtures), assert our codec produces equivalent output. This targets the
   silent-HA-breakage risk directly.
2. **Integration against virtual devices:** CI commissions a matter.js example
   device (on/off) and exercises read/subscribe/invoke/write, availability
   transitions, and re-subscription after a device restart. rs-matter's own
   interop suite against `chip-all-clusters-app` is the pattern.
3. **Unit tests** for storage atomicity, node-id allocation, error mapping,
   backpressure queue behavior, QR/manual-code parsing.
4. **The real gate:** a staging HA instance pointed at the Rust server with
   one sacrificial real device (one WiFi, one Thread), before the production
   fabric is touched.

## Phasing

- **Phase 0 (spike, go/no-go):** minimal rs-matter program commissions and
  controls one real device from the homelab (WiFi first, then Thread). Proves
  the pinned commit's commissioner path against real hardware before the
  server is built. If it fails in a way upstream can't/won't fix soon,
  re-evaluate the foundation (documented fallback: `phunapps/matter-rust`).
- **Phase 1:** the v1 server as specified above.
- **Phase 2:** Thread diagnostics + border routers, network topology, ICD.
- **Later, separate:** matter.js-storage migration tool; PAA attestation;
  dashboard serving (the existing dashboard SPA is reusable as static files);
  OTA; BLE proxy.

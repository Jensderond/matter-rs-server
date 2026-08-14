# matter-rs-server

Rust port of the OHF matterjs-server: a Matter controller daemon speaking the
python-matter-server-compatible WebSocket API that Home Assistant's Matter
integration uses.

**Status: plan 2 — real controller.** The Matter stack is rs-matter (pinned rev
`03bc8f2`), all 31 WS commands are implemented, and commissioning / control /
subscriptions / restart persistence are verified end to end against a virtual
matter.js device and, at spike time, against real Thread hardware. See
`docs/superpowers/specs/` for the design, `spike/SPIKE-RESULTS.md` for the
rs-matter validation, and `scripts/e2e-virtual-device.md` for the acceptance
runbook.

## Run

    cargo run -p matter-rs-server -- \
      --storage-path /var/lib/matter-rs-server --primary-interface eth0

- `GET /health` -> `{"version", "node_count"}`
- `ws://host:5580/ws` -> python-matter-server WS API (schema 13)

The CLI is matterjs-server's. Flags that are out of scope for v1
(`--bluetooth-adapter`, `--ble-proxy`, `--disable-ota`, `--ota-provider-dir`,
`--disable-dashboard`, `--enable-test-net-dcl`, `--production-mode`) are accepted,
warned about once at startup and ignored, so an existing unit file still starts.

### `--primary-interface`

**Pass it.** One interface carries mDNS and the Matter transport, and the
auto-pick heuristic has no way to tell your LAN from a VM or container bridge — a
dev machine with Docker or virtualisation installed typically offers several
`bridgeN`/`docker0` interfaces, and picking one of those means no device is ever
discovered (spike finding 3). Use the interface that holds the address your Matter
devices can reach: `eth0` on the Debian/LXC target, `en0` on a dev Mac.

On macOS an IPv4 multicast join on the LAN address fails
(`join_multicast_v4 … Invalid argument (os error 22)`) and rs-matter logs a
recurring `Failed to send mDNS broadcast to 224.0.0.251:5353`. Both are harmless:
discovery runs over IPv6 link-local, which is what devices advertise anyway.
Neither appears on Linux.

## Storage layout

Everything lives under `--storage-path` (default `~/.matter_server`), JSON,
written atomically (temp file + rename). Key material is `0600`.

    server.json      fabric identity: CA private key, RCAC, controller key + NOC, IPK
    config.json      fabric_label, next_node_id, wifi_credentials, thread_datasets
    nodes/<id>.json  one file per commissioned node: dates, device fabric index,
                     addresses, and the last full attribute snapshot
    sessions/        rs-matter's DirKvBlobStore: fabric blob + CASE resumption records

`sessions/` being a **directory** is a deliberate deviation from the design spec's
single `sessions.json` — rs-matter's `DirKvBlobStore` owns that tree and we hand it
the path rather than reimplementing its layout. Same best-effort intent: losing it
costs one CASE handshake per node, nothing more.

`server.json` is the fabric. Lose it and every commissioned node is orphaned —
they will still hold a fabric entry pointing at a CA that no longer exists, and
have to be factory-reset. Back it up with the same care as HA's own storage.

A `server.json` that exists but will not parse is a **hard startup error**, never
a "first run": regenerating over it would destroy recoverable key material. A
stored identity also always beats `--fabricid` / `--vendorid`, with a warning,
for the same reason.

## RCAC-direct mode

The controller signs node certificates with the root CA key directly and ships an
empty ICAC, rather than the RCAC → ICAC → NOC chain rs-matter defaults to. This is
spec-legal and explicitly supported upstream (`NocGenerator`'s "RCAC-direct
mode"), and it keeps the stored identity smaller.

It is not a preference: with an ICAC, matter.js rejects `AddNOC` outright (spike
finding 1). The underlying cause turned out to be rs-matter emitting a
certificate's random serial number verbatim as a DER INTEGER, so half of all
generated RCACs and ICACs are *negative* integers that any peer re-encoding the
TLV to DER — matter.js does — hashes differently and rejects with "Signature
verification failed". Dropping the ICAC halves the exposure; the server also
redraws the RCAC until its serial is DER-canonical, which removes it. See the
amendment on spike finding 1 and `crates/stack/src/identity.rs`.

The redraw only protects fabrics minted after it existed. A `server.json` written
earlier has a 50% chance of holding a non-canonical RCAC, and it is never reminted
(that would discard the CA key every commissioned node trusts), so startup logs a
warning naming the symptom instead. If you see it, the only fix is to delete
`server.json` and re-commission every node — a deliberate, destructive operator
decision.

## Deployment notes

    [Service]
    ExecStart=/usr/local/bin/matter-rs-server \
      --storage-path /var/lib/matter-rs-server --primary-interface eth0
    Restart=on-failure
    RestartSec=5

`Restart=on-failure` matters: the binary exits non-zero if the Matter stack thread
dies, and there is no in-process recovery (see the limitations below).

**Thread devices need RA route-info accepted** (spike finding 4). A Thread device
sits behind a border router on a `fd??:.../64` prefix advertised by RA
route-information options, which a Debian host — and in particular an
unprivileged LXC container — ignores by default. Without this, sending to a Thread
device fails with `Network is unreachable`:

    # /etc/sysctl.d/99-matter-thread.conf
    net.ipv6.conf.eth0.accept_ra_rt_info_max_plen = 64

then `sysctl --system`, and `rdisc6 eth0` (package `ndisc6`) to solicit an RA
rather than waiting for the next periodic one. This matches the Node server's
`os_requirements` doc.

**Raise the conntrack UDP timeout** if the host firewalls or NATs Matter traffic.
The controller's UDP flows are long-lived and mostly idle between subscription
heartbeats (60+ s), while `nf_conntrack_udp_timeout_stream` defaults to 120 s — so
the conntrack entry for a live subscription expires between reports, and the next
one from the device is dropped as unsolicited. The symptom is a node that keeps
going unavailable and re-subscribing:

    net.netfilter.nf_conntrack_udp_timeout_stream = 300

**Ports.** Being a controller rather than a commissionable device, the Matter
transport binds an **ephemeral** dual-stack UDP port, not 5540 — nothing to
port-forward, but a stateful firewall has to let the replies back in (hence the
conntrack note above). Devices are reached on *their* UDP 5540. Also needed: UDP
5353 inbound for mDNS (bound with `SO_REUSEPORT` so it coexists with
avahi/mDNSResponder), and TCP 5580 for the WS API. `--listen-address` and `--port`
narrow the WS bind only; they do not affect the Matter transport.

## Known limitations (v1)

- **One Matter stack per process, and `shutdown()` does not release it.**
  rs-matter keeps the `Matter` instance, the exchange buffer pool and the IM state
  in process-wide statics. Restarting the controller means restarting the process;
  a dead stack thread is reported by exiting non-zero rather than by rebooting
  itself.
- **Fixed mDNS hostname `matter-rs-server`.** rs-matter's `BuiltinMdns` does no
  name-conflict resolution, so two instances on one LAN segment publish
  conflicting records for `matter-rs-server.local`.
- **mDNS degradation is log-only** — no event and no flag on the WS API. If
  discovery misbehaves, the mDNS runner's warning in the log is the only signal;
  the stack stays up in a degraded state rather than failing loudly.
- **`node_addresses` is empty for nodes this process run did not commission**
  until their first CASE session refreshes it, so `get_node_ip_addresses` and
  `ping_node` answer from the stored record alone right after a restart.
- **Lists split across `ReportData` *messages* are not merged.** Within one
  message chunked lists merge correctly; a long list (a bridge's `PartsList`) split
  across messages of a subscription report is reported incomplete, with a runtime
  warning.
- **Shutdown drops in-flight work.** A commissioning attempt in progress is
  abandoned, and up to 500 ms of CASE resumption records are lost (cost: one
  handshake).
- **IM status `NotFound` (0x8b) on a command surfaces as "could not resolve node
  via mDNS".** rs-matter converts a bare non-success `StatusResponse` to
  `ErrorCode::NotFound` before our code sees the chunk, and our mapping turns that
  into the wire code `NodeNotResolving`. The command did reach the node.
- **`sessions/` is a directory, not the spec's `sessions.json`.** rs-matter's
  `DirKvBlobStore` owns that tree (fabric blob + CASE resumption records); same
  best-effort intent as the spec, different shape on disk.
- **Out of scope in v1, not deviations:** no BLE commissioning, no OTA provider, no
  dashboard, no DCL. Device attestation runs with `allow_test_attestation` because
  rs-matter has no PAA-chain verification path yet, so a device presenting a test
  DAC is accepted. `ping_node` pings a node's addresses sequentially rather than
  concurrently.

### Accepted parity gaps vs the Node server

The plan fixes these seven deliberately (its "known, accepted v1 deviations"),
recorded here **so nobody "fixes" them silently.** Plan 3's fixtures are where the
ones that need tightening get tightened.

1. **`read_attribute` issues one IM read for all paths**, letting rs-matter chunk
   the response, instead of Node's 9-paths-per-request batching.
2. **Epoch conversion is top-level only.** A `epoch-us`/`epoch-s` attribute whose
   type `gen` knows is converted; the same field nested inside a struct `gen` does
   not know passes through as a raw number.
3. **`node_updated` fires on priming, interview and availability changes**, not on
   Node's 6-second basic-information debounce — so a client can see more of them,
   and sooner, than the Node server would send.
4. **`set_loglevel` drives one global filter.** `file_loglevel` mirrors
   `console_loglevel` when `--log-file` is set and is `null` otherwise; there is no
   independent per-sink level.
5. **`discover` / `discover_commissionable_nodes` return instance name + address
   only.** rs-matter's browse does not expose the mDNS TXT metadata, so every other
   field carries Node's own default (`host_name: "000000000000"`,
   `vendor_id: -1`, `product_id: -1`, …) rather than the device's real value.
6. **No backpressure send-classes.** Node's reliable/ordered/coalescable event
   classes stay deferred; the WS API fans out over one broadcast channel, so a slow
   client drops events (with a warning) instead of being throttled.
7. **`get_vendor_names` is the static CSA table only** — 1245 entries compiled in,
   no DCL lookup, so a vendor id newer than the table has no name.

## Test

    cargo test --workspace

The virtual-device acceptance test is `#[ignore]`d and additionally gated on
`MRS_E2E=1`, since it needs `npx`, a LAN interface and multicast:

    MRS_E2E=1 cargo test -p matter-rs-server --test e2e_virtual -- --ignored --nocapture

`MRS_E2E_INTERFACE` overrides the interface it passes (default `en0`).
`scripts/e2e-virtual-device.md` is the manual runbook and the authoritative gate.

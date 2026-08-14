# E2E: matter-rs-server vs virtual matter.js device

The acceptance gate for the real controller. matter.js is the strictest peer we
have — it re-encodes and re-verifies certificates that CHIP accepts — so a pass
here is worth more than any number of rs-matter-to-rs-matter tests.

Everything below is a transcript of an actual run (macOS, `en0`, node v24.19.0,
`@matter/examples@0.15.6`, rs-matter `03bc8f2`), lightly trimmed. The
`#[ignore]`d test `crates/server/tests/e2e_virtual.rs` automates steps 1–3;
**this runbook is authoritative** — device startup is flaky enough that the test
is a convenience, not the gate.

## 0. Prerequisites

- `node` + `npx` on PATH.
- The LAN interface name. `ifconfig`/`ip addr`; on a dev Mac with Docker or VMs
  installed there will be several `bridgeN` interfaces and the auto-pick heuristic
  will choose one of them, so **always pass `--primary-interface`**.
- A fresh `--storage-path` per attempt. A half-commissioned fabric from a failed
  attempt will confuse the next one.

## 1. Start the virtual device (separate terminal, leave running)

```bash
mkdir -p /tmp/mrs-dev && cd /tmp/mrs-dev
MATTER_STORAGE_PATH=/tmp/mrs-dev/storage npx -y -p @matter/examples matter-device
```

**`-p` is required.** The plan's `npx -y @matter/examples matter-device` fails
with `npm error could not determine executable to run`: the package ships nine
bins and none is named after the package, so npx cannot pick a default. `-p`
installs the package and runs the named bin.

The first invocation downloads ~100 MB; warm it before timing anything. Note the
pairing lines:

```
Commissioning  1786696143690 is uncommissioned passcode: 20202021 discriminator: 3840 manual pairing code: 34970112332
  QR code URL: https://project-chip.github.io/connectedhomeip/qrcode.html?data=MT:Y.K90AFN00KA0648G00
```

The `matter-device` example is a single OnOffLight on endpoint 1 (`0/29/3` =
`[1]`), which is why every command below uses `1/6/0`.

To reset the device to uncommissioned, delete its storage directory — it persists
the fabric, and a device that thinks it is already commissioned will not advertise
`_matterc._udp`. Kill it with `pkill -f '\.bin/matter-device'`; `pkill -f
matter-device` also matches the npx wrapper shell, and `pkill -f DeviceNode.js`
matches nothing at all (the bin is exec'd under its own name). **Check with
`pgrep -f '\.bin/matter-device'` that exactly one is running** — two instances
sharing one storage directory both answer, and the resulting CASE-resumption and
MRP-retransmission noise looks like a bug in our stack.

## 2. Start the server (fresh storage)

```bash
cargo run -p matter-rs-server -- \
  --storage-path /tmp/mrs-e2e --listen-address 127.0.0.1 --primary-interface en0
```

```
INFO matter_rs_stack::runtime: Matter transport bound on [::]:53165
INFO matter_rs_stack::identity: no stored identity; generating a fabric (id 1) for this controller
INFO matter_rs_server: Matter stack ready: fabric 1 (compressed 0x21fdf12a647dc308), fabric index 1, controller node 112233
INFO matter_rs_stack::mdns: using network interface en0 with 192.168.1.216/fdea:… for mDNS
WARN matter_rs_stack::mdns: join_multicast_v4 on 192.168.1.216 failed: Invalid argument (os error 22)
listening on 127.0.0.1:5580
```

Two warnings are **expected on macOS and harmless**: the `join_multicast_v4`
failure above and rs-matter's recurring `Failed to send mDNS broadcast to
224.0.0.251:5353: StdIoError: Invalid argument`. Discovery runs over IPv6
link-local, which is what the device advertises anyway (spike finding 3). On Linux
neither appears.

## 3. Drive it over WS

Any WS client works. `websocat ws://127.0.0.1:5580/ws` and paste; the transcripts
below came from a small `node` script using the built-in `WebSocket`.

On connect the server pushes a bare `server_info` frame with no envelope:

```json
{"fabric_id":1,"compressed_fabric_id":2449378936736301832,"fabric_index":1,
 "schema_version":13,"min_supported_schema_version":11,
 "sdk_version":"matter-rs-server/0.1.0 (rs-matter/03bc8f2)","wifi_credentials_set":false,
 "thread_credentials_set":false,"bluetooth_enabled":false,"ble_proxy_enabled":false,
 "controller_node_id":112233}
```

Send `start_listening` **first**: events are dropped until a connection is
listening, and everything interesting below is an event.

```json
{"message_id":"1","command":"start_listening"}
-> {"message_id":"1","result":[]}

{"message_id":"2","command":"commission_with_code","args":{"code":"MT:Y.K90AFN00KA0648G00"}}
-> {"message_id":"2","result":{"node_id":1,"available":false,"attributes":{…121 paths…},…}}
   {"event":"node_added","data":{"node_id":1,"available":false,…}}
   {"event":"node_updated","data":{"node_id":1,"available":true,…}}
```

Observed timings: `commission_with_code` answered **2.24 s** after the request
(discovery → PASE → ArmFailSafe/CSR/AddNOC → CASE → CommissioningComplete →
interview), and `available` flipped to `true` 27 ms later when the supervisor's
subscription came up. The result is a `MatterNodeData` with `node_id: 1` and 121
attribute paths; `available` is `false` in it because the supervisor connects
after commissioning returns.

```json
{"message_id":"3","command":"get_node_ip_addresses","args":{"node_id":1}}
-> {"message_id":"3","result":["fe80::87f:8d29:2561:f7fb"]}

{"message_id":"4","command":"read_attribute","args":{"node_id":1,"attribute_path":"1/6/0"}}
-> {"message_id":"4","result":{"1/6/0":false}}

{"message_id":"5","command":"device_command",
 "args":{"node_id":1,"endpoint_id":1,"cluster_id":6,"command_name":"toggle","payload":{}}}
-> {"message_id":"5","result":null}
   {"event":"attribute_updated","data":[1,"1/6/0",true]}      # 58 ms later

{"message_id":"6","command":"read_attribute","args":{"node_id":1,"attribute_path":"1/6/0"}}
-> {"message_id":"6","result":{"1/6/0":true}}
```

`toggle` returns `null` (a `DefaultSuccess` command has no response payload) and
the state change arrives on the subscription as `attribute_updated`, not in the
command's reply.

## 4. Restart the server: storage + re-subscription

There is **one Matter stack per process** and `shutdown()` does not release it, so
a restart means a new process — kill the binary and start it again on the same
`--storage-path`.

```bash
pkill -TERM -f 'storage-path /tmp/mrs-e2e'   # clean exit 0
# …same command line as step 2…
```

```
INFO rs_matter::sc::case::resumption: Loaded 1 CASE session resumption record(s) from storage
INFO rs_matter::sc::case::initiator: CASE session resumed (initiator): local_sessid=1, peer_sessid=58748, fabric=1, peer_nodeid=0x1
INFO matter_rs_stack::supervisor: node 1: subscription 2007838464 established, max_int 67s, 121 attribute(s)
INFO matter_rs_controller::node_manager: Node 1 availability changed to true
```

Boot to `available: true` took **2.1 s** here (well inside the ~30 s the plan
allows), because the stored CASE resumption record skipped a full handshake. Then:

```json
{"message_id":"1","command":"start_listening"}
-> {"message_id":"1","result":[{"node_id":1,"available":true,"attributes":{…121…},…}]}
{"message_id":"3","command":"get_node_ip_addresses","args":{"node_id":1}}
-> {"message_id":"3","result":["fe80::87f:8d29:2561:f7fb"]}
{"message_id":"4","command":"read_attribute","args":{"node_id":1,"attribute_path":"1/6/0"}}
-> {"message_id":"4","result":{"1/6/0":true}}     # the toggle from step 3 survived
```

Storage after this point:

```
/tmp/mrs-e2e/server.json    0600  fabric identity: CA key, RCAC, controller key + NOC, IPK
/tmp/mrs-e2e/config.json    0600  fabric_label, next_node_id, wifi_credentials, thread_datasets
/tmp/mrs-e2e/nodes/1.json         one file per node: ids, dates, device fabric index, addresses, attributes
/tmp/mrs-e2e/sessions/k_010b      rs-matter's CASE resumption records
```

## 5. Kill the device: the 3-minute offline grace

```bash
pkill -f '\.bin/matter-device'
```

The subscription's heartbeat lapses first, then the grace timer runs:

```
09:00:30  device killed
09:00:37  WARN matter_rs_stack::supervisor: node 1: subscription 2007838466 went silent, resubscribing
09:02:26  DEBUG matter_rs_stack::supervisor: node 1: subscribe attempt failed: Error::RxTimeout
09:03:37  WARN matter_rs_controller::node_manager: Node 1 offline grace period expired, marking unavailable
```

and on the WS, 3 min 0.004 s after the `went silent` line:

```json
{"event":"node_updated","data":{"node_id":1,"available":false,…}}
```

Total wall time from `pkill` to `available: false` is ~3 min 7 s: up to
`max_int + margin` for the subscription to be declared silent, then the flat
`RECONNECT_GRACE` of 180 s. Start the device again and it comes back on the next
resubscribe attempt (backoff, so up to ~2 min if several attempts have already
failed).

## 6. Optional: a node event with an epoch timestamp

Worth knowing how to reproduce, because it is the only way to see an
`EventDataTimestamp::EpochTimestamp` — rs-matter *devices* only ever emit
`SystemTimestamp` (`rs-matter-ref/rs-matter/src/im/events.rs:161`), so no
rs-matter-to-rs-matter test can exercise that branch.

Priming events are deliberately **not** forwarded (they seed the high-water mark;
see `supervisor::establish` rule 3), so the device's boot events are invisible.
Interrupt the device *while a subscription is live* instead and it emits
`BasicInformation.shutDown` on the way out:

```bash
kill -INT "$(pgrep -f '\.bin/matter-device' | head -1)"
```

```
device: Recorded event #5003: {"eventId":1,"clusterId":40,…,"epochTimestamp":1786698881562,…}
   ws:  {"event":"node_event","data":{"node_id":1,"endpoint_id":0,"cluster_id":40,"event_id":1,
         "event_number":5003,"priority":2,"timestamp":1786698881562,"timestamp_type":1,"data":{}}}
```

`timestamp` is the device's value unchanged: **Posix milliseconds since 1970**
(1786698881562 = 2026-08-14T09:14:41.562Z, matching the observed arrival). Racy —
the report has to get out before the socket closes — so expect a couple of tries.

## Automated version

```bash
MRS_E2E=1 cargo test -p matter-rs-server --test e2e_virtual -- --ignored --nocapture
```

Skips cleanly (early return, no failure) without `MRS_E2E=1`, which is how
`cargo test --workspace` stays hermetic.

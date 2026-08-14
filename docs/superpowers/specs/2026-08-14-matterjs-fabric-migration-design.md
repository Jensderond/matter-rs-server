# matter.js → matter-rs-server fabric migration: design

**Status:** design approved 2026-08-14, plan not yet written.
**Scope:** a one-shot tool that moves an existing matterjs-server fabric onto
matter-rs-server so that **no device is re-commissioned**.

The v1 server spec
(`docs/superpowers/specs/2026-08-13-matter-rs-server-design.md`, decision 5)
deferred this deliberately: "Migration of the existing matter.js fabric is a
**later, separate one-shot tool** — not part of the server. v1 starts a fresh
fabric; storage is designed so the migration tool can write it directly." This
document is that tool.

## Goal

After migration, `matter-rs-server` serves Home Assistant the same fabric the
Node server did: the same devices, reachable, controllable, with their existing
ACLs intact and nothing re-paired. The Node server can be stopped and the Rust
one started in its place.

**Non-goals.** Not a live/incremental sync — it runs once, offline, with both
servers stopped. Not a rollback tool (rollback is "keep the matter.js store and
start the Node server again", which works because the tool never writes to it).
Not a migration of python-matter-server storage, even though a store that was
itself migrated from Python retains those files.

## Why zero-touch is possible

The v1 spec assumed a constraint that turned out not to apply. It said "the
CHIP/matter.js operational key handling means migration re-issues a NOC from the
preserved CA". Inspecting a real store (a clone of the production install, CT 109)
established four facts:

1. **The root CA private key is stored, in plaintext**, at
   `.data.certificates.rootKeyPair.privateKey` (and mirrored under
   `.data.credentials`), next to `rootCertBytes`. So the migrated server can not
   only keep existing devices but still commission new ones — it inherits the CA.
2. **Every identity scalar already equals our default.** `fabricId` = 1,
   `nodeId` = `rootNodeId` = **112233**, `rootVendorId` = 65521 (0xFFF1).
   112233 is the CHIP convention both implementations adopted independently, and
   `matter_rs_stack::identity::CONTROLLER_NODE_ID` is the same value.
3. **The server's boot path and storage schema need no changes.** `CONTROLLER_NODE_ID` is consulted only when
   *generating* a fresh identity; the stored-identity path reads
   `controller_node_id` out of `server.json` and merely requires it to agree with
   the NOC alongside it (`identity.rs`, the `noc_node_id != id.controller_node_id`
   check). A migrated `server.json` therefore boots through the ordinary path.
4. A device authorises an admin by two things: that the presented NOC chains to a
   **root it already trusts**, and that the NOC's subject node id appears in its
   **ACL**. Preserving the root key, the fabric id and node id 112233 satisfies
   both. Devices cannot distinguish the new controller from the old one.

## Source format: matter.js storage

The store root (`/var/lib/matterjs-server`) holds several namespaces as
directories. The one that matters is `server-<fabricIndex>-<vendorIdHex>` —
`server-1-fff1` for the reference install. Each namespace is a KV store:

```
server-1-fff1/
├── driver.json          {"kind":"wal","type":"kv"}
├── snapshot.json.gz     gzip( {commitId:{segment,offset}, ts, data:{…}} )
└── wal/
    └── 00000001.jsonl   one JSON object per line, appended
```

**Reading is snapshot-then-replay.** `snapshot.json.gz` carries `data` as a flat
map of dotted string keys to objects; the WAL then applies mutations recorded
after that snapshot. On the reference install the snapshot is 15 KB and the WAL is
7.2 MB over 38,723 lines, so **replaying the WAL is mandatory, not an
optimisation** — the snapshot alone is badly stale.

Each WAL line is `{"ts":<posix-ms>,"ops":[…]}`. Two op kinds occur:

- `{"op":"upd","key":"<kvkey>","values":{<field>:<value>,…}}` — merge these
  fields into `data[key]`, creating the key if absent.
- `{"op":"del","key":"<kvkey>", …}` — the delete form. **The plan must confirm
  from the data whether `del` carries a field list (deleting fields) or deletes
  the whole key**; both shapes exist in KV WALs and guessing wrong silently
  corrupts the replayed state.

Ordering is file order. `commitId` in the snapshot marks the WAL position the
snapshot already includes; replay must not re-apply records at or before it, or
resurrect deleted fields.

**Value encoding.** Scalars are plain JSON. Anything else is a JSON *string*
containing a nested tagged document:

```json
"{\"__object__\":\"BigInt\",\"__value__\":\"112233\"}"
"{\"__object__\":\"Uint8Array\",\"__value__\":\"0424fed0b3…\"}"
```

`BigInt` carries a decimal string; `Uint8Array` carries **lowercase hex**. Our
storage uses **base64** for byte blobs, so every key and certificate is a
hex→base64 conversion. The decoder must reject an unknown `__object__` tag rather
than pass the raw string through — a silently-unconverted key is a boot failure at
best and a wrong fabric at worst.

### Fields the tool reads

From `.data.credentials.fabric` (equivalently `.data.fabrics.fabrics[0]`):
`fabricId`, `nodeId`, `rootVendorId`, `identityProtectionKey`,
`operationalIdentityProtectionKey`, `operationalId`, `label`.

From `.data.certificates`: `rootKeyPair.privateKey`, `rootCertBytes`.

From `.data.nodes.commissionedNodes` — a serialised `Map`, i.e. an array of
`[nodeIdTagged, {discoveryData:{discoveredAt}, operationalServerAddress:{type,ip,port}, deviceData:{…}}]`.

## What migrates

| Our file | Field | Source |
|---|---|---|
| `server.json` | `fabric_id` | `fabric.fabricId` |
| | `vendor_id` | `fabric.rootVendorId` |
| | `controller_node_id` | `fabric.nodeId` |
| | `ca_private_key` | `certificates.rootKeyPair.privateKey` |
| | `rcac_tlv` | `certificates.rootCertBytes` |
| | `ipk` | `fabric.identityProtectionKey` (see below) |
| | `compressed_fabric_id` | **derived**, then asserted against `fabric.operationalId` |
| | `controller_private_key`, `controller_noc_tlv` | **freshly minted** (see below) |
| | `version` | `IDENTITY_VERSION` |
| `config.json` | `fabric_label` | `fabric.label`, or the default when empty (it is empty on the reference install) |
| | `next_node_id` | `max(commissioned node ids) + 1` |
| | `wifi_credentials`, `thread_datasets` | **empty** — see below |
| `nodes/<id>.json` | `node_id` | the `commissionedNodes` key |
| | `date_commissioned` | `discoveryData.discoveredAt` (Posix ms → our local-time format) |
| | `last_interview` | same as `date_commissioned` |
| | `addresses` | `operationalServerAddress.ip`, bracket-free per `controller::addr` |
| | `device_fabric_index` | **matched** from the device's cached fabric table — see below. Never guessed. |
| | `attributes` | **`{}`** — see below |
| `sessions/` | — | not migrated; a missed CASE resumption costs one handshake |

**The NOC is re-issued, not copied.** matter.js's chain is
root → ICAC → NOC, and ours is RCAC-direct (an accepted v1 deviation, and the
shape `ServerIdentity` encodes). Rather than teach `ServerIdentity` and the
install path about intermediate certs for no behavioural gain, the tool mints a
fresh controller NOC signed **directly** by the preserved root, for node id
112233. Devices accept it: the chain still terminates at the root they trust, and
the subject still matches their ACL. They never see the old ICAC.

This is also the safer direction given a bug found during v1 acceptance testing:
rs-matter emits a certificate's random serial verbatim as a DER INTEGER, so
roughly half of generated certs are negative integers that strict peers reject.
`NocGenerator::encode_serial_asn1` pads correctly, so a *minted NOC* is safe;
`RcacGenerator` does not, which is why the server redraws its own RCACs. Here we
reuse matter.js's root rather than generating one, so that path is not exercised.

**Attribute caches start empty.** Each node file is written with `attributes: {}`.
On boot the server starts a supervisor per node which subscribes and performs a
priming read, repopulating the cache — measured at roughly two seconds per node
during v1 acceptance testing. Translating matter.js's per-endpoint/per-cluster
cache into our tag-based JSON would be, in effect, the wire-parity work a later
plan exists for: a large surface, cluster by cluster, with real risk of subtle
wrongness, bought to avoid a few seconds of empty state. Not worth it.

**`device_fabric_index` must be matched, never assumed — this one is
destructive if wrong.** `remove_node` reads the *stored* index and issues a Matter
`RemoveFabric` with it (`commands/nodes.rs:66-68`), so a wrong non-zero value makes
Home Assistant's "remove device" evict **someone else's admin** from the device.
This is not hypothetical: on the reference install, device `peer1` carries three
fabrics — index 1 labelled "Mijn huis" (another ecosystem), index 2 unlabelled, and
**our fabric at index 3**. Writing the plausible-looking `1` would have removed the
user's other ecosystem from their own device on the first removal, and it would have
looked like a Home Assistant bug.

The real index is recoverable offline. Each node's cached Operational Credentials
cluster (`nodes.peer<N>.endpoints.0.62`, attribute `1` = `fabrics`) lists every
fabric on that device as `{rootPublicKey, vendorId, fabricId, nodeId, label,
fabricIndex}`. The tool finds the entry whose `rootPublicKey` equals the preserved
root's public key and takes its `fabricIndex`.

**When it cannot be determined, write `0`, not a guess.** Fabric index 0 is invalid
in Matter, so the device rejects `RemoveFabric(0)` with a constraint error; the
existing code already warns and proceeds with local removal
(`nodes.rs:68-70`). That degrades to "the fabric is left on the device", which is
untidy and reversible. Guessing a valid-looking index degrades to destroying an
unrelated pairing, which is neither. This asymmetry is the whole argument: **fail
safe, never plausible.**

Note this is the one place the attribute cache is read, for exactly one attribute —
it does not reopen the decision to skip cache translation below.

**No network credentials are stored to migrate.** Searching the reference store
for WiFi/Thread credential material finds only *device diagnostic attributes*
(`wifiActive`, `threadChannel`, `threadPan`, … from clusters 51/53) — not a saved
password or Thread dataset. `config.json` therefore starts with both maps empty,
and Home Assistant supplies credentials on the next commission. This is expected,
not a gap: do not go looking for them.

**`ipk` — which of the two.** matter.js stores both `identityProtectionKey` and
`operationalIdentityProtectionKey` (16 bytes each). Matter's `AddNOC` carries the
IPK *epoch* key, and the operational IPK is derived from it by HKDF salted with
the compressed fabric id; rs-matter's install takes the epoch key and derives the
operational one itself. So the tool reads `identityProtectionKey` — and proves the
choice offline via self-check 3 below, because picking the wrong one produces a
fabric that looks correct and then fails subtly.

## Architecture

A new workspace crate `crates/migrate`, binary `matter-rs-migrate`. Separate from
the server per the v1 spec, and it links `matter-rs-controller` (for `Storage`,
`ServerIdentity`, `NodeRecord`, `addr`) plus `matter-rs-stack` so that **the tool
writes our storage through the same code the server reads it with**. Hand-rolling
the JSON would let the two drift.

**One new public helper is required in `crates/stack`.** Minting a NOC and deriving
a compressed fabric id both need rs-matter, and the architecture confines rs-matter
to that crate — `identity.rs` currently exposes only `ensure_identity`, whose whole
job is the generate-or-load decision the tool is bypassing. So the plan adds
something like

```rust
pub fn identity_from_preserved_ca(
    ca_private_key: &[u8], rcac_tlv: &[u8],
    fabric_id: u64, vendor_id: u16, node_id: u64, ipk: &[u8],
) -> Result<ServerIdentity, Error>
```

which mints the controller key pair and NOC against the supplied CA and returns a
complete `ServerIdentity` with `compressed_fabric_id` derived. This is additive:
no existing signature, behaviour or stored format changes, and the tool stays free
of any direct rs-matter dependency. It also keeps self-checks 1–3 implementable
without duplicating rs-matter's KDFs.

```
matter-rs-migrate --from /var/lib/matterjs-server \
                  --to   /var/lib/matter-rs-server [--write]
```

Four units, each independently testable:

- **`jsdb`** — the matter.js KV reader: locate the namespace, decompress the
  snapshot, replay the WAL, expose `get(key) -> Option<&Map>`. Knows nothing
  about Matter.
- **`decode`** — the tagged-value codec: `BigInt → u64`, `Uint8Array → Vec<u8>`,
  unknown tag → error. Knows nothing about our storage.
- **`convert`** — the mapping table above, producing an in-memory
  `ServerIdentity`, `ConfigData` and `Vec<NodeRecord>`.
- **`main`** — CLI, the self-checks, the report, and the guarded write.

Dry-run is the default. Without `--write` the tool reads, runs every self-check,
prints what it found (fabric id, compressed fabric id, node ids, the files it
would create) and exits non-zero if any check fails. It is **read-only on the
matter.js store under all circumstances** — the WAL is replayed in memory and the
source is never opened for writing, which is what makes rollback simply "start the
Node server again".

## Self-checks

Run in both dry-run and write mode; any failure aborts before writing.

1. **Fabric identity, offline.** Derive the compressed fabric id from the
   preserved root public key and fabric id, and assert it equals matter.js's
   stored `operationalId` (`ca88e679a3505b0a` on the reference install). The
   compressed fabric id is a KDF over exactly those two inputs, so equality
   *proves* it is the same fabric — which is the only thing devices check. No
   network required.
2. **Admin identity, offline.** The minted NOC verifies against the preserved
   RCAC, and its subject node id equals the source `fabric.nodeId`.
3. **IPK choice, offline.** Derive the operational IPK from the chosen epoch key
   and the compressed fabric id, and assert it equals matter.js's stored
   `operationalIdentityProtectionKey`. This is what turns "we think
   `identityProtectionKey` is the epoch key" into a proven statement.
4. **Node accounting.** `next_node_id` exceeds every migrated node id, and the
   number of `nodes/*.json` written equals the number of `commissionedNodes`.
5. **Fabric-index sanity.** For every node, either `device_fabric_index` was
   matched by root public key, or it is `0`. Never a value that was inferred,
   defaulted or copied from another node. The dry-run report lists the resolved
   index per node and says loudly which nodes fell back to `0`, because that list
   is the operator's warning that removing those devices will leave our fabric
   behind on them.

**Online verification is a cutover step, not a development one.** Booting the
migrated server necessarily puts a *second* controller with node id 112233 on the
live fabric, which is exactly what two controllers sharing an operational identity
must not do — observed on the CT 109 clone as devices' subscriptions timing out
and being replaced in a loop as each server evicted the other's slot. So the
online test (control a real device, no re-commissioning) happens only with the
Node server stopped. Checks 1–3 are what make arriving at that step low-risk.

## Error handling

Every failure must leave both stores usable.

- The source is opened read-only; a corrupt or truncated WAL is a hard error
  naming the line number, never a partial migration.
- The destination write reuses `Storage`, so all writes are atomic
  (temp-file-plus-rename) and `server.json`/`config.json` land at 0600.
- `Storage::create_identity` refuses to overwrite an existing `server.json`. The
  tool relies on that rather than checking itself: pointing `--to` at a live
  install fails loudly instead of destroying a fabric.
- An unknown `__object__` tag, a missing required field, or any failed self-check
  aborts before the first write.

## Testing

- **Unit:** `jsdb` snapshot+WAL replay including `del` and the `commitId`
  boundary; `decode` round-trips and unknown-tag rejection; `convert`'s mapping
  including the empty-label default and `next_node_id` arithmetic.
- **Fixture:** a redacted copy of the CT 109 store — real structure, real WAL
  size, key material replaced with generated equivalents — committed as the
  integration fixture. **No production key material in the repo.**
- **Integration:** migrate the fixture into a temp dir, then assert the four
  self-checks pass and that `Storage::load_identity`/`load_nodes` read back what
  was intended. This is the gate that the tool and the server agree.
- **Acceptance (manual, cutover):** migrate the CT 109 clone, boot the server
  against the result with the Node server stopped, and confirm one real device
  responds to a command with no re-commissioning.

## Risks and open questions

- **`rootCertBytes` encoding is unconfirmed** — our field is `rcac_tlv`, i.e.
  Matter TLV. matter.js stores operational certs as TLV, but the plan must verify
  against the actual bytes before trusting it; a DER blob in a TLV field fails at
  fabric install with an unhelpful error.
- **`--primary-interface` is single-valued, and this fabric is not.** The
  reference install has devices on `eth0` (nodes 10, 12), on **`eth1`** (node 22),
  and a Thread ULA (node 23, `fd6a:…`). mDNS binds to one interface, so a single
  `--primary-interface` may leave part of the fabric unresolvable. This is a
  *deployment* question the migration surfaces rather than causes, but it must be
  answered before cutover — otherwise migration will look like it failed.
- **The RCAC serial warn may fire spuriously.** The server warns when a stored
  RCAC's serial is not DER-canonical. matter.js's root was generated by matter.js,
  so its serial may well trip our check even though every device already accepts
  it. The tool should detect this during dry-run and say plainly that it is
  expected and harmless for a migrated fabric, so the warning does not read as a
  failed migration.
- **The devices are multi-admin, which raises the stakes of getting this right and
  the value of getting it done.** Three fabrics on the sampled device means
  re-commissioning would have meant re-pairing with every other ecosystem too —
  and it means the `device_fabric_index` matching above is load-bearing rather
  than pedantic.
- **Five nodes, ids up to 23** on the reference install — the two extra beyond the
  three visible in the stale snapshot are exactly why WAL replay is mandatory. A
  reader that skipped the WAL would silently migrate a subset, which is the
  failure mode most likely to look like success.
- **`del` semantics** must be read off the data, not assumed.
- **A python-migrated store keeps its old files** (`chip_*.ini`,
  `<compressedFabricId>.json`, `certificates/`, `credentials/`). They are
  irrelevant to this tool but will mislead a reader into thinking the CHIP files
  are the source of truth. The tool should ignore them and say so.

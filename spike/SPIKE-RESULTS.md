# Phase 0 spike results

**rs-matter rev:** `03bc8f2aeb7765a93e7863e2263f73c7bbc3d401` (main, 2026-08-01)

## Leg 1 — virtual matter.js device (macOS, 2026-08-13)

Setup: `matter-device` from `@matter/examples` (matter.js) running on
localhost; spike run with `SPIKE_IFACE=en0` against QR `MT:Y.K90AFN00KA0648G00`.

| Stage | Result |
|---|---|
| QR pairing-code parse | OK (passcode + long discriminator + filter) |
| mDNS browse (`_matterc._udp`, by discriminator) | OK, sub-second |
| PASE | OK |
| Commission (over PASE: failsafe, CSR, AddNOC) | **FAIL with ICAC**, OK in RCAC-direct mode (see finding 1) |
| CASE + CommissioningComplete | OK |
| BasicInformation reads (vendor/product) | OK |
| OnOff read + Toggle + verify + restore | OK |

End-to-end run (`SPIKE_NO_ICAC=1`): discovery → commission → control in ~3 s.

### Finding 1 (upstream bug): matter.js rejects rs-matter's ICAC

> **Amended 2026-08-14 (plan 2, Task 19) — the diagnosis below was wrong, and so
> was "OK in RCAC-direct mode".** The real defect is that rs-matter's ASN.1 writer
> emits a certificate's serial number verbatim as the X.509 `serialNumber` INTEGER
> (`cert/asn1_writer.rs:183`), while `RcacGenerator`/`IcacGenerator` fill that
> serial with 8 *random* bytes. Half of those have the top bit set, which is a
> **negative** DER integer. rs-matter signs its own conversion and so verifies its
> own certs, but matter.js re-encodes the Matter TLV to DER, inserts the `0x00`
> sign pad DER requires, hashes a different TBS certificate, and rejects the cert
> — `Signature verification failed`, which is what the ICAC hit here.
> RCAC-direct did not fix it; it only halved the number of coin flips, and the
> spike's RCAC happened to land positive. Task 19's first run failed at
> `AddTrustedRootCertificate` with the identical error on the **RCAC**.
> `NocGenerator::encode_serial_asn1` (`onboard/noc.rs:237`) pads correctly, so NOCs
> were never affected. The server now redraws RCACs until the serial is
> DER-canonical (`crates/stack/src/identity.rs`). The upstream report should be
> about the serial encoding, not about ICAC TLV/DER.

With the default RCAC→ICAC→NOC chain, the device rejects `AddNOC` with
NOCResponse status 3; matter.js log:

```
OperationalCredentials  Building fabric for addNoc failed [crypto-verify] Signature verification failed
  at Icac.verify (@matter/protocol/certificate/kinds/Icac.js:112)
```

matter.js re-encodes the TLV ICAC to DER and verifies its signature against
the RCAC — and that fails, so rs-matter's ICAC TLV/DER encoding and its
signature disagree somewhere. rs-matter's own CI commissions CHIP's
`chip-all-clusters-app` with the same chain, so CHIP is either laxer here or
the encodings happen to agree in the fields CHIP checks. No existing upstream
issue found (nearest: #445, #450 — other matter.js interop fixes, both closed).

- **Workaround (adopted for the server):** RCAC-direct mode — NOC signed by
  the RCAC, empty ICAC. Spec-legal, explicitly supported by
  `NocGenerator` ("RCAC-direct mode"), and simpler for our storage anyway.
- **TODO:** minimal repro + upstream issue against rs-matter (needs
  Jens's go-ahead to file from his account).

### Finding 2 (spike hygiene): failed-attempt lockout

After a failed commissioning attempt the device holds its PASE session +
failsafe for 60 s and answers new PASE attempts with "Pairing already in
progress". Retries must wait or the run times out (`RxTimeout`). The server's
commissioning path should surface this distinctly.

### Finding 3 (portability): mDNS socket + interface selection

- Port 5353 needs `SO_REUSEPORT` (not just `SO_REUSEADDR`) to coexist with a
  system mDNS daemon (macOS mDNSResponder; same applies to avahi on Linux).
- The example interface auto-pick heuristic chose a VM bridge (`bridge100`)
  on the Mac; added `SPIKE_IFACE` override. The server needs a
  `--primary-interface` equivalent from day one (already in the design).
- Multicast join failures should be warnings, not fatal (an interface without
  IPv4 multicast still discovers over IPv6).

### Other notes

- rs-matter advertises IM revision 13; matter.js supports 12 — logged as a
  notice by the device, no functional impact seen.
- After the AddNOC failure, rs-matter did not ack the failing InvokeResponse,
  so the device retransmitted it for ~14 s. Cosmetic here, worth an upstream
  mention.
- The whole spike (incl. the offline CA, commissioning, typed IM reads and
  invoke) compiled against the pinned rev **first try** — the commissioner API
  surface documented in the design holds.

## Leg 1.5 — same virtual device, on the deployment platform (2026-08-13)

Repeated on CT 110 (`dev-jensderond`): Debian 13 amd64, unprivileged Proxmox
LXC, bridged `eth0` — i.e. the exact target environment. `SPIKE_IFACE=eth0
SPIKE_NO_ICAC=1`, device + spike both on the box.

Identical full pass: discovery (link-local IPv6, sub-second) → PASE → AddNOC
→ CASE → reads → toggle, ~2 s end-to-end, exit 0. Validates LXC mDNS
multicast + link-local UDP alongside the protocol flow. Debug build compiles
in ~3 min warm / ~30 min cold on the container.

## Leg 2 — real device: IKEA TOFSMYGGA outdoor plug, Thread (2026-08-13)

HA "Share device" manual pairing code, commissioned into a second (spike)
fabric from CT 110. The plug is a **Thread** device behind the existing
border routers — so this leg also exercised the Thread data path.

| Stage | Result |
|---|---|
| Manual pairing code parse (short discriminator) | OK |
| Discovery (BR advertising proxy → Thread ULA) | OK, sub-second |
| PASE over Thread | OK |
| Commission (RCAC-direct) + CASE + Complete | OK (device fabric #3) |
| BasicInformation | `IKEA of Sweden` / `TOFSMYGGA plug outdoor` |
| OnOff toggle (physical plug) | OK, false→true→false |

### Finding 4 (deployment requirement): Thread mesh route via RA route-info

First attempt failed with `Network is unreachable` sending to the plug's
Thread ULA: the LXC ignores RA route-information options by default, so the
border routers' `fd6a:.../64` route never got installed. Fix (now persisted
on CT 110 in `/etc/sysctl.d/99-matter-thread.conf`):

```
net.ipv6.conf.eth0.accept_ra_rt_info_max_plen = 64
```

plus an RA solicitation (`rdisc6 eth0`, package `ndisc6`) to avoid waiting
for the periodic RA. Matches the Node server's os_requirements doc. Must be
part of the server's deployment docs/checks — a fresh LXC will hit this.

Leftover: the spike fabric remains on the plug (visible in HA under the
device's "connected fabrics"); remove it there, or ignore it.

## Verdict

**GO.** Full commissioner pipeline validated against matter.js (strictest
peer), on the target platform, and against real Thread hardware. Known
constraints for the server: RCAC-direct mode (finding 1), PASE lockout
handling (finding 2), mDNS socket/interface care (finding 3), RA route-info
deployment requirement (finding 4).

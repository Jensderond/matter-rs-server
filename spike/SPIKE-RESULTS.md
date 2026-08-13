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

## Leg 2 — real devices (pending)

Blocked on: dev machine tailnet connectivity + a pairing code from HA
("Share device" on a plug/light, second fabric — production untouched).

## Verdict so far

**GO (provisional).** Everything the server design depends on works against
the strictest available peer (matter.js — the same stack the current
production matterjs-server runs). RCAC-direct sidesteps the one blocker.
Final GO after Leg 2 on real hardware.

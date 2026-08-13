# Phase 0 spike — rs-matter go/no-go

**Prereq reading:** `2026-08-13-matter-rs-server-design.md` (the design this
spike gates).

## Goal

Prove that rs-matter (pinned `main`) can, against Jens's real homelab
hardware:

1. Discover a commissionable device via mDNS (`_matterc._udp`).
2. Commission it: PASE → (attestation accepted in test mode) → CA/NOC issuance
   via rs-matter's `onboard` module → CASE.
3. Control it: read Basic Information, invoke OnOff toggle.
4. Subscribe and receive live attribute reports (toggle the device physically
   or via its own app and see the report arrive).

Target devices: one WiFi/Ethernet device first; if that passes, one Thread
device behind the existing border router.

## Low-risk pairing tip (no factory reset needed)

Don't decommission anything from the production fabric. In Home Assistant,
use **"Share device"** (or `open_commissioning_window` on the current
matterjs-server) to get a temporary pairing code, then commission the device
into a *second* fabric from the spike program. Afterwards the spike fabric can
be removed from the device via HA or ignored.

## How

- Repo: https://github.com/Jensderond/matter-rs-server (private).
- Create `spike/` at the repo root: a standalone Cargo project (not part of
  the future workspace), `rs-matter = { git = "https://github.com/project-chip/rs-matter", rev = "<recent main sha>" }`.
- Start from rs-matter's own controller test harness for API usage patterns:
  `tests/src/bin/commissioner_tests.rs` in the rs-matter repo (it drives
  `Commissioner::commission` + `complete_via_case` against
  `chip-all-clusters-app`).
- Optional warm-up before touching real hardware: run
  matter.js's example on/off device (`matterjs-server` clone is available, or
  `npx @matter/examples` device) locally and commission that first.
- Keep it throwaway-quality; no error-handling polish. Hardcoding the pairing
  code / device IP as CLI args is fine.

## Machine requirements

Linux on the same L2 network as the devices, IPv6 link-local enabled, Rust
stable toolchain. (A Proxmox LXC with a bridged NIC works — that's the
eventual deployment shape.)

## Deliverable

`spike/SPIKE-RESULTS.md` recording, per device:

- rs-matter rev used
- discovery: worked / issues
- commissioning: each stage reached (PASE, attestation, NOC install, CASE)
  and any failures verbatim
- control: read + invoke results
- subscription: did reports arrive, latency impressions
- **Verdict: GO / NO-GO** for building the server on this foundation, plus
  any upstream issues that should be filed/tracked.

If NO-GO: capture enough detail (logs, packet captures if easy) that we can
decide between waiting on upstream, patching a fork, or falling back to
`phunapps/matter-rust` (see design doc, Phasing).

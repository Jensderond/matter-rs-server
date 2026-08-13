# matter-rs-server

Rust port of the OHF matterjs-server: a Matter controller daemon with the
python-matter-server-compatible WebSocket API used by Home Assistant.
Status: plan 1 (protocol skeleton, stub controller). See
`docs/superpowers/specs/` for the design and `spike/SPIKE-RESULTS.md` for
the rs-matter validation.

## Run

    cargo run -p matter-rs-server -- --storage-path /tmp/mrs --listen-address 127.0.0.1

- `GET /health` -> `{"version", "node_count"}`
- `ws://host:5580/ws` -> python-matter-server WS API (schema 13)

## systemd (target deployment)

    [Service]
    ExecStart=/usr/local/bin/matter-rs-server --storage-path /var/lib/matter-rs-server
    Restart=on-failure
    RestartSec=5

Thread devices require the host to accept RA route-info
(`net.ipv6.conf.eth0.accept_ra_rt_info_max_plen = 64`) — see spike finding 4.

## Test

    cargo test

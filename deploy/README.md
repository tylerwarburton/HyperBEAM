# deploy/ — stand up an identical HyperBEAM node

This overlay reproduces this node's runtime setup on another machine (e.g. a
second box physically closer to a test client). The **code** comes from the
`edge-local` branch (this branch is based on it and carries the same local
patches). The files here are the **operational config** — everything the code
tree deliberately leaves out.

## What is NOT here (you must supply these yourself — they are secrets)

- **Arweave node wallet** — the `arweave-keyfile-<address>.json`. Generate or
  copy your own; never commit it. The node's identity/address is derived from it.
- **`hyperbeam-key.json`** — node key; supply your own.
- **TLS certificates** — issued per-host by certbot (see below); not portable.
- **`tools/node_modules/`** — regenerate with `npm install` in `tools/`.

## Prerequisites

- Erlang/OTP 27 + rebar3 (see `.github/workflows/cd.yaml` for the exact build).
- Build deps: `make cmake gcc g++ libssl-dev ncurses-dev`.

## Steps

1. Clone the fork and check out the code branch, then this overlay:
   ```
   git clone git@github.com:tylerwarburton/HyperBEAM.git
   cd HyperBEAM
   git checkout node-deploy      # code (edge-local) + this deploy/ overlay
   ```
2. Build the release (same profile this node runs):
   ```
   rebar3 as rocksdb+genesis_wasm release
   ```
3. Put your config and wallet in place at the repo root:
   ```
   cp deploy/node-config.json ./node-config.json
   # copy YOUR wallet in, then point HB_KEY at it (next step)
   ```
4. Install the service (edit paths/host first — see "Per-host changes"):
   ```
   sudo cp deploy/systemd/hyperbeam*.service deploy/systemd/hyperbeam-maintain.timer /etc/systemd/system/
   sudo cp deploy/tools/* /root/HyperBEAM/tools/   # (or wherever WorkingDirectory points)
   sudo systemctl daemon-reload
   sudo systemctl enable --now hyperbeam.service
   sudo systemctl enable --now hyperbeam-maintain.timer
   ```
5. (Optional) Reverse proxy + TLS:
   ```
   sudo cp deploy/nginx/hyperbeam.tylerw.ai.conf /etc/nginx/sites-available/<your-host>
   # edit server_name, then:
   sudo ln -s /etc/nginx/sites-available/<your-host> /etc/nginx/sites-enabled/
   sudo certbot --nginx -d <your-host>     # issues + wires TLS
   ```
6. (Optional) Log rotation: `sudo cp deploy/logrotate/hyperbeam-events /etc/logrotate.d/`.

## Per-host changes (this box's values are baked in as reference)

- `deploy/systemd/hyperbeam.service` — `HB_KEY` points at THIS node's wallet
  filename; change it to yours. `HB_PORT`, `WorkingDirectory`, and the
  `ExecStart` release path assume `/root/HyperBEAM`.
- `deploy/node-config.json` — `node-host` is `hyperbeam.tylerw.ai`; set it to
  the new host (or drop it for a bare test node). `trusted-devices` are public
  device IDs and can stay.
- `deploy/nginx/*.conf` — `server_name` + the certbot-managed cert paths refer
  to this host; certbot rewrites them for your host on issuance.

## Notes

- `node-config.json` points the store at `cache-mainnet/…`; it is created on
  first run. A fresh node starts with an empty cache and warms via the gateway.
- Keep this branch rebased on `edge-local` so the test node tracks the same
  code + patches as production.

#!/usr/bin/env bash
# RuneRealm production HyperBEAM container. Touches only NAME and BASE.
set -euo pipefail

NAME="${HB_CONTAINER:-hyperbeam-prod}"
# Must match the running image, or `recreate` silently downgrades prod.
# Check with: docker inspect hyperbeam-prod --format '{{.Config.Image}}'
IMAGE="${HB_IMAGE:-sha256:c6d96095d75a406567b09f8bd9edff08675c61aecb00959842b1852629f7316a}"
BASE="${HB_BASE:-/opt/runerealm-hb}"

start() {
  docker run -d \
    --name "$NAME" \
    --restart unless-stopped \
    --ulimit nofile=524288:524288 \
    --workdir /app \
    -p 127.0.0.1:10000:10000 \
    -v "$BASE/store:/app/cache-mainnet" \
    -v "$BASE/cache-http:/app/_build/rocksdb+genesis_wasm/rel/hb/cache-http" \
    -v "$BASE/store-arweave:/app/cache-arweave" \
    -v "$BASE/keys:/app/keys" \
    --mount type=bind,source=/mnt/pxe-backup/hyperbeam,target=/app/export,bind-propagation=rslave \
    -v "$BASE/config/node-config.json:/app/node-config.json:ro" \
    -e HB_PORT=10000 \
    -e HB_KEY=/app/keys/node-key.json \
    -e HB_CONFIG=/app/node-config.json \
    --log-opt max-size=50m --log-opt max-file=5 \
    "$IMAGE"
  echo "started $NAME"
  no_swap
}

# The BEAM's live Erlang heap must not be paged out. Most of this node's RSS is
# clean LMDB mmap, which the kernel can drop and re-read for free; the Erlang
# heap cannot. Banning swap for the cgroup makes the kernel evict those file
# pages instead of the heap. vm.swappiness=1 alone only biases this and loses
# under real pressure. Do NOT add --memory here: a cap below the mapped working
# set evicts the very pages that keep LMDB reads near 0.35 ms.
# The cgroup is recreated with the container, so this is re-applied on start and
# restart. A Docker-initiated restart (--restart unless-stopped, e.g. after a
# host reboot) does not run this: use `hb-prod.sh no-swap` afterwards.
no_swap() {
  local cg
  cg="/sys/fs/cgroup/system.slice/docker-$(docker inspect -f '{{.Id}}' "$NAME").scope"
  if [ -w "$cg/memory.swap.max" ]; then
    echo 0 > "$cg/memory.swap.max"
    echo "swap disabled for $NAME"
  else
    echo "warning: could not set memory.swap.max for $NAME" >&2
  fi
}

case "${1:-}" in
  start)    start ;;
  stop)     docker stop -t 120 "$NAME" ;;
  restart)  docker restart -t 120 "$NAME"; no_swap ;;
  no-swap)  no_swap ;;
  logs)     shift; docker logs "${@:---tail=100}" "$NAME" ;;
  status)   docker ps -a --filter "name=^/${NAME}$" --format "{{.Names}} | {{.Status}} | {{.Ports}}" ;;
  recreate) docker rm -f "$NAME" 2>/dev/null || true; start ;;
  teardown)
    docker rm -f "$NAME" 2>/dev/null || true
    if [ "${2:-}" = "--wipe" ]; then
      echo "refusing an implicit recursive wipe; remove the verified store paths explicitly" >&2
      exit 2
    fi
    echo "container removed"
    ;;
  *) echo "usage: $0 {start|stop|restart|no-swap|logs|status|recreate|teardown}"; exit 2 ;;
esac

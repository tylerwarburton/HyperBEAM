#!/usr/bin/env bash
# Roll async checkpoints to prod in ONE clean restart (~10 s, relayer paused):
#   - new image (default hyperbeam:upgrades-j, built from feat/async-checkpoint)
#   - process-async-checkpoints: true
#   - cache limits 4096/1024 MB and recent-slots 512 (no-op if cache-fix.sh already ran)
# Auto-rolls back if the node does not come up.
#   bash deploy-async.sh                 # apply
#   bash deploy-async.sh rollback STAMP  # restore config, deploy script and previous image
set -euo pipefail
BASE=/opt/runerealm-hb
HB=$BASE/hb-prod.sh
C=$BASE/config/node-config.json
NEW_IMAGE="${NEW_IMAGE:-hyperbeam:upgrades-j}"

wait_up() {
  for i in $(seq 1 90); do
    code=$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:10000/~meta@1.0/info || true)
    [ "$code" = 200 ] && return 0; sleep 2
  done
  return 1
}
rollback() {
  local s="$1"
  systemctl stop paralith-bridge-relayer || true
  "$HB" stop || true
  cp -p "$C.pre-async-$s" "$C"; cp -p "$HB.pre-async-$s" "$HB"
  HB_IMAGE="$(cat "$BASE/image.pre-async-$s")" "$HB" recreate
  wait_up && echo "node up" || echo "NODE NOT UP after rollback" >&2
  systemctl start paralith-bridge-relayer
  echo "rolled back to $(cat "$BASE/image.pre-async-$s")"
}
if [ "${1:-}" = rollback ]; then rollback "$2"; exit 0; fi

echo "== preflight"
NEW_ID=$(docker image inspect "$NEW_IMAGE" --format '{{.Id}}') || { echo "image $NEW_IMAGE not built yet" >&2; exit 2; }
docker run --rm --entrypoint sh "$NEW_IMAGE" -c \
  'grep -rlq delete_batch_guarded /app/_build/rocksdb+genesis_wasm/lib/elmdb/src/ && grep -rlq luerl_gc_deferred /app/_build/rocksdb+genesis_wasm/lib/luerl/src/' \
  || { echo "image lacks the elmdb-delete or luerl patch" >&2; exit 2; }

S=$(date -u +%Y%m%dT%H%M%SZ)
echo "== 1. backups (stamp $S)"
docker inspect hyperbeam-prod --format '{{.Image}}' > "$BASE/image.pre-async-$S"
cp -p "$C" "$C.pre-async-$S"; cp -p "$HB" "$HB.pre-async-$S"

echo "== 2. config"
python3 - <<'EOF'
import json
p='/opt/runerealm-hb/config/node-config.json'
c=json.load(open(p))
want={'process-async-checkpoints':True,'process-hot-cache-mb':4096,'process-replay-cache-mb':1024,
      'store-retention-recent-slots':512,'process-hot-cache-slots':512}
print('before', {k:c.get(k) for k in want})
for k in ['process-hot-cache-mb','process-replay-cache-mb','store-retention-recent-slots','process-hot-cache-slots']:
    if k not in c: print('  note: %s not in live config; adding it' % k)
c.update(want)
json.dump(c,open(p,'w'),indent=2)
print('after ', {k:c[k] for k in want})
EOF
sed -i "s|^IMAGE=\"\${HB_IMAGE:-[^}]*}\"|IMAGE=\"\${HB_IMAGE:-$NEW_ID}\"|" "$HB"
grep -n '^IMAGE=' "$HB"

echo "== 3. relayer pause + clean restart on $NEW_IMAGE"
systemctl stop paralith-bridge-relayer
docker stop -t 120 hyperbeam-prod
HB_IMAGE="$NEW_ID" "$HB" recreate
if ! wait_up; then echo "node did not come up -- rolling back" >&2; rollback "$S"; exit 4; fi
systemctl start paralith-bridge-relayer
systemctl is-active paralith-bridge-relayer
docker inspect hyperbeam-prod --format 'image={{.Image}} restarts={{.RestartCount}} status={{.State.Status}} started={{.State.StartedAt}}'
echo
echo "DONE stamp=$S   rollback: bash $0 rollback $S"

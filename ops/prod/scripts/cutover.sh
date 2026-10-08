#!/usr/bin/env bash
# Cutover: wipe the HyperBEAM prod store and start the audit-fix image.
#
# Operator-run. Every step is reversible until the moved-aside store is deleted,
# which this script never does. Run as root on the prod box:
#
#   NEW_IMAGE=runerealm-hb:audit-fixes-20261006 ./cutover.sh
#
# Rollback (restores the exact pre-cutover node, data included):
#   ./cutover.sh rollback <STAMP printed by the run>
set -euo pipefail

BASE=/opt/runerealm-hb
HB=$BASE/hb-prod.sh
CONFIG=$BASE/config/node-config.json
HERE="$(cd "$(dirname "$0")" && pwd)"
PROPOSED="$HERE/node-config.PROPOSED.json"
OLD_IMAGE="$(docker inspect hyperbeam-prod --format '{{.Image}}')"

rollback() {
  local stamp="$1"
  [ -d "$BASE/store.pre-wipe-$stamp" ] || { echo "no $BASE/store.pre-wipe-$stamp" >&2; exit 2; }
  "$HB" stop || true
  mv "$BASE/store" "$BASE/store.failed-$stamp"
  mv "$BASE/store.pre-wipe-$stamp" "$BASE/store"
  cp -p "$CONFIG.pre-wipe-$stamp" "$CONFIG"
  cp -p "$HB.pre-wipe-$stamp" "$HB"
  HB_IMAGE="$(cat "$BASE/image.pre-wipe-$stamp")" "$HB" recreate
  echo "rolled back to $(cat "$BASE/image.pre-wipe-$stamp"); relayer left STOPPED — start it yourself once verified"
}

if [ "${1:-}" = rollback ]; then rollback "$2"; exit 0; fi

: "${NEW_IMAGE:?set NEW_IMAGE}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"

echo "== preflight"
docker image inspect "$NEW_IMAGE" >/dev/null
NEW_ID="$(docker image inspect "$NEW_IMAGE" --format '{{.Id}}')"
# A committed image once kept a `sleep` ENTRYPOINT and crash-looped prod (OPS-CHANGELOG 2026-09-26).
EP="$(docker image inspect "$NEW_IMAGE" --format '{{json .Config}}' | python3 -c 'import json,sys; c=json.load(sys.stdin); print(c.get("Entrypoint"), c.get("Cmd"))')"
echo "entrypoint/cmd: $EP"
case "$EP" in *sleep*) echo "refusing: entrypoint contains sleep" >&2; exit 2 ;; esac
python3 -m json.tool "$PROPOSED" >/dev/null
# The new luerl patch must be in the release, not just the source.
docker run --rm --entrypoint sh "$NEW_IMAGE" -c \
  'grep -rq luerl_gc_deferred /app/_build/rocksdb+genesis_wasm/lib/luerl/src/' \
  || echo "warning: could not confirm the deferred-GC luerl patch marker; check by hand" >&2

echo "== 1. freeze the bridge relayer (stays stopped until a new pusd is deployed)"
systemctl stop paralith-bridge-relayer
systemctl is-active paralith-bridge-relayer || true

echo "== 2. stop the node"
docker stop -t 120 hyperbeam-prod || true

echo "== 3. move the store aside (instant, same filesystem; NOT deleted)"
echo "$OLD_IMAGE" > "$BASE/image.pre-wipe-$STAMP"
mv "$BASE/store" "$BASE/store.pre-wipe-$STAMP"
install -d -m 0755 "$BASE/store"

echo "== 4. config + deploy-script default (the stale HB_IMAGE default has downgraded prod twice)"
cp -p "$CONFIG" "$CONFIG.pre-wipe-$STAMP"
cp -p "$HB" "$HB.pre-wipe-$STAMP"
install -m 0644 "$PROPOSED" "$CONFIG"
sed -i "s|^IMAGE=\"\${HB_IMAGE:-[^}]*}\"|IMAGE=\"\${HB_IMAGE:-$NEW_ID}\"|" "$HB"
grep -n '^IMAGE=' "$HB"

echo "== 5. start the new image on the empty store"
HB_IMAGE="$NEW_ID" "$HB" recreate

echo "== 6. smoke"
for i in $(seq 1 60); do
  code="$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:10000/~meta@1.0/info || true)"
  [ "$code" = 200 ] && break; sleep 2
done
echo "meta: $code"
cat "/sys/fs/cgroup/system.slice/docker-$(docker inspect -f '{{.Id}}' hyperbeam-prod).scope/memory.swap.max"
docker inspect hyperbeam-prod --format 'image={{.Image}} restarts={{.RestartCount}} status={{.State.Status}}'
docker logs --tail 40 hyperbeam-prod 2>&1 | grep -iE "error|crash|badarg|exception" || echo "no errors in last 40 log lines"

cat <<EOF

DONE  stamp=$STAMP
  old store kept at $BASE/store.pre-wipe-$STAMP  (delete only after you are satisfied)
  rollback:  $0 rollback $STAMP
  relayer:   STOPPED — repoint it at the new pusd process, then: systemctl start paralith-bridge-relayer
EOF

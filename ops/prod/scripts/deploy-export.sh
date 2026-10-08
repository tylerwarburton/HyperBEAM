#!/usr/bin/env bash
# Turn on the essentials export to the pxe NFS share and essentials pruning
# (dry-run first), on a new image. One clean restart (~10 s), relayer paused.
#
#   NEW_IMAGE=hyperbeam:upgrades-retention-h ./deploy-export.sh
#   ./deploy-export.sh rollback <STAMP>
set -euo pipefail

BASE=/opt/runerealm-hb
HB=$BASE/hb-prod.sh
CONFIG=$BASE/config/node-config.json
HERE="$(cd "$(dirname "$0")" && pwd)"
NEWCONF="$HERE/node-config.EXPORT.json"
EXPORT_HOST=/mnt/pxe-backup/hyperbeam
MOUNT_LINE="    --mount type=bind,source=$EXPORT_HOST,target=/app/export,bind-propagation=rslave \\\\"

rollback() {
  local stamp="$1"
  systemctl stop paralith-bridge-relayer || true
  "$HB" stop || true
  cp -p "$CONFIG.pre-export-$stamp" "$CONFIG"
  cp -p "$HB.pre-export-$stamp" "$HB"
  HB_IMAGE="$(cat "$BASE/image.pre-export-$stamp")" "$HB" recreate
  systemctl start paralith-bridge-relayer
  echo "rolled back to $(cat "$BASE/image.pre-export-$stamp")"
}
if [ "${1:-}" = rollback ]; then rollback "$2"; exit 0; fi

: "${NEW_IMAGE:?set NEW_IMAGE}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OLD_IMAGE="$(docker inspect hyperbeam-prod --format '{{.Image}}')"
NEW_ID="$(docker image inspect "$NEW_IMAGE" --format '{{.Id}}')"

echo "== preflight"
python3 -m json.tool "$NEWCONF" >/dev/null
docker run --rm --entrypoint sh "$NEW_IMAGE" -c \
  'grep -rlq delete_batch_guarded /app/_build/rocksdb+genesis_wasm/lib/elmdb/src/ && grep -rlq luerl_gc_deferred /app/_build/rocksdb+genesis_wasm/lib/luerl/src/' \
  || { echo "image lacks the elmdb-delete or luerl patch" >&2; exit 2; }
findmnt -no FSTYPE /mnt/pxe-backup | grep -q nfs || ls /mnt/pxe-backup >/dev/null   # trigger automount
findmnt -no FSTYPE /mnt/pxe-backup | grep -q nfs || { echo "pxe share not mounted" >&2; exit 2; }
mkdir -p "$EXPORT_HOST/essentials"

echo "== 1. back up config, deploy script, image id"
echo "$OLD_IMAGE" > "$BASE/image.pre-export-$STAMP"
cp -p "$CONFIG" "$CONFIG.pre-export-$STAMP"
cp -p "$HB" "$HB.pre-export-$STAMP"

echo "== 2. config + deploy script (image default, export mount)"
install -m 0644 "$NEWCONF" "$CONFIG"
sed -i "s|^IMAGE=\"\${HB_IMAGE:-[^}]*}\"|IMAGE=\"\${HB_IMAGE:-$NEW_ID}\"|" "$HB"
grep -q 'target=/app/export' "$HB" || \
  sed -i "s|^\(    -v \"\$BASE/keys:/app/keys\" \\\\\)$|\1\n$MOUNT_LINE|" "$HB"
grep -q 'target=/app/export' "$HB" || { echo "failed to add the export mount" >&2; exit 3; }
grep -n '^IMAGE=\|target=/app/export\|-t 120' "$HB"

echo "== 3. relayer pause + clean restart on the new image"
systemctl stop paralith-bridge-relayer
docker stop -t 120 hyperbeam-prod
HB_IMAGE="$NEW_ID" "$HB" recreate
for i in $(seq 1 90); do
  code="$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:10000/~meta@1.0/info || true)"
  [ "$code" = 200 ] && break; sleep 2
done
echo "meta: $code"
if [ "$code" != 200 ]; then echo "node did not come up -- rolling back" >&2; rollback "$STAMP"; exit 4; fi
systemctl start paralith-bridge-relayer
systemctl is-active paralith-bridge-relayer

echo "== 4. smoke"
docker inspect hyperbeam-prod --format 'image={{.Image}} restarts={{.RestartCount}} status={{.State.Status}}'
docker logs --since 2m hyperbeam-prod 2>&1 | grep -ciE "unclean" || true
cat <<EOF

DONE stamp=$STAMP
  export: shipping to $EXPORT_HOST/essentials (first base of the existing store runs in the background)
  essentials pruning: DRY-RUN; switch essentials-retention-dry-run to false after a clean report
  rollback: $0 rollback $STAMP
EOF

#!/usr/bin/env bash
# Enable the essentials store + NFS export + retention (dry-run) on hyperbeam-prod.
#
# Operator-run. Reversible: the main store is only read; the old config, the old
# deploy script and the old image id are kept beside the originals.
#
#   NEW_IMAGE=hyperbeam:upgrades-retention-g ./enable-essentials.sh
#   ./enable-essentials.sh rollback <STAMP>
set -euo pipefail

BASE=/opt/runerealm-hb
HB=$BASE/hb-prod.sh
CONFIG=$BASE/config/node-config.json
HERE="$(cd "$(dirname "$0")" && pwd)"
NEWCONF="${NEWCONF:-$HERE/node-config.ESSENTIALS-NOEXPORT.json}"
EXPORT_HOST=/mnt/pxe-backup/hyperbeam
MOUNT_LINE="    --mount type=bind,source=$EXPORT_HOST,target=/app/export,bind-propagation=rslave \\\\"

rollback() {
  local stamp="$1"
  "$HB" stop || true
  cp -p "$CONFIG.pre-essentials-$stamp" "$CONFIG"
  cp -p "$HB.pre-essentials-$stamp" "$HB"
  HB_IMAGE="$(cat "$BASE/image.pre-essentials-$stamp")" "$HB" recreate
  echo "rolled back; relayer left STOPPED -- start it once verified"
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
if [ "${WITH_EXPORT:-0}" = 1 ]; then
  mkdir -p "$EXPORT_HOST/essentials"            # also triggers the automount
  findmnt -no FSTYPE "/mnt/pxe-backup" | grep -q nfs || { echo "pxe share not mounted" >&2; exit 2; }
fi

echo "== 1. freeze the bridge relayer"
systemctl stop paralith-bridge-relayer

echo "== 2. clean stop (writes the export's stopped marker on new images; harmless on the old one)"
docker stop -t 120 hyperbeam-prod

echo "== 3. config + deploy script"
echo "$OLD_IMAGE" > "$BASE/image.pre-essentials-$STAMP"
cp -p "$CONFIG" "$CONFIG.pre-essentials-$STAMP"
cp -p "$HB" "$HB.pre-essentials-$STAMP"
install -m 0644 "$NEWCONF" "$CONFIG"
sed -i "s|^IMAGE=\"\${HB_IMAGE:-[^}]*}\"|IMAGE=\"\${HB_IMAGE:-$NEW_ID}\"|" "$HB"
if [ "${WITH_EXPORT:-0}" = 1 ]; then
  grep -q 'target=/app/export' "$HB" || \
    sed -i "s|^\(    -v \"\$BASE/keys:/app/keys\" \\\\\)$|\1\n$MOUNT_LINE|" "$HB"
  grep -q 'target=/app/export' "$HB" || { echo "failed to add the export mount to hb-prod.sh" >&2; exit 3; }
fi
# A clean shutdown under load takes >10 s (docker's default), and a SIGKILL'd node
# counts as an unclean stop. Give stop/restart the same 120 s this script uses.
sed -i 's|^  stop)     docker stop "\$NAME" ;;|  stop)     docker stop -t 120 "$NAME" ;;|; s|^  restart)  docker restart "\$NAME"; no_swap ;;|  restart)  docker restart -t 120 "$NAME"; no_swap ;;|' "$HB"
grep -n '^IMAGE=\|target=/app/export\|-t 120' "$HB" || true

echo "== 4. start the new image (same store)"
HB_IMAGE="$NEW_ID" "$HB" recreate
for i in $(seq 1 90); do
  code="$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:10000/~meta@1.0/info || true)"
  [ "$code" = 200 ] && break; sleep 2
done
echo "meta: $code"; [ "$code" = 200 ] || { echo "node did not come up; run: $0 rollback $STAMP" >&2; exit 4; }

echo "== 5. migrate existing essentials (main store only read) + byte-for-byte verify"
if ! "$HERE/migrate.sh" hyperbeam-prod; then
  echo "MIGRATION FAILED -- node is up on the new image; essentials store incomplete." >&2
  echo "Roll back with: $0 rollback $STAMP" >&2
  exit 5
fi

echo "== 6. smoke"
docker inspect hyperbeam-prod --format 'image={{.Image}} restarts={{.RestartCount}} status={{.State.Status}}'
docker logs --since 2m hyperbeam-prod 2>&1 | grep -iE "unclean|crash|badarg|exception" | head -5 || true
[ "${WITH_EXPORT:-0}" = 1 ] && ls -la "$EXPORT_HOST/essentials" | head || true

cat <<EOF

DONE stamp=$STAMP  (retention is in DRY-RUN; nothing is deleted yet)
  rollback:  $0 rollback $STAMP
  relayer:   STOPPED -- start it after checking: systemctl start paralith-bridge-relayer
  next:      after a clean dry-run report, set store-retention-dry-run false and docker restart
EOF

#!/usr/bin/env bash
# The ship gate for a node build (PARALITH.md, "Ship gate"): the process
# worker's ms per game slot, in the built image, on a game-sized process.
#
#   bench/game-slot-bench.sh <image> [players=300] [slots=200] [max-worker-ms=20]
#
# Runs docker locally, or on a box with GAME_BENCH_SSH="root@<host> -i <key>".
# A throwaway container (3 CPUs); nothing running is touched. Exits non-zero
# when the worker's mean ms per slot is above the limit. ~2 minutes.
set -euo pipefail
IMAGE=${1:?usage: game-slot-bench.sh <image> [players] [slots] [max-worker-ms]}
PLAYERS=${2:-300}
SLOTS=${3:-200}
MAX=${4:-20}
SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/game_slot_bench.erl"
D() {
  if [ -n "${GAME_BENCH_SSH:-}" ]; then
    # shellcheck disable=SC2086
    ssh -o IdentitiesOnly=yes $GAME_BENCH_SSH "docker $(printf '%q ' "$@")"
  else docker "$@"; fi
}
NAME=game-slot-bench-$$
trap 'D rm -f "$NAME" >/dev/null 2>&1 || true' EXIT
D run -d --name "$NAME" --cpus 3 --entrypoint sleep "$IMAGE" infinity >/dev/null
D exec -i "$NAME" sh -c 'cat > /app/src/core/game_slot_bench.erl' < "$SRC"
OUT=$(D exec -e "GAME_BENCH=$PLAYERS:$SLOTS:/tmp/game-bench:$MAX" "$NAME" sh -c \
  'cd /app && rebar3 device test --with-core --module=game_slot_bench 2>&1' || true)
LINE=$(printf '%s' "$OUT" | tr -d '\n' | tr -s ' ' | grep -o 'GAME_BENCH.*\] ok\|GAME_BENCH.*failed' || true)
if [ -z "$LINE" ]; then
  printf '%s\n' "$OUT" | tail -40; echo "game slot bench: no result" >&2; exit 1
fi
echo "$LINE"
case "$OUT" in
  *"Test passed."*) echo "game slot bench: PASS (worker <= $MAX ms/slot)";;
  *) echo "game slot bench: FAIL (worker above $MAX ms/slot, or the run broke)" >&2; exit 1;;
esac

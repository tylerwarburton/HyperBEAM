#!/usr/bin/env bash
# The message trace's overhead (`hb_msg_trace'): the game slot bench on this
# source tree, trace on and off, alternating, in one throwaway container.
#
#   bench/trace-overhead.sh <image> [runs=2] [players=300] [slots=200]
#
# Copies src/ and bench/game_slot_bench.erl into the image, then prints one
# GAME_BENCH line per run labelled trace=on|off. Local docker only.
set -euo pipefail
IMAGE=${1:?usage: trace-overhead.sh <image> [runs] [players] [slots]}
RUNS=${2:-2}; PLAYERS=${3:-300}; SLOTS=${4:-200}
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NAME=trace-overhead-$$
trap 'docker rm -f "$NAME" >/dev/null 2>&1 || true' EXIT
MSYS_NO_PATHCONV=1 docker run -d --name "$NAME" --cpus 3 -v "$ROOT/src:/src-new:ro" \
  --entrypoint sleep "$IMAGE" infinity >/dev/null
docker exec -i "$NAME" sh -c 'cat > /tmp/game_slot_bench.erl' < "$ROOT/bench/game_slot_bench.erl"
docker exec "$NAME" sh -c 'cd /app && cp -r /src-new/. src/ && cp /tmp/game_slot_bench.erl src/core/'
for i in $(seq 1 "$RUNS"); do
  for T in on off; do
    OUT=$(docker exec -e "GAME_BENCH=$PLAYERS:$SLOTS:/tmp/game-bench" \
      -e "GAME_BENCH_TRACE=$([ $T = on ] && echo true || echo false)" "$NAME" sh -c \
      'cd /app && rebar3 device test --with-core --module=game_slot_bench 2>&1' || true)
    echo "trace=$T $(printf '%s' "$OUT" | tr -d '\n' | tr -s ' ' | grep -o 'GAME_BENCH players[^|]*' || echo 'no result')"
  done
done

#!/bin/bash
# kill -9 test for a wasm-patch@1.0 process: schedule 260 slots, then compute
# them while the VM is killed at random points 12 times; a last run computes
# to the end and checks every slot against a crash-free model.
#   HB_WASM_LUA=<wasm-lua runtime .wasm> bench/wasm-patch-crash.sh   (from a built tree)
set -u
cd "$(dirname "$0")/.."
D=/tmp/crash; rm -rf $D; mkdir -p $D
E="erl -noshell -pa _build/default/lib/*/ebin -pa /tmp/wpc"
: "${HB_WASM_LUA:?the wasm-lua runtime .wasm}"
mkdir -p /tmp/wpc && erlc -o /tmp/wpc bench/wasm_patch_crash.erl
$E -eval 'wasm_patch_crash:schedule("/tmp/crash", 260), halt().' 2>&1 | tail -1
for k in $(seq 1 12); do
  $E -eval 'wasm_patch_crash:compute("/tmp/crash"), halt().' > $D/run-$k.log 2>&1 &
  P=$!
  S=$(( (RANDOM % 6) + 2 ))
  sleep $S
  kill -9 $P 2>/dev/null && echo "kill $k after ${S}s at $(grep -c ^slot $D/run-$k.log) slots" || echo "run $k finished before kill"
  wait $P 2>/dev/null
done
$E -eval 'wasm_patch_crash:compute("/tmp/crash"), halt().' 2>&1 | tail -1
$E -eval 'wasm_patch_crash:check("/tmp/crash"), halt().' 2>&1 | grep CHECK

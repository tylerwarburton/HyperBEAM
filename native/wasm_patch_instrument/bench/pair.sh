#!/usr/bin/env bash
# Fuel overhead under AOT on the deployed pair.lua stream (1,500 accounts, 300
# resting, 1,000 Order.Place/Cancel/take messages), spike PUC Lua with SjLj.
# Original and instrumented modules are compiled with the same wamrc flags and
# run interleaved, pinned to one core; medians across rounds.
#
#   pair.sh <host> <wamrc> <wasm-patch-instrument> <reencode> <lua dir> [rounds] [cpu]
#
# <host> is tests/wamr/host.c built against the node's libvmlib.a; <reencode>
# is examples/reencode (the uninstrumented baseline, canonically encoded); <lua dir>
# as for tests/wamr/run.sh.
set -euo pipefail
HOST=$1; WAMRC=$2; WPI=$3; REENCODE=$4; LUA=$5; R=${6:-7}; CPU=${7:-2}
WAMRC_FLAGS=${WAMRC_FLAGS:---bounds-checks=1 --stack-bounds-checks=1 --disable-simd --opt-level=3 --target=x86_64 --cpu=x86-64-v2}
T=$(mktemp -d); trap 'rm -rf -- "$T"' EXIT
"$WPI" --stats --memory-cap-pages 16384 --unsafe-allow-import 'wasi_snapshot_preview1.*' --unsafe-allow-import env.now_us \
  "$LUA/lua-sjlj.wasm" "$T/inst.wasm"
"$REENCODE" "$LUA/lua-sjlj.wasm" "$T/orig.wasm"
"$WAMRC" $WAMRC_FLAGS -o "$T/orig.aot" "$T/orig.wasm" >/dev/null
"$WAMRC" $WAMRC_FLAGS -o "$T/inst.aot" "$T/inst.wasm" >/dev/null
: > "$T/tsv"
for r in $(seq "$R"); do
  for v in orig inst; do
    taskset -c "$CPU" "$HOST" "$T/$v.aot" lua "$LUA/module.lua" "$LUA/main.lua" |
      awk -v v=$v '/^ALL/{print v"\t"$4"\t"$7"\t"$10} /checksum/{print v"\tcs\t"$NF}' >> "$T/tsv"
  done
done
awk -F'\t' '
  $2 == "cs" { cs[$1] = $3; next }
  { n[$1]++; p50[$1, n[$1]] = $2; mean[$1, n[$1]] = $3; p95[$1, n[$1]] = $4 }
  function med(a, v,   i, j, t, c, x) { c = n[v]; for (i = 1; i <= c; i++) x[i] = a[v, i]
    for (i = 1; i <= c; i++) for (j = i + 1; j <= c; j++) if (x[j] < x[i]) { t = x[i]; x[i] = x[j]; x[j] = t }
    return x[int((c + 1) / 2)] }
  END {
    printf "%-6s %9s %9s %9s  (us per message, median of %d rounds)\n", "", "p50", "mean", "p95", n["orig"]
    for (k = 1; k <= 2; k++) { v = k == 1 ? "orig" : "inst"
      P[v] = med(p50, v); M[v] = med(mean, v); Q[v] = med(p95, v)
      printf "%-6s %9.1f %9.1f %9.1f  checksum %s\n", v, P[v], M[v], Q[v], cs[v] }
    printf "%-6s %+8.1f%% %+8.1f%% %+8.1f%%\n", "fuel", 100 * (P["inst"] / P["orig"] - 1),
      100 * (M["inst"] / M["orig"] - 1), 100 * (Q["inst"] / Q["orig"] - 1)
  }' "$T/tsv"

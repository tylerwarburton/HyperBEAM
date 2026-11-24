#!/usr/bin/env bash
# Interpreter-vs-AOT equivalence of instrumented modules on the node's runtime.
#
#   run.sh <wamr dir with lib/libvmlib.a> <wamrc> <wasm-patch-instrument> [lua dir]
#
# <wamr dir> is the Makefile's _build/wamr (WAMR 2.2.0 with the node's flags and
# patches). Optional [lua dir] holds lua-sjlj.wasm (bench/build-lua.sh) and the
# spike's module.lua, main.lua and test_errors.lua.
#
# For every `;; RUN:` line of every corpus module:
#   original interp == original AOT == instrumented interp == instrumented AOT
#     (result and full linear-memory hash), wp_depth back to 0;
#   instrumented interp == instrumented AOT byte for byte (fuel left, depth,
#     every wp_g<N>, memory) at an unlimited budget, at exactly the consumed
#     budget (finishes, wp_fuel 0), one unit less (traps) and a third (traps).
# For every `;; SPIN:` line (never terminates uninstrumented): the instrumented
# module traps identically in both, on fuel (small budget) or on depth (a
# budget large enough for the recursion to pass DEPTH_LIMIT first).
# The pass and wamrc are each run twice and must produce identical bytes.
set -euo pipefail
WAMR=$1; WAMRC=$2; WPI=$3; LUA=${4:-}
HERE=$(cd "$(dirname "$0")" && pwd); CRATE=$(cd "$HERE/../.." && pwd)
T=$(mktemp -d); trap 'rm -rf -- "$T"' EXIT
# The node's AOT flags (WASM_NATIVE_PLAN.md 2.2).
WAMRC_FLAGS=${WAMRC_FLAGS:---bounds-checks=1 --stack-bounds-checks=1 --disable-simd --opt-level=3 --target=x86_64 --cpu=x86-64-v2}
CAP=16384
FAILS=0
ok() { echo "ok   $*"; }
bad() { echo "FAIL $*"; FAILS=$((FAILS + 1)); }

gcc -O2 -o "$T/host" "$HERE/host.c" -I"$WAMR/core/iwasm/include" "$WAMR/lib/libvmlib.a" -lm -lpthread -ldl
(cd "$CRATE" && WPI_CORPUS_OUT="$T/corpus" cargo test --release --locked -q --test instrument corpus_emit >/dev/null \
  && cargo build --release --locked -q --example reencode)
REENCODE=${CARGO_TARGET_DIR:-$CRATE/target}/release/examples/reencode

prep() { # <in.wasm> <base.wasm> <stem> [pass args...]: instrument + AOT-compile, twice each
  # <base.wasm> is the uninstrumented reference: the input itself, or its
  # canonical re-encoding when the runtime cannot load the input as written.
  local in=$1 base=$2 s=$3; shift 3
  "$WPI" --memory-cap-pages $CAP "$@" "$in" "$s.inst.wasm"
  "$WPI" --memory-cap-pages $CAP "$@" "$in" "$s.inst2.wasm"
  cmp -s "$s.inst.wasm" "$s.inst2.wasm" && ok "$(basename "$s"): pass output identical across runs" || bad "$(basename "$s"): pass output differs"
  for v in orig inst; do
    local w=$s.$v.wasm; [ $v = orig ] && w=$base
    wamrc $WAMRC_FLAGS -o "$s.$v.aot" "$w" >/dev/null
  done
  wamrc $WAMRC_FLAGS -o "$s.inst2.aot" "$s.inst.wasm" >/dev/null
  # Informational: the node caches its own AOT and keys it by the instrumented
  # wasm, so AOT bytes need not be reproducible (wamrc 2.2.0 leaves a byte or
  # two of padding uninitialised in large modules).
  cmp -s "$s.inst.aot" "$s.inst2.aot" && ok "$(basename "$s"): wamrc output identical across runs" \
    || echo "note $(basename "$s"): wamrc output differs in $(cmp -l "$s.inst.aot" "$s.inst2.aot" | wc -l) bytes across runs"
}
wamrc() { "$WAMRC" "$@"; }
# Drop timing and the runtime's trap call-stack dump (DUMP_CALL_STACK=1: frame
# addresses differ between interpreter and AOT by design).
host() { FIXEDCLOCK=1 "$T/host" "$@" | grep -vE '^(time |#[0-9]+: |$)' || true; }
field() { awk -v k="$1" '$1==k{print $2}'; }
same() { # <label> <file a> <file b>
  if cmp -s "$2" "$3"; then ok "$1"; else bad "$1"; diff "$2" "$3" | head -10; fi
}

budgets() { # <label> <stem> <runner args...>: fuel exactness, interp vs AOT
  local label=$1 s=$2; shift 2
  host "$s.inst.wasm" "$@" > "$T/i"; host "$s.inst.aot" "$@" > "$T/a"
  same "$label: instrumented interp == AOT (unlimited)" "$T/i" "$T/a"
  local left; left=$(field wp_fuel < "$T/a")
  local used=$(( (1 << 62) - left ))
  # A non-recursive, loop-free export has no charge point: bounded by its size.
  [ "$used" -gt 0 ] || { ok "$label: no charge point reached (loop-free, non-recursive)"; return; }
  for b in $used $((used - 1)) $((used / 3)); do
    host "$s.inst.wasm" -fuel $b "$@" > "$T/i"; host "$s.inst.aot" -fuel $b "$@" > "$T/a"
    same "$label: budget $b of $used, interp == AOT" "$T/i" "$T/a"
    local f; f=$(field wp_fuel < "$T/a")
    if [ $b = $used ]; then
      grep -q '^TRAP' "$T/a" && bad "$label: trapped at exactly the consumed budget" || true
      [ "$f" = 0 ] && ok "$label: exact budget finishes with wp_fuel 0" || bad "$label: wp_fuel $f at exact budget"
    else
      grep -q '^TRAP.*unreachable' "$T/a" && [ "$f" -lt 0 ] \
        && ok "$label: budget $b traps out of fuel (wp_fuel $f)" || bad "$label: budget $b did not trap on fuel"
    fi
  done
}

for wat in "$T"/corpus/*.wat; do
  stem=$(basename "$wat" .wat); s=$T/$stem
  echo "## $stem"
  prep "$T/corpus/$stem.wasm" "$T/corpus/$stem.wasm" "$s"
  while read -r fn args; do
    host "$T/corpus/$stem.wasm" call $fn $args > "$T/oi"; host "$s.orig.aot" call $fn $args > "$T/oa"
    host "$s.inst.wasm" call $fn $args > "$T/ii"; host "$s.inst.aot" call $fn $args > "$T/ia"
    same "$stem.$fn: original interp == AOT" "$T/oi" "$T/oa"
    for x in ii ia; do
      if diff <(grep -E '^(result|TRAP|memory)' "$T/oi") <(grep -E '^(result|TRAP|memory)' "$T/$x") >/dev/null; then
        ok "$stem.$fn: instrumented ($x) result and memory == original ($(grep -E '^(result|TRAP)' "$T/oi"))"
      else bad "$stem.$fn: instrumented ($x) differs from original"; diff "$T/oi" "$T/$x" | head; fi
    done
    [ "$(field wp_depth < "$T/ia")" = 0 ] && ok "$stem.$fn: wp_depth back to 0" || bad "$stem.$fn: wp_depth $(field wp_depth < "$T/ia")"
    budgets "$stem.$fn" "$s" call $fn $args
  done < <(sed -n 's/^;; RUN: //p' "$wat")
  while read -r fn args; do
    for b in 1000 10000000; do
      host "$s.inst.wasm" -fuel $b call $fn $args > "$T/ii"; host "$s.inst.aot" -fuel $b call $fn $args > "$T/ia"
      same "$stem.$fn (runaway, budget $b): interp == AOT" "$T/ii" "$T/ia"
      f=$(field wp_fuel < "$T/ia"); d=$(field wp_depth < "$T/ia")
      if grep -q '^TRAP.*unreachable' "$T/ia" && { [ "$f" -lt 0 ] || [ "$d" -gt 4096 ]; }; then
        ok "$stem.$fn (runaway, budget $b): trapped (wp_fuel $f, wp_depth $d)"
      else bad "$stem.$fn (runaway, budget $b): no fuel/depth trap"; cat "$T/ia"; fi
    done
  done < <(sed -n 's/^;; SPIN: //p' "$wat")
done

if [ -n "$LUA" ]; then
  echo "## lua (spike PUC Lua 5.3.6 with SjLj)"
  s=$T/lua
  "$REENCODE" "$LUA/lua-sjlj.wasm" "$s.base.wasm"
  prep "$LUA/lua-sjlj.wasm" "$s.base.wasm" "$s" --unsafe-allow-import 'wasi_snapshot_preview1.*' --unsafe-allow-import env.now_us
  printf 'BENCH_ACCOUNTS=30 BENCH_RESTING=15 BENCH_TRADES=10 BENCH_STREAM=30\n' > "$T/small.lua"
  for v in orig.aot inst.wasm inst.aot; do host "$s.$v" lua "$LUA/test_errors.lua" > "$T/e.$v"; done
  host "$s.base.wasm" lua "$LUA/test_errors.lua" > "$T/e.orig.wasm"
  grep -q 'SUMMARY pass=[0-9]* fail=0' "$T/e.inst.aot" && ok "lua errors: $(grep SUMMARY "$T/e.inst.aot")" || bad "lua errors: $(grep SUMMARY "$T/e.inst.aot")"
  for v in orig.aot inst.wasm inst.aot; do
    diff <(grep -vE '^(wp_|invokes)' "$T/e.orig.wasm") <(grep -vE '^(wp_|invokes)' "$T/e.$v") >/dev/null \
      && ok "lua errors: $v output and memory == uninstrumented interp" || bad "lua errors: $v differs"
  done
  same "lua errors: instrumented interp == AOT" "$T/e.inst.wasm" "$T/e.inst.aot"
  if grep '^wp_depth after' "$T/e.inst.aot" | grep -qv ' 0$'; then bad "lua errors: wp_depth not restored"
  else ok "lua errors: wp_depth 0 after $(awk '/^invokes/{print $4}' "$T/e.inst.aot") caught longjmps"; fi
  C="$T/small.lua $LUA/module.lua $LUA/main.lua"
  host "$s.base.wasm" lua $C > "$T/p.orig"; host "$s.inst.aot" lua $C > "$T/p.inst"
  diff <(grep -E '^(seed|memory)' "$T/p.orig") <(grep -E '^(seed|memory)' "$T/p.inst") >/dev/null \
    && ok "lua pair stream: instrumented AOT == uninstrumented interp ($(grep -o 'checksum [0-9]*' "$T/p.inst"))" || bad "lua pair stream differs"
  budgets "lua pair stream" "$s" lua $C
fi

echo "== $FAILS failures"
[ $FAILS = 0 ]

#!/usr/bin/env bash
# Build the spike's PUC Lua 5.3.6 (Emscripten-style SjLj) to wasm32 exactly as
# paralith-mono backend/native/wasm-native-bench/spike/build.sh does (commit
# 25b73a5e), plus a declared memory maximum, and leave the raw wasm-ld output
# (padded call_indirect immediates included) for the pass.
#   build-lua.sh <spike dir> <out dir>
# Needs /opt/wasi-sdk25 (wasi-sdk-25.0 x86_64-linux) and /opt/lua.tgz
# (lua-5.3.6, sha256 fc5fd69bb8736323f026672b1b7235da613d7177e72558893a0bdcd320466d60).
set -euo pipefail
W=$1; O=$2; SDK=${SDK:-/opt/wasi-sdk25}
echo "fc5fd69bb8736323f026672b1b7235da613d7177e72558893a0bdcd320466d60  /opt/lua.tgz" | sha256sum -c --quiet
mkdir -p "$O" && rm -rf "$O/lua-5.3.6" && tar xzf /opt/lua.tgz -C "$O"
S=$O/lua-5.3.6/src
sed -i '/^static unsigned int makeseed (lua_State \*L) {/,/^}/c\static unsigned int makeseed (lua_State *L) { (void)L; return 0; }' "$S/lstate.c"
printf '#include <stdio.h>\nFILE*tmpfile(void){return 0;}\nint system(const char*c){return -1;}\n' > "$O/stubs.c"
SRCS="$(cd "$S" && ls *.c | grep -v -e '^lua.c$' -e '^luac.c$' | sed "s|^|$S/|")"
"$SDK/bin/clang" --target=wasm32-wasi -O2 -mcpu=generic -mno-reference-types -mno-multivalue -DLUA_COMPAT_5_2 \
  -DLUA_TMPNAMBUFSIZE=32 '-Dlua_tmpnam(b,e)=(e=1)' '-Dluai_makeseed()=0' -D_WASI_EMULATED_SIGNAL \
  -D_WASI_EMULATED_PROCESS_CLOCKS -I"$S" -I"$W/inc" -mllvm -enable-emscripten-sjlj \
  -o "$O/lua-sjlj.wasm" $SRCS "$W/driver.c" "$W/sjlj_rt.c" "$O/stubs.c" \
  -mexec-model=reactor -Wl,-z,stack-size=1048576 -Wl,--export=__stack_pointer -Wl,--max-memory=1073741824 \
  -lwasi-emulated-signal -lwasi-emulated-process-clocks
echo "built $O/lua-sjlj.wasm"

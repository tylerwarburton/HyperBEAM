#!/bin/sh
# Rebuild wp-test.wasm (checked in, so the tests need no toolchain).
# Needs a clang with the wasm32 target and wasm-ld, e.g. wasi-sdk 25:
#   CC=/opt/wasi-sdk25/bin/clang sh test/wasm-patch/build.sh
set -eu
D=$(dirname "$0")
${CC:-clang} --target=wasm32 -O2 -mcpu=mvp -mmutable-globals -fno-builtin -nostdlib \
    -Wl,--no-entry -Wl,--export=__stack_pointer -Wl,--export=__heap_base \
    -Wl,-z,stack-size=65536 -Wl,--max-memory=16777216 \
    -o "$D/wp-test.wasm" "$D/wp-test.c"

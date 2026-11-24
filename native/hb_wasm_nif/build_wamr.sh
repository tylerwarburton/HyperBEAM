#!/bin/sh
# Build the WAMR runtime that `hb_wasm_nif' links, from the source `make wamr'
# already cloned (_build/wamr, WAMR 2.2.0). The port driver's own build
# (_build/wamr/lib) is left alone.
#
# Same core flags as the Makefile's build, with two differences:
# - no call-stack dumps or AOT stack frames: a dump is printed to stdout on
#   every exception, and every Lua error is one (the longjmp sentinel);
# - the classic-interpreter fix below, which a module that grows memory
#   inside an invoke_* needs (AOT is unaffected).
set -eu
SRC=_build/wamr
OUT=_build/wamr-patch
LIB=$OUT/lib/libvmlib.a
[ -f "$LIB" ] && exit 0
[ -d "$SRC/core" ] || { echo "build_wamr.sh: $SRC missing; run make wamr" >&2; exit 1; }
rm -rf "$OUT"
mkdir -p "$OUT/src"
(cd "$SRC" && tar cf - --exclude=./lib .) | (cd "$OUT/src" && tar xf -)
# WAMR 2.2.0: after a native returns, the classic interpreter refreshes its
# cached linear-memory size from the cached local itself (THREAD_MGR=0), so
# memory grown inside a nested call (invoke_* -> call_indirect) reads as out
# of bounds in the caller. Read it from the memory instance instead.
F="$OUT/src/core/iwasm/interpreter/wasm_interp_classic.c"
sed -i '/wasm_interp_call_func_native(module, exec_env, cur_func,/,/wasm_copy_exception(module, NULL)/ s/linear_mem_size = get_linear_mem_size();/linear_mem_size = GET_LINEAR_MEMORY_SIZE(memory);/' "$F"
if sed -n '/wasm_interp_call_func_native(module, exec_env, cur_func,/,/wasm_copy_exception(module, NULL)/p' "$F" \
        | grep -q 'get_linear_mem_size()'; then
    echo "build_wamr.sh: interpreter patch did not apply" >&2
    exit 1
fi
case "$(uname -m)" in
    arm64|aarch64) TARGET=AARCH64 ;;
    *) TARGET=X86_64 ;;
esac
case "$(uname -s)" in
    Darwin) PLATFORM=darwin ;;
    *) PLATFORM=linux ;;
esac
cmake -S "$OUT/src" -B "$OUT/lib" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
    -DWAMR_BUILD_TARGET=$TARGET \
    -DWAMR_BUILD_PLATFORM=$PLATFORM \
    -DWAMR_BUILD_MEMORY64=1 \
    -DWAMR_DISABLE_HW_BOUND_CHECK=1 \
    -DWAMR_BUILD_EXCE_HANDLING=1 \
    -DWAMR_BUILD_SHARED_MEMORY=0 \
    -DWAMR_BUILD_AOT=1 \
    -DWAMR_BUILD_LIBC_WASI=0 \
    -DWAMR_BUILD_LIBC_BUILTIN=0 \
    -DWAMR_BUILD_FAST_INTERP=0 \
    -DWAMR_BUILD_INTERP=1 \
    -DWAMR_BUILD_JIT=0 \
    -DWAMR_BUILD_FAST_JIT=0 \
    -DWAMR_BUILD_SIMD=0 \
    -DWAMR_BUILD_TAIL_CALL=1 \
    -DWAMR_BUILD_DEBUG_AOT=0 \
    -DWAMR_BUILD_AOT_STACK_FRAME=0 \
    -DWAMR_BUILD_MEMORY_PROFILING=0 \
    -DWAMR_BUILD_DUMP_CALL_STACK=0
make -C "$OUT/lib" -j8 iwasm_static

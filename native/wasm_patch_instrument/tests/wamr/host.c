/* WAMR 2.2.0 test host for wasm-patch-instrument, linked against the node's own
 * libvmlib.a (Makefile flags). Runs a module (.wasm = classic interpreter,
 * .aot = AOT) and prints everything that must match between the two.
 *
 *   host <module> [-fuel N] [-stack BYTES] call <export> [int args...]
 *   host <module> [-fuel N] [-stack BYTES] lua <chunk.lua>...
 *
 * `call` invokes one export with integer arguments. `lua` drives the spike's
 * Lua driver (lua_init / buf / run) over each chunk in turn.
 *
 * Output: the result or the trap, wp_fuel left, wp_depth, every wp_g<N>, and an
 * FNV-1a hash of all of linear memory. Timing lines start with "time"; with
 * FIXEDCLOCK=1 every other line is deterministic.
 *
 * Natives, registered the way the node registers them (never as callbacks):
 *   env.invoke_vii     raw: call_indirect(idx, a, b); catch ONLY the longjmp
 *                      sentinel (restore __stack_pointer); every other trap,
 *                      fuel and depth included, propagates.
 *   env.throw_longjmp  raise the sentinel.
 *   env.now_us, wasi_snapshot_preview1.*: the spike Lua's benchmark clock and
 *                      stdio stubs. Not ABI imports; the pass only accepts them
 *                      with --unsafe-allow-import.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include "wasm_export.h"

#define SENTINEL "wp-longjmp"
static wasm_module_inst_t inst;
static uint32_t *sp_ptr;
static uint64_t n_invoke, n_catch;

static double real_us(void) {
  struct timespec t; clock_gettime(CLOCK_MONOTONIC, &t);
  return t.tv_sec * 1e6 + t.tv_nsec / 1e3;
}
/* FIXEDCLOCK=1 makes the Lua bench clock a counter, so memory is comparable. */
static int fixed_clock; static double ticks;
static double now_us(wasm_exec_env_t e) { (void)e; return fixed_clock ? ++ticks : real_us(); }
static void throw_longjmp(wasm_exec_env_t e) {
  wasm_runtime_set_exception(wasm_runtime_get_module_inst(e), SENTINEL);
}
static void invoke_vii(wasm_exec_env_t e, uint64_t *args) {
  uint32_t argv[2] = {(uint32_t)args[1], (uint32_t)args[2]};
  uint32_t sp = sp_ptr ? *sp_ptr : 0;
  n_invoke++;
  if (!wasm_runtime_call_indirect(e, (uint32_t)args[0], 2, argv)) {
    wasm_module_inst_t mi = wasm_runtime_get_module_inst(e);
    const char *x = wasm_runtime_get_exception(mi);
    if (x && strstr(x, SENTINEL)) {
      wasm_runtime_clear_exception(mi);
      if (sp_ptr) *sp_ptr = sp;
      n_catch++;
    }
  }
}
/* WASI stubs. Signatures must equal WAMR's own libc-wasi ones ('*' pointers):
 * wamrc converts pointer arguments of those names inside AOT code. */
static int32_t w_fd_write(wasm_exec_env_t e, int32_t fd, uint32_t *io, int32_t n, uint32_t *nw) {
  wasm_module_inst_t mi = wasm_runtime_get_module_inst(e); uint32_t tot = 0;
  for (int i = 0; i < n; i++) {
    char *b = wasm_runtime_addr_app_to_native(mi, io[2 * i]);
    fwrite(b, 1, io[2 * i + 1], fd == 2 ? stderr : stdout); tot += io[2 * i + 1];
  }
  *nw = tot; return 0;
}
static int32_t w_zero2(wasm_exec_env_t e, uint32_t *a, uint32_t *b) { (void)e; *a = 0; *b = 0; return 0; }
static int32_t w_badf(wasm_exec_env_t e) { (void)e; return 8; }
static int32_t w_nosys(wasm_exec_env_t e) { (void)e; return 52; }
static void w_exit(wasm_exec_env_t e, int32_t c) { (void)c; wasm_runtime_set_exception(wasm_runtime_get_module_inst(e), "proc_exit"); }
static int32_t w_clock(wasm_exec_env_t e, int32_t id, int64_t prec, uint64_t *out) { (void)e; (void)id; (void)prec; *out = 0; return 0; }

static char *slurp(const char *p, uint32_t *n) {
  FILE *f = fopen(p, "rb"); if (!f) { perror(p); exit(2); }
  fseek(f, 0, SEEK_END); long s = ftell(f); fseek(f, 0, SEEK_SET);
  char *b = malloc(s + 1); if (fread(b, 1, s, f) != (size_t)s) exit(2); b[s] = 0; fclose(f); *n = s; return b;
}

static int64_t *g_fuel; static int32_t *g_depth;

static void report(wasm_module_t mod) {
  if (g_fuel) printf("wp_fuel %lld\n", (long long)*g_fuel);
  if (g_depth) printf("wp_depth %d\n", *g_depth);
  int32_t n = wasm_runtime_get_export_count(mod);
  for (int32_t i = 0; i < n; i++) {
    wasm_export_t ex; wasm_runtime_get_export_type(mod, i, &ex);
    if (ex.kind != WASM_IMPORT_EXPORT_KIND_GLOBAL || strncmp(ex.name, "wp_g", 4)) continue;
    wasm_global_inst_t g; wasm_runtime_get_export_global_inst(inst, ex.name, &g);
    long long v = 0;
    if (g.kind == WASM_I32 || g.kind == WASM_F32) v = *(int32_t *)g.global_data; else memcpy(&v, g.global_data, 8);
    printf("%s %lld\n", ex.name, v);
  }
  wasm_memory_inst_t mem = wasm_runtime_get_default_memory(inst);
  if (mem) {
    uint64_t pages = wasm_memory_get_cur_page_count(mem);
    const unsigned char *m = wasm_memory_get_base_address(mem); uint64_t h = 1469598103934665603ull;
    for (uint64_t i = 0; i < pages * 65536; i++) h = (h ^ m[i]) * 1099511628211ull;
    printf("memory %llu pages hash %016llx\n", (unsigned long long)pages, (unsigned long long)h);
  }
  printf("invokes %llu longjmps %llu\n", (unsigned long long)n_invoke, (unsigned long long)n_catch);
}

int main(int argc, char **argv) {
  long long fuel = (long long)1 << 62; uint32_t stack = 8 << 20; int ai = 2;
  if (argc < 3) { fprintf(stderr, "usage: see host.c\n"); return 2; }
  fixed_clock = getenv("FIXEDCLOCK") != NULL;
  for (; ai < argc && argv[ai][0] == '-'; ai++) {
    if (!strcmp(argv[ai], "-fuel")) fuel = atoll(argv[++ai]);
    else if (!strcmp(argv[ai], "-stack")) stack = (uint32_t)atoll(argv[++ai]);
  }
  static NativeSymbol envs[] = {{"now_us", now_us, "()F", NULL}, {"throw_longjmp", throw_longjmp, "()", NULL}};
  static NativeSymbol invokes[] = {{"invoke_vii", invoke_vii, "(iii)", NULL}};
  static NativeSymbol wasi[] = {
    {"fd_write", w_fd_write, "(i*i*)i", NULL}, {"environ_sizes_get", w_zero2, "(**)i", NULL},
    {"environ_get", w_zero2, "(**)i", NULL}, {"fd_prestat_get", w_badf, "(i*)i", NULL},
    {"fd_prestat_dir_name", w_badf, "(i*~)i", NULL}, {"fd_close", w_badf, "(i)i", NULL},
    {"fd_seek", w_badf, "(iIi*)i", NULL}, {"fd_read", w_badf, "(i*i*)i", NULL},
    {"fd_fdstat_get", w_badf, "(i*)i", NULL}, {"fd_renumber", w_badf, "(ii)i", NULL},
    {"fd_fdstat_set_flags", w_badf, "(ii)i", NULL}, {"path_open", w_nosys, "(ii*~iIIi*)i", NULL},
    {"clock_time_get", w_clock, "(iI*)i", NULL}, {"proc_exit", w_exit, "(i)", NULL}};
  RuntimeInitArgs init; memset(&init, 0, sizeof init);
  init.mem_alloc_type = Alloc_With_System_Allocator;
  if (!wasm_runtime_full_init(&init)) return 3;
  wasm_runtime_register_natives("env", envs, 2);
  wasm_runtime_register_natives_raw("env", invokes, 1);
  wasm_runtime_register_natives("wasi_snapshot_preview1", wasi, sizeof wasi / sizeof *wasi);

  uint32_t sz; char *bin = slurp(argv[1], &sz); char err[256];
  wasm_module_t mod = wasm_runtime_load((uint8_t *)bin, sz, err, sizeof err);
  if (!mod) { printf("load: %s\n", err); return 4; }
  inst = wasm_runtime_instantiate(mod, stack, 0, err, sizeof err);
  if (!inst) { printf("instantiate: %s\n", err); return 5; }
  wasm_exec_env_t ee = wasm_runtime_create_exec_env(inst, stack);
  wasm_global_inst_t g;
  if (wasm_runtime_get_export_global_inst(inst, "__stack_pointer", &g)) sp_ptr = g.global_data;
  if (wasm_runtime_get_export_global_inst(inst, "wp_fuel", &g)) g_fuel = g.global_data;
  if (wasm_runtime_get_export_global_inst(inst, "wp_depth", &g)) g_depth = g.global_data;
  if (g_fuel) *g_fuel = fuel;
  int rc = 0;
  const char *mode = argv[ai++];

  if (!strcmp(mode, "call")) {
    wasm_function_inst_t f = wasm_runtime_lookup_function(inst, argv[ai]);
    if (!f) { printf("no export %s\n", argv[ai]); return 6; }
    uint32_t np = wasm_func_get_param_count(f, inst), nr = wasm_func_get_result_count(f, inst);
    wasm_valkind_t pt[16]; wasm_func_get_param_types(f, inst, pt);
    wasm_val_t args[16], res[16]; memset(args, 0, sizeof args); memset(res, 0, sizeof res);
    for (uint32_t i = 0; i < np; i++) {
      args[i].kind = pt[i];
      long long v = ai + 1 + (int)i < argc ? atoll(argv[ai + 1 + i]) : 0;
      if (pt[i] == WASM_I32) args[i].of.i32 = (int32_t)v; else args[i].of.i64 = v;
    }
    if (!wasm_runtime_call_wasm_a(ee, f, nr, res, np, args)) {
      printf("TRAP %s\n", wasm_runtime_get_exception(inst)); rc = 7;
    } else {
      printf("result");
      for (uint32_t i = 0; i < nr; i++)
        printf(" %lld", res[i].kind == WASM_I32 ? (long long)res[i].of.i32 : (long long)res[i].of.i64);
      printf("\n");
    }
  } else if (!strcmp(mode, "lua")) {
    uint32_t a[4] = {0};
    wasm_function_inst_t fi = wasm_runtime_lookup_function(inst, "_initialize");
    if ((fi && !wasm_runtime_call_wasm(ee, fi, 0, a)) ||
        !wasm_runtime_call_wasm(ee, wasm_runtime_lookup_function(inst, "lua_init"), 0, a)) {
      printf("TRAP init %s\n", wasm_runtime_get_exception(inst)); rc = 7; goto done;
    }
    wasm_function_inst_t f_buf = wasm_runtime_lookup_function(inst, "buf"),
                         f_run = wasm_runtime_lookup_function(inst, "run"),
                         f_len = wasm_runtime_lookup_function(inst, "out_len");
    for (; ai < argc; ai++) {
      uint32_t n; char *src = slurp(argv[ai], &n);
      a[0] = n; wasm_runtime_call_wasm(ee, f_buf, 1, a);
      memcpy(wasm_runtime_addr_app_to_native(inst, a[0]), src, n);
      a[1] = n;
      if (g_depth) *g_depth = 0;
      double t0 = real_us();
      if (!wasm_runtime_call_wasm(ee, f_run, 2, a)) {
        printf("TRAP %s: %s\n", argv[ai], wasm_runtime_get_exception(inst)); rc = 7; goto done;
      }
      printf("time %s %.1f ms\n", argv[ai], (real_us() - t0) / 1e3);
      uint32_t p = a[0];
      wasm_runtime_call_wasm(ee, f_len, 0, a);
      printf("== %s\n%.*s\n", argv[ai], (int)a[0], (char *)wasm_runtime_addr_app_to_native(inst, p));
      if (g_depth) printf("wp_depth after %s %d\n", argv[ai], *g_depth);
    }
  } else { printf("unknown mode %s\n", mode); return 2; }
done:
  report(mod);
  return rc;
}

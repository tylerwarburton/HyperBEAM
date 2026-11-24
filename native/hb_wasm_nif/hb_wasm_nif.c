/* hb_wasm_nif: the in-process WAMR embedding behind `wasm-patch@1.0'.
 *
 * One dirty-CPU NIF call per slot: write the request into the module's memory
 * (`wp_alloc'), call `wp_init' or `wp_handle', copy the `[u32 len][bytes]'
 * reply out. The port driver (`hb_beamr') is untouched; this links its own
 * WAMR build (`build_wamr.sh': same source and core flags, no call-stack
 * dumps, the classic-interpreter memory-size fix applied).
 *
 * The only imports a module may have are the natives below, all C, never
 * Erlang callbacks:
 *   env.invoke_<sig>        call_indirect(idx, args...). Catches ONLY the
 *                           longjmp sentinel (restores __stack_pointer, returns
 *                           0); every other trap -- fuel, bounds -- unwinds to
 *                           the top, so a Lua pcall never swallows it.
 *   env.throw_longjmp       raise the sentinel.
 *   env.setThrew            emscripten: set exported __THREW__/__threwValue.
 *   env.getTempRet0 / env.setTempRet0
 *
 * An instance lives in a resource with its own mutex. `gen' is the number of
 * successful calls applied to its memory; a call names the gen it expects and
 * is refused if the instance is elsewhere, so a stale state can never compute
 * on memory that moved on. A trap leaves gen at -1 (dirty) until a restore. */
#include <erl_nif.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "wasm_export.h"

#define SENTINEL "wasm-patch-longjmp"
#define ERRLEN 256
#define MAX_SIG 48
/* The instrumentation pass's call-depth limit (DEPTH_LIMIT). */
#define DEPTH_LIMIT 4096

typedef struct {
    uint8_t *bytes;
    uint32_t size;
    wasm_module_t module;
    int is_aot;
} wp_module;

typedef struct {
    ErlNifMutex *lock;
    wp_module *mod;
    wasm_module_inst_t inst;
    wasm_exec_env_t env;
    uint32_t stack_size;
    uint32_t heap_size;
    int64_t gen;
    int32_t temp_ret0;
    int wide;                       /* memory64 ABI: pointers are i64 */
    wasm_function_inst_t f_alloc, f_init, f_handle, f_initialize;
    wasm_global_inst_t sp, fuel, depth, threw, threw_value;
    int has_sp, has_fuel, has_depth, has_threw;
} wp_inst;

static ErlNifResourceType *MODULE_RES;
static ErlNifResourceType *INST_RES;
static ErlNifMutex *REG_LOCK;
static int RUNTIME_READY = 0;

typedef struct reg_node { char *name; struct reg_node *next; } reg_node;
static reg_node *REGISTERED = NULL;

static ERL_NIF_TERM A_OK, A_ERROR, A_TRAP, A_FAULT, A_STALE, A_DIRTY, A_I32, A_I64, A_F32, A_F64;

/* ---------- helpers ---------- */

static ERL_NIF_TERM mk_bin(ErlNifEnv *env, const char *s) {
    ERL_NIF_TERM t;
    size_t n = strlen(s);
    unsigned char *d = enif_make_new_binary(env, n, &t);
    memcpy(d, s, n);
    return t;
}

static ERL_NIF_TERM error_msg(ErlNifEnv *env, const char *s) {
    return enif_make_tuple2(env, A_ERROR, mk_bin(env, s));
}

static void thread_env(void) {
    if (!wasm_runtime_thread_env_inited()) wasm_runtime_init_thread_env();
}

static uint64_t global_get(wasm_global_inst_t *g) {
    uint64_t v = 0;
    if (g->kind == WASM_I32 || g->kind == WASM_F32) memcpy(&v, g->global_data, 4);
    else memcpy(&v, g->global_data, 8);
    return v;
}

static void global_set(wasm_global_inst_t *g, uint64_t v) {
    if (g->kind == WASM_I32 || g->kind == WASM_F32) memcpy(g->global_data, &v, 4);
    else memcpy(g->global_data, &v, 8);
}

static int64_t global_signed(wasm_global_inst_t *g) {
    uint64_t v = global_get(g);
    if (g->kind == WASM_I32) return (int64_t)(int32_t)(uint32_t)v;
    return (int64_t)v;
}

/* ---------- natives ---------- */

static wp_inst *self_of(wasm_exec_env_t e) {
    return (wp_inst *)wasm_runtime_get_custom_data(wasm_runtime_get_module_inst(e));
}

static void n_throw_longjmp(wasm_exec_env_t e) {
    wasm_runtime_set_exception(wasm_runtime_get_module_inst(e), SENTINEL);
}

static void n_set_threw(wasm_exec_env_t e, int32_t threw, int32_t value) {
    wp_inst *w = self_of(e);
    if (!w || !w->has_threw) {
        wasm_runtime_set_exception(wasm_runtime_get_module_inst(e),
            "setThrew needs exported __THREW__ and __threwValue");
        return;
    }
    if (global_get(&w->threw) == 0) {
        global_set(&w->threw, (uint64_t)(uint32_t)threw);
        global_set(&w->threw_value, (uint64_t)(uint32_t)value);
    }
}

static int32_t n_get_temp_ret0(wasm_exec_env_t e) {
    wp_inst *w = self_of(e);
    return w ? w->temp_ret0 : 0;
}

static void n_set_temp_ret0(wasm_exec_env_t e, int32_t v) {
    wp_inst *w = self_of(e);
    if (w) w->temp_ret0 = v;
}

/* Raw native for every invoke_<sig>; the attachment is "<ret><params>", each
 * of v (ret only), i, j (i64), f, d. args[0] is the table index. */
static void n_invoke(wasm_exec_env_t e, uint64_t *args) {
    const char *sig = (const char *)wasm_runtime_get_function_attachment(e);
    wasm_module_inst_t mi = wasm_runtime_get_module_inst(e);
    wp_inst *w = self_of(e);
    uint32_t argv[2 * MAX_SIG + 2];
    uint32_t c = 0;
    int k = 1;
    memset(argv, 0, sizeof argv);
    for (const char *p = sig + 1; *p; p++, k++) {
        if (*p == 'i' || *p == 'f') argv[c++] = (uint32_t)args[k];
        else { memcpy(&argv[c], &args[k], 8); c += 2; }
    }
    uint64_t sp = (w && w->has_sp) ? global_get(&w->sp) : 0;
    if (!wasm_runtime_call_indirect(e, (uint32_t)args[0], c, argv)) {
        const char *x = wasm_runtime_get_exception(mi);
        if (x && strstr(x, SENTINEL)) {
            wasm_runtime_clear_exception(mi);
            if (w && w->has_sp) global_set(&w->sp, sp);
            args[0] = 0;
        }
        return;
    }
    if (sig[0] == 'i' || sig[0] == 'f') args[0] = argv[0];
    else if (sig[0] == 'j' || sig[0] == 'd') memcpy(&args[0], argv, 8);
}

static NativeSymbol FIXED_NATIVES[] = {
    { "throw_longjmp", (void *)n_throw_longjmp, "()", NULL },
    { "setThrew", (void *)n_set_threw, "(ii)", NULL },
    { "getTempRet0", (void *)n_get_temp_ret0, "()i", NULL },
    { "setTempRet0", (void *)n_set_temp_ret0, "(i)", NULL },
};

static int allowed_import(const char *module, const char *name) {
    if (strcmp(module, "env") != 0) return 0;
    if (strncmp(name, "invoke_", 7) == 0) return 1;
    for (size_t i = 0; i < sizeof FIXED_NATIVES / sizeof *FIXED_NATIVES; i++)
        if (strcmp(name, FIXED_NATIVES[i].symbol) == 0) return 1;
    return 0;
}

/* register_invoke(<<"invoke_vii">>): register the raw native for that name
 * once per runtime. Must run before a module importing it is loaded. */
static ERL_NIF_TERM nif_register_invoke(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    ErlNifBinary b;
    if (!enif_inspect_binary(env, argv[0], &b) || b.size < 8 || b.size > 7 + MAX_SIG)
        return enif_make_badarg(env);
    if (memcmp(b.data, "invoke_", 7) != 0) return enif_make_badarg(env);
    const unsigned char *s = b.data + 7;
    size_t n = b.size - 7;
    char ret = s[0];
    if (ret != 'v' && ret != 'i' && ret != 'j' && ret != 'f' && ret != 'd')
        return error_msg(env, "bad invoke signature");
    char wsig[MAX_SIG + 8];
    size_t w = 0;
    wsig[w++] = '(';
    wsig[w++] = 'i';
    for (size_t i = 1; i < n; i++) {
        switch (s[i]) {
            case 'i': wsig[w++] = 'i'; break;
            case 'j': wsig[w++] = 'I'; break;
            case 'f': wsig[w++] = 'f'; break;
            case 'd': wsig[w++] = 'F'; break;
            default: return error_msg(env, "bad invoke signature");
        }
    }
    wsig[w++] = ')';
    if (ret == 'i') wsig[w++] = 'i';
    else if (ret == 'j') wsig[w++] = 'I';
    else if (ret == 'f') wsig[w++] = 'f';
    else if (ret == 'd') wsig[w++] = 'F';
    wsig[w] = 0;

    char name[MAX_SIG + 16];
    memcpy(name, b.data, b.size);
    name[b.size] = 0;

    enif_mutex_lock(REG_LOCK);
    for (reg_node *r = REGISTERED; r; r = r->next) {
        if (strcmp(r->name, name) == 0) { enif_mutex_unlock(REG_LOCK); return A_OK; }
    }
    /* WAMR keeps these pointers for the life of the runtime: never freed. */
    NativeSymbol *sym = calloc(1, sizeof(NativeSymbol));
    char *nm = strdup(name), *sg = strdup(wsig), *att = strdup((const char *)name + 7);
    reg_node *node = calloc(1, sizeof(reg_node));
    if (!sym || !nm || !sg || !att || !node) { enif_mutex_unlock(REG_LOCK); return error_msg(env, "oom"); }
    sym->symbol = nm;
    sym->func_ptr = (void *)n_invoke;
    sym->signature = sg;
    sym->attachment = att;
    if (!wasm_runtime_register_natives_raw("env", sym, 1)) {
        enif_mutex_unlock(REG_LOCK);
        return error_msg(env, "register failed");
    }
    node->name = nm;
    node->next = REGISTERED;
    REGISTERED = node;
    enif_mutex_unlock(REG_LOCK);
    return A_OK;
}

/* ---------- module ---------- */

static void module_dtor(ErlNifEnv *env, void *obj) {
    wp_module *m = obj;
    if (m->module) wasm_runtime_unload(m->module);
    if (m->bytes) enif_free(m->bytes);
}

/* load(Bytes) -> {ok, Module, #{...}} | {error, Msg}. Bytes are a .wasm or an
 * .aot the node compiled itself (the caller never passes outside AOT). */
static ERL_NIF_TERM nif_load(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    ErlNifBinary b;
    char err[ERRLEN];
    if (!enif_inspect_binary(env, argv[0], &b) || b.size < 8 || b.size > UINT32_MAX)
        return enif_make_badarg(env);
    thread_env();
    wp_module *m = enif_alloc_resource(MODULE_RES, sizeof(wp_module));
    memset(m, 0, sizeof *m);
    m->bytes = enif_alloc(b.size);
    m->size = (uint32_t)b.size;
    memcpy(m->bytes, b.data, b.size);
    m->is_aot = memcmp(b.data, "\0aot", 4) == 0;
    err[0] = 0;
    enif_mutex_lock(REG_LOCK);
    m->module = wasm_runtime_load(m->bytes, m->size, err, sizeof err);
    enif_mutex_unlock(REG_LOCK);
    if (!m->module) {
        enif_release_resource(m);
        return error_msg(env, err[0] ? err : "load failed");
    }
    /* Imports: functions in the allow-list, all linked. Nothing else. */
    int32_t ni = wasm_runtime_get_import_count(m->module);
    for (int32_t i = 0; i < ni; i++) {
        wasm_import_t imp;
        wasm_runtime_get_import_type(m->module, i, &imp);
        if (imp.kind != WASM_IMPORT_EXPORT_KIND_FUNC
                || !allowed_import(imp.module_name, imp.name)
                || !imp.linked) {
            char msg[ERRLEN];
            snprintf(msg, sizeof msg, "import not allowed: %s.%s%s", imp.module_name, imp.name,
                (imp.kind == WASM_IMPORT_EXPORT_KIND_FUNC && allowed_import(imp.module_name, imp.name))
                    ? " (unlinked signature)" : "");
            enif_release_resource(m);
            return error_msg(env, msg);
        }
    }
    /* Exports: a non-shared memory and the three ABI functions. */
    int32_t ne = wasm_runtime_get_export_count(m->module);
    int have_mem = 0, have_alloc = 0, have_init = 0, have_handle = 0;
    uint64_t max_pages = 0, init_pages = 0;
    for (int32_t i = 0; i < ne; i++) {
        wasm_export_t ex;
        wasm_runtime_get_export_type(m->module, i, &ex);
        if (ex.kind == WASM_IMPORT_EXPORT_KIND_MEMORY && strcmp(ex.name, "memory") == 0) {
            if (wasm_memory_type_get_shared(ex.u.memory_type)) {
                enif_release_resource(m);
                return error_msg(env, "shared memory is not allowed");
            }
            have_mem = 1;
            max_pages = wasm_memory_type_get_max_page_count(ex.u.memory_type);
            init_pages = wasm_memory_type_get_init_page_count(ex.u.memory_type);
        } else if (ex.kind == WASM_IMPORT_EXPORT_KIND_FUNC) {
            if (strcmp(ex.name, "wp_alloc") == 0) have_alloc = 1;
            else if (strcmp(ex.name, "wp_init") == 0) have_init = 1;
            else if (strcmp(ex.name, "wp_handle") == 0) have_handle = 1;
        }
    }
    if (!have_mem || !have_alloc || !have_init || !have_handle) {
        enif_release_resource(m);
        return error_msg(env, "module must export memory, wp_alloc, wp_init and wp_handle");
    }
    ERL_NIF_TERM info = enif_make_new_map(env);
    enif_make_map_put(env, info, enif_make_atom(env, "aot"),
        enif_make_atom(env, m->is_aot ? "true" : "false"), &info);
    enif_make_map_put(env, info, enif_make_atom(env, "max_pages"),
        enif_make_uint64(env, max_pages), &info);
    enif_make_map_put(env, info, enif_make_atom(env, "init_pages"),
        enif_make_uint64(env, init_pages), &info);
    ERL_NIF_TERM res = enif_make_resource(env, m);
    enif_release_resource(m);
    return enif_make_tuple3(env, A_OK, res, info);
}

/* ---------- instance ---------- */

static void inst_teardown(wp_inst *w) {
    if (w->env) wasm_runtime_destroy_exec_env(w->env);
    if (w->inst) wasm_runtime_deinstantiate(w->inst);
    w->env = NULL;
    w->inst = NULL;
}

static void inst_dtor(ErlNifEnv *env, void *obj) {
    wp_inst *w = obj;
    inst_teardown(w);
    if (w->mod) enif_release_resource(w->mod);
    if (w->lock) enif_mutex_destroy(w->lock);
}

static int lookup_global(wp_inst *w, const char *name, wasm_global_inst_t *g) {
    return wasm_runtime_get_export_global_inst(w->inst, name, g) && g->is_mutable;
}

/* Build a fresh WAMR instance inside `w'. Caller holds the lock (or owns w). */
static int inst_build(wp_inst *w, char *err, uint32_t errlen) {
    w->inst = wasm_runtime_instantiate(w->mod->module, w->stack_size, w->heap_size, err, errlen);
    if (!w->inst) return 0;
    w->env = wasm_runtime_create_exec_env(w->inst, w->stack_size);
    if (!w->env) { snprintf(err, errlen, "exec env failed"); inst_teardown(w); return 0; }
    wasm_runtime_set_custom_data(w->inst, w);
    w->f_alloc = wasm_runtime_lookup_function(w->inst, "wp_alloc");
    w->f_init = wasm_runtime_lookup_function(w->inst, "wp_init");
    w->f_handle = wasm_runtime_lookup_function(w->inst, "wp_handle");
    w->f_initialize = wasm_runtime_lookup_function(w->inst, "_initialize");
    if (!w->f_alloc || !w->f_init || !w->f_handle) {
        snprintf(err, errlen, "ABI functions missing");
        inst_teardown(w);
        return 0;
    }
    wasm_valkind_t pk[2];
    if (wasm_func_get_param_count(w->f_handle, w->inst) != 2) {
        snprintf(err, errlen, "wp_handle must take (ptr, len)");
        inst_teardown(w);
        return 0;
    }
    wasm_func_get_param_types(w->f_handle, w->inst, pk);
    w->wide = pk[0] == WASM_I64;
    w->has_sp = lookup_global(w, "__stack_pointer", &w->sp);
    w->has_fuel = lookup_global(w, "wp_fuel", &w->fuel);
    w->has_depth = lookup_global(w, "wp_depth", &w->depth);
    w->has_threw = lookup_global(w, "__THREW__", &w->threw)
        && lookup_global(w, "__threwValue", &w->threw_value);
    w->temp_ret0 = 0;
    return 1;
}

/* instantiate(Module, StackSize, HeapSize) -> {ok, Inst, #{...}} | {error, Msg} */
static ERL_NIF_TERM nif_instantiate(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    wp_module *m;
    unsigned int stack, heap;
    char err[ERRLEN];
    if (!enif_get_resource(env, argv[0], MODULE_RES, (void **)&m)
            || !enif_get_uint(env, argv[1], &stack)
            || !enif_get_uint(env, argv[2], &heap))
        return enif_make_badarg(env);
    thread_env();
    wp_inst *w = enif_alloc_resource(INST_RES, sizeof(wp_inst));
    memset(w, 0, sizeof *w);
    w->mod = m;
    enif_keep_resource(m);
    w->stack_size = stack;
    w->heap_size = heap;
    w->lock = enif_mutex_create("hb_wasm_inst");
    err[0] = 0;
    if (!inst_build(w, err, sizeof err)) {
        enif_release_resource(w);
        return error_msg(env, err[0] ? err : "instantiate failed");
    }
    w->gen = 0;
    ERL_NIF_TERM info = enif_make_new_map(env);
    enif_make_map_put(env, info, enif_make_atom(env, "fuel"),
        enif_make_atom(env, w->has_fuel ? "true" : "false"), &info);
    enif_make_map_put(env, info, enif_make_atom(env, "wide"),
        enif_make_atom(env, w->wide ? "true" : "false"), &info);
    enif_make_map_put(env, info, enif_make_atom(env, "initialize"),
        enif_make_atom(env, w->f_initialize ? "true" : "false"), &info);
    ERL_NIF_TERM res = enif_make_resource(env, w);
    enif_release_resource(w);
    return enif_make_tuple3(env, A_OK, res, info);
}

/* A failed call. Out of fuel (wp_fuel < 0) and too deep (wp_depth above the
 * pass's limit) are counted by wasm code, identically under the interpreter
 * and AOT: `{trap, Reason}', a slot result. A native or operand stack overflow
 * before the depth limit depends on the runtime and the thread: `{fault,
 * Reason}', a node fault that must never become a slot result. */
static ERL_NIF_TERM trap(ErlNifEnv *env, wp_inst *w) {
    const char *x = wasm_runtime_get_exception(w->inst);
    char msg[ERRLEN];
    ERL_NIF_TERM kind = A_TRAP;
    if (w->has_fuel && global_signed(&w->fuel) < 0) snprintf(msg, sizeof msg, "out of fuel");
    else if (w->has_depth && global_signed(&w->depth) > DEPTH_LIMIT)
        snprintf(msg, sizeof msg, "call depth limit exceeded");
    else {
        snprintf(msg, sizeof msg, "%s", x ? x : "trap");
        if (x && strstr(x, "stack overflow")) kind = A_FAULT;
    }
    wasm_runtime_clear_exception(w->inst);
    w->gen = -1;
    return enif_make_tuple2(env, kind, mk_bin(env, msg));
}

static int call_fn(wp_inst *w, wasm_function_inst_t f, uint32_t argc, uint32_t *args) {
    wasm_runtime_clear_exception(w->inst);
    return wasm_runtime_call_wasm(w->env, f, argc, args);
}

/* call(Inst, Which, ExpectedGen, Request, Fuel) ->
 *   {ok, Reply, FuelLeft} | {trap, Reason} | {fault, Reason}
 *   | {error, stale | dirty | Msg}
 * Which: 0 = `_initialize' (if any) then wp_init, 1 = wp_handle. Fuel < 0
 * leaves the fuel global alone. */
static ERL_NIF_TERM nif_call(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    wp_inst *w;
    int which;
    ErlNifSInt64 expected, fuel;
    ErlNifBinary req;
    if (!enif_get_resource(env, argv[0], INST_RES, (void **)&w)
            || !enif_get_int(env, argv[1], &which)
            || !enif_get_int64(env, argv[2], &expected)
            || !enif_inspect_binary(env, argv[3], &req)
            || !enif_get_int64(env, argv[4], &fuel)
            || req.size > UINT32_MAX)
        return enif_make_badarg(env);
    thread_env();
    enif_mutex_lock(w->lock);
    ERL_NIF_TERM out;
    if (!w->inst) { out = enif_make_tuple2(env, A_ERROR, A_DIRTY); goto done; }
    if (w->gen != expected) {
        out = enif_make_tuple2(env, A_ERROR, w->gen < 0 ? A_DIRTY : A_STALE);
        goto done;
    }
    if (w->has_fuel && fuel >= 0) global_set(&w->fuel, (uint64_t)fuel);
    if (w->has_depth) global_set(&w->depth, 0);
    uint32_t a[4];
    if (which == 0 && w->f_initialize) {
        if (!call_fn(w, w->f_initialize, 0, a)) { out = trap(env, w); goto done; }
    }
    /* wp_alloc(len) -> ptr */
    uint64_t ptr;
    memset(a, 0, sizeof a);
    if (w->wide) { uint64_t l = req.size; memcpy(a, &l, 8); }
    else a[0] = (uint32_t)req.size;
    if (!call_fn(w, w->f_alloc, w->wide ? 2 : 1, a)) { out = trap(env, w); goto done; }
    if (w->wide) memcpy(&ptr, a, 8); else ptr = a[0];
    if (req.size > 0) {
        if (!wasm_runtime_validate_app_addr(w->inst, ptr, req.size)) {
            w->gen = -1;
            out = enif_make_tuple2(env, A_TRAP, mk_bin(env, "wp_alloc returned an invalid buffer"));
            goto done;
        }
        memcpy(wasm_runtime_addr_app_to_native(w->inst, ptr), req.data, req.size);
    }
    /* wp_init / wp_handle(ptr, len) -> rptr */
    memset(a, 0, sizeof a);
    if (w->wide) { uint64_t l = req.size; memcpy(a, &ptr, 8); memcpy(a + 2, &l, 8); }
    else { a[0] = (uint32_t)ptr; a[1] = (uint32_t)req.size; }
    if (!call_fn(w, which == 0 ? w->f_init : w->f_handle, w->wide ? 4 : 2, a)) {
        out = trap(env, w);
        goto done;
    }
    uint64_t rptr;
    if (w->wide) memcpy(&rptr, a, 8); else rptr = a[0];
    ERL_NIF_TERM reply;
    if (rptr == 0) {
        enif_make_new_binary(env, 0, &reply);
    } else {
        uint32_t len;
        if (!wasm_runtime_validate_app_addr(w->inst, rptr, 4)) goto bad_reply;
        memcpy(&len, wasm_runtime_addr_app_to_native(w->inst, rptr), 4);
        if (len > 0 && !wasm_runtime_validate_app_addr(w->inst, rptr + 4, len)) goto bad_reply;
        unsigned char *d = enif_make_new_binary(env, len, &reply);
        if (len) memcpy(d, wasm_runtime_addr_app_to_native(w->inst, rptr + 4), len);
    }
    w->gen = expected + 1;
    out = enif_make_tuple3(env, A_OK, reply,
        enif_make_int64(env, w->has_fuel ? global_signed(&w->fuel) : -1));
    goto done;
bad_reply:
    w->gen = -1;
    out = enif_make_tuple2(env, A_TRAP, mk_bin(env, "reply pointer out of bounds"));
done:
    enif_mutex_unlock(w->lock);
    return out;
}

/* snapshot(Inst) -> {ok, Gen, Memory, [{Name, Kind, Bits}]} | {error, dirty}
 * Every exported mutable global is kept (`wp_g<N>' and `__stack_pointer'
 * among them). */
static ERL_NIF_TERM nif_snapshot(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    wp_inst *w;
    if (!enif_get_resource(env, argv[0], INST_RES, (void **)&w)) return enif_make_badarg(env);
    enif_mutex_lock(w->lock);
    ERL_NIF_TERM out;
    if (!w->inst || w->gen < 0) { out = enif_make_tuple2(env, A_ERROR, A_DIRTY); goto done; }
    wasm_memory_inst_t mem = wasm_runtime_get_default_memory(w->inst);
    uint64_t size = wasm_memory_get_cur_page_count(mem) * (uint64_t)wasm_memory_get_bytes_per_page(mem);
    ERL_NIF_TERM membin;
    unsigned char *d = enif_make_new_binary(env, size, &membin);
    if (size) memcpy(d, wasm_memory_get_base_address(mem), size);
    ERL_NIF_TERM globals = enif_make_list(env, 0);
    int32_t ne = wasm_runtime_get_export_count(w->mod->module);
    for (int32_t i = ne - 1; i >= 0; i--) {
        wasm_export_t ex;
        wasm_global_inst_t g;
        wasm_runtime_get_export_type(w->mod->module, i, &ex);
        if (ex.kind != WASM_IMPORT_EXPORT_KIND_GLOBAL) continue;
        if (!wasm_runtime_get_export_global_inst(w->inst, ex.name, &g) || !g.is_mutable) continue;
        ERL_NIF_TERM kind = g.kind == WASM_I32 ? A_I32 : g.kind == WASM_I64 ? A_I64
            : g.kind == WASM_F32 ? A_F32 : A_F64;
        if (g.kind != WASM_I32 && g.kind != WASM_I64 && g.kind != WASM_F32 && g.kind != WASM_F64)
            continue;
        globals = enif_make_list_cell(env,
            enif_make_tuple3(env, mk_bin(env, ex.name), kind,
                enif_make_uint64(env, global_get(&g))),
            globals);
    }
    out = enif_make_tuple4(env, A_OK, enif_make_int64(env, w->gen), membin, globals);
done:
    enif_mutex_unlock(w->lock);
    return out;
}

/* restore(Inst, Gen, Memory, Globals) -> ok | {error, Msg}
 * A fresh WAMR instance replaces the old one inside the same resource (memory
 * never shrinks, so the old one may be larger than the snapshot), then the
 * memory and the globals are written and the gen is set. */
static ERL_NIF_TERM nif_restore(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    wp_inst *w;
    ErlNifSInt64 gen;
    ErlNifBinary mem;
    char err[ERRLEN];
    if (!enif_get_resource(env, argv[0], INST_RES, (void **)&w)
            || !enif_get_int64(env, argv[1], &gen)
            || !enif_inspect_binary(env, argv[2], &mem)
            || !enif_is_list(env, argv[3]))
        return enif_make_badarg(env);
    thread_env();
    enif_mutex_lock(w->lock);
    ERL_NIF_TERM out;
    inst_teardown(w);
    w->gen = -1;
    err[0] = 0;
    if (!inst_build(w, err, sizeof err)) { out = error_msg(env, err[0] ? err : "instantiate failed"); goto done; }
    wasm_memory_inst_t m = wasm_runtime_get_default_memory(w->inst);
    uint64_t psize = wasm_memory_get_bytes_per_page(m);
    if (psize == 0 || mem.size % psize != 0) { out = error_msg(env, "snapshot memory is not whole pages"); goto done; }
    uint64_t want = mem.size / psize, have = wasm_memory_get_cur_page_count(m);
    if (have > want) { out = error_msg(env, "snapshot memory smaller than the initial memory"); goto done; }
    if (want > have && !wasm_memory_enlarge(m, want - have)) { out = error_msg(env, "cannot grow memory to the snapshot"); goto done; }
    if (mem.size) memcpy(wasm_memory_get_base_address(m), mem.data, mem.size);
    ERL_NIF_TERM list = argv[3], head;
    while (enif_get_list_cell(env, list, &head, &list)) {
        const ERL_NIF_TERM *t;
        int arity;
        ErlNifBinary name;
        ErlNifUInt64 bits;
        wasm_global_inst_t g;
        char nm[256];
        if (!enif_get_tuple(env, head, &arity, &t) || arity != 3
                || !enif_inspect_binary(env, t[0], &name) || name.size >= sizeof nm
                || !enif_get_uint64(env, t[2], &bits)) {
            out = error_msg(env, "bad global");
            goto done;
        }
        memcpy(nm, name.data, name.size);
        nm[name.size] = 0;
        if (!wasm_runtime_get_export_global_inst(w->inst, nm, &g) || !g.is_mutable) {
            out = error_msg(env, "snapshot global missing from module");
            goto done;
        }
        global_set(&g, bits);
    }
    w->gen = gen;
    out = A_OK;
done:
    enif_mutex_unlock(w->lock);
    return out;
}

static ERL_NIF_TERM nif_gen(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    wp_inst *w;
    if (!enif_get_resource(env, argv[0], INST_RES, (void **)&w)) return enif_make_badarg(env);
    enif_mutex_lock(w->lock);
    int64_t g = w->gen;
    enif_mutex_unlock(w->lock);
    return enif_make_int64(env, g);
}

static ERL_NIF_TERM nif_memory_size(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    wp_inst *w;
    if (!enif_get_resource(env, argv[0], INST_RES, (void **)&w)) return enif_make_badarg(env);
    enif_mutex_lock(w->lock);
    uint64_t size = 0;
    if (w->inst) {
        wasm_memory_inst_t m = wasm_runtime_get_default_memory(w->inst);
        size = wasm_memory_get_cur_page_count(m) * (uint64_t)wasm_memory_get_bytes_per_page(m);
    }
    enif_mutex_unlock(w->lock);
    return enif_make_uint64(env, size);
}

/* ---------- load ---------- */

static int load(ErlNifEnv *env, void **priv, ERL_NIF_TERM info) {
    A_OK = enif_make_atom(env, "ok");
    A_ERROR = enif_make_atom(env, "error");
    A_TRAP = enif_make_atom(env, "trap");
    A_FAULT = enif_make_atom(env, "fault");
    A_STALE = enif_make_atom(env, "stale");
    A_DIRTY = enif_make_atom(env, "dirty");
    A_I32 = enif_make_atom(env, "i32");
    A_I64 = enif_make_atom(env, "i64");
    A_F32 = enif_make_atom(env, "f32");
    A_F64 = enif_make_atom(env, "f64");
    MODULE_RES = enif_open_resource_type(env, NULL, "hb_wasm_module", module_dtor,
        ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER, NULL);
    INST_RES = enif_open_resource_type(env, NULL, "hb_wasm_inst", inst_dtor,
        ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER, NULL);
    if (!MODULE_RES || !INST_RES) return 1;
    if (!RUNTIME_READY) {
        RuntimeInitArgs init;
        memset(&init, 0, sizeof init);
        init.mem_alloc_type = Alloc_With_System_Allocator;
        if (!wasm_runtime_full_init(&init)) return 2;
        wasm_runtime_set_log_level(WASM_LOG_LEVEL_FATAL);
        REG_LOCK = enif_mutex_create("hb_wasm_registry");
        if (!wasm_runtime_register_natives("env", FIXED_NATIVES,
                sizeof FIXED_NATIVES / sizeof *FIXED_NATIVES))
            return 3;
        RUNTIME_READY = 1;
    }
    return 0;
}

static int upgrade(ErlNifEnv *env, void **priv, void **old, ERL_NIF_TERM info) {
    return load(env, priv, info);
}

static ErlNifFunc FUNCS[] = {
    { "register_invoke", 1, nif_register_invoke, 0 },
    { "load", 1, nif_load, ERL_NIF_DIRTY_JOB_CPU_BOUND },
    { "instantiate", 3, nif_instantiate, ERL_NIF_DIRTY_JOB_CPU_BOUND },
    { "call", 5, nif_call, ERL_NIF_DIRTY_JOB_CPU_BOUND },
    { "snapshot", 1, nif_snapshot, ERL_NIF_DIRTY_JOB_CPU_BOUND },
    { "restore", 4, nif_restore, ERL_NIF_DIRTY_JOB_CPU_BOUND },
    { "gen", 1, nif_gen, 0 },
    { "memory_size", 1, nif_memory_size, 0 },
};

ERL_NIF_INIT(hb_wasm_nif, FUNCS, load, NULL, upgrade, NULL)

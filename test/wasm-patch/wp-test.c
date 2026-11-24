/* wp-test: a minimal module implementing the wasm-patch@1.0 ABI
 * (docs/WASM_PATCH_ABI.md in paralith-mono), for the device's tests.
 * Build: test/wasm-patch/build.sh. Freestanding: no libc, no WASI.
 *
 * State is a counter in linear memory. The request's `body/action' picks:
 *   (anything else)  count += 1; set /count, /last-action, /echo-number
 *   Fail             count += 100, then status 1 with the error "boom"
 *   Trap             count += 1000, then `unreachable'
 *   Types            one patch of every value type
 *   Delete           delete /last-action
 *   Grow             grow memory by 4 pages, then as the default
 *   Jump             longjmp out of a call through invoke_vii
 *   Bad              a malformed result
 *   Loop             never returns (only with fuel)
 * wp_init reads `start' (an integer) from the definition into the counter. */
typedef unsigned char u8;
typedef unsigned int u32;
typedef long long i64;
typedef unsigned long u_ptr;

#define IMPORT(n) __attribute__((import_module("env"), import_name(n)))
IMPORT("invoke_vii") void invoke_vii(int fn, int a, int b);
IMPORT("throw_longjmp") void throw_longjmp(void);

extern u8 __heap_base;

static i64 count;
static i64 init_calls;
static i64 jumped;
static u32 out_at;   /* reply buffer for the current call */
static u32 out_len;

void *memcpy(void *d, const void *s, unsigned long n) {
    u8 *dp = d;
    const u8 *sp = s;
    while (n--) *dp++ = *sp++;
    return d;
}

void *memset(void *d, int c, unsigned long n) {
    u8 *dp = d;
    while (n--) *dp++ = (u8)c;
    return d;
}

static u32 rd32(const u8 *p) { return p[0] | (p[1] << 8) | (p[2] << 16) | ((u32)p[3] << 24); }

static void ensure(u32 end) {
    u32 have = __builtin_wasm_memory_size(0) * 65536u;
    if (end > have) __builtin_wasm_memory_grow(0, (end - have + 65535u) / 65536u);
}

/* The request buffer starts at the heap base; the reply follows it. */
__attribute__((export_name("wp_alloc"))) u32 wp_alloc(u32 len) {
    u32 at = ((u32)(u_ptr)&__heap_base + 15u) & ~15u;
    ensure(at + len + 16);
    out_at = (at + len + 16) & ~15u;
    out_len = 0;
    return at;
}

/* ---- reply writer ---- */
static void put(const void *src, u32 n) {
    ensure(out_at + 4 + out_len + n + 64);
    memcpy((u8 *)(u_ptr)(out_at + 4 + out_len), src, n);
    out_len += n;
}
static void put8(u8 b) { put(&b, 1); }
static void put32(u32 v) { u8 b[4] = { v, v >> 8, v >> 16, v >> 24 }; put(b, 4); }
static void put64(i64 v) { for (int i = 0; i < 8; i++) put8((u8)((unsigned long long)v >> (8 * i))); }
static u32 slen(const char *s) { u32 n = 0; while (s[n]) n++; return n; }
static void bin(const void *s, u32 n) { put8(1); put32(n); put(s, n); }
static void str(const char *s) { bin(s, slen(s)); }
static void int64(i64 v) { put8(2); put64(v); }
static void dec(i64 v) {
    char b[24];
    int i = 23, neg = v < 0;
    unsigned long long u = neg ? -(unsigned long long)v : (unsigned long long)v;
    b[i] = 0;
    do { b[--i] = '0' + (u % 10); u /= 10; } while (u);
    if (neg) b[--i] = '-';
    str(b + i);
}
static void set(const char *path) { put8(1); put32(slen(path)); put(path, slen(path)); }
static u32 finish(void) {
    memcpy((u8 *)(u_ptr)out_at, &out_len, 4);
    return out_at;
}

/* ---- request reader ---- */
static const u8 *skip(const u8 *p) {
    u8 t = *p++;
    u32 n;
    switch (t) {
        case 1: return p + 4 + rd32(p);
        case 2: case 3: return p + 8;
        case 4: case 5: return p;
        case 6:
            n = rd32(p); p += 4;
            while (n--) { p = skip(p); p = skip(p); }
            return p;
        case 7:
            n = rd32(p); p += 4;
            while (n--) p = skip(p);
            return p;
    }
    return p;
}

static int key_is(const u8 *k, const char *s) {
    u32 n = slen(s);
    if (k[0] != 1 || rd32(k + 1) != n) return 0;
    for (u32 i = 0; i < n; i++) if (k[5 + i] != (u8)s[i]) return 0;
    return 1;
}

static const u8 *find(const u8 *map, const char *key) {
    if (!map || map[0] != 6) return 0;
    u32 n = rd32(map + 1);
    const u8 *p = map + 5;
    while (n--) {
        const u8 *v = skip(p);
        if (key_is(p, key)) return v;
        p = skip(v);
    }
    return 0;
}

static int bin_is(const u8 *v, const char *s) { return v && key_is(v, s); }

/* ---- longjmp through invoke_vii ---- */
static void thrower(int a, int b) {
    volatile char pad[256];
    pad[0] = (char)a;
    jumped = a + pad[0] - a;
    throw_longjmp();
}

static u32 ok_results_count(void) {
    /* results = { output = { data = "<count>" } } */
    put8(6); put32(1);
    str("output"); put8(6); put32(1); str("data"); dec(count);
    return finish();
}

__attribute__((export_name("wp_init"))) u32 wp_init(u32 ptr, u32 len) {
    const u8 *def = (const u8 *)(u_ptr)ptr;
    const u8 *start = find(def, "start");
    init_calls++;
    if (start && start[0] == 2) {
        i64 v;
        memcpy(&v, start + 1, 8);
        count = v;
    }
    put8(0);
    put8(6); put32(0);
    return finish();
}

__attribute__((export_name("wp_handle"))) u32 wp_handle(u32 ptr, u32 len) {
    const u8 *req = (const u8 *)(u_ptr)ptr;
    const u8 *body = find(req, "body");
    if (!body) body = req;
    const u8 *action = find(body, "action");
    const u8 *number = find(body, "number");
    if (bin_is(action, "Fail")) {
        count += 100;
        put8(1); str("boom");
        return finish();
    }
    if (bin_is(action, "Trap")) {
        count += 1000;
        __builtin_trap();
    }
    if (bin_is(action, "Loop")) {
        for (;;) count++;
    }
    if (bin_is(action, "Bad")) {
        put8(0); put32(1); put8(9);
        return finish();
    }
    if (bin_is(action, "Types")) {
        put8(0); put32(8);
        put8(1); set("/t-int"); int64(42);
        put8(1); set("/t-float"); { double d = 1.5; put8(3); put(&d, 8); }
        put8(1); set("/t-true"); put8(4);
        put8(1); set("/t-false"); put8(5);
        put8(1); set("/t-map"); put8(6); put32(2); str("a"); str("x"); str("n"); int64(7);
        put8(1); set("/t-list"); put8(7); put32(2); str("p"); str("q");
        put8(1); set("/t-empty"); put8(7); put32(0);
        /* { B = { { true }, { x = 1 } }, [3] = 42, x = 'y' } */
        put8(1); set("/t-mixed"); put8(6); put32(3);
        str("B"); put8(7); put32(2); put8(7); put32(1); put8(4); put8(6); put32(1); str("x"); int64(1);
        int64(3); int64(42);
        str("x"); str("y");
        put8(6); put32(0);
        return finish();
    }
    if (bin_is(action, "Delete")) {
        put8(0); put32(1);
        put8(2); set("/last-action");
        put8(6); put32(0);
        return finish();
    }
    if (bin_is(action, "Jump")) {
        jumped = 0;
        invoke_vii((int)(u_ptr)&thrower, 7, 0);
        put8(0); put32(1);
        put8(1); set("/jumped"); int64(jumped);
        put8(6); put32(0);
        return finish();
    }
    if (bin_is(action, "Grow")) __builtin_wasm_memory_grow(0, 4);
    count++;
    put8(0); put32(number ? 3 : 2);
    put8(1); set("/count"); dec(count);
    put8(1); set("/last-action");
    if (action && action[0] == 1) bin(action + 5, rd32(action + 1)); else str("");
    if (number) {
        put8(1); set("/echo-number");
        const u8 *end = skip(number);
        put(number, (u32)(end - number));
    }
    return ok_results_count();
}

/* Read-only probes for the tests (never called by the device). */
__attribute__((export_name("wp_test_init_calls"))) i64 wp_test_init_calls(void) { return init_calls; }

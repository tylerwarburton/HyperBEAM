//! Structural tests of the pass. Runtime equivalence (interpreter vs AOT, fuel
//! and depth traps) is in tests/wamr/run.sh, which uses the corpus written by
//! `corpus_emit` when WPI_CORPUS_OUT is set.
use wasm_patch_instrument::{Config, DEPTH_LIMIT, Output, Reject, VERSION_SECTION, instrument, version};
use wasmparser::{ExternalKind, Parser, Payload};

const CAP: u64 = 64;

fn cfg() -> Config {
    Config { memory_cap_pages: CAP, unsafe_extra_imports: vec![] }
}
fn run(wat: &str) -> Result<Output, Reject> {
    instrument(&wat::parse_str(wat).expect("wat"), &cfg())
}
fn refused(wat: &str, why: &str) {
    match run(wat) {
        Ok(_) => panic!("accepted, expected refusal containing {why:?}:\n{wat}"),
        Err(Reject(m)) => assert!(m.contains(why), "refusal {m:?} does not contain {why:?}"),
    }
}
fn exports(wasm: &[u8]) -> Vec<(String, ExternalKind, u32)> {
    let mut out = vec![];
    for p in Parser::new(0).parse_all(wasm) {
        if let Payload::ExportSection(r) = p.unwrap() {
            for e in r {
                let e = e.unwrap();
                out.push((e.name.to_string(), e.kind, e.index));
            }
        }
    }
    out
}
fn custom(wasm: &[u8], name: &str) -> Option<Vec<u8>> {
    Parser::new(0).parse_all(wasm).find_map(|p| match p.unwrap() {
        Payload::CustomSection(c) if c.name() == name => Some(c.data().to_vec()),
        _ => None,
    })
}
const CONTROL: &str = include_str!("corpus/control.wat");

#[test]
fn output_is_deterministic() {
    let input = wat::parse_str(CONTROL).unwrap();
    let a = instrument(&input, &cfg()).unwrap().wasm;
    let b = instrument(&input, &cfg()).unwrap().wasm;
    assert_eq!(a, b);
    // A golden digest would pin this across processes and builds; the
    // run.sh script compares two separate CLI runs byte for byte.
}

#[test]
fn version_is_embedded() {
    let out = run(CONTROL).unwrap();
    assert_eq!(custom(&out.wasm, VERSION_SECTION).unwrap(), version().into_bytes());
    assert!(version().contains(&format!("depth-limit={DEPTH_LIMIT}")));
    assert!(version().starts_with(concat!("wasm-patch-instrument/", env!("CARGO_PKG_VERSION"))));
}

#[test]
fn exports_fuel_depth_and_every_mutable_global() {
    let out = run(CONTROL).unwrap();
    let ex = exports(&out.wasm);
    // control.wat: globals 0 and 1 are mutable, 2 is not; ours are 3 and 4.
    let globals: Vec<_> = ex.iter().filter(|e| e.1 == ExternalKind::Global).map(|e| (e.0.as_str(), e.2)).collect();
    assert_eq!(globals, vec![("wp_fuel", 3), ("wp_depth", 4), ("wp_g0", 0), ("wp_g1", 1)]);
    assert_eq!(out.stats.exported_globals, 2);
}

#[test]
fn placement_is_scc() {
    let s = run(CONTROL).unwrap().stats;
    // fib, brtable, early, divmod and the ind_a/ind_b table cycle
    assert_eq!(s.cyclic_functions, 6);
    assert_eq!(s.loops, 5);
}

#[test]
fn module_without_globals_or_exports_gets_both_sections() {
    let out = run("(module (func (loop (br 0))))").unwrap();
    let names: Vec<_> = exports(&out.wasm).into_iter().map(|e| e.0).collect();
    assert_eq!(names, vec!["wp_fuel", "wp_depth"]);
    let out = run("(module)").unwrap();
    assert_eq!(exports(&out.wasm).len(), 2);
}

#[test]
fn name_section_kept_debug_dropped() {
    let mut m = wat::parse_str("(module $m (func $f (loop (br 0))))").unwrap();
    // append a producers-like custom section
    let mut sec = vec![0u8];
    let body = [&[7u8][..], b"junkjun", b"xyz"].concat();
    sec.push(body.len() as u8);
    sec.extend(body);
    m.extend(sec);
    let out = instrument(&m, &cfg()).unwrap().wasm;
    assert!(custom(&out, "name").is_some());
    assert!(custom(&out, "junkjun").is_none());
}

/// wasm-ld writes call_indirect's table index as a padded LEB when the
/// sysroot was built with reference types. Accept it; emit the single zero
/// byte a runtime without reference types requires.
#[test]
fn overlong_call_indirect_is_canonicalised() {
    fn sec(id: u8, body: &[u8]) -> Vec<u8> {
        let mut v = vec![id, body.len() as u8];
        v.extend(body);
        v
    }
    let code_body: &[u8] = &[0x00, 0x41, 0x00, 0x11, 0x00, 0x80, 0x80, 0x80, 0x80, 0x00, 0x0b];
    let mut code = vec![1u8, code_body.len() as u8];
    code.extend(code_body);
    let m = [
        &b"\0asm\x01\0\0\0"[..],
        &sec(1, &[1, 0x60, 0, 0]),
        &sec(3, &[1, 0]),
        &sec(4, &[1, 0x70, 0, 1]),
        &sec(10, &code),
    ]
    .concat();
    let out = instrument(&m, &cfg()).unwrap().wasm;
    let needle = [0x11, 0x00, 0x00];
    assert!(out.windows(3).any(|w| w == needle));
    assert!(!out.windows(2).any(|w| w == [0x80, 0x80]));
}

#[test]
fn allowlisted_imports_accepted() {
    run(r#"(module
        (import "env" "invoke_vii" (func (param i32 i32 i32)))
        (import "env" "throw_longjmp" (func))
        (import "env" "wp_bls_fav" (func (param i32 i32 i32 i32 i32 i32 i32) (result i32)))
        (import "env" "wp_rsa_pss_sha256" (func (param i32 i32 i32 i32 i32) (result i32))))"#)
    .unwrap();
}

#[test]
fn refuses_imports_outside_the_allowlist() {
    refused(r#"(module (import "env" "abort" (func)))"#, "env.abort is not allowed");
    refused(r#"(module (import "env" "invoke_vi" (func (param i32 i32))))"#, "not allowed");
    refused(r#"(module (import "env" "setThrew" (func (param i32 i32))))"#, "not allowed");
    refused(r#"(module (import "wasi_snapshot_preview1" "fd_write" (func (param i32 i32 i32 i32) (result i32))))"#, "not allowed");
    refused(r#"(module (import "env" "invoke_vii" (func (param i32 i32))))"#, "wrong signature");
    refused(r#"(module (import "env" "throw_longjmp" (func (param i32))))"#, "wrong signature");
    refused(r#"(module (import "env" "wp_bls_fav" (func (param i32 i32 i32 i32 i32 i32 i32))))"#, "wrong signature");
    refused(r#"(module (import "env" "wp_rsa_pss_sha256" (func (param i64 i64 i64 i64 i64) (result i32))))"#, "wrong signature");
    refused(r#"(module (import "env" "memory" (memory 1 2)))"#, "not a function");
    refused(r#"(module (import "env" "g" (global i32)))"#, "not a function");
    refused(r#"(module (import "env" "t" (table 1 funcref)))"#, "not a function");
}

#[test]
fn unsafe_extra_imports_only_when_asked() {
    let m = wat::parse_str(r#"(module (import "wasi_snapshot_preview1" "fd_write" (func (param i32 i32 i32 i32) (result i32))))"#).unwrap();
    let c = Config { memory_cap_pages: CAP, unsafe_extra_imports: vec!["wasi_snapshot_preview1.*".into()] };
    instrument(&m, &c).unwrap();
    let c = Config { memory_cap_pages: CAP, unsafe_extra_imports: vec!["wasi_snapshot_preview1.fd_read".into()] };
    assert!(instrument(&m, &c).is_err());
}

#[test]
fn refuses_disallowed_features() {
    refused("(module (func (drop (v128.const i64x2 0 0))))", "invalid or unsupported");
    refused("(module (memory 1 1 shared))", "invalid or unsupported");
    refused("(module (memory 1 1) (func (drop (i32.atomic.load (i32.const 0)))))", "invalid or unsupported");
    refused("(module (func $f (return_call $f)))", "invalid or unsupported");
    refused("(module (table 1 funcref) (func (drop (table.get 0 (i32.const 0)))))", "invalid or unsupported");
    refused("(module (func $f (drop (ref.func $f))) (elem declare func $f))", "invalid or unsupported");
    refused("(module (func (local externref)))", "invalid or unsupported");
    refused("(module (tag) (func (try_table (catch_all 0))))", "invalid or unsupported");
    refused("(module (memory 1 1) (memory 1 1))", "invalid or unsupported");
    refused("(module (table 1 funcref) (table 1 funcref))", "invalid or unsupported");
}

#[test]
fn memory_needs_a_maximum_within_the_cap() {
    refused("(module (memory 1))", "no maximum");
    refused(&format!("(module (memory 1 {}))", CAP + 1), "above the cap");
    refused(&format!("(module (memory i64 1 {}))", CAP + 1), "above the cap");
    run(&format!("(module (memory 1 {CAP}))")).unwrap();
    run(&format!("(module (memory i64 1 {CAP}))")).unwrap();
}

#[test]
fn start_function_must_not_reach_an_import() {
    let imp = r#"(import "env" "throw_longjmp" (func $t))"#;
    refused(&format!("(module {imp} (start $t))"), "start function");
    refused(&format!("(module {imp} (func $s (call $t)) (start $s))"), "start function");
    refused(&format!("(module {imp} (func $s (call $m)) (func $m (call $t)) (start $s))"), "start function");
    // reaching an import through the table
    refused(
        &format!(
            r#"(module {imp} (type $v (func)) (table 1 funcref) (elem (i32.const 0) $w)
               (func $w (call $t)) (func $s (call_indirect (type $v) (i32.const 0))) (start $s))"#
        ),
        "start function",
    );
    run(&format!("(module {imp} (global $g (mut i32) (i32.const 0)) (func $s (global.set $g (i32.const 1))) (start $s))"))
        .unwrap();
}

#[test]
fn refuses_reserved_export_names_and_reinstrumentation() {
    refused(r#"(module (global (export "wp_fuel") (mut i64) (i64.const 0)))"#, "reserved");
    refused(r#"(module (func (export "wp_depth")))"#, "reserved");
    refused(r#"(module (func (export "wp_g0")))"#, "reserved");
    let once = run("(module (func))").unwrap().wasm;
    assert!(instrument(&once, &cfg()).is_err());
    let mut tagged = wat::parse_str("(module)").unwrap();
    tagged.extend([0u8, 1 + VERSION_SECTION.len() as u8, VERSION_SECTION.len() as u8]);
    tagged.extend(VERSION_SECTION.as_bytes());
    assert_eq!(instrument(&tagged, &cfg()).err().unwrap(), Reject("module is already instrumented".into()));
}

#[test]
fn refuses_garbage() {
    assert!(instrument(b"\0asm\x01\0\0\0\x01", &cfg()).is_err());
    assert!(instrument(b"not wasm", &cfg()).is_err());
}

#[test]
fn corpus_emit() {
    let Ok(dir) = std::env::var("WPI_CORPUS_OUT") else { return };
    std::fs::create_dir_all(&dir).unwrap();
    for e in std::fs::read_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/corpus")).unwrap() {
        let p = e.unwrap().path();
        if p.extension().is_some_and(|x| x == "wat") {
            let wasm = wat::parse_file(&p).unwrap();
            let stem = p.file_stem().unwrap().to_str().unwrap();
            std::fs::write(format!("{dir}/{stem}.wasm"), wasm).unwrap();
            std::fs::copy(&p, format!("{dir}/{stem}.wat")).unwrap();
        }
    }
}

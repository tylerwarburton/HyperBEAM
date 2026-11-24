//! Benchmark baseline: re-encode a module with no instrumentation, so the
//! comparison isolates fuel/depth. (A wasm-ld build with reference-types
//! sysroot libraries has padded call_indirect immediates that WAMR without
//! reference types refuses; this writes them canonically, as the pass does.)
//!   cargo run --release --example reencode -- <in.wasm> <out.wasm>
use wasm_encoder::reencode::{Reencode, RoundtripReencoder};

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let wasm = std::fs::read(&a[1]).expect("read");
    let mut m = wasm_encoder::Module::new();
    RoundtripReencoder.parse_core_module(&mut m, wasmparser::Parser::new(0), &wasm).expect("re-encode");
    std::fs::write(&a[2], m.finish()).expect("write");
}

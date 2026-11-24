//! wasm-patch-instrument --memory-cap-pages <N> [--stats] [--unsafe-allow-import <module.name>]... <in.wasm> <out.wasm>
//! wasm-patch-instrument --version
//! Exit 0: written. Exit 1: module refused (reason on stderr). Exit 2: usage or I/O.
use std::process::exit;

use wasm_patch_instrument::{Config, instrument, version};

fn usage() -> ! {
    eprintln!(
        "usage: wasm-patch-instrument --memory-cap-pages <N> [--stats] [--unsafe-allow-import <module.name>]... <in.wasm> <out.wasm>\n       wasm-patch-instrument --version"
    );
    exit(2)
}

fn main() {
    let mut args = std::env::args().skip(1);
    let mut cap = None;
    let mut extra = vec![];
    let mut stats = false;
    let mut files = vec![];
    while let Some(a) = args.next() {
        match a.as_str() {
            "--version" => {
                println!("{}", version());
                return;
            }
            "--memory-cap-pages" => cap = args.next().and_then(|v| v.parse::<u64>().ok()).or_else(|| usage()),
            "--unsafe-allow-import" => extra.push(args.next().unwrap_or_else(|| usage())),
            "--stats" => stats = true,
            _ if a.starts_with('-') => usage(),
            _ => files.push(a),
        }
    }
    let ([input, output], Some(memory_cap_pages)) = (<[String; 2]>::try_from(files).unwrap_or_else(|_| usage()), cap) else {
        usage()
    };
    let wasm = std::fs::read(&input).unwrap_or_else(|e| {
        eprintln!("{input}: {e}");
        exit(2)
    });
    let out = instrument(&wasm, &Config { memory_cap_pages, unsafe_extra_imports: extra }).unwrap_or_else(|e| {
        eprintln!("refused: {e}");
        exit(1)
    });
    let tmp = format!("{output}.tmp");
    if let Err(e) = std::fs::write(&tmp, &out.wasm).and_then(|_| std::fs::rename(&tmp, &output)) {
        eprintln!("{output}: {e}");
        exit(2)
    }
    if stats {
        let s = out.stats;
        eprintln!(
            "{}: {} functions ({} cyclic), {} loops, {} invoke sites, {} globals exported, {} -> {} bytes",
            version(),
            s.functions,
            s.cyclic_functions,
            s.loops,
            s.invoke_sites,
            s.exported_globals,
            wasm.len(),
            out.wasm.len()
        );
    }
}

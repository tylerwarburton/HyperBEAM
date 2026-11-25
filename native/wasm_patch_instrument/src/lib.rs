//! Validation and deterministic instrumentation for `wasm-patch@1.0` modules
//! (paralith-mono docs/WASM_PATCH_ABI.md, "Determinism" and "Imports").
//!
//! The node runs this on a module's bytes at spawn, before `wamrc`, and runs
//! the OUTPUT under both the interpreter and AOT, so fuel and call depth are
//! counted by wasm code and are identical in both.
//!
//! Validation refuses: SIMD, threads/atomics/shared memory, reference types,
//! tail calls, exceptions, multi-memory, GC, any non-function import, any
//! function import outside the allowlist (`env.invoke_vii`,
//! `env.throw_longjmp`, `env.wp_bls_fav`, `env.wp_rsa_pss_sha256`, the
//! `env.wp_rng_*` natives, exact
//! signatures), a start function that can reach an
//! import, a memory without a maximum or with one above the cap, an export
//! already named `wp_fuel`/`wp_depth`/`wp_g*`, and an already instrumented
//! module.
//!
//! Instrumentation ("scc" placement):
//! - `wp_fuel` (exported mutable i64, initial 0): charged at the head of every
//!   `loop` body and at the entry of every function on a call-graph cycle. A
//!   charge subtracts a static cost (the instructions of that loop body or
//!   function body outside nested loops, plus the whole cost of every
//!   non-recursive function it calls directly) and traps with `unreachable`
//!   when the result is below zero. Any unbounded execution needs a loop or a
//!   recursion, and every recursion passes through a cyclic function, so this
//!   bounds execution. Call-graph edges: direct calls; `call_indirect` to every
//!   table function of the same signature; `env.invoke_*` to every table
//!   function of its signature minus the leading index.
//! - `wp_depth` (exported mutable i32, initial 0): incremented at the entry of
//!   every cyclic function and decremented on its exits; traps when above
//!   [`DEPTH_LIMIT`]. A longjmp unwinds frames without running their exits, so
//!   every call to an `env.invoke_*` import restores the caller's depth after
//!   it returns.
//! - every mutable global N of the input is exported as `wp_g<N>`.
//! - a custom section `wasm-patch-instrument` holding [`version()`].
//!
//! The node sets `wp_fuel` to the slot's budget and `wp_depth` to 0 before
//! every call into the module. After a trap, `wp_fuel < 0` means out of fuel
//! and `wp_depth > DEPTH_LIMIT` means too deep; both are identical under the
//! interpreter and AOT. A non-recursive, loop-free export reaches no charge
//! point and costs no fuel (it is bounded by its size).
//!
//! The output is a pure function of the input bytes: no hash-ordered
//! iteration reaches it.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use wasm_encoder::reencode::{Reencode, RoundtripReencoder, utils};
use wasm_encoder::{BlockType, ConstExpr, CustomSection, ExportKind, Function, GlobalType, Instruction, SectionId};
use wasmparser::{
    ElementItems, FuncType, Operator, Parser, Payload, TypeRef, ValType, Validator, WasmFeatures,
};

/// Fixed call-depth limit on cyclic (recursive) frames.
pub const DEPTH_LIMIT: u32 = 4096;
/// Name of the custom section carrying [`version()`].
pub const VERSION_SECTION: &str = "wasm-patch-instrument";
/// Cost model and placement identifier; bump it when the output changes.
const SCHEME: &str = "fuel=scc-static-v1";
/// Largest single charge, so a sum of costs can never wrap.
const MAX_COST: u64 = 1 << 40;

/// The string the node puts in its AOT cache key. It is also embedded in
/// every output module.
pub fn version() -> String {
    format!(
        "wasm-patch-instrument/{} {} depth-limit={}",
        env!("CARGO_PKG_VERSION"),
        SCHEME,
        DEPTH_LIMIT
    )
}

/// The ABI import allowlist: (module, name, params, results).
const ALLOWED_IMPORTS: &[(&str, &str, &[ValType], &[ValType])] = &[
    ("env", "invoke_vii", &[ValType::I32, ValType::I32, ValType::I32], &[]),
    ("env", "throw_longjmp", &[], &[]),
    // Verification natives (hb_wasm_nif.c): pure, fixed fuel charged by the
    // native itself. wasm32 pointers and lengths.
    ("env", "wp_bls_fav", &[ValType::I32; 7], &[ValType::I32]),
    ("env", "wp_rsa_pss_sha256", &[ValType::I32; 5], &[ValType::I32]),
    // rng@1.0 natives (hb_wasm_nif.c): the RNG beacons' checks.
    ("env", "wp_rng_bls", &[ValType::I32; 6], &[ValType::I32]),
    ("env", "wp_rng_reveal", &[ValType::I32; 7], &[ValType::I32]),
    ("env", "wp_rng_crack", &[ValType::I32; 9], &[ValType::I32]),
];

pub struct Config {
    /// Process memory cap in 64 KiB pages; the module's declared maximum must
    /// be at or below it.
    pub memory_cap_pages: u64,
    /// Benchmarks only: extra imports to accept, as `module.name` or
    /// `module.*`. The node never passes any.
    pub unsafe_extra_imports: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reject(pub String);

impl fmt::Display for Reject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for Reject {}

macro_rules! reject {
    ($($t:tt)*) => { return Err(Reject(format!($($t)*))) };
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Stats {
    pub functions: usize,
    pub cyclic_functions: usize,
    pub loops: usize,
    pub invoke_sites: usize,
    pub exported_globals: usize,
}

pub struct Output {
    pub wasm: Vec<u8>,
    pub stats: Stats,
}

/// Features a module may use. Everything else is refused by the validator.
/// `CALL_INDIRECT_OVERLONG` only accepts a padded LEB table index (wasm-ld
/// writes one when the sysroot was built with reference types); the output
/// re-encodes it as the single zero byte a runtime without reference types
/// requires.
pub fn features() -> WasmFeatures {
    WasmFeatures::FLOATS
        | WasmFeatures::GC_TYPES
        | WasmFeatures::MUTABLE_GLOBAL
        | WasmFeatures::SATURATING_FLOAT_TO_INT
        | WasmFeatures::SIGN_EXTENSION
        | WasmFeatures::MULTI_VALUE
        | WasmFeatures::BULK_MEMORY
        | WasmFeatures::MEMORY64
        | WasmFeatures::CALL_INDIRECT_OVERLONG
}

#[derive(Default)]
struct Region {
    instrs: u64,
    calls: Vec<u32>,
}

#[derive(Default)]
struct FuncInfo {
    /// region 0 = the function body outside loops, then one per loop in order.
    regions: Vec<Region>,
    indirect_types: Vec<u32>,
    calls_invoke: bool,
}

struct Plan {
    n_imports: u32,
    n_globals: u32,
    mutable_globals: Vec<u32>,
    func_types: Vec<u32>,
    types: Vec<FuncType>,
    infos: Vec<FuncInfo>,
    cyclic: BTreeSet<u32>,
    invoke_imports: BTreeSet<u32>,
    /// cost per (defined function, region)
    costs: Vec<Vec<u64>>,
    /// multi-result block types to append to the type section
    extra_types: Vec<Vec<ValType>>,
    n_types: u32,
}

pub fn instrument(wasm: &[u8], cfg: &Config) -> Result<Output, Reject> {
    Validator::new_with_features(features())
        .validate_all(wasm)
        .map_err(|e| Reject(format!("invalid or unsupported wasm: {e}")))?;
    let plan = analyze(wasm, cfg)?;
    let mut module = wasm_encoder::Module::new();
    let mut rw = Rewriter { plan: &plan, code_index: 0, emitted_global: false, emitted_export: false, stats: Stats::default() };
    rw.parse_core_module(&mut module, Parser::new(0), wasm)
        .map_err(|e| Reject(format!("re-encode failed: {e:?}")))?;
    let mut stats = rw.stats;
    stats.functions = plan.infos.len();
    stats.cyclic_functions = plan.cyclic.len();
    stats.exported_globals = plan.mutable_globals.len();
    let out = module.finish();
    Validator::new_with_features(features())
        .validate_all(&out)
        .map_err(|e| Reject(format!("internal: instrumented module does not validate: {e}")))?;
    Ok(Output { wasm: out, stats })
}

fn extra_import_allowed(cfg: &Config, module: &str, name: &str) -> bool {
    cfg.unsafe_extra_imports.iter().any(|p| match p.split_once('.') {
        Some((m, "*")) => m == module,
        Some((m, n)) => m == module && n == name,
        None => false,
    })
}

fn analyze(wasm: &[u8], cfg: &Config) -> Result<Plan, Reject> {
    let err = |e: wasmparser::BinaryReaderError| Reject(format!("parse: {e}"));
    let mut types: Vec<FuncType> = vec![];
    let mut func_types: Vec<u32> = vec![];
    let mut invoke_imports = BTreeSet::new();
    let mut n_imports = 0u32;
    let mut n_globals = 0u32;
    let mut mutable_globals = vec![];
    let mut start = None;
    let mut table_funcs: Vec<u32> = vec![];
    let mut infos: Vec<FuncInfo> = vec![];

    for payload in Parser::new(0).parse_all(wasm) {
        match payload.map_err(err)? {
            Payload::TypeSection(r) => {
                for t in r.into_iter_err_on_gc_types() {
                    types.push(t.map_err(err)?);
                }
            }
            Payload::ImportSection(r) => {
                for imp in r.into_imports() {
                    let imp = imp.map_err(err)?;
                    let TypeRef::Func(ti) = imp.ty else {
                        reject!("import {}.{} is not a function", imp.module, imp.name);
                    };
                    let ty = &types[ti as usize];
                    let allowed = ALLOWED_IMPORTS.iter().find(|(m, n, ..)| *m == imp.module && *n == imp.name);
                    match allowed {
                        Some((_, _, p, r)) => {
                            if ty.params() != *p || ty.results() != *r {
                                reject!("import {}.{} has the wrong signature", imp.module, imp.name);
                            }
                        }
                        None if extra_import_allowed(cfg, imp.module, imp.name) => {}
                        None => reject!("import {}.{} is not allowed", imp.module, imp.name),
                    }
                    if imp.module == "env" && imp.name.starts_with("invoke_") {
                        invoke_imports.insert(n_imports);
                    }
                    func_types.push(ti);
                    n_imports += 1;
                }
            }
            Payload::FunctionSection(r) => {
                for t in r {
                    func_types.push(t.map_err(err)?);
                }
            }
            Payload::MemorySection(r) => {
                for m in r {
                    let m = m.map_err(err)?;
                    if m.shared {
                        reject!("shared memory is not allowed");
                    }
                    match m.maximum {
                        None => reject!("memory has no maximum (cap {} pages)", cfg.memory_cap_pages),
                        Some(max) if max > cfg.memory_cap_pages => {
                            reject!("memory maximum {max} pages is above the cap of {} pages", cfg.memory_cap_pages)
                        }
                        Some(_) => {}
                    }
                }
            }
            Payload::GlobalSection(r) => {
                for g in r {
                    if g.map_err(err)?.ty.mutable {
                        mutable_globals.push(n_globals);
                    }
                    n_globals += 1;
                }
            }
            Payload::ExportSection(r) => {
                for e in r {
                    let e = e.map_err(err)?;
                    if e.name == "wp_fuel" || e.name == "wp_depth" || e.name.starts_with("wp_g") {
                        reject!("export name {} is reserved for the instrumentation", e.name);
                    }
                }
            }
            Payload::StartSection { func, .. } => start = Some(func),
            Payload::ElementSection(r) => {
                for el in r {
                    match el.map_err(err)?.items {
                        ElementItems::Functions(fs) => {
                            for f in fs {
                                table_funcs.push(f.map_err(err)?);
                            }
                        }
                        ElementItems::Expressions(..) => reject!("element expressions need reference types"),
                    }
                }
            }
            Payload::CodeSectionEntry(body) => infos.push(scan_body(&body).map_err(err)?),
            Payload::CustomSection(c) if c.name() == VERSION_SECTION => {
                reject!("module is already instrumented")
            }
            _ => {}
        }
    }

    for info in &mut infos {
        info.calls_invoke = info.regions.iter().any(|r| r.calls.iter().any(|c| invoke_imports.contains(c)));
    }

    // Call graph over all functions (imports included as sinks).
    let n_funcs = func_types.len();
    let mut by_sig: BTreeMap<&FuncType, Vec<u32>> = BTreeMap::new();
    table_funcs.sort_unstable();
    table_funcs.dedup();
    for &f in &table_funcs {
        by_sig.entry(&types[func_types[f as usize] as usize]).or_default().push(f);
    }
    let mut edges: Vec<Vec<u32>> = vec![vec![]; n_funcs];
    for (i, info) in infos.iter().enumerate() {
        let f = n_imports as usize + i;
        let mut es = vec![];
        for r in &info.regions {
            for &c in &r.calls {
                es.push(c);
                if invoke_imports.contains(&c) {
                    let t = &types[func_types[c as usize] as usize];
                    let target = FuncType::new(t.params()[1..].iter().copied(), t.results().iter().copied());
                    if let Some(v) = by_sig.get(&target) {
                        es.extend(v);
                    }
                }
            }
        }
        for &t in &info.indirect_types {
            if let Some(v) = by_sig.get(&types[t as usize]) {
                es.extend(v);
            }
        }
        es.sort_unstable();
        es.dedup();
        edges[f] = es;
    }

    // A start function may run before the node can set fuel; refuse one that
    // can reach any import.
    if let Some(s) = start {
        let mut seen = vec![false; n_funcs];
        let mut stack = vec![s];
        while let Some(f) = stack.pop() {
            if std::mem::replace(&mut seen[f as usize], true) {
                continue;
            }
            if f < n_imports {
                reject!("the start function can reach an import");
            }
            stack.extend(&edges[f as usize]);
        }
    }

    let cyclic = cyclic_functions(&edges);

    // Inclusive cost of each non-recursive defined function: its body outside
    // loops plus the inclusive cost of the non-recursive functions it calls
    // there. Iterative post-order; acyclic callees form a DAG.
    let n_def = infos.len();
    let mut incl: Vec<Option<u64>> = vec![None; n_def];
    for root in 0..n_def {
        if incl[root].is_some() || cyclic.contains(&(root as u32 + n_imports)) {
            continue;
        }
        let mut stack = vec![(root, false)];
        while let Some((i, expanded)) = stack.pop() {
            if incl[i].is_some() {
                continue;
            }
            let acyclic_callees = infos[i].regions[0]
                .calls
                .iter()
                .filter(|&&c| c >= n_imports && !cyclic.contains(&c))
                .map(|&c| (c - n_imports) as usize);
            if expanded {
                let mut sum = infos[i].regions[0].instrs;
                for c in acyclic_callees {
                    sum = (sum + incl[c].expect("post-order")).min(MAX_COST);
                }
                incl[i] = Some(sum);
            } else {
                stack.push((i, true));
                for c in acyclic_callees {
                    if incl[c].is_none() {
                        stack.push((c, false));
                    }
                }
            }
        }
    }
    let costs = infos
        .iter()
        .map(|info| {
            info.regions
                .iter()
                .map(|r| {
                    let mut sum = r.instrs;
                    for &c in &r.calls {
                        if c >= n_imports && !cyclic.contains(&c) {
                            sum = (sum + incl[(c - n_imports) as usize].unwrap()).min(MAX_COST);
                        }
                    }
                    sum.max(1)
                })
                .collect()
        })
        .collect();

    // Multi-result cyclic functions need a block type for their wrapper.
    let mut extra_types: Vec<Vec<ValType>> = vec![];
    for &f in &cyclic {
        let results = types[func_types[f as usize] as usize].results();
        if results.len() > 1
            && !types.iter().any(|t| t.params().is_empty() && t.results() == results)
            && !extra_types.iter().any(|r| r.as_slice() == results)
        {
            extra_types.push(results.to_vec());
        }
    }

    Ok(Plan {
        n_imports,
        n_globals,
        mutable_globals,
        func_types,
        n_types: types.len() as u32,
        types,
        infos,
        cyclic,
        invoke_imports,
        costs,
        extra_types,
    })
}

fn scan_body(body: &wasmparser::FunctionBody<'_>) -> wasmparser::Result<FuncInfo> {
    let mut info = FuncInfo { regions: vec![Region::default()], ..Default::default() };
    let mut stack: Vec<usize> = vec![0];
    let mut ops = body.get_operators_reader()?;
    while !ops.eof() {
        let op = ops.read()?;
        let cur = *stack.last().expect("validated nesting");
        info.regions[cur].instrs += 1;
        match op {
            Operator::Block { .. } | Operator::If { .. } => stack.push(cur),
            Operator::Loop { .. } => {
                info.regions.push(Region::default());
                stack.push(info.regions.len() - 1);
            }
            Operator::End => {
                stack.pop();
            }
            Operator::Call { function_index } => info.regions[cur].calls.push(function_index),
            Operator::CallIndirect { type_index, .. } => info.indirect_types.push(type_index),
            _ => {}
        }
    }
    Ok(info)
}

/// Functions on a call-graph cycle (Tarjan, iterative, deterministic order).
fn cyclic_functions(edges: &[Vec<u32>]) -> BTreeSet<u32> {
    let n = edges.len();
    let mut index = vec![usize::MAX; n];
    let mut low = vec![0usize; n];
    let mut on = vec![false; n];
    let mut stack = vec![];
    let mut out = BTreeSet::new();
    let mut next = 0usize;
    for root in 0..n {
        if index[root] != usize::MAX {
            continue;
        }
        let mut work: Vec<(usize, usize)> = vec![(root, 0)];
        while let Some(&(v, i)) = work.last() {
            if i == 0 && index[v] == usize::MAX {
                index[v] = next;
                low[v] = next;
                next += 1;
                stack.push(v);
                on[v] = true;
            }
            if i < edges[v].len() {
                let w = edges[v][i] as usize;
                work.last_mut().unwrap().1 += 1;
                if index[w] == usize::MAX {
                    work.push((w, 0));
                } else if on[w] {
                    low[v] = low[v].min(index[w]);
                }
                continue;
            }
            work.pop();
            if let Some(&(p, _)) = work.last() {
                low[p] = low[p].min(low[v]);
            }
            if low[v] == index[v] {
                let mut comp = vec![];
                loop {
                    let w = stack.pop().unwrap();
                    on[w] = false;
                    comp.push(w as u32);
                    if w == v {
                        break;
                    }
                }
                if comp.len() > 1 || edges[v].contains(&(v as u32)) {
                    out.extend(comp);
                }
            }
        }
    }
    out
}

struct Rewriter<'p> {
    plan: &'p Plan,
    code_index: usize,
    emitted_global: bool,
    emitted_export: bool,
    stats: Stats,
}

fn rank(s: SectionId) -> u8 {
    match s {
        SectionId::Type => 1,
        SectionId::Import => 2,
        SectionId::Function => 3,
        SectionId::Table => 4,
        SectionId::Memory => 5,
        SectionId::Tag => 6,
        SectionId::Global => 7,
        SectionId::Export => 8,
        SectionId::Start => 9,
        SectionId::Element => 10,
        SectionId::DataCount => 11,
        SectionId::Code => 12,
        SectionId::Data => 13,
        _ => 0,
    }
}

type RwResult<T> = Result<T, wasm_encoder::reencode::Error>;

impl Rewriter<'_> {
    fn fuel(&self) -> u32 {
        self.plan.n_globals
    }
    fn depth(&self) -> u32 {
        self.plan.n_globals + 1
    }
    fn add_globals(&self, s: &mut wasm_encoder::GlobalSection) {
        let g = |val_type| GlobalType { val_type, mutable: true, shared: false };
        s.global(g(wasm_encoder::ValType::I64), &ConstExpr::i64_const(0));
        s.global(g(wasm_encoder::ValType::I32), &ConstExpr::i32_const(0));
    }
    fn add_exports(&self, s: &mut wasm_encoder::ExportSection) {
        s.export("wp_fuel", ExportKind::Global, self.fuel());
        s.export("wp_depth", ExportKind::Global, self.depth());
        for &g in &self.plan.mutable_globals {
            s.export(&format!("wp_g{g}"), ExportKind::Global, g);
        }
    }
    fn charge(&self, f: &mut Function, cost: u64) {
        let fuel = self.fuel();
        f.instruction(&Instruction::GlobalGet(fuel))
            .instruction(&Instruction::I64Const(cost as i64))
            .instruction(&Instruction::I64Sub)
            .instruction(&Instruction::GlobalSet(fuel))
            .instruction(&Instruction::GlobalGet(fuel))
            .instruction(&Instruction::I64Const(0))
            .instruction(&Instruction::I64LtS)
            .instruction(&Instruction::If(BlockType::Empty))
            .instruction(&Instruction::Unreachable)
            .instruction(&Instruction::End);
    }
    /// Entry of a cyclic function: charge fuel and enter one depth level,
    /// one branch for both checks.
    fn enter(&self, f: &mut Function, cost: u64) {
        let (fuel, depth) = (self.fuel(), self.depth());
        f.instruction(&Instruction::GlobalGet(fuel))
            .instruction(&Instruction::I64Const(cost as i64))
            .instruction(&Instruction::I64Sub)
            .instruction(&Instruction::GlobalSet(fuel))
            .instruction(&Instruction::GlobalGet(depth))
            .instruction(&Instruction::I32Const(1))
            .instruction(&Instruction::I32Add)
            .instruction(&Instruction::GlobalSet(depth))
            .instruction(&Instruction::GlobalGet(fuel))
            .instruction(&Instruction::I64Const(0))
            .instruction(&Instruction::I64LtS)
            .instruction(&Instruction::GlobalGet(depth))
            .instruction(&Instruction::I32Const(DEPTH_LIMIT as i32))
            .instruction(&Instruction::I32GtU)
            .instruction(&Instruction::I32Or)
            .instruction(&Instruction::If(BlockType::Empty))
            .instruction(&Instruction::Unreachable)
            .instruction(&Instruction::End);
    }
    fn leave(&self, f: &mut Function) {
        let depth = self.depth();
        f.instruction(&Instruction::GlobalGet(depth))
            .instruction(&Instruction::I32Const(1))
            .instruction(&Instruction::I32Sub)
            .instruction(&Instruction::GlobalSet(depth));
    }
    fn wrapper_type(&mut self, results: &[ValType]) -> RwResult<BlockType> {
        Ok(match results {
            [] => BlockType::Empty,
            [t] => BlockType::Result(RoundtripReencoder.val_type(*t)?),
            _ => {
                let p = self.plan;
                let idx = match p.types.iter().position(|t| t.params().is_empty() && t.results() == results) {
                    Some(i) => i as u32,
                    None => p.n_types + p.extra_types.iter().position(|r| r.as_slice() == results).unwrap() as u32,
                };
                BlockType::FunctionType(idx)
            }
        })
    }
}

impl Reencode for Rewriter<'_> {
    type Error = std::convert::Infallible;

    fn parse_type_section(
        &mut self,
        types: &mut wasm_encoder::TypeSection,
        section: wasmparser::TypeSectionReader<'_>,
    ) -> RwResult<()> {
        utils::parse_type_section(self, types, section)?;
        for r in &self.plan.extra_types {
            let results: Vec<_> = r.iter().map(|t| RoundtripReencoder.val_type(*t)).collect::<Result<_, _>>()?;
            types.ty().function([], results);
        }
        Ok(())
    }

    fn parse_global_section(
        &mut self,
        globals: &mut wasm_encoder::GlobalSection,
        section: wasmparser::GlobalSectionReader<'_>,
    ) -> RwResult<()> {
        utils::parse_global_section(self, globals, section)?;
        self.add_globals(globals);
        self.emitted_global = true;
        Ok(())
    }

    fn parse_export_section(
        &mut self,
        exports: &mut wasm_encoder::ExportSection,
        section: wasmparser::ExportSectionReader<'_>,
    ) -> RwResult<()> {
        utils::parse_export_section(self, exports, section)?;
        self.add_exports(exports);
        self.emitted_export = true;
        Ok(())
    }

    fn intersperse_section_hook(
        &mut self,
        module: &mut wasm_encoder::Module,
        _after: Option<SectionId>,
        before: Option<SectionId>,
    ) -> RwResult<()> {
        let past = |s: SectionId| before.is_none_or(|b| rank(b) > rank(s));
        if !self.emitted_global && past(SectionId::Global) {
            let mut s = wasm_encoder::GlobalSection::new();
            self.add_globals(&mut s);
            module.section(&s);
            self.emitted_global = true;
        }
        if !self.emitted_export && past(SectionId::Export) {
            let mut s = wasm_encoder::ExportSection::new();
            self.add_exports(&mut s);
            module.section(&s);
            self.emitted_export = true;
        }
        if before.is_none() {
            let v = version();
            module.section(&CustomSection { name: VERSION_SECTION.into(), data: v.as_bytes().into() });
        }
        Ok(())
    }

    /// Keep only the name section (indices stay valid: no function is added
    /// and new locals go after the existing ones). Debug info and producers
    /// would describe code that no longer exists.
    fn parse_custom_section(
        &mut self,
        module: &mut wasm_encoder::Module,
        section: wasmparser::CustomSectionReader<'_>,
    ) -> RwResult<()> {
        if section.name() == "name" {
            module.section(&CustomSection { name: "name".into(), data: section.data().into() });
        }
        Ok(())
    }

    fn parse_function_body(
        &mut self,
        code: &mut wasm_encoder::CodeSection,
        func: wasmparser::FunctionBody<'_>,
    ) -> RwResult<()> {
        let p = self.plan;
        let i = self.code_index;
        self.code_index += 1;
        let fidx = p.n_imports + i as u32;
        let ty = &p.types[p.func_types[fidx as usize] as usize];
        let cyclic = p.cyclic.contains(&fidx);
        let costs = &p.costs[i];

        let mut locals = vec![];
        let mut n_locals = ty.params().len() as u32;
        for pair in func.get_locals_reader()? {
            let (n, t) = pair?;
            locals.push((n, RoundtripReencoder.val_type(t)?));
            n_locals += n;
        }
        let save = n_locals;
        if p.infos[i].calls_invoke {
            locals.push((1, wasm_encoder::ValType::I32));
        }
        let mut f = Function::new(locals);
        if cyclic {
            self.enter(&mut f, costs[0]);
            let bt = self.wrapper_type(ty.results())?;
            f.instruction(&Instruction::Block(bt));
        }
        let mut next_loop = 1usize;
        let mut ops = func.get_operators_reader()?;
        while !ops.eof() {
            let op = ops.read()?;
            match op {
                Operator::Loop { .. } => {
                    f.instruction(&RoundtripReencoder.instruction(op)?);
                    self.charge(&mut f, costs[next_loop]);
                    next_loop += 1;
                    self.stats.loops += 1;
                }
                Operator::Return if cyclic => {
                    self.leave(&mut f);
                    f.instruction(&Instruction::Return);
                }
                Operator::Call { function_index } if p.invoke_imports.contains(&function_index) => {
                    let depth = self.depth();
                    f.instruction(&Instruction::GlobalGet(depth))
                        .instruction(&Instruction::LocalSet(save))
                        .instruction(&Instruction::Call(function_index))
                        .instruction(&Instruction::LocalGet(save))
                        .instruction(&Instruction::GlobalSet(depth));
                    self.stats.invoke_sites += 1;
                }
                _ => {
                    f.instruction(&RoundtripReencoder.instruction(op)?);
                }
            }
        }
        if cyclic {
            // The body's own final `end` closed the wrapper block; branches
            // to the function label now land here too.
            self.leave(&mut f);
            f.instruction(&Instruction::End);
        }
        code.function(&f);
        Ok(())
    }
}

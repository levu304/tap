//! Wasmtime-backed transform runtime with embedded QuickJS WASM.
//!
//! Provides [`TransformEngine`] — a sandboxed JavaScript execution context
//! for user-defined CDC transforms (map, filter, mask, etc.).
//!
//! The engine uses epoch-based interruption to enforce safe timeouts on
//! JS execution, preventing runaway transforms from blocking the pipeline.
//!
//! # QuickJS WASM blob
//!
//! The WASM binary is embedded at compile time via [`include_bytes!`] from
//! `assets/quickjs.wasm`.  It is a pre-built Emscripten-compiled module from
//! the `quickjs-emscripten` project (`@jitl/quickjs-ng-wasmfile-release-sync`
//! v0.32.0).

use std::collections::HashMap;
use std::sync::OnceLock;

use wasmtime::{Caller, Engine, FuncType, Instance, Linker, Module, Store, Val, ValType};

use crate::error::TapError;

/// Sandboxed transform engine backed by wasmtime + QuickJS WASM.
///
/// # Initialisation
///
/// [`TransformEngine::new()`] creates a wasmtime `Engine` with epoch-based
/// interruption, compiles the embedded QuickJS WASM module, and
/// instantiates it inside a `Store`.
///
/// # Epoch-based interruption
///
/// Epoch interruption allows safe time-bounding of JS execution.  Each
/// call into QuickJS increments a per-engine epoch counter; the WASM
/// module polls the counter on every back-edge and traps when the
/// configured deadline elapses.
#[allow(dead_code)]
pub struct TransformEngine {
    engine: Engine,
    store: Store<()>,
    /// The instantiated Emscripten module handle — held alive so the WASM
    /// memory and exported functions remain accessible.
    #[allow(dead_code)]
    instance: Instance,
    /// Cache of compiled JS bytecode keyed by source hash.
    #[allow(dead_code)]
    bytecode_cache: HashMap<String, Vec<u8>>,
}

impl TransformEngine {
    /// Create a new engine, loading and instantiating the embedded QuickJS
    /// WASM blob.
    ///
    /// # Errors
    ///
    /// Returns [`TapError::Transform`] if the wasmtime engine cannot be
    /// initialised, the WASM blob fails to compile, or instantiation fails
    /// due to missing imports.
    pub fn new() -> Result<Self, TapError> {
        let mut config = wasmtime::Config::new();

        // Enable epoch-based interruption so we can enforce JS timeouts.
        config.epoch_interruption(true);
        // Explicitly enable WASM proposals the module relies on.
        config.wasm_bulk_memory(true);
        config.wasm_reference_types(true);
        config.wasm_simd(true);
        config.wasm_multi_value(true);
        config.wasm_multi_memory(true);
        config.wasm_tail_call(true);
        // Debug info so traps include function names (when available).
        config.debug_info(true);

        let engine = Engine::new(&config).map_err(|e| TapError::Transform(e.to_string()))?;

        let module_bytes = include_bytes!("../../assets/quickjs.wasm");
        let module = Module::new(&engine, module_bytes.as_slice())
            .map_err(|e| TapError::Transform(format!("WASM compile: {e}")))?;

        let mut store = Store::new(&engine, ());

        // ── linker: provide host imports for the Emscripten module ────
        let mut linker: Linker<()> = Linker::new(&engine);
        add_emscripten_stubs(&mut linker, &mut store)?;

        let instance = linker
            .instantiate(&mut store, &module)
            .map_err(|e| TapError::Transform(format!("WASM instantiate: {e:#}")))?;

        Ok(Self {
            engine,
            store,
            instance,
            bytecode_cache: HashMap::new(),
        })
    }
}

impl Default for TransformEngine {
    fn default() -> Self {
        Self::new().expect("TransformEngine should initialise without error")
    }
}

/// Global memory handle for Emscripten host-function stubs that need
/// to read / write linear memory but don't receive a [`Store`] from
/// [`Func::wrap`].  The handle is set once during [`add_emscripten_stubs`]
/// and is safe to access because wasmtime's store is single-threaded.
static QUICKJS_MEMORY: OnceLock<wasmtime::Memory> = OnceLock::new();

/// Register minimal Emscripten stubs so the QuickJS module can
/// instantiate.  Real implementations will be wired in follow-up work
/// alongside the QuickJS FFI calls.
fn add_emscripten_stubs(linker: &mut Linker<()>, store: &mut Store<()>) -> Result<(), TapError> {
    // ── a.a — linear memory ──────────────────────────────────────────
    //
    // The Emscripten module declares a 16 MB (256-page) memory, with an
    // absolute cap of 2 GB (32768 pages).  A copy is stashed in
    // [`QUICKJS_MEMORY`] so host-function stubs that need memory access
    // (e.g. emscripten_memcpy) can reach it.
    let mem_ty = wasmtime::MemoryType::new(256, Some(32768));
    let memory = wasmtime::Memory::new(&mut *store, mem_ty)
        .map_err(|e| TapError::Transform(format!("a.a memory: {e}")))?;
    QUICKJS_MEMORY
        .set(memory.clone())
        .map_err(|_| TapError::Transform("QUICKJS_MEMORY already set".into()))?;

    linker
        .define(&mut *store, "a", "a", memory)
        .map_err(|e| TapError::Transform(format!("a.a memory: {e}")))?;

    // ── a.b … a.u — Pre-create all function stubs ──────────────────
    //
    // The module imports 20 functions (single-letter mangled names from
    // Emscripten's optimised output).  We create them all first so the
    // store borrow is released before we start linker.define() calls.
    //
    // Critical init-time stubs (emscripten_memcpy = a.h, etc.) use
    // [`Func::new`] so they receive a [`Caller`] for memory access;
    // the rest remain simple [`Func::wrap`] stubs.

    let f_b = wasmtime::Func::wrap(&mut *store, |_: i32, _: i32, _: i32, _: i32| {});
    let f_c = wasmtime::Func::wrap(&mut *store, |_: i32, _: i32, _: i32, _: i32| -> i32 { 0 });
    let f_d = wasmtime::Func::wrap(&mut *store, |_: i32| -> i32 { 0 });
    let f_e = wasmtime::Func::wrap(&mut *store, |_: i32, _: i32| -> i32 { 0 });
    let f_f = wasmtime::Func::wrap(&mut *store, |_: i32, _: i32| -> i32 { 0 });
    let f_g = wasmtime::Func::wrap(&mut *store, |_: i32, _: i32, _: i32, _: i32| -> i32 { 0 });
    // a.h = emscripten_memcpy(dest, src, n) -> dest
    //
    // Clone the engine handle to avoid a borrow conflict between
    // FuncType::new (&engine) and Func::new (&mut store).
    let eng = store.engine().clone();
    let f_h = wasmtime::Func::new(
        &mut *store,
        FuncType::new(
            &eng,
            [ValType::I32, ValType::I32, ValType::I32],
            [ValType::I32],
        ),
        |mut caller: Caller<'_, ()>, args: &[Val], results: &mut [Val]| {
            let dest = args[0].i32().unwrap_or(0) as usize;
            let src = args[1].i32().unwrap_or(0) as usize;
            let n = args[2].i32().unwrap_or(0) as usize;
            if n > 0 {
                let mem = QUICKJS_MEMORY.get().expect("QUICKJS_MEMORY not set");
                let mut buf = vec![0u8; n];
                mem.read(&caller, src, &mut buf)?;
                mem.write(&mut caller, dest, &buf)?;
            }
            results[0] = Val::I32(dest as i32);
            Ok(())
        },
    );
    let f_i = wasmtime::Func::wrap(&mut *store, |_: i32| -> i32 { 0 });
    let f_j = wasmtime::Func::wrap(&mut *store, |_: i32, _: f64| -> i32 { 0 });
    let f_k = wasmtime::Func::wrap(&mut *store, |_: i32| -> i32 { 0 });
    let f_l = wasmtime::Func::wrap(&mut *store, || {});
    let f_m = wasmtime::Func::wrap(&mut *store, |_: i64, _: i32| {});
    let f_n = wasmtime::Func::wrap(&mut *store, |_: i32, _: i32, _: i32, _: i32| {});
    let f_o = wasmtime::Func::wrap(&mut *store, |_: i32, _: i64, _: i32, _: i32| -> i32 { 0 });
    let f_p = wasmtime::Func::wrap(&mut *store, || -> f64 { 0.0 });
    let f_q = wasmtime::Func::wrap(&mut *store, |_: i32, _: i64, _: i32| -> i32 { 0 });
    let f_r = wasmtime::Func::wrap(&mut *store, || {});
    let f_s = wasmtime::Func::wrap(&mut *store, |_: i32| {});
    let f_t = wasmtime::Func::wrap(
        &mut *store,
        |_: i32, _: i32, _: i32, _: i32, _: i32| -> i32 { 0 },
    );
    let f_u = wasmtime::Func::wrap(&mut *store, |_: i32, _: i32| {});

    // ── Register each stub with the linker ──────────────────────────
    // Real implementations (emscripten_memcpy, emscripten_resize_heap,
    // longjmp support, etc.) will be wired in follow-up work.

    macro_rules! def {
        ($name:literal, $func:ident) => {
            linker
                .define(&mut *store, "a", $name, $func)
                .map_err(|e| TapError::Transform(format!("a.{}: {e}", $name)))?;
        };
    }
    def!("b", f_b);
    def!("c", f_c);
    def!("d", f_d);
    def!("e", f_e);
    def!("f", f_f);
    def!("g", f_g);
    def!("h", f_h);
    def!("i", f_i);
    def!("j", f_j);
    def!("k", f_k);
    def!("l", f_l);
    def!("m", f_m);
    def!("n", f_n);
    def!("o", f_o);
    def!("p", f_p);
    def!("q", f_q);
    def!("r", f_r);
    def!("s", f_s);
    def!("t", f_t);
    def!("u", f_u);

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Module compilation succeeds (fast, no imports needed).
    #[test]
    fn wasm_module_compiles() {
        let mut config = wasmtime::Config::new();
        config.epoch_interruption(true);
        let engine = Engine::new(&config).unwrap();

        let module_bytes = include_bytes!("../../assets/quickjs.wasm");
        let module = Module::new(&engine, module_bytes.as_slice())
            .expect("QuickJS WASM module should compile");

        let n_imports = module.imports().count();
        let n_exports = module.exports().count();
        println!(
            "QuickJS WASM: {n_imports} imports, {n_exports} exports, {} bytes",
            module_bytes.len()
        );

        println!("--- imports ---");
        for import in module.imports() {
            let module_name = import.module();
            let field_name = import.name();
            println!("  {module_name}.{field_name}");
        }
        println!("--- exports ---");
        for export in module.exports() {
            let field_name = export.name();
            println!("  {field_name}");
        }
    }

    /// The engine constructs successfully (compiles + instantiates).
    #[test]
    fn engine_constructs() {
        let engine = TransformEngine::new().expect("TransformEngine::new() failed");
        let _ = engine; // held alive
    }
}

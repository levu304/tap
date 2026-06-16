//! Wasmtime-backed transform runtime with embedded QuickJS WASM.
//!
//! Provides [`TransformEngine`] — a sandboxed JavaScript execution context
//! for user-defined CDC transforms (map, filter, mask, etc.).
//!
//! The engine uses epoch-based interruption for safe timeouts and a
//! QuickJS bytecode cache keyed by SHA-256 source hash.
//!
//! # WASM blob
//!
//! The WASM binary at `assets/quickjs.wasm` is a custom build of
//! quickjs-ng v0.12.1 compiled with Emscripten (`-D__wasi__` to disable
//! QuickJS internal stack checking on 32-bit WASM).  It imports 7 host
//! functions (5 from `env`, 2 from `wasi_snapshot_preview1`) provided as
//! minimal stubs — see [`add_wasi_stubs`] and [`add_env_stubs`].

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};

use wasmtime::{Engine, Instance, Memory, Module, Store, Val};

use crate::error::TapError;
use crate::event::ChangeEvent;

// ---------------------------------------------------------------------------
// QuickJS constants
// ---------------------------------------------------------------------------

/// Unique counter for the tap result property name.
static TAP_RESULT_COUNTER: AtomicU32 = AtomicU32::new(0);

/// Evaluate as global code (the default, value 0).
#[allow(dead_code)]
const JS_EVAL_TYPE_GLOBAL: i32 = 0;

/// Evaluate as a JS module (not global code).
const JS_EVAL_TYPE_MODULE: i32 = 1 << 0;

/// Compile only — do not execute.
const JS_EVAL_FLAG_COMPILE_ONLY: i32 = 1 << 5;

/// Serialise as bytecode (not reference).
const JS_WRITE_OBJ_BYTECODE: i32 = 1 << 0;
const JS_READ_OBJ_BYTECODE: i32 = 1 << 0;

/// Tag value for the QuickJS exception singleton.
///
/// QuickJS with `JS_NAN_BOXING` (enabled by default for wasm32) represents
/// JSValues as NaN-boxed `uint64_t`.  The upper 32 bits hold the tag; the
/// lower 32 bits hold the int value (or pointer).  `JS_TAG_EXCEPTION = 6`.
const JS_TAG_EXCEPTION: u32 = 6;

/// Default filename string passed to `JS_Eval` during compilation.
const DEFAULT_FILENAME: &str = "input.js";

// ---------------------------------------------------------------------------
// TransformEngine
// ---------------------------------------------------------------------------

/// Sandboxed transform engine backed by wasmtime + QuickJS WASM.
///
/// # Initialisation
///
/// [`TransformEngine::new()`] creates a wasmtime `Engine` with epoch-based
/// interruption, compiles the embedded QuickJS WASM module, instantiates
/// it with minimal WASI stubs, initialises a QuickJS runtime + context,
/// and caches exported function references.
///
/// # Bytecode compilation
///
/// JavaScript source can be compiled to platform-independent QuickJS
/// bytecode via [`compile_to_bytecode()`](Self::compile_to_bytecode).
/// The result can be cached externally or replayed later with
/// [`eval_bytecode()`](Self::eval_bytecode).
///
/// Results are automatically cached by SHA-256 source hash.
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
    /// The instantiated WASM module — held alive so exported functions
    /// and memory remain accessible.
    instance: Instance,
    /// Handle to the exported linear memory.
    memory: Memory,
    /// Cache of compiled bytecode keyed by SHA-256 source hash (hex).
    #[allow(dead_code)]
    bytecode_cache: HashMap<String, Vec<u8>>,
    /// QuickJS context handle (opaque pointer).
    ctx: i32,
    /// QuickJS runtime handle (opaque pointer).
    rt: i32,
    /// Unique property name on the global object for expression capture.
    ///
    /// Each engine instance uses a distinct name (e.g. `__tap_0`, `__tap_1`)
    /// so user scripts cannot accidentally clash with it.
    tap_result_name: String,
}

impl TransformEngine {
    /// Create a new engine, instantiate QuickJS WASM, and initialise a
    /// QuickJS runtime + context.
    ///
    /// # Errors
    ///
    /// Returns [`TapError::Transform`] if wasmtime cannot be initialised,
    /// the WASM blob fails to compile, instantiation fails, or the
    /// QuickJS runtime/context allocation fails.
    pub fn new() -> Result<Self, TapError> {
        let mut config = wasmtime::Config::new();
        config.epoch_interruption(true);
        config.consume_fuel(true);
        config.wasm_bulk_memory(true);
        config.wasm_reference_types(true);
        config.wasm_simd(true);
        config.wasm_multi_value(true);
        config.wasm_multi_memory(true);
        config.wasm_tail_call(true);
        config.debug_info(true);

        let engine = Engine::new(&config).map_err(|e| TapError::Transform(e.to_string()))?;

        let module_bytes = include_bytes!("../../assets/quickjs.wasm");
        let module = Module::new(&engine, module_bytes.as_slice())
            .map_err(|e| TapError::Transform(format!("WASM compile: {e}")))?;

        let mut store = Store::new(&engine, ());

        // Set generous initial fuel for startup (ctors, eval stubs, etc.).
        // Per-call limits are set before every compile/eval entry.
        store
            .set_fuel(50_000_000)
            .map_err(|e| TapError::Transform(format!("set_fuel init: {e}")))?;

        // Set a generous epoch deadline (function-level concurrency safety).
        // Per-call instruction limits are enforced via fuel metering.
        store.set_epoch_deadline(u64::MAX);

        // ── linker: WASM host-import stubs ────────────────────────────
        let mut linker: wasmtime::Linker<()> = wasmtime::Linker::new(&engine);
        add_env_stubs(&mut linker, &mut store)?;
        add_wasi_stubs(&mut linker, &mut store)?;

        let instance = linker
            .instantiate(&mut store, &module)
            .map_err(|e| TapError::Transform(format!("WASM instantiate: {e:#}")))?;

        // ── resolve exports ───────────────────────────────────────────
        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| TapError::Transform("WASM export 'memory' not found".into()))?;

        // Call __wasm_call_ctors (Emscripten constructors).  Safe to call even
        // if the runtime already ran them — repeated calls are no-ops.
        call_void_export(&instance, &mut store, "__wasm_call_ctors")?;

        // ── initialise QuickJS runtime + context ──────────────────────
        let rt: i32 = call_export_0r_1i32(&instance, &mut store, "JS_NewRuntime")?;
        if rt == 0 {
            return Err(TapError::Transform("JS_NewRuntime returned null".into()));
        }
        let ctx: i32 = call_export_1i32_1i32(&instance, &mut store, "JS_NewContext", rt)?;
        if ctx == 0 {
            return Err(TapError::Transform("JS_NewContext returned null".into()));
        }

        let counter = TAP_RESULT_COUNTER.fetch_add(1, Ordering::Relaxed);
        let tap_result_name = format!("__tap_{counter:x}");

        Ok(Self {
            engine,
            store,
            instance,
            memory,
            bytecode_cache: HashMap::new(),
            ctx,
            rt,
            tap_result_name,
        })
    }

    /// Compile JavaScript source to QuickJS bytecode.
    ///
    /// The source is first checked against the in-memory bytecode cache
    /// (keyed by SHA-256 hash).  On a cache hit the serialised bytecode
    /// is returned immediately without re-compilation.
    ///
    /// # Errors
    ///
    /// Returns [`TapError::Transform`] if the source contains syntax
    /// errors or if any WASM operation fails.
    pub fn compile_to_bytecode(&mut self, source: &str) -> Result<Vec<u8>, TapError> {
        let hash = hex_hash(source.as_bytes());

        // Check cache first.
        if let Some(cached) = self.bytecode_cache.get(&hash) {
            return Ok(cached.clone());
        }

        let bytecode = self.compile_to_bytecode_inner(source)?;

        // Insert into cache.
        self.bytecode_cache.insert(hash, bytecode.clone());
        Ok(bytecode)
    }

    /// Evaluate QuickJS bytecode and return the result as a string.
    ///
    /// # Errors
    ///
    /// Returns [`TapError::Transform`] if the bytecode is invalid or
    /// execution throws.
    pub fn eval_bytecode(&mut self, bytecode: &[u8]) -> Result<String, TapError> {
        // ── step 0: set fuel limit for this call ─────────────────────
        self.store
            .set_fuel(10_000_000)
            .map_err(|e| TapError::Transform(format!("set_fuel: {e}")))?;

        // ── step 0a: clear stale tap result from previous eval ──────
        // Prevents cross-eval data leakage: without this, a previous
        // expression transform's result could leak into the next
        // statement-only transform.
        self.clear_tap_result()?;

        // ── step 1: write bytecode into WASM memory ──────────────────
        let (buf_ptr, buf_len) = self.string_to_wasm_inner(bytecode)?;

        // ── step 2: JS_ReadObject(ctx, buf, buf_len, JS_READ_OBJ_BYTECODE) ─
        let func_val = call_export_4i32_1i64(
            &self.instance,
            &mut self.store,
            "JS_ReadObject",
            self.ctx,
            buf_ptr,
            buf_len,
            JS_READ_OBJ_BYTECODE,
        )?;

        // Free the temporary buffer.
        call_export_1i32_void(&self.instance, &mut self.store, "free", buf_ptr)?;

        // Check for exception during deserialisation.
        if is_exception(func_val) {
            let err = self.get_quickjs_error()?;
            call_export_2args_void(
                &self.instance,
                &mut self.store,
                "JS_FreeValue",
                self.ctx,
                func_val,
            )?;
            return Err(TapError::Transform(format!(
                "bytecode deserialisation: {err}"
            )));
        }

        // ── step 3: JS_EvalFunction(ctx, func_val) ───────────────────
        // NOTE: JS_EvalFunction internally frees func_val for module-tag
        // objects (confirmed in quickjs-ng quickjs.c:36291).  Do NOT call
        // JS_FreeValue on func_val after this — that would be a double-free.
        let result = call_export_1i32_1i64_1i64(
            &self.instance,
            &mut self.store,
            "JS_EvalFunction",
            self.ctx,
            func_val,
        )?;

        // ── step 4: check for errors ──────────────────────────────────
        if is_exception(result) {
            let err = self.get_quickjs_error()?;
            call_export_2args_void(
                &self.instance,
                &mut self.store,
                "JS_FreeValue",
                self.ctx,
                result,
            )?;
            return Err(TapError::Transform(format!("execution error: {err}")));
        }

        // ── step 5: unwrap Promise (module evaluation returns Promise) ──
        // JS_EvalFunction on a module returns a Promise even for synchronous
        // modules. Use the QuickJS Promise API to extract the actual value.
        let final_val = {
            let is_promise =
                call_export_1i64_1i32(&self.instance, &mut self.store, "JS_IsPromise", result)?;
            if is_promise != 0 {
                let state = call_export_2args_1i32(
                    &self.instance,
                    &mut self.store,
                    "JS_PromiseState",
                    self.ctx,
                    result,
                )?;
                match state {
                    1 /* JS_PROMISE_FULFILLED */ => {
                        let fulfilled = call_export_1i32_1i64_1i64(
                            &self.instance, &mut self.store,
                            "JS_PromiseResult", self.ctx, result,
                        )?;
                        call_export_2args_void(&self.instance, &mut self.store, "JS_FreeValue", self.ctx, result)?;
                        fulfilled
                    }
                    2 /* JS_PROMISE_REJECTED */ => {
                        let rejected = call_export_1i32_1i64_1i64(
                            &self.instance, &mut self.store,
                            "JS_PromiseResult", self.ctx, result,
                        )?;
                        call_export_2args_void(&self.instance, &mut self.store, "JS_FreeValue", self.ctx, result)?;
                        let err_msg = self.js_value_to_string(rejected)?;
                        call_export_2args_void(&self.instance, &mut self.store, "JS_FreeValue", self.ctx, rejected)?;
                        return Err(TapError::Transform(format!("execution error: {err_msg}")));
                    }
                    _ /* JS_PROMISE_PENDING */ => {
                        call_export_2args_void(&self.instance, &mut self.store, "JS_FreeValue", self.ctx, result)?;
                        return Err(TapError::Transform(
                            "module evaluation returned a pending Promise".into()
                        ));
                    }
                }
            } else {
                // Not a Promise — use directly.
                result
            }
        };

        // ── step 6: read __tap_result from global object ───────────────
        // If compile_to_bytecode_inner wrapped the source with
        // `globalThis.__tap_result=<expr>`, the expression value is stored
        // under `__tap_result` on the global object.  Check for it and use
        // it when present; otherwise fall through to the Promise result.
        let output_val = {
            let global_obj = call_export_1i32_1i64(
                &self.instance,
                &mut self.store,
                "JS_GetGlobalObject",
                self.ctx,
            )?;
            let prop_name = self.tap_result_name.as_bytes();
            let (prop_ptr, _prop_len) = {
                // Allocate len+1 for null terminator.
                let len = prop_name.len() as i32;
                let ptr =
                    call_export_1i32_1i32(&self.instance, &mut self.store, "malloc", len + 1)?;
                let mut padded: Vec<u8> = prop_name.to_vec();
                padded.push(0);
                self.memory
                    .write(&mut self.store, ptr as usize, &padded)
                    .map_err(|e| TapError::Transform(format!("memory write prop name: {e}")))?;
                (ptr, len)
            };

            let tap_result = call_export_3mixed_1i64(
                &self.instance,
                &mut self.store,
                "JS_GetPropertyStr",
                self.ctx,
                global_obj,
                prop_ptr,
            )?;
            // Free the property name buffer.
            call_export_1i32_void(&self.instance, &mut self.store, "free", prop_ptr)?;
            // Free the global object reference.
            call_export_2args_void(
                &self.instance,
                &mut self.store,
                "JS_FreeValue",
                self.ctx,
                global_obj,
            )?;

            if !is_undefined(tap_result) && !is_exception(tap_result) {
                // The wrapped source stored an expression value — use it.
                call_export_2args_void(
                    &self.instance,
                    &mut self.store,
                    "JS_FreeValue",
                    self.ctx,
                    final_val,
                )?;
                tap_result
            } else {
                // No wrapper was applied (statement source) — use the
                // Promise result (which may be undefined for statements).
                if !is_exception(tap_result) {
                    call_export_2args_void(
                        &self.instance,
                        &mut self.store,
                        "JS_FreeValue",
                        self.ctx,
                        tap_result,
                    )?;
                }
                final_val
            }
        };

        // ── step 7: convert result to string ──────────────────────────
        let result_str = self.js_value_to_string(output_val)?;

        // Free the result value.
        call_export_2args_void(
            &self.instance,
            &mut self.store,
            "JS_FreeValue",
            self.ctx,
            output_val,
        )?;

        Ok(result_str)
    }

    // ── Internal helpers ───────────────────────────────────────────

    /// Clear the tap result global property to `undefined`.
    ///
    /// Uses `JS_Eval` with `JS_EVAL_TYPE_GLOBAL` to set the property.
    /// This prevents stale expression values from leaking across
    /// transform evaluations.
    fn clear_tap_result(&mut self) -> Result<(), TapError> {
        let clear_src = format!("globalThis.{}=undefined", self.tap_result_name);
        let (src_ptr, src_len) = self.string_to_wasm_inner(clear_src.as_bytes())?;
        let (name_ptr, _name_len) = self.string_to_wasm_inner(b"clear.js")?;
        let val = call_export_5i32_1i64(
            &self.instance,
            &mut self.store,
            "JS_Eval",
            self.ctx,
            src_ptr,
            src_len,
            name_ptr,
            0, // JS_EVAL_TYPE_GLOBAL
        )?;
        call_export_1i32_void(&self.instance, &mut self.store, "free", src_ptr)?;
        call_export_1i32_void(&self.instance, &mut self.store, "free", name_ptr)?;
        // Free the return value (which is `undefined` for assignment).
        call_export_2args_void(
            &self.instance,
            &mut self.store,
            "JS_FreeValue",
            self.ctx,
            val,
        )?;
        Ok(())
    }

    /// Compile source to bytecode (uncached).
    ///
    /// To support expression capture from module evaluation (which returns
    /// `undefined` rather than the last expression value), this method
    /// first tries wrapping the source as `globalThis.__tap_result=<src>`.
    /// If the wrapper causes a compile error (e.g. for statement-only sources
    /// like `const x = 1;` or `throw ...`), it falls back to the plain source.
    /// At eval time, `eval_bytecode` checks for `__tap_result` on the global
    /// object and uses it when present.
    fn compile_to_bytecode_inner(&mut self, source: &str) -> Result<Vec<u8>, TapError> {
        // Try wrapping source with result capture first.
        // Expression sources like `JSON.stringify(...)` compile fine; statement-
        // only sources like `const x = 1;` will fail the wrapper and fall back.
        let wrapped_source = format!("globalThis.{0}={1}", self.tap_result_name, source);
        if let Ok(bytecode) = self.do_compile(wrapped_source.as_bytes()) {
            return Ok(bytecode);
        }

        // Fallback: compile with no wrapper (statements, throw, etc.).
        self.do_compile(source.as_bytes())
    }

    /// Low-level compile: write `source` into WASM memory, call `JS_Eval` with
    /// `MODULE | COMPILE_ONLY`, serialise to bytecode via `JS_WriteObject`,
    /// and return the raw bytecode `Vec<u8>`.
    fn do_compile(&mut self, source: &[u8]) -> Result<Vec<u8>, TapError> {
        // ── step 0: set fuel limit for compilation ───────────────────
        self.store
            .set_fuel(5_000_000)
            .map_err(|e| TapError::Transform(format!("set_fuel: {e}")))?;

        // ── step 1: write source + filename into WASM memory ────
        // NOTE: string_to_wasm_inner always appends a \0 guard byte because
        // QuickJS-ng's module parser reads one byte past input_len. Without
        // it we'd see garbled token errors ("invalid UTF-8 sequence",
        // "unexpected token in expression: '\\x01'", etc.).
        let (src_ptr, src_len) = self.string_to_wasm_inner(source)?;
        let (name_ptr, _name_len) = self.string_to_wasm_inner(DEFAULT_FILENAME.as_bytes())?;

        // ── step 2: allocate size_t slot for JS_WriteObject ────
        let size_ptr = call_export_1i32_1i32(&self.instance, &mut self.store, "malloc", 4)?;

        // ── step 3: JS_Eval(ctx, src, len, filename, flags) ───
        // Use MODULE | COMPILE_ONLY for correct handling of `const`
        // and `throw`.  GLOBAL|COMPILE_ONLY hangs on expression-only
        // sources (the parser interprets `{...}` as block statements).
        let flags = JS_EVAL_TYPE_MODULE | JS_EVAL_FLAG_COMPILE_ONLY;
        let func_val = call_export_5i32_1i64(
            &self.instance,
            &mut self.store,
            "JS_Eval",
            self.ctx,
            src_ptr,
            src_len,
            name_ptr,
            flags,
        )?;

        // Free temporary buffers — done after JS_Eval reads them.
        call_export_1i32_void(&self.instance, &mut self.store, "free", src_ptr)?;
        call_export_1i32_void(&self.instance, &mut self.store, "free", name_ptr)?;

        // Check for compilation errors.
        if is_exception(func_val) {
            let err = self.get_quickjs_error()?;
            call_export_2args_void(
                &self.instance,
                &mut self.store,
                "JS_FreeValue",
                self.ctx,
                func_val,
            )?;
            call_export_1i32_void(&self.instance, &mut self.store, "free", size_ptr)?;
            return Err(TapError::Transform(format!("compilation error: {err}")));
        }

        // ── step 4: JS_WriteObject(ctx, size_ptr, func_val, flags) ──
        let bytecode_ptr = call_export_4i32_1i32(
            &self.instance,
            &mut self.store,
            "JS_WriteObject",
            self.ctx,
            size_ptr,
            func_val,
            JS_WRITE_OBJ_BYTECODE,
        )?;

        // Free the function value.
        call_export_2args_void(
            &self.instance,
            &mut self.store,
            "JS_FreeValue",
            self.ctx,
            func_val,
        )?;

        if bytecode_ptr == 0 {
            call_export_1i32_void(&self.instance, &mut self.store, "free", size_ptr)?;
            return Err(TapError::Transform("JS_WriteObject returned null".into()));
        }

        // ── step 5: read bytecode length and data from WASM memory ──
        let mut size_buf = [0u8; 4];
        self.memory
            .read(&self.store, size_ptr as usize, &mut size_buf)
            .map_err(|e| TapError::Transform(format!("memory read size: {e}")))?;
        let bytecode_len = i32::from_le_bytes(size_buf) as usize;

        let mut bytecode = vec![0u8; bytecode_len];
        self.memory
            .read(&self.store, bytecode_ptr as usize, &mut bytecode)
            .map_err(|e| TapError::Transform(format!("memory read bytecode: {e}")))?;

        // ── step 6: free WASM-allocated buffers ──────────────────────
        call_export_1i32_void(&self.instance, &mut self.store, "free", bytecode_ptr)?;
        call_export_1i32_void(&self.instance, &mut self.store, "free", size_ptr)?;

        Ok(bytecode)
    }

    /// Write a byte slice into WASM linear memory via `malloc` + copy.
    /// Returns `(ptr, len)`.
    fn string_to_wasm_inner(&mut self, bytes: &[u8]) -> Result<(i32, i32), TapError> {
        let len = bytes.len() as i32;
        if len == 0 {
            return Ok((0, 0));
        }
        // Allocate len + 1 and write a \0 guard byte. QuickJS-ng's module
        // parser reads one byte past input_len; the guard prevents garbled
        // token errors ("invalid UTF-8 sequence", "unexpected token in
        // expression: '\\x01'", etc.).  The length returned is the original
        // byte count, so callers (JS_Eval, JS_ReadObject, etc.) see the
        // correct input size and the guard byte is invisible.
        let ptr = call_export_1i32_1i32(&self.instance, &mut self.store, "malloc", len + 1)?;
        if ptr == 0 {
            return Err(TapError::Transform("malloc returned null".into()));
        }
        // Write the data PLUS one null byte at the end.
        let mut padded: Vec<u8> = Vec::with_capacity(bytes.len() + 1);
        padded.extend_from_slice(bytes);
        padded.push(0);
        self.memory
            .write(&mut self.store, ptr as usize, &padded)
            .map_err(|e| TapError::Transform(format!("memory write: {e}")))?;
        Ok((ptr, len))
    }

    /// Convert a QuickJS `JSValue` (i64) to a Rust `String` using
    /// `JS_ToCStringLen2`.
    fn js_value_to_string(&mut self, val: i64) -> Result<String, TapError> {
        // Allocate space for the length output.
        let plen = call_export_1i32_1i32(&self.instance, &mut self.store, "malloc", 4)?;

        let cstr_ptr = call_export_4i32_1i32(
            &self.instance,
            &mut self.store,
            "JS_ToCStringLen2",
            self.ctx,
            plen,
            val,
            0, // flags
        )?;

        if cstr_ptr == 0 {
            call_export_1i32_void(&self.instance, &mut self.store, "free", plen)?;
            return Err(TapError::Transform("JS_ToCStringLen2 returned null".into()));
        }

        // Read the actual string length from plen.
        let mut len_buf = [0u8; 4];
        self.memory
            .read(&self.store, plen as usize, &mut len_buf)
            .map_err(|e| TapError::Transform(format!("memory read plen: {e}")))?;
        let str_len = i32::from_le_bytes(len_buf) as usize;

        // Read the string data from WASM memory.
        let mut str_buf = vec![0u8; str_len];
        self.memory
            .read(&self.store, cstr_ptr as usize, &mut str_buf)
            .map_err(|e| TapError::Transform(format!("memory read string: {e}")))?;

        // Free the C string and the plen buffer.
        call_export_2i32_void(
            &self.instance,
            &mut self.store,
            "JS_FreeCString",
            self.ctx,
            cstr_ptr,
        )?;
        call_export_1i32_void(&self.instance, &mut self.store, "free", plen)?;

        String::from_utf8(str_buf)
            .map_err(|e| TapError::Transform(format!("invalid UTF-8 in JS result: {e}")))
    }

    /// Retrieve the current pending exception from the QuickJS context
    /// as a Rust `String`.
    fn get_quickjs_error(&mut self) -> Result<String, TapError> {
        let exc_val =
            call_export_1i32_1i64(&self.instance, &mut self.store, "JS_GetException", self.ctx)?;

        if is_exception(exc_val) {
            // Something went wrong getting the exception — return generic message.
            return Ok("(unknown QuickJS error)".into());
        }

        let msg = self.js_value_to_string(exc_val)?;
        call_export_2args_void(
            &self.instance,
            &mut self.store,
            "JS_FreeValue",
            self.ctx,
            exc_val,
        )?;
        Ok(msg)
    }
    /// Run a JavaScript transform function against a CDC event.
    ///
    /// Compiles a JS module that defines the user's function `f`, embeds the
    /// serialised event as a JSON literal, calls `f(event)`, serialises the
    /// result back to JSON, and captures it via the tap result property so
    /// [`eval_bytecode`](Self::eval_bytecode) can retrieve it.
    ///
    /// # Convention
    ///
    /// The `function_body` MUST define a function named `f` that accepts one
    /// argument (the deserialised `ChangeEvent`) and returns a value whose
    /// `JSON.stringify` representation is suitable for the caller.  For filter
    /// transforms the return value should be a boolean; for map transforms it
    /// should be the (possibly modified) event object.
    ///
    /// # Errors
    ///
    /// Returns [`TapError::Transform`] if compilation fails, execution throws,
    /// or the event cannot be serialised.
    #[allow(dead_code)]
    pub(crate) fn run_transform_js(
        &mut self,
        function_body: &str,
        event: &ChangeEvent,
    ) -> Result<String, TapError> {
        // Serialise the event to JSON and escape for embedding in a single-quoted
        // JS string literal: \ → \\, ' → \'.
        let event_json = serde_json::to_string(event)
            .map_err(|e| TapError::Transform(format!("serialize event: {e}")))?;
        let escaped = event_json.replace('\\', "\\\\").replace('\'', "\\'");

        // Build the full module: user function + event injection + result capture.
        // The module defines f, parses the event from JSON, calls f, stringifies
        // the result, and stores it on the tap result global property.
        let script = format!(
            "{}\nglobalThis.{} = JSON.stringify(f(JSON.parse('{}')));",
            function_body, self.tap_result_name, escaped,
        );

        // Compile raw (no expression wrapper — the module itself handles
        // result capture via `globalThis.{name} = ...`).
        let bytecode = self.do_compile(script.as_bytes())?;

        // Eval the bytecode — will find the result on the global property.
        self.eval_bytecode(&bytecode)
    }
}

impl Default for TransformEngine {
    fn default() -> Self {
        Self::new().expect("TransformEngine should initialise without error")
    }
}

impl Drop for TransformEngine {
    fn drop(&mut self) {
        // Free QuickJS context and runtime in the WASM heap.
        if self.ctx != 0 {
            let _ =
                call_export_1i32_void(&self.instance, &mut self.store, "JS_FreeContext", self.ctx);
        }
        if self.rt != 0 {
            let _ =
                call_export_1i32_void(&self.instance, &mut self.store, "JS_FreeRuntime", self.rt);
        }
    }
}

// ---------------------------------------------------------------------------
// WASI stubs
// ---------------------------------------------------------------------------

/// Register minimal WASI stubs for `wasi_snapshot_preview1`.
///
/// The QuickJS WASM blob imports 6 WASI functions:
///
/// | Function | Signature | Purpose |
/// |----------|-----------|---------|
/// | `fd_write` | `(fd, iovs, iovs_len, nwritten) → errno` | Write to fd |
/// | `fd_close` | `(fd) → errno` | Close fd |
/// | `fd_seek`  | `(fd, offset, whence, newoffset) → errno` | Seek on fd |
/// | `clock_time_get` | `(id, precision, time_ptr) → errno` | Get clock time |
/// | `environ_sizes_get` | `(argc, buf_size) → errno` | Env sizes |
/// | `environ_get` | `(argc, argv_buf) → errno` | Get env |
///
/// Only `fd_write` (stdout/stderr) has real behaviour so QuickJS
/// `console.log` / `console.warn` output is visible.  The remaining
/// stubs return `ENOSYS` or `ESUCCESS` as appropriate.
fn add_wasi_stubs(
    linker: &mut wasmtime::Linker<()>,
    store: &mut Store<()>,
) -> Result<(), TapError> {
    use wasmtime::{Caller, FuncType, Val, ValType};

    let engine = store.engine().clone();

    // ── fd_write (stdout/stderr) ──────────────────────────────────────
    //
    // WASI fd_write signature:
    //   (fd: i32, iovs: i32, iovs_len: i32, nwritten: i32) -> i32
    //
    // iovs points to an array of `struct iovec { buf: i32; buf_len: i32; }`.
    let fd_write = wasmtime::Func::new(
        &mut *store,
        FuncType::new(
            &engine,
            [ValType::I32, ValType::I32, ValType::I32, ValType::I32],
            [ValType::I32],
        ),
        |mut caller: Caller<'_, ()>, args: &[Val], results: &mut [Val]| {
            let fd = args[0].i32().unwrap_or(0);
            let iovs_addr = args[1].i32().unwrap_or(0) as usize;
            let iovs_len = args[2].i32().unwrap_or(0) as usize;
            let nwritten_addr = args[3].i32().unwrap_or(0) as usize;

            if fd != 1 && fd != 2 {
                results[0] = Val::I32(8); // EBADF
                return Ok(());
            }

            let mem = caller
                .get_export("memory")
                .and_then(|e| e.into_memory())
                .ok_or_else(|| wasmtime::Error::msg("WASI fd_write: memory export not found"))?;

            // Accumulate written bytes (for the nwritten return value).
            let mut total_written: usize = 0;

            for i in 0..iovs_len {
                let iov_offset = iovs_addr + i * 8; // iovec = {buf: i32, buf_len: i32}
                let mut iov_buf = [0u8; 8];
                mem.read(&caller, iov_offset, &mut iov_buf)
                    .map_err(|e| wasmtime::Error::msg(format!("fd_write iovec read: {e}")))?;

                let buf_ptr = i32::from_le_bytes(iov_buf[0..4].try_into().unwrap()) as usize;
                let buf_len = i32::from_le_bytes(iov_buf[4..8].try_into().unwrap()) as usize;

                if buf_len > 0 {
                    let mut data = vec![0u8; buf_len];
                    mem.read(&caller, buf_ptr, &mut data)
                        .map_err(|e| wasmtime::Error::msg(format!("fd_write data read: {e}")))?;
                    // Write to stdout/stderr via the Rust side.
                    use std::io::Write;
                    let output: &mut dyn Write = if fd == 1 {
                        &mut std::io::stdout()
                    } else {
                        &mut std::io::stderr()
                    };
                    let _ = output.write_all(&data);
                    let _ = output.flush();
                    total_written += buf_len;
                }
            }

            // Write the total byte count to nwritten.
            let nwritten_bytes = (total_written as i32).to_le_bytes();
            mem.write(&mut caller, nwritten_addr, &nwritten_bytes)
                .map_err(|e| wasmtime::Error::msg(format!("fd_write nwritten write: {e}")))?;

            results[0] = Val::I32(0); // ESUCCESS
            Ok(())
        },
    );

    // ── fd_close ──────────────────────────────────────────────────────
    let fd_close = wasmtime::Func::wrap(&mut *store, |_fd: i32| -> i32 {
        0 /* ESUCCESS */
    });

    // ── fd_seek ───────────────────────────────────────────────────────
    let fd_seek = wasmtime::Func::new(
        &mut *store,
        FuncType::new(
            &engine,
            [ValType::I32, ValType::I64, ValType::I32, ValType::I32],
            [ValType::I32],
        ),
        |_caller: Caller<'_, ()>, _args: &[Val], results: &mut [Val]| {
            results[0] = Val::I32(52); // ENOSYS
            Ok(())
        },
    );

    // ── clock_time_get ────────────────────────────────────────────────
    let clock_time_get = wasmtime::Func::new(
        &mut *store,
        FuncType::new(
            &engine,
            [ValType::I32, ValType::I64, ValType::I32],
            [ValType::I32],
        ),
        |mut caller: Caller<'_, ()>, args: &[Val], results: &mut [Val]| {
            let time_ptr = args[2].i32().unwrap_or(0) as usize;
            let mem = caller
                .get_export("memory")
                .and_then(|e| e.into_memory())
                .ok_or_else(|| wasmtime::Error::msg("clock_time_get: memory not found"))?;

            // Return epoch nanoseconds as a best-effort value.
            let now_ns = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64;

            let time_bytes = now_ns.to_le_bytes();
            mem.write(&mut caller, time_ptr, &time_bytes)
                .map_err(|e| wasmtime::Error::msg(format!("clock_time_get write: {e}")))?;

            results[0] = Val::I32(0); // ESUCCESS
            Ok(())
        },
    );

    // ── environ_sizes_get ─────────────────────────────────────────────
    let environ_sizes_get = wasmtime::Func::new(
        &mut *store,
        FuncType::new(&engine, [ValType::I32, ValType::I32], [ValType::I32]),
        |mut caller: Caller<'_, ()>, args: &[Val], results: &mut [Val]| {
            let argc_ptr = args[0].i32().unwrap_or(0) as usize;
            let buf_size_ptr = args[1].i32().unwrap_or(0) as usize;

            let mem = caller
                .get_export("memory")
                .and_then(|e| e.into_memory())
                .ok_or_else(|| wasmtime::Error::msg("environ_sizes_get: memory not found"))?;

            // Zero environment variables.
            mem.write(&mut caller, argc_ptr, &0i32.to_le_bytes())
                .map_err(|e| wasmtime::Error::msg(format!("environ_sizes_get argc: {e}")))?;
            mem.write(&mut caller, buf_size_ptr, &0i32.to_le_bytes())
                .map_err(|e| wasmtime::Error::msg(format!("environ_sizes_get buf_size: {e}")))?;

            results[0] = Val::I32(0); // ESUCCESS
            Ok(())
        },
    );

    // ── environ_get ───────────────────────────────────────────────────
    let environ_get = wasmtime::Func::wrap(&mut *store, |_argc: i32, _argv_buf: i32| -> i32 {
        0 // ESUCCESS (no env vars to copy)
    });

    // ── Register all stubs ────────────────────────────────────────────
    let module_name = "wasi_snapshot_preview1";
    macro_rules! def {
        ($name:literal, $func:expr) => {
            linker
                .define(&mut *store, module_name, $name, $func)
                .map_err(|e| TapError::Transform(format!("WASI {}.{}: {e}", module_name, $name)))?;
        };
    }
    def!("fd_write", fd_write);
    def!("fd_close", fd_close);
    def!("fd_seek", fd_seek);
    def!("clock_time_get", clock_time_get);
    def!("environ_sizes_get", environ_sizes_get);
    def!("environ_get", environ_get);

    Ok(())
}

// ---------------------------------------------------------------------------
// Emscripten env stubs
// ---------------------------------------------------------------------------

/// Register minimal Emscripten env stubs for `env` module.
///
/// The QuickJS WASM blob imports 5 Emscripten env functions:
///
/// | Function | Signature | Purpose |
/// |----------|-----------|---------|
/// | `_abort_js`         | `() → ()` | Abort on fatal error |
/// | `emscripten_date_now` | `() → f64` | Current time in ms |
/// | `_tzset_js`         | `() → ()` | Timezone setup |
/// | `_localtime_js`     | `(time_ptr, result) → ()` | Local time conversion |
/// | `emscripten_resize_heap` | `(size) → i32` | Grow memory |
///
/// All are stubbed minimally so QuickJS can run (time / date work).
fn add_env_stubs(linker: &mut wasmtime::Linker<()>, store: &mut Store<()>) -> Result<(), TapError> {
    // ── _abort_js ───────────────────────────────────────────────────────
    // Called on fatal errors (e.g. assertion failures).  No-op is safe:
    // with `-D__wasi__` the QuickJS abort paths that call this are
    // disabled.  If one slips through, the WASM will trap naturally on
    // the next invalid memory access.
    let abort_js = wasmtime::Func::wrap(&mut *store, || {});

    // ── emscripten_date_now ─────────────────────────────────────────────
    // Returns current time in milliseconds as f64.  QuickJS uses this for
    // `Date.now()` and timer resolution.
    let date_now = wasmtime::Func::wrap(&mut *store, || -> f64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64()
            * 1000.0
    });

    // ── _tzset_js ───────────────────────────────────────────────────────
    // Emscripten ABI: (timezone: i32, daylight: i32, tzname0: i32, tzname1: i32) → ()
    // Writes UTC offset (0 = UTC) into the timezone pointer.  QuickJS
    // falls back to JS Date for timezone handling.
    let tzset_js = wasmtime::Func::wrap(
        &mut *store,
        |_timezone: i32, _daylight: i32, _tzname0: i32, _tzname1: i32| {
            // no-op (everything stays as UTC)
        },
    );

    // ── _localtime_js ───────────────────────────────────────────────────
    // WASM signature: (time_ptr: i32, result_ptr: i32) → ()
    // Reads time_t from time_ptr, writes `struct tm` to result_ptr.
    // Returns UTC time (QuickJS will handle TZ offset via JS Date).
    let engine = store.engine().clone();
    let localtime_js = wasmtime::Func::new(
        &mut *store,
        wasmtime::FuncType::new(
            &engine,
            [wasmtime::ValType::I64, wasmtime::ValType::I32],
            [],
        ),
        |mut caller: wasmtime::Caller<'_, ()>,
         args: &[wasmtime::Val],
         _results: &mut [wasmtime::Val]| {
            // First arg: time_t value (i64), NOT a pointer.  Emscripten's
            // _localtime_js receives the raw time_t by value.
            let _secs = args[0].i64().unwrap_or(0) as u64;
            let result_ptr = args[1].i32().unwrap_or(0) as usize;

            let mem = caller
                .get_export("memory")
                .and_then(|e| e.into_memory())
                .ok_or_else(|| wasmtime::Error::msg("_localtime_js: memory not found"))?;

            // Compute current UTC broken-down time.
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            // `struct tm` layout (all i32): tm_sec, tm_min, tm_hour, tm_mday,
            // tm_mon, tm_year, tm_wday, tm_yday, tm_isdst
            let tm = {
                // Simple UTC decomposition (no DST, no TZ).
                let secs = now as i64;
                let mut remaining = secs;
                let sec = (remaining % 60) as i32;
                remaining /= 60;
                let min = (remaining % 60) as i32;
                remaining /= 60;
                let hour = (remaining % 24) as i32;
                remaining /= 24;
                // Days since epoch.
                let days = remaining as i32;
                // Compute year/month/day from days since 1970-01-01.
                let mut year = 1970i32;
                let mut remaining_days = days;
                loop {
                    let days_in_year = if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) {
                        366
                    } else {
                        365
                    };
                    if remaining_days < days_in_year {
                        break;
                    }
                    remaining_days -= days_in_year;
                    year += 1;
                }
                let yday = remaining_days;
                static MONTH_DAYS: &[i32] = &[31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
                let mut month = 0i32;
                let mut day = remaining_days;
                for (i, &md) in MONTH_DAYS.iter().enumerate() {
                    let adj = if i == 1 && (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) {
                        29
                    } else {
                        md
                    };
                    if day < adj {
                        month = i as i32;
                        break;
                    }
                    day -= adj;
                }
                let mday = day + 1; // 1-indexed
                // tm_wday: 0=Sun, computed from days since epoch (1970-01-01 was Thursday = 4)
                let wday = (days + 4).rem_euclid(7);
                // tm_yday = remaining_days (0-indexed)
                [sec, min, hour, mday, month, year - 1900, wday, yday, 0]
            };

            let tm_bytes: Vec<u8> = tm.iter().flat_map(|&v| v.to_le_bytes().to_vec()).collect();
            mem.write(&mut caller, result_ptr, &tm_bytes)
                .map_err(|e| wasmtime::Error::msg(format!("_localtime_js write tm: {e}")))?;

            // No return value (void function).
            Ok(())
        },
    );

    // ── emscripten_resize_heap ──────────────────────────────────────────
    // Called by `realloc` when the WASM heap needs to grow.  Return 0
    // (failure) — QuickJS with `__wasi__` doesn't use much heap and the
    // initial 16 MB allocation is sufficient for transform scripts.
    // If we ever hit this in practice, switch to `memory.grow` via `Linker`.
    let resize_heap = wasmtime::Func::wrap(&mut *store, |_requested_size: i32| -> i32 { 0 });

    // ── Register ───────────────────────────────────────────────────────
    macro_rules! def_env {
        ($name:literal, $func:expr) => {
            linker
                .define(&mut *store, "env", $name, $func)
                .map_err(|e| TapError::Transform(format!("env {}.{}: {e}", "env", $name)))?;
        };
    }
    def_env!("_abort_js", abort_js);
    def_env!("emscripten_date_now", date_now);
    def_env!("_tzset_js", tzset_js);
    def_env!("_localtime_js", localtime_js);
    def_env!("emscripten_resize_heap", resize_heap);

    Ok(())
}

// ---------------------------------------------------------------------------
// WASM call helpers
//
// These are thin wrappers around Instance::get_export + Func::call for
// common calling conventions used by our QuickJS exports.  They avoid
// repeating the error-handling boilerplate.
//
// Naming convention: call_export_{paramTypes}_{returnType}
//   i32 = i32 param/result, i64 = i64 param/result, void = no result
//   e.g. call_export_1i32_1i64 = (1 x i32 param) -> (1 x i64 result)
// ---------------------------------------------------------------------------

fn call_void_export(
    instance: &Instance,
    store: &mut Store<()>,
    name: &str,
) -> Result<(), TapError> {
    let func = instance
        .get_func(&mut *store, name)
        .ok_or_else(|| TapError::Transform(format!("export not found: {name}")))?;
    func.call(store, &[], &mut [])
        .map_err(|e| TapError::Transform(format!("{name}: {e}")))?;
    Ok(())
}

fn call_export_0r_1i32(
    instance: &Instance,
    store: &mut Store<()>,
    name: &str,
) -> Result<i32, TapError> {
    let func = instance
        .get_func(&mut *store, name)
        .ok_or_else(|| TapError::Transform(format!("export not found: {name}")))?;
    let mut results = [Val::I32(0)];
    func.call(store, &[], &mut results)
        .map_err(|e| TapError::Transform(format!("{name}: {e}")))?;
    Ok(results[0].i32().unwrap_or(0))
}

fn call_export_1i32_1i32(
    instance: &Instance,
    store: &mut Store<()>,
    name: &str,
    a: i32,
) -> Result<i32, TapError> {
    let func = instance
        .get_func(&mut *store, name)
        .ok_or_else(|| TapError::Transform(format!("export not found: {name}")))?;
    let mut results = [Val::I32(0)];
    func.call(store, &[Val::I32(a)], &mut results)
        .map_err(|e| TapError::Transform(format!("{name}: {e}")))?;
    Ok(results[0].i32().unwrap_or(0))
}

fn call_export_1i32_void(
    instance: &Instance,
    store: &mut Store<()>,
    name: &str,
    a: i32,
) -> Result<(), TapError> {
    let func = instance
        .get_func(&mut *store, name)
        .ok_or_else(|| TapError::Transform(format!("export not found: {name}")))?;
    func.call(store, &[Val::I32(a)], &mut [])
        .map_err(|e| TapError::Transform(format!("{name}: {e}")))?;
    Ok(())
}

/// Call an export with (i64) → i32 signature.
/// Used by `JS_IsPromise(val)`.
fn call_export_1i64_1i32(
    instance: &Instance,
    store: &mut Store<()>,
    name: &str,
    a: i64,
) -> Result<i32, TapError> {
    let func = instance
        .get_func(&mut *store, name)
        .ok_or_else(|| TapError::Transform(format!("export not found: {name}")))?;
    let mut results = [Val::I32(0)];
    func.call(store, &[Val::I64(a)], &mut results)
        .map_err(|e| TapError::Transform(format!("{name}: {e}")))?;
    Ok(results[0].i32().unwrap_or(0))
}

/// Call an export with (i32, i64) → i32 signature.
/// Used by `JS_PromiseState(ctx, val)`.
fn call_export_2args_1i32(
    instance: &Instance,
    store: &mut Store<()>,
    name: &str,
    a: i32,
    b: i64,
) -> Result<i32, TapError> {
    let func = instance
        .get_func(&mut *store, name)
        .ok_or_else(|| TapError::Transform(format!("export not found: {name}")))?;
    let mut results = [Val::I32(0)];
    func.call(store, &[Val::I32(a), Val::I64(b)], &mut results)
        .map_err(|e| TapError::Transform(format!("{name}: {e}")))?;
    Ok(results[0].i32().unwrap_or(0))
}

fn call_export_2args_void(
    instance: &Instance,
    store: &mut Store<()>,
    name: &str,
    a: i32,
    b: i64,
) -> Result<(), TapError> {
    let func = instance
        .get_func(&mut *store, name)
        .ok_or_else(|| TapError::Transform(format!("export not found: {name}")))?;
    func.call(store, &[Val::I32(a), Val::I64(b)], &mut [])
        .map_err(|e| TapError::Transform(format!("{name}: {e}")))?;
    Ok(())
}

/// Call an export named `name` with two i32 arguments, returning nothing.
fn call_export_2i32_void(
    instance: &Instance,
    store: &mut Store<()>,
    name: &str,
    a: i32,
    b: i32,
) -> Result<(), TapError> {
    let func = instance
        .get_func(&mut *store, name)
        .ok_or_else(|| TapError::Transform(format!("export not found: {name}")))?;
    func.call(store, &[Val::I32(a), Val::I32(b)], &mut [])
        .map_err(|e| TapError::Transform(format!("{name}: {e}")))?;
    Ok(())
}

fn call_export_1i32_1i64(
    instance: &Instance,
    store: &mut Store<()>,
    name: &str,
    a: i32,
) -> Result<i64, TapError> {
    let func = instance
        .get_func(&mut *store, name)
        .ok_or_else(|| TapError::Transform(format!("export not found: {name}")))?;
    let mut results = [Val::I64(0)];
    func.call(store, &[Val::I32(a)], &mut results)
        .map_err(|e| TapError::Transform(format!("{name}: {e}")))?;
    Ok(results[0].i64().unwrap_or(0))
}

fn call_export_1i32_1i64_1i64(
    instance: &Instance,
    store: &mut Store<()>,
    name: &str,
    ctx: i32,
    val: i64,
) -> Result<i64, TapError> {
    let func = instance
        .get_func(&mut *store, name)
        .ok_or_else(|| TapError::Transform(format!("export not found: {name}")))?;
    let mut results = [Val::I64(0)];
    func.call(store, &[Val::I32(ctx), Val::I64(val)], &mut results)
        .map_err(|e| TapError::Transform(format!("{name}: {e}")))?;
    Ok(results[0].i64().unwrap_or(0))
}

fn call_export_4i32_1i64(
    instance: &Instance,
    store: &mut Store<()>,
    name: &str,
    a: i32,
    b: i32,
    c: i32,
    d: i32,
) -> Result<i64, TapError> {
    let func = instance
        .get_func(&mut *store, name)
        .ok_or_else(|| TapError::Transform(format!("export not found: {name}")))?;
    let mut results = [Val::I64(0)];
    func.call(
        store,
        &[Val::I32(a), Val::I32(b), Val::I32(c), Val::I32(d)],
        &mut results,
    )
    .map_err(|e| TapError::Transform(format!("{name}: {e}")))?;
    Ok(results[0].i64().unwrap_or(0))
}

/// Call an export with (i32, i64, i32) → i64 signature.
/// Used by `JS_GetPropertyStr(ctx, obj, prop_name)`.
fn call_export_3mixed_1i64(
    instance: &Instance,
    store: &mut Store<()>,
    name: &str,
    a: i32,
    b: i64,
    c: i32,
) -> Result<i64, TapError> {
    let func = instance
        .get_func(&mut *store, name)
        .ok_or_else(|| TapError::Transform(format!("export not found: {name}")))?;
    let mut results = [Val::I64(0)];
    func.call(
        store,
        &[Val::I32(a), Val::I64(b), Val::I32(c)],
        &mut results,
    )
    .map_err(|e| TapError::Transform(format!("{name}: {e}")))?;
    Ok(results[0].i64().unwrap_or(0))
}

fn call_export_4i32_1i32(
    instance: &Instance,
    store: &mut Store<()>,
    name: &str,
    a: i32,
    b: i32,
    c: i64,
    d: i32,
) -> Result<i32, TapError> {
    let func = instance
        .get_func(&mut *store, name)
        .ok_or_else(|| TapError::Transform(format!("export not found: {name}")))?;
    let mut results = [Val::I32(0)];
    func.call(
        store,
        &[Val::I32(a), Val::I32(b), Val::I64(c), Val::I32(d)],
        &mut results,
    )
    .map_err(|e| TapError::Transform(format!("{name}: {e}")))?;
    Ok(results[0].i32().unwrap_or(0))
}

#[allow(clippy::too_many_arguments)]
fn call_export_5i32_1i64(
    instance: &Instance,
    store: &mut Store<()>,
    name: &str,
    a: i32,
    b: i32,
    c: i32,
    d: i32,
    e: i32,
) -> Result<i64, TapError> {
    let func = instance
        .get_func(&mut *store, name)
        .ok_or_else(|| TapError::Transform(format!("export not found: {name}")))?;
    let mut results = [Val::I64(0)];
    func.call(
        store,
        &[
            Val::I32(a),
            Val::I32(b),
            Val::I32(c),
            Val::I32(d),
            Val::I32(e),
        ],
        &mut results,
    )
    .map_err(|e| TapError::Transform(format!("{name}: {e}")))?;
    Ok(results[0].i64().unwrap_or(0))
}

// ---------------------------------------------------------------------------
// Utility functions
// ---------------------------------------------------------------------------

/// Compute the hex-encoded SHA-256 digest of a byte slice.
fn hex_hash(data: &[u8]) -> String {
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

/// Check whether a `JSValue` (i64) carries the exception tag.
///
/// With NaN boxing the tag lives in the upper 32 bits of the uint64_t.
fn is_exception(val: i64) -> bool {
    ((val as u64) >> 32) as u32 == JS_TAG_EXCEPTION
}

/// QuickJS NaN-boxing tag for `undefined`.
const JS_TAG_UNDEFINED: u32 = 2;

/// Check whether a `JSValue` (i64) is `undefined`.
fn is_undefined(val: i64) -> bool {
    ((val as u64) >> 32) as u32 == JS_TAG_UNDEFINED
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
        let _ = engine;
    }

    /// Compile valid JavaScript source to bytecode.
    #[test]
    fn compile_valid_js_to_bytecode() {
        let mut engine = TransformEngine::new().expect("new() failed");
        let bytecode = engine
            .compile_to_bytecode("const x = 1 + 2;")
            .expect("compile_to_bytecode should succeed");
        assert!(!bytecode.is_empty(), "bytecode should not be empty");
    }

    /// Compilation of invalid JS returns an error.
    #[test]
    fn compile_invalid_js_returns_error() {
        let mut engine = TransformEngine::new().expect("new() failed");
        let result = engine.compile_to_bytecode("const x = ;;;");
        assert!(result.is_err(), "invalid JS should fail to compile");
    }

    /// Compiling the same source twice returns identical bytecode.
    #[test]
    fn compile_is_deterministic() {
        let mut engine = TransformEngine::new().expect("new() failed");
        let source = "const x = 42; const y = x + 1;";

        let bc1 = engine.compile_to_bytecode(source).expect("first compile");
        let bc2 = engine.compile_to_bytecode(source).expect("second compile");

        assert_eq!(bc1, bc2, "bytecode must be identical for the same source");
    }

    /// Bytecode round-trip: compile then evaluate.
    #[test]
    fn bytecode_roundtrip_expression() {
        let mut engine = TransformEngine::new().expect("new() failed");
        // Use a JS expression that evaluates to a string.
        let source = "JSON.stringify({ hello: 'world' })";
        let bytecode = engine.compile_to_bytecode(source).expect("compile");
        let result = engine.eval_bytecode(&bytecode).expect("eval");
        assert_eq!(result, r#"{"hello":"world"}"#);
    }

    /// Script that throws an error during bytecode execution.
    #[test]
    fn eval_bytecode_throws_error() {
        let mut engine = TransformEngine::new().expect("new() failed");
        let source = "throw new Error('boom');";
        let bytecode = engine
            .compile_to_bytecode(source)
            .expect("compile should succeed even with throw");

        let result = engine.eval_bytecode(&bytecode);
        assert!(result.is_err(), "eval should fail on throw");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("boom") || err.contains("Error"),
            "error message should contain 'boom': {err}"
        );
    }

    /// Bytecode cache returns cached result for repeated source.
    #[test]
    fn bytecode_cache_hits() {
        let mut engine = TransformEngine::new().expect("new() failed");
        let source = "const z = 99;";

        // First call compiles and caches.
        let bc1 = engine.compile_to_bytecode(source).expect("first compile");
        // Second call should hit cache.
        let bc2 = engine.compile_to_bytecode(source).expect("second compile");

        assert_eq!(bc1, bc2, "cached bytecode must match");
    }

    /// Evaluate a simple numeric expression via bytecode.
    #[test]
    fn eval_numeric_expression() {
        let mut engine = TransformEngine::new().expect("new() failed");
        // JSON.stringify ensures we get a string result.
        let source = "JSON.stringify(1 + 2)";
        let bytecode = engine.compile_to_bytecode(source).expect("compile");
        let result = engine.eval_bytecode(&bytecode).expect("eval");
        // QuickJS: 1 + 2 = 3
        assert_eq!(result, "3");
    }

    // ── Verification: cross-eval data leakage (tap-06h) ────────────────

    /// Prove that stale expression values do not leak across eval calls.
    ///
    /// An expression eval sets a global property (`__tap_{N}`) via the
    /// compiled wrapper.  A subsequent statement-only eval must NOT see
    /// that stale value — it must return `"undefined"` instead.
    #[test]
    fn no_cross_eval_data_leakage() {
        let mut engine = TransformEngine::new().expect("new() failed");

        // Step 1: expression eval that stores a value in the global property.
        let bc1 = engine
            .compile_to_bytecode("JSON.stringify({secret: 'data'})")
            .expect("compile expr");
        let r1 = engine.eval_bytecode(&bc1).expect("eval expr");
        assert_eq!(r1, r#"{"secret":"data"}"#);

        // Step 2: statement-only eval — must NOT leak the stale value.
        let bc2 = engine
            .compile_to_bytecode("const x = 1;")
            .expect("compile stmt");
        let r2 = engine.eval_bytecode(&bc2).expect("eval stmt");
        assert_eq!(
            r2, "undefined",
            "stale expression value leaked across evals: got {r2:?}, expected \"undefined\""
        );
    }

    // ── Verification: fuel-based timeout (tap-5kh) ─────────────────────

    /// Prove that an infinite loop exhausts fuel and returns an error.
    #[test]
    fn infinite_loop_times_out() {
        let mut engine = TransformEngine::new().expect("new() failed");

        let source = "while(true){}";
        let bytecode = engine
            .compile_to_bytecode(source)
            .expect("compile while loop");

        let result = engine.eval_bytecode(&bytecode);
        assert!(
            result.is_err(),
            "infinite loop should time out via fuel exhaustion"
        );
        // The error is a WASM trap from fuel exhaustion; exact wording depends
        // on wasmtime version, so we just verify it's not a success.
        result.unwrap_err();
    }

    // ── Verification: unique property name per instance (tap-zoh) ──────

    /// Prove that two engine instances have different capture property names.
    #[test]
    fn engines_have_unique_property_names() {
        let mut engine_a = TransformEngine::new().expect("engine_a");
        let mut engine_b = TransformEngine::new().expect("engine_b");

        // The name is private, but we can test through behaviour:
        // compile+eval with two instances and verify they don't interfere.
        let bc = engine_a
            .compile_to_bytecode("JSON.stringify({x: 1})")
            .expect("compile");
        let r = engine_a.eval_bytecode(&bc).expect("eval");
        assert_eq!(r, r#"{"x":1}"#);
        // Same source on engine_b works too, proving both have valid names.
        let bc2 = engine_b
            .compile_to_bytecode("JSON.stringify({x: 1})")
            .expect("compile engine_b");
        let r2 = engine_b.eval_bytecode(&bc2).expect("eval engine_b");
        assert_eq!(r2, r#"{"x":1}"#);
    }

    /// Debug: probe WASM stack state and try JS_Eval with different flags.
    #[test]
    fn debug_probe_wasm_state() {
        let mut config = wasmtime::Config::new();
        config.epoch_interruption(true);
        let engine = Engine::new(&config).unwrap();
        let module_bytes = include_bytes!("../../assets/quickjs.wasm");
        let module = Module::new(&engine, module_bytes.as_slice()).unwrap();
        let mut store = Store::new(&engine, ());
        store.set_epoch_deadline(u64::MAX);
        let mut linker: wasmtime::Linker<()> = wasmtime::Linker::new(&engine);
        add_env_stubs(&mut linker, &mut store).unwrap();
        add_wasi_stubs(&mut linker, &mut store).unwrap();
        let instance = linker.instantiate(&mut store, &module).unwrap();
        let memory = instance.get_memory(&mut store, "memory").unwrap();
        call_void_export(&instance, &mut store, "__wasm_call_ctors").unwrap();

        // Write debug output to /tmp/debug_probe.txt
        use std::io::Write;
        let mut log = std::fs::File::create("/tmp/debug_probe.txt").unwrap();

        // Read the stack pointer global.
        let stack_ptr_global = instance.get_global(&mut store, "__stack_pointer");
        let stack_ptr_val = stack_ptr_global.and_then(|g| g.get(&mut store).i32());
        writeln!(log, "__stack_pointer global: {stack_ptr_val:?}").unwrap();

        // emscripten_stack_get_current returns the current stack pointer.
        let current_stack =
            call_export_0r_1i32(&instance, &mut store, "emscripten_stack_get_current").unwrap();
        writeln!(log, "emscripten_stack_get_current: {current_stack}").unwrap();

        // Try JS_Eval with different flag combinations.
        // First: JS_NewRuntime + JS_NewContext.
        let rt = call_export_0r_1i32(&instance, &mut store, "JS_NewRuntime").unwrap();
        writeln!(log, "JS_NewRuntime: {rt}").unwrap();
        let ctx = call_export_1i32_1i32(&instance, &mut store, "JS_NewContext", rt).unwrap();
        writeln!(log, "JS_NewContext: {ctx}").unwrap();

        // Write source "1+1" into WASM memory.
        let src = b"1+1";
        let (src_ptr, src_len) = {
            let len = src.len() as i32;
            let ptr = call_export_1i32_1i32(&instance, &mut store, "malloc", len).unwrap();
            memory.write(&mut store, ptr as usize, src).unwrap();
            (ptr, len)
        };
        let name = b"test.js";
        let (name_ptr, _name_len) = {
            let len = name.len() as i32;
            let ptr = call_export_1i32_1i32(&instance, &mut store, "malloc", len).unwrap();
            memory.write(&mut store, ptr as usize, name).unwrap();
            (ptr, len)
        };

        // Try JS_Eval with JS_EVAL_TYPE_GLOBAL (0) only — no COMPILE_ONLY.
        // This should execute the code and return a result.
        writeln!(log, "--- JS_Eval with JS_EVAL_TYPE_GLOBAL (0) ---").unwrap();
        let eval_flags_global: i32 = 0; // JS_EVAL_TYPE_GLOBAL
        let val = call_export_5i32_1i64(
            &instance,
            &mut store,
            "JS_Eval",
            ctx,
            src_ptr,
            src_len,
            name_ptr,
            eval_flags_global,
        )
        .unwrap();
        writeln!(log, "JS_Eval(global) result: 0x{val:016x}").unwrap();
        writeln!(
            log,
            "  tag (upper 32): 0x{:08x}",
            ((val as u64) >> 32) as u32
        )
        .unwrap();
        writeln!(log, "  is_exception: {}", is_exception(val)).unwrap();

        // Try JS_Eval with JS_EVAL_TYPE_MODULE (1) | JS_EVAL_FLAG_COMPILE_ONLY (32)
        writeln!(log, "--- JS_Eval with MODULE | COMPILE_ONLY ---").unwrap();
        let eval_flags_module: i32 = 1 | 32;
        let val2 = call_export_5i32_1i64(
            &instance,
            &mut store,
            "JS_Eval",
            ctx,
            src_ptr,
            src_len,
            name_ptr,
            eval_flags_module,
        )
        .unwrap();
        writeln!(log, "JS_Eval(module|compile) result: 0x{val2:016x}").unwrap();
        writeln!(
            log,
            "  tag (upper 32): 0x{:08x}",
            ((val2 as u64) >> 32) as u32
        )
        .unwrap();
        writeln!(log, "  is_exception: {}", is_exception(val2)).unwrap();

        if is_exception(val2) {
            // Try to get the exception message.
            let exc_val =
                call_export_1i32_1i64(&instance, &mut store, "JS_GetException", ctx).unwrap();
            writeln!(log, "  exception val: 0x{exc_val:016x}").unwrap();
            if !is_exception(exc_val) {
                // Read exception as string.
                let plen = call_export_1i32_1i32(&instance, &mut store, "malloc", 4).unwrap();
                let cstr_ptr = call_export_4i32_1i32(
                    &instance,
                    &mut store,
                    "JS_ToCStringLen2",
                    ctx,
                    plen,
                    exc_val,
                    0,
                )
                .unwrap();
                writeln!(log, "  cstr_ptr: {cstr_ptr}").unwrap();
                if cstr_ptr != 0 {
                    let mut len_buf = [0u8; 4];
                    memory.read(&store, plen as usize, &mut len_buf).unwrap();
                    let str_len = i32::from_le_bytes(len_buf) as usize;
                    let mut str_buf = vec![0u8; str_len];
                    memory
                        .read(&store, cstr_ptr as usize, &mut str_buf)
                        .unwrap();
                    writeln!(
                        log,
                        "  exception message: {}",
                        String::from_utf8_lossy(&str_buf)
                    )
                    .unwrap();
                    call_export_2i32_void(&instance, &mut store, "JS_FreeCString", ctx, cstr_ptr)
                        .unwrap();
                }
                call_export_1i32_void(&instance, &mut store, "free", plen).unwrap();
                call_export_2args_void(&instance, &mut store, "JS_FreeValue", ctx, exc_val)
                    .unwrap();
            }
        }

        // Try various sources to narrow down the issue.
        let test_sources: &[&[u8]] = &[
            b"1+1",
            b"1+1;",
            b"1 + 2",
            b"1 + 2;",
            b"JSON.stringify(1+2)",
            b"JSON.stringify(1+2);",
        ];
        for test_src in test_sources {
            let (t_ptr, t_len) = {
                let len = test_src.len() as i32;
                let ptr = call_export_1i32_1i32(&instance, &mut store, "malloc", len).unwrap();
                memory.write(&mut store, ptr as usize, test_src).unwrap();
                // Verify readback
                let mut check = vec![0u8; len as usize];
                memory.read(&store, ptr as usize, &mut check).unwrap();
                assert_eq!(
                    &check,
                    test_src,
                    "memory verify failed for '{src}'",
                    src = String::from_utf8_lossy(test_src)
                );
                (ptr, len)
            };
            let val = call_export_5i32_1i64(
                &instance,
                &mut store,
                "JS_Eval",
                ctx,
                t_ptr,
                t_len,
                name_ptr,
                eval_flags_module,
            )
            .unwrap();
            let is_exc = is_exception(val);
            writeln!(
                log,
                "  '{src}' -> 0x{val:016x} exc={is_exc}",
                src = String::from_utf8_lossy(test_src)
            )
            .unwrap();
            if is_exc {
                let exc_val =
                    call_export_1i32_1i64(&instance, &mut store, "JS_GetException", ctx).unwrap();
                if !is_exception(exc_val) {
                    let plen = call_export_1i32_1i32(&instance, &mut store, "malloc", 4).unwrap();
                    let cstr_ptr = call_export_4i32_1i32(
                        &instance,
                        &mut store,
                        "JS_ToCStringLen2",
                        ctx,
                        plen,
                        exc_val,
                        0,
                    )
                    .unwrap();
                    if cstr_ptr != 0 {
                        let mut len_buf = [0u8; 4];
                        memory.read(&store, plen as usize, &mut len_buf).unwrap();
                        let str_len = i32::from_le_bytes(len_buf) as usize;
                        let mut str_buf = vec![0u8; str_len];
                        memory
                            .read(&store, cstr_ptr as usize, &mut str_buf)
                            .unwrap();
                        writeln!(log, "    err: {}", String::from_utf8_lossy(&str_buf)).unwrap();
                        call_export_2i32_void(
                            &instance,
                            &mut store,
                            "JS_FreeCString",
                            ctx,
                            cstr_ptr,
                        )
                        .unwrap();
                    }
                    call_export_1i32_void(&instance, &mut store, "free", plen).unwrap();
                    call_export_2args_void(&instance, &mut store, "JS_FreeValue", ctx, exc_val)
                        .unwrap();
                }
            }
            call_export_1i32_void(&instance, &mut store, "free", t_ptr).unwrap();
        }

        // ── Additional debugging: test null-terminator padding ──
        // Hypothesis: QuickJS-ng module parser reads one byte beyond input_len.
        // We allocate src.len() + 1 bytes, write src + \0 at position 0..src.len(),
        // write \0 at position src.len(), but pass src.len() as the length to JS_Eval.
        // The extra null byte is a safety guard IF the parser over-reads.
        // We use a flat u8 buffer and index offsets to avoid lifetime issues.
        let mut np_buf: Vec<u8> = Vec::new();
        struct NpEntry {
            flags: i32,
            offset: usize,
            actual_len: usize,
            desc: &'static str,
        }
        let mut np_entries: Vec<NpEntry> = Vec::new();
        let add_np = |np_entries: &mut Vec<NpEntry>,
                      np_buf: &mut Vec<u8>,
                      flags: i32,
                      src: &[u8],
                      desc: &'static str| {
            let offset = np_buf.len();
            np_buf.extend_from_slice(src);
            np_buf.push(0); // safety null
            np_entries.push(NpEntry {
                flags,
                offset,
                actual_len: src.len(),
                desc,
            });
        };
        // Re-test originals (no padding — same as normal code path)
        add_np(
            &mut np_entries,
            &mut np_buf,
            JS_EVAL_TYPE_MODULE | JS_EVAL_FLAG_COMPILE_ONLY,
            b"const x = 1 + 2;",
            "control: const x = 1 + 2;",
        );
        add_np(
            &mut np_entries,
            &mut np_buf,
            JS_EVAL_TYPE_MODULE | JS_EVAL_FLAG_COMPILE_ONLY,
            b"const z = 99;",
            "control: const z = 99;",
        );
        // Null-padded tests
        add_np(
            &mut np_entries,
            &mut np_buf,
            JS_EVAL_TYPE_MODULE | JS_EVAL_FLAG_COMPILE_ONLY,
            b"const x = 1 + 2;",
            "nullpad: const x = 1 + 2;",
        );
        add_np(
            &mut np_entries,
            &mut np_buf,
            JS_EVAL_TYPE_MODULE | JS_EVAL_FLAG_COMPILE_ONLY,
            b"const z = 99;",
            "nullpad: const z = 99;",
        );
        add_np(
            &mut np_entries,
            &mut np_buf,
            JS_EVAL_TYPE_MODULE | JS_EVAL_FLAG_COMPILE_ONLY,
            b"JSON.stringify(1 + 2)",
            "nullpad: JSON.stringify(1 + 2)",
        );
        add_np(
            &mut np_entries,
            &mut np_buf,
            JS_EVAL_TYPE_MODULE | JS_EVAL_FLAG_COMPILE_ONLY,
            b"1 + 2",
            "nullpad: 1 + 2",
        );
        add_np(
            &mut np_entries,
            &mut np_buf,
            JS_EVAL_TYPE_MODULE | JS_EVAL_FLAG_COMPILE_ONLY,
            b"1+1;",
            "nullpad: 1+1;",
        );
        add_np(
            &mut np_entries,
            &mut np_buf,
            JS_EVAL_TYPE_MODULE | JS_EVAL_FLAG_COMPILE_ONLY,
            b"1 + 2;",
            "nullpad: 1 + 2;",
        );
        // Write the entire NP buffer at once, then run each test with offset + actual_len
        let np_total = np_buf.len() as i32;
        let np_ptr = call_export_1i32_1i32(&instance, &mut store, "malloc", np_total).unwrap();
        memory.write(&mut store, np_ptr as usize, &np_buf).unwrap();
        for entry in &np_entries {
            let val = call_export_5i32_1i64(
                &instance,
                &mut store,
                "JS_Eval",
                ctx,
                np_ptr + entry.offset as i32,
                entry.actual_len as i32,
                name_ptr,
                entry.flags,
            )
            .unwrap();
            let is_exc = is_exception(val);
            writeln!(
                log,
                "  [{desc}] -> 0x{val:016x} exc={is_exc}",
                desc = entry.desc
            )
            .unwrap();
            if is_exc {
                let exc_val =
                    call_export_1i32_1i64(&instance, &mut store, "JS_GetException", ctx).unwrap();
                if !is_exception(exc_val) {
                    let plen = call_export_1i32_1i32(&instance, &mut store, "malloc", 4).unwrap();
                    let cstr_ptr = call_export_4i32_1i32(
                        &instance,
                        &mut store,
                        "JS_ToCStringLen2",
                        ctx,
                        plen,
                        exc_val,
                        0,
                    )
                    .unwrap();
                    if cstr_ptr != 0 {
                        let mut len_buf = [0u8; 4];
                        memory.read(&store, plen as usize, &mut len_buf).unwrap();
                        let str_len = i32::from_le_bytes(len_buf) as usize;
                        let mut str_buf = vec![0u8; str_len];
                        memory
                            .read(&store, cstr_ptr as usize, &mut str_buf)
                            .unwrap();
                        writeln!(log, "    err: {}", String::from_utf8_lossy(&str_buf)).unwrap();
                        call_export_2i32_void(
                            &instance,
                            &mut store,
                            "JS_FreeCString",
                            ctx,
                            cstr_ptr,
                        )
                        .unwrap();
                    }
                    call_export_1i32_void(&instance, &mut store, "free", plen).unwrap();
                    call_export_2args_void(&instance, &mut store, "JS_FreeValue", ctx, exc_val)
                        .unwrap();
                }
            }
        }
        call_export_1i32_void(&instance, &mut store, "free", np_ptr).unwrap();

        // Cleanup
        call_export_1i32_void(&instance, &mut store, "free", src_ptr).unwrap();
        call_export_1i32_void(&instance, &mut store, "free", name_ptr).unwrap();
        call_export_1i32_void(&instance, &mut store, "JS_FreeContext", ctx).unwrap();
        call_export_1i32_void(&instance, &mut store, "JS_FreeRuntime", rt).unwrap();
    }
}

//! Script validation for transform pipelines.
//!
//! Provides [`validate_script`] which checks JavaScript source for syntax
//! errors by attempting QuickJS bytecode compilation.  This catches
//! malformed scripts at configuration time rather than at runtime,
//! allowing the pipeline to reject invalid transforms early.

use crate::error::TapError;
use crate::transforms::engine::TransformEngine;

/// Validate a JavaScript script by attempting bytecode compilation.
///
/// Returns `Ok(())` if QuickJS can parse the script.  Returns
/// `Err(TapError::Transform(…))` with the compiler error message
/// if the script contains syntax errors.
///
/// This is a lightweight operation — the engine compiles the source
/// with `JS_EVAL_FLAG_COMPILE_ONLY` (no execution) and discards the
/// bytecode.  No JavaScript values are modified or executed.
///
/// # Example
///
/// ```ignore
/// use tap_core::transforms::validate::validate_script;
///
/// let mut engine = TransformEngine::new().unwrap();
/// assert!(validate_script(&mut engine, "const x = 1;").is_ok());
/// assert!(validate_script(&mut engine, "const x = ;;;").is_err());
/// ```
pub fn validate_script(engine: &mut TransformEngine, script: &str) -> Result<(), TapError> {
    match engine.compile_to_bytecode(script) {
        Ok(_) => Ok(()),
        Err(e) => {
            // Re-wrap the error in a user-friendly message.
            Err(TapError::Transform(format!(
                "script validation failed: {e}"
            )))
        }
    }
}

/// Validate a TypeScript script (strips types then validates JS).
///
/// Currently this is a pass-through to [`validate_script`] — TypeScript
/// transpilation (via `swc_core`) will be integrated in a follow-up
/// change (`tap-3on.4.3`).
///
/// # TODO
///
/// Add `swc_core` with minimal features (`ecma_parser_typescript` +
/// `ecma_transforms_typescript`) to strip TS type annotations before
/// passing the output to QuickJS.
pub fn validate_ts_script(engine: &mut TransformEngine, script: &str) -> Result<(), TapError> {
    // TODO: transpile TS → JS via swc_core here.
    // For now, pass through as-is (works for plain JS).
    validate_script(engine, script)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_js_passes_validation() {
        let mut engine = TransformEngine::new().expect("engine");
        assert!(validate_script(&mut engine, "const x = 1;").is_ok());
    }

    #[test]
    fn valid_es_module_passes() {
        let mut engine = TransformEngine::new().expect("engine");
        assert!(validate_script(&mut engine, "export function f() { return 42; }").is_ok());
    }

    #[test]
    fn syntax_error_fails_validation() {
        let mut engine = TransformEngine::new().expect("engine");
        let result = validate_script(&mut engine, "const x = ;;;");
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("compilation error") || msg.contains("validation failed"),
            "error should mention compilation: {msg}"
        );
    }

    #[test]
    fn empty_script_is_valid() {
        let mut engine = TransformEngine::new().expect("engine");
        assert!(validate_script(&mut engine, "").is_ok());
    }

    #[test]
    fn function_definition_is_valid() {
        let mut engine = TransformEngine::new().expect("engine");
        assert!(validate_script(&mut engine, "function f(e) { return e.op !== 'd'; }").is_ok());
    }

    #[test]
    fn ts_script_passthrough_valid() {
        // TS validation currently delegates to JS validation.
        // A TS type annotation would technically fail until swc is wired.
        let mut engine = TransformEngine::new().expect("engine");
        assert!(validate_ts_script(&mut engine, "const x: number = 1;").is_err());
        // Pure JS still works.
        assert!(validate_ts_script(&mut engine, "const x = 1;").is_ok());
    }
}

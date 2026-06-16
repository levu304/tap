//! Validation for transform pipelines.
//!
//! Two categories of validation:
//!
//! * **Pre-run (script validation)** — [`validate_script`] checks JavaScript
//!   source for syntax errors by attempting QuickJS bytecode compilation.
//!   This catches malformed scripts at configuration time.
//!
//! * **Post-run (envelope + error routing)** — [`validate_event_envelope`]
//!   checks that a [`ChangeEvent`] has all required fields after a map
//!   transform, and [`route_transform_result`] decides whether a transform
//!   error should pass-through, reject, or drop the event based on the
//!   transform type and `fail_closed` setting.

use crate::error::TapError;
use crate::event::ChangeEvent;
use crate::transforms::config::{TransformDescriptor, TransformResult};
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
// Post-transform validation
// ---------------------------------------------------------------------------

/// Maximum time (milliseconds) allowed for a full transform pipeline to run.
///
/// If the pipeline exceeds this budget the event passes through unchanged
/// (fail-open behaviour).  The timeout is enforced by the pipeline runner;
/// this constant is the single source of truth.
pub const TRANSFORM_TIMEOUT_MS: u64 = 100;

/// Validate a [`ChangeEvent`] envelope after it has been through a map
/// transform.
///
/// A buggy or malicious map function could drop or corrupt required fields.
/// This check ensures the event is still structurally sound before it
/// reaches downstream consumers.
///
/// # Checks
///
/// * `id` is non-empty.
/// * `source.db`, `source.schema`, and `source.table` are non-empty.
///
/// # Errors
///
/// Returns an error description if any check fails.
pub fn validate_event_envelope(event: &ChangeEvent) -> Result<(), String> {
    if event.id.is_empty() {
        return Err("event id is empty".into());
    }
    if event.source.db.is_empty() {
        return Err("source.db is empty".into());
    }
    if event.source.schema.is_empty() {
        return Err("source.schema is empty".into());
    }
    if event.source.table.is_empty() {
        return Err("source.table is empty".into());
    }
    Ok(())
}

/// Route a [`TransformResult`] through error-handling logic based on the
/// transform type and the pipeline's `fail_closed` setting.
///
/// # Error routing rules
///
/// | Transform | `fail_closed` | Error behaviour |
/// |-----------|--------------|-----------------|
/// | Filter    | *any*        | `PassThrough` (never silently drop data) |
/// | Map       | `true`       | Keep `Error` (reject event) |
/// | Map       | `false`      | `PassThrough` (log and continue) |
/// | Mask      | *any*        | Keep `Error` (structural integrity) |
///
/// Non-error results (`PassThrough`, `Dropped`, `Modified`) are returned
/// unchanged.
pub fn route_transform_result(
    result: TransformResult,
    descriptor: &TransformDescriptor,
    fail_closed: bool,
) -> TransformResult {
    match (&result, descriptor) {
        // Filter errors → always pass-through.  We never discard an event
        // just because the filter script is buggy — better to let it through
        // than lose data silently.
        (TransformResult::Error(_), TransformDescriptor::Filter { .. }) => {
            TransformResult::PassThrough
        }
        // Mask errors → always fatal (structural integrity issue).
        (TransformResult::Error(_), TransformDescriptor::Mask { .. }) => result,
        // Map errors → depends on fail_closed.
        (TransformResult::Error(_), TransformDescriptor::Map { .. }) => {
            if fail_closed {
                result
            } else {
                TransformResult::PassThrough
            }
        }
        // Non-error results pass through as-is.
        _ => result,
    }
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

    // ── Envelope validation ─────────────────────────────────────────────

    fn minimal_event() -> ChangeEvent {
        ChangeEvent {
            op: crate::event::Operation::Create,
            before: None,
            after: None,
            source: crate::event::SourceMetadata {
                db: "db".into(),
                schema: "public".into(),
                table: "users".into(),
                ..Default::default()
            },
            ts_ms: 0,
            id: "evt-1".into(),
        }
    }

    #[test]
    fn envelope_valid_event() {
        let event = minimal_event();
        assert!(validate_event_envelope(&event).is_ok());
    }

    #[test]
    fn envelope_empty_id_rejected() {
        let mut event = minimal_event();
        event.id.clear();
        let err = validate_event_envelope(&event).unwrap_err();
        assert!(err.contains("id"), "error should mention id: {err}");
    }

    #[test]
    fn envelope_empty_source_db_rejected() {
        let mut event = minimal_event();
        event.source.db.clear();
        let err = validate_event_envelope(&event).unwrap_err();
        assert!(err.contains("source.db"), "error: {err}");
    }

    #[test]
    fn envelope_empty_source_schema_rejected() {
        let mut event = minimal_event();
        event.source.schema.clear();
        let err = validate_event_envelope(&event).unwrap_err();
        assert!(err.contains("source.schema"), "error: {err}");
    }

    #[test]
    fn envelope_empty_source_table_rejected() {
        let mut event = minimal_event();
        event.source.table.clear();
        let err = validate_event_envelope(&event).unwrap_err();
        assert!(err.contains("source.table"), "error: {err}");
    }

    // ── Error routing ───────────────────────────────────────────────────

    fn make_filter_desc() -> TransformDescriptor {
        TransformDescriptor::Filter { script: "".into() }
    }

    fn make_map_desc() -> TransformDescriptor {
        TransformDescriptor::Map { script: "".into() }
    }

    fn make_mask_desc() -> TransformDescriptor {
        TransformDescriptor::Mask {
            fields: vec![],
            strategy: crate::transforms::config::MaskStrategy::Redact,
        }
    }

    #[test]
    fn filter_error_routed_to_passthrough() {
        let result = TransformResult::Error("filter broke".into());
        let routed = route_transform_result(result, &make_filter_desc(), false);
        assert_eq!(routed, TransformResult::PassThrough);
    }

    #[test]
    fn filter_error_passthrough_even_when_fail_closed() {
        let result = TransformResult::Error("filter broke".into());
        let routed = route_transform_result(result, &make_filter_desc(), true);
        assert_eq!(routed, TransformResult::PassThrough);
    }

    #[test]
    fn mask_error_kept() {
        let result = TransformResult::Error("mask broke".into());
        let routed = route_transform_result(result, &make_mask_desc(), false);
        assert!(matches!(routed, TransformResult::Error(_)));
    }

    #[test]
    fn mask_error_kept_even_when_fail_open() {
        let result = TransformResult::Error("mask broke".into());
        let routed = route_transform_result(result, &make_mask_desc(), false);
        assert!(matches!(routed, TransformResult::Error(_)));
    }

    #[test]
    fn map_error_with_fail_closed_kept() {
        let result = TransformResult::Error("map broke".into());
        let routed = route_transform_result(result, &make_map_desc(), true);
        assert!(matches!(routed, TransformResult::Error(_)));
    }

    #[test]
    fn map_error_with_fail_open_passthrough() {
        let result = TransformResult::Error("map broke".into());
        let routed = route_transform_result(result, &make_map_desc(), false);
        assert_eq!(routed, TransformResult::PassThrough);
    }

    #[test]
    fn passthrough_result_unchanged() {
        let result = TransformResult::PassThrough;
        let routed = route_transform_result(result, &make_filter_desc(), false);
        assert_eq!(routed, TransformResult::PassThrough);
    }

    #[test]
    fn dropped_result_unchanged() {
        let result = TransformResult::Dropped;
        let routed = route_transform_result(result, &make_map_desc(), false);
        assert_eq!(routed, TransformResult::Dropped);
    }

    #[test]
    fn modified_result_unchanged() {
        let event = minimal_event();
        let result = TransformResult::Modified(Box::new(event));
        let routed = route_transform_result(result, &make_map_desc(), false);
        assert!(matches!(routed, TransformResult::Modified(_)));
    }

    // ── Timeout constant ────────────────────────────────────────────────

    #[test]
    fn timeout_constant_is_100ms() {
        assert_eq!(TRANSFORM_TIMEOUT_MS, 100);
    }
}

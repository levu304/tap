//! JavaScript filter-transform implementation.
//!
//! Applies a user-defined JavaScript filter function to a [`ChangeEvent`].
//! If the function returns `true` the event passes through unchanged; if it
//! returns `false` the event is dropped from the pipeline.
//!
//! # Filter function convention
//!
//! The user's script MUST define a function named `f` that accepts a single
//! argument — the deserialised [`ChangeEvent`] — and returns a boolean:
//!
//! ```js
//! function f(event) {
//!     return event.op !== 'd';
//! }
//! ```
//!
//! The function is called once per event.  Returning a truthy value passes
//! the event through; returning a falsy value drops it.
//!
//! # Integration with the transform engine
//!
//! At runtime [`apply_filter`] serialises the event to JSON, embeds it into
//! a temporary JS module that calls the user's function, and uses
//! [`TransformEngine::run_transform_js`] to compile and execute the module.
//! The result string (`"true"` or `"false"`) is mapped to
//! [`TransformResult::PassThrough`] or [`TransformResult::Dropped`].

use crate::event::ChangeEvent;
use crate::transforms::config::{TransformDescriptor, TransformResult};
use crate::transforms::engine::TransformEngine;

/// Apply a `Filter` descriptor to a [`ChangeEvent`].
///
/// The user's filter script is compiled + executed inside the sandboxed
/// QuickJS runtime.  If the function returns `true` the event passes
/// through unchanged; `false` drops it.
///
/// # Returns
///
/// | Result | Meaning |
/// |--------|---------|
/// | [`TransformResult::PassThrough`] | Filter returned `true` — event passes. |
/// | [`TransformResult::Dropped`] | Filter returned `false` — event is dropped. |
/// | [`TransformResult::Error`] | Non-filter descriptor, JS error, or non-boolean result. |
///
/// # Errors
///
/// Returns [`TransformResult::Error`] when:
///
/// * `descriptor` is not a [`TransformDescriptor::Filter`] variant.
/// * The JavaScript source cannot be compiled (syntax error).
/// * The JavaScript execution throws at runtime.
/// * The function returns a non-boolean value.
pub fn apply_filter(
    event: ChangeEvent,
    engine: &mut TransformEngine,
    descriptor: &TransformDescriptor,
) -> TransformResult {
    let script = match descriptor {
        TransformDescriptor::Filter { script } => script.clone(),
        _ => {
            return TransformResult::Error(
                "expected Filter descriptor, got a different variant".into(),
            );
        }
    };

    match engine.run_transform_js(&script, &event) {
        Ok(result) => match result.as_str() {
            "true" => TransformResult::PassThrough,
            "false" => TransformResult::Dropped,
            other => {
                TransformResult::Error(format!("filter returned non-boolean value: {other:?}"))
            }
        },
        Err(e) => TransformResult::Error(format!("filter transform failed: {e}")),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{ChangeEvent, Operation, SourceMetadata};

    // ── Helpers ─────────────────────────────────────────────────────────

    fn make_event(after: Option<serde_json::Value>, op: Operation) -> ChangeEvent {
        ChangeEvent {
            op,
            before: None,
            after,
            source: SourceMetadata {
                db: "test".into(),
                schema: "public".into(),
                table: "users".into(),
                ..Default::default()
            },
            ts_ms: 0,
            id: "filter-test-id".into(),
        }
    }

    fn make_update_event(
        before: Option<serde_json::Value>,
        after: Option<serde_json::Value>,
    ) -> ChangeEvent {
        ChangeEvent {
            op: Operation::Update,
            before,
            after,
            source: SourceMetadata {
                db: "test".into(),
                schema: "public".into(),
                table: "users".into(),
                ..Default::default()
            },
            ts_ms: 0,
            id: "filter-test-update".into(),
        }
    }

    fn make_filter(script: &str) -> TransformDescriptor {
        TransformDescriptor::Filter {
            script: script.into(),
        }
    }

    // ── Basic filter behaviour ──────────────────────────────────────────

    /// Filter returning `true` passes the event through.
    #[test]
    fn filter_true_passthrough() {
        let mut engine = TransformEngine::new().expect("engine");
        let event = make_event(Some(serde_json::json!({"id": 1})), Operation::Create);
        let desc = make_filter("function f(e) { return true; }");
        let result = apply_filter(event, &mut engine, &desc);
        assert_eq!(result, TransformResult::PassThrough);
    }

    /// Filter returning `false` drops the event.
    #[test]
    fn filter_false_drops() {
        let mut engine = TransformEngine::new().expect("engine");
        let event = make_event(Some(serde_json::json!({"id": 1})), Operation::Create);
        let desc = make_filter("function f(e) { return false; }");
        let result = apply_filter(event, &mut engine, &desc);
        assert_eq!(result, TransformResult::Dropped);
    }

    /// Filtering based on operation type — keep non-delete events.
    #[test]
    fn filter_keeps_create_ops() {
        let mut engine = TransformEngine::new().expect("engine");
        let event = make_event(
            Some(serde_json::json!({"id": 1, "name": "Alice"})),
            Operation::Create,
        );
        let desc = make_filter("function f(e) { return e.op !== 'd'; }");
        let result = apply_filter(event, &mut engine, &desc);
        assert_eq!(result, TransformResult::PassThrough);
    }

    /// Filtering based on operation type — drop delete events.
    #[test]
    fn filter_drops_delete_ops() {
        let mut engine = TransformEngine::new().expect("engine");
        let event = make_event(None, Operation::Delete);
        // Manually set before for delete events
        let event = ChangeEvent {
            before: Some(serde_json::json!({"id": 1})),
            ..event
        };
        let desc = make_filter("function f(e) { return e.op !== 'd'; }");
        let result = apply_filter(event, &mut engine, &desc);
        assert_eq!(result, TransformResult::Dropped);
    }

    /// Filter preserves update events (neither create nor delete).
    #[test]
    fn filter_preserves_update() {
        let mut engine = TransformEngine::new().expect("engine");
        let event = make_update_event(
            Some(serde_json::json!({"id": 1, "name": "Alice"})),
            Some(serde_json::json!({"id": 1, "name": "Updated"})),
        );
        let desc = make_filter("function f(e) { return e.op === 'u'; }");
        let result = apply_filter(event, &mut engine, &desc);
        assert_eq!(result, TransformResult::PassThrough);
    }

    /// Filter accessing `e.after` fields works.
    #[test]
    fn filter_on_after_field() {
        let mut engine = TransformEngine::new().expect("engine");
        let event = make_event(
            Some(serde_json::json!({"id": 1, "status": "active"})),
            Operation::Create,
        );
        let desc = make_filter("function f(e) { return e.after && e.after.status === 'active'; }");
        let result = apply_filter(event, &mut engine, &desc);
        assert_eq!(result, TransformResult::PassThrough);
    }

    /// Filter returning false based on a field value.
    #[test]
    fn filter_on_after_field_drops() {
        let mut engine = TransformEngine::new().expect("engine");
        let event = make_event(
            Some(serde_json::json!({"id": 1, "status": "inactive"})),
            Operation::Create,
        );
        let desc = make_filter("function f(e) { return e.after && e.after.status === 'active'; }");
        let result = apply_filter(event, &mut engine, &desc);
        assert_eq!(result, TransformResult::Dropped);
    }

    // ── Error cases ─────────────────────────────────────────────────────

    /// Non-filter descriptor returns an error.
    #[test]
    fn non_filter_descriptor_returns_error() {
        let mut engine = TransformEngine::new().expect("engine");
        let event = make_event(Some(serde_json::json!({"x": 1})), Operation::Create);
        let desc = TransformDescriptor::Mask {
            fields: vec!["x".into()],
            strategy: crate::transforms::config::MaskStrategy::Redact,
        };
        let result = apply_filter(event, &mut engine, &desc);
        assert!(matches!(result, TransformResult::Error(_)));
    }

    /// Invalid JS syntax returns an error.
    #[test]
    fn invalid_js_syntax_returns_error() {
        let mut engine = TransformEngine::new().expect("engine");
        let event = make_event(Some(serde_json::json!({"x": 1})), Operation::Create);
        let desc = make_filter("function f(e) { syntax error !!! }");
        let result = apply_filter(event, &mut engine, &desc);
        assert!(matches!(result, TransformResult::Error(_)));
    }

    /// JS runtime error returns an error.
    #[test]
    fn js_runtime_error_returns_error() {
        let mut engine = TransformEngine::new().expect("engine");
        let event = make_event(Some(serde_json::json!({"x": 1})), Operation::Create);
        let desc = make_filter("function f(e) { throw new Error('boom'); }");
        let result = apply_filter(event, &mut engine, &desc);
        assert!(matches!(result, TransformResult::Error(_)));
    }

    /// Non-boolean return value produces an error.
    #[test]
    fn non_boolean_return_errors() {
        let mut engine = TransformEngine::new().expect("engine");
        let event = make_event(Some(serde_json::json!({"x": 1})), Operation::Create);
        // Function returns a number, not a boolean.
        let desc = make_filter("function f(e) { return 42; }");
        let result = apply_filter(event, &mut engine, &desc);
        assert!(matches!(result, TransformResult::Error(_)));
    }

    /// Missing function `f` produces a runtime error.
    #[test]
    fn missing_function_f_errors() {
        let mut engine = TransformEngine::new().expect("engine");
        let event = make_event(Some(serde_json::json!({"x": 1})), Operation::Create);
        // Script does not define `f` — error when engine tries to call it.
        let desc = make_filter("const x = 1;");
        let result = apply_filter(event, &mut engine, &desc);
        assert!(matches!(result, TransformResult::Error(_)));
    }

    // ── Edge cases ──────────────────────────────────────────────────────

    /// Event with null fields (before=None, after=None) is handled.
    #[test]
    fn null_fields_are_handled() {
        let mut engine = TransformEngine::new().expect("engine");
        let event = ChangeEvent {
            op: Operation::Delete,
            before: None,
            after: None,
            source: SourceMetadata {
                db: "test".into(),
                schema: "public".into(),
                table: "t".into(),
                ..Default::default()
            },
            ts_ms: 0,
            id: "null-id".into(),
        };
        // `after` is `None` → omitted from JSON → `undefined` in JS
        let desc = make_filter("function f(e) { return e.after === undefined; }");
        let result = apply_filter(event, &mut engine, &desc);
        assert_eq!(result, TransformResult::PassThrough);
    }

    /// Filter with a complex expression returns the correct boolean.
    #[test]
    fn complex_expression_filter() {
        let mut engine = TransformEngine::new().expect("engine");
        let event = make_event(
            Some(serde_json::json!({"id": 42, "amount": 100.50})),
            Operation::Create,
        );
        let desc = make_filter("function f(e) { return e.after && e.after.amount > 50.0; }");
        let result = apply_filter(event, &mut engine, &desc);
        assert_eq!(result, TransformResult::PassThrough);
    }

    #[test]
    fn complex_expression_filter_below_threshold() {
        let mut engine = TransformEngine::new().expect("engine");
        let event = make_event(
            Some(serde_json::json!({"id": 42, "amount": 10.0})),
            Operation::Create,
        );
        let desc = make_filter("function f(e) { return e.after && e.after.amount > 50.0; }");
        let result = apply_filter(event, &mut engine, &desc);
        assert_eq!(result, TransformResult::Dropped);
    }

    /// Reproduce the non-deterministic failure: sequential independent engines.
    #[test]
    fn sequential_engines_different_bodies() {
        let bodies_and_events: Vec<(&str, ChangeEvent, TransformResult)> = vec![
            (
                "function f(e) { return true; }",
                make_event(Some(serde_json::json!({"id": 1})), Operation::Create),
                TransformResult::PassThrough,
            ),
            (
                "function f(e) { return false; }",
                make_event(Some(serde_json::json!({"id": 2})), Operation::Create),
                TransformResult::Dropped,
            ),
            (
                "function f(e) { return e.op === 'u'; }",
                make_update_event(
                    Some(serde_json::json!({"id": 3})),
                    Some(serde_json::json!({"id": 3})),
                ),
                TransformResult::PassThrough,
            ),
            (
                "function f(e) { return e.before === undefined; }",
                ChangeEvent {
                    op: Operation::Create,
                    before: None,
                    after: Some(serde_json::json!({"id": 4})),
                    source: SourceMetadata {
                        db: "t".into(),
                        ..Default::default()
                    },
                    ts_ms: 0,
                    id: "t4".into(),
                },
                TransformResult::PassThrough,
            ),
        ];

        for (i, (body, event, expected)) in bodies_and_events.into_iter().enumerate() {
            let mut engine = TransformEngine::new().unwrap_or_else(|e| panic!("engine[{i}]: {e}"));
            let desc = make_filter(body);
            let result = apply_filter(event, &mut engine, &desc);
            assert_eq!(result, expected, "engine[{i}] body={body:?}");
            // Drop engine explicitly.
            drop(engine);
        }
    }

    /// Multiple filter evaluations on the same engine do not leak state.
    #[test]
    fn no_cross_eval_state_leakage() {
        let mut engine = TransformEngine::new().expect("engine");
        let desc = make_filter("function f(e) { return e.after && e.after.id > 0; }");

        // Event 1: id=1 → passes
        let e1 = make_event(Some(serde_json::json!({"id": 1})), Operation::Create);
        assert_eq!(
            apply_filter(e1, &mut engine, &desc),
            TransformResult::PassThrough
        );

        // Event 2: id=-1 → dropped
        let e2 = make_event(Some(serde_json::json!({"id": -1})), Operation::Create);
        assert_eq!(
            apply_filter(e2, &mut engine, &desc),
            TransformResult::Dropped
        );

        // Event 3: id=5 → passes (no stale state from event 2)
        let e3 = make_event(Some(serde_json::json!({"id": 5})), Operation::Create);
        assert_eq!(
            apply_filter(e3, &mut engine, &desc),
            TransformResult::PassThrough
        );
    }
}

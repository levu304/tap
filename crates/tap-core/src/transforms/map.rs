//! JavaScript map-transform implementation.
//!
//! Applies a user-defined JavaScript map function to a [`ChangeEvent`].
//! The function receives the full event and must return the (possibly
//! modified) event, which replaces the original in the pipeline.
//!
//! # Map function convention
//!
//! The user's script MUST define a function named `f` that accepts a single
//! argument — the deserialised [`ChangeEvent`] — and returns the modified
//! event:
//!
//! ```js
//! function f(event) {
//!     event.after.name = 'transformed';
//!     return event;
//! }
//! ```
//!
//! The function may add, remove, or update any field.  Required fields
//! (`op`, `source`, `ts_ms`, `id`) must remain present and valid — if
//! they are absent or malformed the transform will return an error.
//!
//! # Integration with the transform engine
//!
//! At runtime [`apply_map`] serialises the event to JSON, embeds it into
//! a temporary JS module that calls the user's function, and uses
//! [`TransformEngine::run_transform_js`] to compile and execute the module.
//! The result JSON string is deserialised back into a [`ChangeEvent`] and
//! returned as [`TransformResult::Modified`].

use crate::event::ChangeEvent;
use crate::transforms::config::{TransformDescriptor, TransformResult};
use crate::transforms::engine::TransformEngine;
use crate::transforms::validate::validate_event_envelope;

/// Apply a `Map` descriptor to a [`ChangeEvent`].
///
/// The user's map script is compiled + executed inside the sandboxed
/// QuickJS runtime.  The modified event returned by the JS function is
/// deserialised and replaces the original in the pipeline.
///
/// # Returns
///
/// | Result | Meaning |
/// |--------|---------|
/// | [`TransformResult::Modified`] | Map returned a valid modified event. |
/// | [`TransformResult::Error`] | Non-map descriptor, JS error, or invalid event returned. |
///
/// # Errors
///
/// Returns [`TransformResult::Error`] when:
///
/// * `descriptor` is not a [`TransformDescriptor::Map`] variant.
/// * The JavaScript source cannot be compiled (syntax error).
/// * The JavaScript execution throws at runtime.
/// * The function returns a value that cannot be deserialised into a
///   [`ChangeEvent`] (e.g. `null`, a number, or an object missing
///   required fields like `id` or `op`).
pub fn apply_map(
    event: ChangeEvent,
    engine: &mut TransformEngine,
    descriptor: &TransformDescriptor,
) -> TransformResult {
    let script = match descriptor {
        TransformDescriptor::Map { script } => script.clone(),
        _ => {
            return TransformResult::Error(
                "expected Map descriptor, got a different variant".into(),
            );
        }
    };

    match engine.run_transform_js(&script, &event) {
        Ok(json_str) => match serde_json::from_str::<ChangeEvent>(&json_str) {
            Ok(modified) => match validate_event_envelope(&modified) {
                Ok(()) => TransformResult::Modified(Box::new(modified)),
                Err(e) => TransformResult::Error(format!("map produced invalid event: {e}")),
            },
            Err(e) => TransformResult::Error(format!("map returned invalid ChangeEvent JSON: {e}")),
        },
        Err(e) => TransformResult::Error(format!("map transform failed: {e}")),
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
            ts_ms: 1_700_000_000_000,
            id: "map-test-id".into(),
        }
    }

    fn make_map(script: &str) -> TransformDescriptor {
        TransformDescriptor::Map {
            script: script.into(),
        }
    }

    /// Assert that the event passes through as `Modified`, then extract the
    /// inner event for further assertions.
    fn expect_modified(result: TransformResult) -> ChangeEvent {
        match result {
            TransformResult::Modified(event) => *event,
            other => panic!("expected Modified, got {other:?}"),
        }
    }

    /// Assert that the result is an error.
    fn expect_error(result: TransformResult) -> String {
        match result {
            TransformResult::Error(msg) => msg,
            other => panic!("expected Error, got {other:?}"),
        }
    }

    // ── Basic map behaviour ─────────────────────────────────────────────

    /// Identity map — returns the event unchanged.
    #[test]
    fn identity_map() {
        let mut engine = TransformEngine::new().expect("engine");
        let event = make_event(
            Some(serde_json::json!({"id": 1, "name": "Alice"})),
            Operation::Create,
        );
        let desc = make_map("function f(e) { return e; }");
        let result = apply_map(event.clone(), &mut engine, &desc);
        let modified = expect_modified(result);
        assert_eq!(modified, event);
    }

    /// Map adds a field to `after`.
    #[test]
    fn map_adds_field() {
        let mut engine = TransformEngine::new().expect("engine");
        let event = make_event(
            Some(serde_json::json!({"id": 1, "name": "Alice"})),
            Operation::Create,
        );
        let desc = make_map("function f(e) { e.after.extra = 'added'; return e; }");
        let result = apply_map(event, &mut engine, &desc);
        let modified = expect_modified(result);
        assert_eq!(modified.after.unwrap().get("extra").unwrap(), "added");
    }

    /// Map removes a field from `after`.
    #[test]
    fn map_removes_field() {
        let mut engine = TransformEngine::new().expect("engine");
        let event = make_event(
            Some(serde_json::json!({"id": 1, "name": "Alice", "secret": "s3cret"})),
            Operation::Create,
        );
        let desc = make_map("function f(e) { delete e.after.secret; return e; }");
        let result = apply_map(event, &mut engine, &desc);
        let modified = expect_modified(result);
        let after = modified.after.unwrap();
        assert!(!after.as_object().unwrap().contains_key("secret"));
        assert_eq!(after.get("name").unwrap(), "Alice");
    }

    /// Map changes the operation type.
    #[test]
    fn map_changes_op() {
        let mut engine = TransformEngine::new().expect("engine");
        let event = make_event(
            Some(serde_json::json!({"id": 1, "name": "Alice"})),
            Operation::Create,
        );
        // Change op from 'c' (create) to 'u' (update)
        let desc = make_map("function f(e) { e.op = 'u'; return e; }");
        let result = apply_map(event, &mut engine, &desc);
        let modified = expect_modified(result);
        assert_eq!(modified.op, Operation::Update);
    }

    /// Map changes source metadata.
    #[test]
    fn map_changes_source_table() {
        let mut engine = TransformEngine::new().expect("engine");
        let event = make_event(Some(serde_json::json!({"id": 1})), Operation::Create);
        let desc = make_map("function f(e) { e.source.table = 'transformed_users'; return e; }");
        let result = apply_map(event, &mut engine, &desc);
        let modified = expect_modified(result);
        assert_eq!(modified.source.table, "transformed_users");
    }

    /// Map returns a completely new event (replaces `after` entirely).
    #[test]
    fn map_replaces_event() {
        let mut engine = TransformEngine::new().expect("engine");
        let event = make_event(
            Some(serde_json::json!({"id": 1, "name": "Alice"})),
            Operation::Create,
        );
        let desc = make_map("function f(e) { e.after = { id: 99, name: 'Bob' }; return e; }");
        let result = apply_map(event, &mut engine, &desc);
        let modified = expect_modified(result);
        let after = modified.after.unwrap();
        assert_eq!(after.get("id").unwrap(), 99);
        assert_eq!(after.get("name").unwrap(), "Bob");
    }

    // ── Error cases ─────────────────────────────────────────────────────

    /// Non-map descriptor returns an error.
    #[test]
    fn non_map_descriptor_returns_error() {
        let mut engine = TransformEngine::new().expect("engine");
        let event = make_event(Some(serde_json::json!({"x": 1})), Operation::Create);
        let desc = TransformDescriptor::Mask {
            fields: vec!["x".into()],
            strategy: crate::transforms::config::MaskStrategy::Redact,
        };
        let result = apply_map(event, &mut engine, &desc);
        assert!(matches!(result, TransformResult::Error(_)));
    }

    /// Invalid JS syntax returns an error.
    #[test]
    fn invalid_js_syntax_returns_error() {
        let mut engine = TransformEngine::new().expect("engine");
        let event = make_event(Some(serde_json::json!({"x": 1})), Operation::Create);
        let desc = make_map("function f(e) { syntax error !!! }");
        let result = apply_map(event, &mut engine, &desc);
        assert!(matches!(result, TransformResult::Error(_)));
    }

    /// JS runtime error returns an error.
    #[test]
    fn js_runtime_error_returns_error() {
        let mut engine = TransformEngine::new().expect("engine");
        let event = make_event(Some(serde_json::json!({"x": 1})), Operation::Create);
        let desc = make_map("function f(e) { throw new Error('map boom'); }");
        let result = apply_map(event, &mut engine, &desc);
        assert!(matches!(result, TransformResult::Error(_)));
    }

    /// Map returning `null` produces an error (cannot deserialise null into
    /// ChangeEvent).
    #[test]
    fn map_returns_null_errors() {
        let mut engine = TransformEngine::new().expect("engine");
        let event = make_event(Some(serde_json::json!({"x": 1})), Operation::Create);
        let desc = make_map("function f(e) { return null; }");
        let result = apply_map(event, &mut engine, &desc);
        let msg = expect_error(result);
        assert!(msg.contains("invalid ChangeEvent"), "msg: {msg}");
    }

    /// Map returning a number produces an error.
    #[test]
    fn map_returns_number_errors() {
        let mut engine = TransformEngine::new().expect("engine");
        let event = make_event(Some(serde_json::json!({"x": 1})), Operation::Create);
        let desc = make_map("function f(e) { return 42; }");
        let result = apply_map(event, &mut engine, &desc);
        let msg = expect_error(result);
        assert!(msg.contains("invalid ChangeEvent"), "msg: {msg}");
    }

    /// Map dropping required fields (e.g. `id`) produces a deserialisation
    /// error.
    #[test]
    fn map_drops_required_field_errors() {
        let mut engine = TransformEngine::new().expect("engine");
        let event = make_event(Some(serde_json::json!({"x": 1})), Operation::Create);
        let desc = make_map("function f(e) { delete e.id; return e; }");
        let result = apply_map(event, &mut engine, &desc);
        let msg = expect_error(result);
        assert!(msg.contains("invalid ChangeEvent"), "msg: {msg}");
    }

    /// Map emptying the `id` field produces an envelope validation error.
    #[test]
    fn map_empty_id_rejected() {
        let mut engine = TransformEngine::new().expect("engine");
        let event = make_event(Some(serde_json::json!({"id": 1})), Operation::Create);
        let desc = make_map("function f(e) { e.id = ''; return e; }");
        let result = apply_map(event, &mut engine, &desc);
        let msg = expect_error(result);
        assert!(msg.contains("invalid event"), "msg: {msg}");
    }

    /// Map returning an invalid `op` value produces a deserialisation error.
    #[test]
    fn map_invalid_op_errors() {
        let mut engine = TransformEngine::new().expect("engine");
        let event = make_event(Some(serde_json::json!({"x": 1})), Operation::Create);
        let desc = make_map("function f(e) { e.op = 'x'; return e; }");
        let result = apply_map(event, &mut engine, &desc);
        let msg = expect_error(result);
        assert!(msg.contains("invalid ChangeEvent"), "msg: {msg}");
    }

    // ── Edge cases ──────────────────────────────────────────────────────

    /// Map preserving a null `after` (delete event) works.
    #[test]
    fn map_preserves_null_after() {
        let mut engine = TransformEngine::new().expect("engine");
        let event = ChangeEvent {
            op: Operation::Delete,
            before: Some(serde_json::json!({"id": 1})),
            after: None,
            source: SourceMetadata {
                db: "test".into(),
                schema: "public".into(),
                table: "t".into(),
                ..Default::default()
            },
            ts_ms: 1_700_000_000_000,
            id: "null-after-id".into(),
        };
        let desc = make_map("function f(e) { return e; }");
        let result = apply_map(event, &mut engine, &desc);
        let modified = expect_modified(result);
        assert!(modified.after.is_none());
        assert!(modified.before.is_some());
    }

    /// Map modifying the `before` field works.
    #[test]
    fn map_modifies_before() {
        let mut engine = TransformEngine::new().expect("engine");
        let event = ChangeEvent {
            op: Operation::Update,
            before: Some(serde_json::json!({"id": 1, "name": "OldName"})),
            after: Some(serde_json::json!({"id": 1, "name": "NewName"})),
            source: SourceMetadata {
                db: "test".into(),
                schema: "public".into(),
                table: "users".into(),
                ..Default::default()
            },
            ts_ms: 1_700_000_000_000,
            id: "mod-before-id".into(),
        };
        let desc = make_map("function f(e) { e.before.name = 'Modified'; return e; }");
        let result = apply_map(event, &mut engine, &desc);
        let modified = expect_modified(result);
        assert_eq!(modified.before.unwrap().get("name").unwrap(), "Modified");
    }

    /// Cross-eval isolation: no stale state between map calls.
    #[test]
    fn no_cross_eval_state_leakage() {
        let mut engine = TransformEngine::new().expect("engine");
        let desc = make_map("function f(e) { e.after.seq = (e.after.seq || 0) + 1; return e; }");

        // First call: seq becomes 1
        let e1 = make_event(Some(serde_json::json!({"id": 1})), Operation::Create);
        let r1 = expect_modified(apply_map(e1, &mut engine, &desc));
        assert_eq!(r1.after.unwrap().get("seq").unwrap(), 1);

        // Second call: starts from 0 again (no global state leak)
        let e2 = make_event(Some(serde_json::json!({"id": 2})), Operation::Create);
        let r2 = expect_modified(apply_map(e2, &mut engine, &desc));
        assert_eq!(r2.after.unwrap().get("seq").unwrap(), 1);
    }

    /// Map preserves Unicode line/paragraph separators in field values
    /// (U+2028 / U+2029) — valid in JSON but must be escaped in JS
    /// single-quoted string literals (tap-bmr).
    #[test]
    fn map_preserves_unicode_line_separator() {
        let mut engine = TransformEngine::new().expect("engine");
        let event = make_event(
            Some(serde_json::json!({"id": 1, "name": "line1\u{2028}line2"})),
            Operation::Create,
        );
        let desc = make_map("function f(e) { return e; }");
        let result = apply_map(event, &mut engine, &desc);
        let modified = expect_modified(result);
        let after = modified.after.unwrap();
        assert_eq!(after.get("name").unwrap(), "line1\u{2028}line2");
    }

    /// Map that only touches `ts_ms` preserves all other fields.
    #[test]
    fn map_only_touches_timestamp() {
        let mut engine = TransformEngine::new().expect("engine");
        let event = make_event(
            Some(serde_json::json!({"id": 1, "name": "Alice"})),
            Operation::Create,
        );
        let desc = make_map("function f(e) { e.ts_ms = 999; return e; }");
        let result = apply_map(event, &mut engine, &desc);
        let modified = expect_modified(result);
        assert_eq!(modified.ts_ms, 999);
        assert_eq!(modified.source.table, "users");
        assert_eq!(modified.op, Operation::Create);
    }
}

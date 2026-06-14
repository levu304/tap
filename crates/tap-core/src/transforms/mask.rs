//! Declarative field-masking transforms.
//!
//! Applies one of three masking strategies (redact, hash, null) to
//! designated fields in a [`ChangeEvent`] without executing JavaScript.
//!
//! # Field paths
//!
//! Fields are specified as dot-separated paths (e.g. `"user.email"`).
//! Nested objects are traversed recursively; non-object intermediates
//! (arrays, primitives) cause the path to be silently skipped.
//!
//! # Masking strategies
//!
//! | Strategy | Result |
//! |----------|--------|
//! | `Redact` | Replaced with the string `***REDACTED***`. |
//! | `Hash`   | Replaced with a deterministic HMAC-SHA-256 hex digest of the original value. |
//! | `Null`   | Replaced with JSON `null`. |
//!
//! # HMAC key
//!
//! The hash strategy uses a fixed internal key.  For production deployments
//! where the hash must not be reproducible with public knowledge, the key
//! should be made configurable (future work).

use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::event::ChangeEvent;
use crate::transforms::config::{MaskStrategy, TransformDescriptor, TransformResult};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default HMAC key for deterministic hashing.
///
/// 32 bytes for HMAC-SHA-256.  This is a fixed development key — replace
/// with a configured secret in production deployments where the hash must
/// be cryptographically opaque.
const HMAC_DEFAULT_KEY: &[u8] = b"tap-mask-hmac-key-v0-32bytes!!!!";

/// Compile-time assertion that [`HMAC_DEFAULT_KEY`] is non-empty.
/// `Hmac::<Sha256>::new_from_slice` accepts any non-empty key length.
const _: () = assert!(!HMAC_DEFAULT_KEY.is_empty(), "HMAC key must not be empty");

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Apply a `Mask` descriptor to a [`ChangeEvent`].
///
/// Traverses each field path in `descriptor.fields` within both
/// `event.before` and `event.after`, replacing leaf values according
/// to `descriptor.strategy`.
///
/// # Returns
///
/// - [`TransformResult::Modified`] — at least one field was masked.
/// - [`TransformResult::PassThrough`] — no matching fields found.
/// - [`TransformResult::Error`] — `descriptor` is not a `Mask` variant.
///
/// Note: [`TransformResult::Dropped`] is not returned by `apply_mask`.
pub fn apply_mask(event: ChangeEvent, descriptor: &TransformDescriptor) -> TransformResult {
    let (fields, strategy) = match descriptor {
        TransformDescriptor::Mask { fields, strategy } => (fields, strategy),
        _ => {
            return TransformResult::Error("expected Mask descriptor".into());
        }
    };

    if fields.is_empty() {
        return TransformResult::PassThrough;
    }

    let mut modified = false;
    let mut event = event;

    // Deduplicate field paths so the same field is never masked twice.
    // Under the Hash strategy a second pass would hash the already-hashed
    // hex digest instead of the original value, producing incorrect results.
    let mut unique_fields = fields.clone();
    unique_fields.sort();
    unique_fields.dedup();

    for field_path in &unique_fields {
        if let Some(ref mut after) = event.after {
            match traverse_and_mask(after, field_path, *strategy) {
                Ok(true) => modified = true,
                Ok(false) => {}
                Err(e) => return TransformResult::Error(e.into()),
            }
        }
        if let Some(ref mut before) = event.before {
            match traverse_and_mask(before, field_path, *strategy) {
                Ok(true) => modified = true,
                Ok(false) => {}
                Err(e) => return TransformResult::Error(e.into()),
            }
        }
    }

    if modified {
        TransformResult::Modified(Box::new(event))
    } else {
        TransformResult::PassThrough
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Walk a dot-separated path into a JSON value tree and mask the leaf.
///
/// Returns `Ok(true)` if a value was modified, `Ok(false)` if no matching
/// field was found, or `Err` if the masking operation itself failed (e.g.
/// invalid HMAC key).
///
/// If `path` contains empty segments (e.g. `"user..email"` or `".email"`)
/// the function silently returns `Ok(false)` — the path is invalid, so no
/// value is modified.
fn traverse_and_mask(
    value: &mut serde_json::Value,
    path: &str,
    strategy: MaskStrategy,
) -> Result<bool, &'static str> {
    // Reject paths with empty segments (leading/trailing dots, double
    // dots) so configuration typos don't silently skip masking.
    if path.is_empty() || path.split('.').any(|s| s.is_empty()) {
        return Ok(false);
    }

    let mut segments = path.split('.');
    // Peel the leaf segment off the end; remaining segments are parents.
    let leaf = segments.next_back();
    let mut current = value;

    for segment in segments {
        current = match current.get_mut(segment) {
            Some(v @ serde_json::Value::Object(_)) => v,
            _ => return Ok(false), // missing or non-object parent → skip
        };
    }

    // SAFETY: `str::split` on any string returns at least one element,
    // so `leaf` is always `Some`.
    match leaf.and_then(|key| current.get_mut(key)) {
        Some(val) => {
            *val = mask_value(val.clone(), strategy)?;
            Ok(true)
        }
        None => Ok(false),
    }
}

/// Produce the masked replacement for a single JSON value.
fn mask_value(
    value: serde_json::Value,
    strategy: MaskStrategy,
) -> Result<serde_json::Value, &'static str> {
    match strategy {
        MaskStrategy::Redact => Ok(serde_json::Value::String("***REDACTED***".into())),
        MaskStrategy::Hash => {
            // Use a single canonical byte representation for all JSON
            // types via serde_json serialization.  Using `serde_json::to_string`
            // ensures String("42") and Number(42) produce different byte
            // sequences (the former includes JSON quotes).
            let input = serde_json::to_string(&value)
                .map_err(|_| "failed to serialize JSON value for hashing")?
                .into_bytes();
            let mut mac = Hmac::<Sha256>::new_from_slice(HMAC_DEFAULT_KEY)
                .map_err(|_| "invalid HMAC key length")?;
            mac.update(&input);
            let code = mac.finalize().into_bytes();
            Ok(serde_json::Value::String(hex::encode(code)))
        }
        MaskStrategy::Null => Ok(serde_json::Value::Null),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{ChangeEvent, Operation, SourceMetadata};
    use crate::transforms::config::MaskStrategy;

    /// Unwrap a [`TransformResult::Modified`], extracting the inner
    /// [`ChangeEvent`] so callers can inspect masked fields.
    fn expect_modified(result: TransformResult) -> ChangeEvent {
        match result {
            TransformResult::Modified(e) => *e,
            other => panic!("expected Modified, got {other:?}"),
        }
    }

    // ── Helpers ────────────────────────────────────────────────────────

    fn make_event(after: Option<serde_json::Value>) -> ChangeEvent {
        ChangeEvent {
            op: Operation::Create,
            before: None,
            after,
            source: SourceMetadata {
                db: "test".into(),
                schema: "public".into(),
                table: "users".into(),
                ..Default::default()
            },
            ts_ms: 0,
            id: "test-id".into(),
        }
    }

    fn make_descriptor(fields: Vec<&str>, strategy: MaskStrategy) -> TransformDescriptor {
        TransformDescriptor::Mask {
            fields: fields.into_iter().map(String::from).collect(),
            strategy,
        }
    }

    // ── Redact ─────────────────────────────────────────────────────────

    #[test]
    fn redact_top_level_string() {
        let event = make_event(Some(serde_json::json!({"email": "alice@example.com"})));
        let desc = make_descriptor(vec!["email"], MaskStrategy::Redact);
        let event = expect_modified(apply_mask(event, &desc));
        assert_eq!(
            event.after.as_ref().and_then(|v| v.get("email")),
            Some(&serde_json::json!("***REDACTED***"))
        );
    }

    #[test]
    fn redact_top_level_number() {
        let event = make_event(Some(serde_json::json!({"ssn": 123456789})));
        let desc = make_descriptor(vec!["ssn"], MaskStrategy::Redact);
        let event = expect_modified(apply_mask(event, &desc));
        assert_eq!(
            event.after.as_ref().and_then(|v| v.get("ssn")),
            Some(&serde_json::json!("***REDACTED***"))
        );
    }

    #[test]
    fn redact_nested_field() {
        let event = make_event(Some(serde_json::json!({
            "user": {"email": "bob@example.com", "name": "Bob"}
        })));
        let desc = make_descriptor(vec!["user.email"], MaskStrategy::Redact);
        let event = expect_modified(apply_mask(event, &desc));
        assert_eq!(
            event.after.as_ref().and_then(|v| v.pointer("/user/email")),
            Some(&serde_json::json!("***REDACTED***"))
        );
        // Sibling field unchanged
        assert_eq!(
            event.after.as_ref().and_then(|v| v.pointer("/user/name")),
            Some(&serde_json::json!("Bob"))
        );
    }

    #[test]
    fn redact_before_and_after() {
        let event = ChangeEvent {
            before: Some(serde_json::json!({"email": "old@example.com"})),
            after: Some(serde_json::json!({"email": "new@example.com"})),
            ..make_event(None)
        };
        let desc = make_descriptor(vec!["email"], MaskStrategy::Redact);
        let event = expect_modified(apply_mask(event, &desc));
        assert_eq!(
            event.before.as_ref().and_then(|v| v.get("email")),
            Some(&serde_json::json!("***REDACTED***"))
        );
        assert_eq!(
            event.after.as_ref().and_then(|v| v.get("email")),
            Some(&serde_json::json!("***REDACTED***"))
        );
    }

    #[test]
    fn redact_delete_event_before_only() {
        // A delete event has `before: Some(...)` and `after: None`.
        // Masking must apply to the `before` value.
        let event = ChangeEvent {
            op: Operation::Delete,
            before: Some(serde_json::json!({"email": "delete@example.com"})),
            after: None,
            ..make_event(None)
        };
        let desc = make_descriptor(vec!["email"], MaskStrategy::Redact);
        let event = expect_modified(apply_mask(event, &desc));
        assert_eq!(
            event.before.as_ref().and_then(|v| v.get("email")),
            Some(&serde_json::json!("***REDACTED***"))
        );
    }

    // ── Hash ───────────────────────────────────────────────────────────

    #[test]
    fn hash_top_level_string() {
        let event = make_event(Some(serde_json::json!({"email": "alice@example.com"})));
        let desc = make_descriptor(vec!["email"], MaskStrategy::Hash);
        let event = expect_modified(apply_mask(event, &desc));
        let masked = event.after.as_ref().and_then(|v| v.get("email"));
        assert!(masked.is_some());
        let hex_str = masked.unwrap().as_str().unwrap();
        // HMAC-SHA-256 produces 32 bytes = 64 hex chars
        assert_eq!(hex_str.len(), 64);
        assert!(hex_str.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn hash_is_deterministic() {
        let json = serde_json::json!({"field": "hello"});
        let desc = make_descriptor(vec!["field"], MaskStrategy::Hash);

        let e1 = expect_modified(apply_mask(make_event(Some(json.clone())), &desc));
        let h1 = e1.after.unwrap()["field"].as_str().unwrap().to_string();

        let e2 = expect_modified(apply_mask(make_event(Some(json)), &desc));
        let h2 = e2.after.unwrap()["field"].as_str().unwrap().to_string();

        assert_eq!(h1, h2, "hash must be deterministic for same input");
    }

    #[test]
    fn duplicate_paths_dont_double_hash() {
        let json = serde_json::json!({"field": "hello"});

        let e1 = expect_modified(apply_mask(
            make_event(Some(json.clone())),
            &make_descriptor(vec!["field"], MaskStrategy::Hash),
        ));
        let ref_hash = e1.after.unwrap()["field"].as_str().unwrap().to_string();

        let e2 = expect_modified(apply_mask(
            make_event(Some(json)),
            &make_descriptor(vec!["field", "field"], MaskStrategy::Hash),
        ));
        let dup_hash = e2.after.unwrap()["field"].as_str().unwrap().to_string();

        assert_eq!(ref_hash, dup_hash, "duplicate paths must not double-hash");
    }

    #[test]
    fn hash_different_inputs_differ() {
        let desc = make_descriptor(vec!["field"], MaskStrategy::Hash);

        let e1 = expect_modified(apply_mask(
            make_event(Some(serde_json::json!({"field": "alice"}))),
            &desc,
        ));
        let h1 = e1.after.unwrap()["field"].as_str().unwrap().to_string();

        let e2 = expect_modified(apply_mask(
            make_event(Some(serde_json::json!({"field": "bob"}))),
            &desc,
        ));
        let h2 = e2.after.unwrap()["field"].as_str().unwrap().to_string();

        assert_ne!(h1, h2, "different inputs must produce different hashes");
    }

    #[test]
    fn hash_number_field() {
        let event = make_event(Some(serde_json::json!({"id": 42})));
        let desc = make_descriptor(vec!["id"], MaskStrategy::Hash);
        let event = expect_modified(apply_mask(event, &desc));
        let masked = event.after.as_ref().and_then(|v| v.get("id"));
        assert!(masked.unwrap().is_string());
        assert_eq!(masked.unwrap().as_str().unwrap().len(), 64);
    }

    #[test]
    fn hash_boolean_field() {
        let event = make_event(Some(serde_json::json!({"active": true})));
        let desc = make_descriptor(vec!["active"], MaskStrategy::Hash);
        let event = expect_modified(apply_mask(event, &desc));
        let masked = event.after.as_ref().and_then(|v| v.get("active"));
        assert!(masked.unwrap().is_string());
    }

    #[test]
    fn hash_string_and_number_produce_distinct_hashes() {
        // Regression: String("42") must NOT produce the same hash as Number(42).
        // The Hash strategy must use a canonical byte representation that
        // distinguishes between JSON types (strings include JSON quotes).
        let desc = make_descriptor(vec!["field"], MaskStrategy::Hash);

        let e_str = expect_modified(apply_mask(
            make_event(Some(serde_json::json!({"field": "42"}))),
            &desc,
        ));
        let h_str = e_str.after.unwrap()["field"].as_str().unwrap().to_string();

        let e_num = expect_modified(apply_mask(
            make_event(Some(serde_json::json!({"field": 42}))),
            &desc,
        ));
        let h_num = e_num.after.unwrap()["field"].as_str().unwrap().to_string();

        assert_ne!(h_str, h_num, "String and Number must hash differently");
    }

    // ── Null ───────────────────────────────────────────────────────────

    #[test]
    fn null_top_level_string() {
        let event = make_event(Some(serde_json::json!({"email": "alice@example.com"})));
        let desc = make_descriptor(vec!["email"], MaskStrategy::Null);
        let event = expect_modified(apply_mask(event, &desc));
        assert_eq!(
            event.after.as_ref().and_then(|v| v.get("email")),
            Some(&serde_json::Value::Null)
        );
    }

    #[test]
    fn null_nested_field() {
        let event = make_event(Some(serde_json::json!({
            "user": {"ssn": "123-45-6789"}
        })));
        let desc = make_descriptor(vec!["user.ssn"], MaskStrategy::Null);
        let event = expect_modified(apply_mask(event, &desc));
        assert_eq!(
            event.after.as_ref().and_then(|v| v.pointer("/user/ssn")),
            Some(&serde_json::Value::Null)
        );
    }

    // ── Edge cases ─────────────────────────────────────────────────────

    #[test]
    fn non_mask_descriptor_returns_error() {
        let event = make_event(Some(serde_json::json!({"x": 1})));
        let desc = TransformDescriptor::Filter {
            script: "() => true".into(),
        };
        let result = apply_mask(event, &desc);
        assert!(matches!(result, TransformResult::Error(_)));
    }

    #[test]
    fn empty_fields_passthrough() {
        let event = make_event(Some(serde_json::json!({"x": 1})));
        let desc = make_descriptor(vec![], MaskStrategy::Redact);
        let result = apply_mask(event, &desc);
        assert_eq!(result, TransformResult::PassThrough);
    }

    #[test]
    fn non_existent_field_passthrough() {
        let event = make_event(Some(serde_json::json!({"x": 1})));
        let desc = make_descriptor(vec!["y"], MaskStrategy::Redact);
        let result = apply_mask(event, &desc);
        assert_eq!(result, TransformResult::PassThrough);
    }

    #[test]
    fn non_existent_nested_field_passthrough() {
        let event = make_event(Some(serde_json::json!({"x": {"y": 1}})));
        let desc = make_descriptor(vec!["x.z"], MaskStrategy::Redact);
        let result = apply_mask(event, &desc);
        assert_eq!(result, TransformResult::PassThrough);
    }

    #[test]
    fn missing_before_and_after_passthrough() {
        let event = ChangeEvent {
            before: None,
            after: None,
            ..make_event(None)
        };
        let desc = make_descriptor(vec!["email"], MaskStrategy::Redact);
        let result = apply_mask(event, &desc);
        assert_eq!(result, TransformResult::PassThrough);
    }

    #[test]
    fn after_only_produces_modified() {
        let event = ChangeEvent {
            before: None,
            after: Some(serde_json::json!({"email": "alice@example.com"})),
            ..make_event(None)
        };
        let desc = make_descriptor(vec!["email"], MaskStrategy::Redact);
        let result = apply_mask(event, &desc);
        assert!(matches!(result, TransformResult::Modified(_)));
    }

    #[test]
    fn empty_path_segment_skips_masking() {
        // A path with an empty segment (e.g., "user..email") must not mask
        // the field — the path is invalid and should silently pass through.
        let event = make_event(Some(serde_json::json!({"user": {"email": "a@b.com"}})));
        let before = event.clone();
        let desc = make_descriptor(vec!["user..email"], MaskStrategy::Redact);
        let result = apply_mask(event, &desc);
        assert_eq!(result, TransformResult::PassThrough);
        // The field must remain unmasked — clone is unchanged.
        assert_eq!(
            before.after.as_ref().and_then(|v| v.pointer("/user/email")),
            Some(&serde_json::json!("a@b.com"))
        );
    }

    #[test]
    fn leading_dot_path_skips_masking() {
        let event = make_event(Some(serde_json::json!({"email": "a@b.com"})));
        let desc = make_descriptor(vec![".email"], MaskStrategy::Redact);
        let result = apply_mask(event, &desc);
        assert_eq!(result, TransformResult::PassThrough);
    }

    #[test]
    fn trailing_dot_path_skips_masking() {
        let event = make_event(Some(serde_json::json!({"email": "a@b.com"})));
        let desc = make_descriptor(vec!["email."], MaskStrategy::Redact);
        let result = apply_mask(event, &desc);
        assert_eq!(result, TransformResult::PassThrough);
    }

    #[test]
    fn empty_path_skips_masking() {
        let event = make_event(Some(serde_json::json!({"email": "a@b.com"})));
        let desc = make_descriptor(vec![""], MaskStrategy::Redact);
        let result = apply_mask(event, &desc);
        assert_eq!(result, TransformResult::PassThrough);
    }

    #[test]
    fn intermediate_is_array_skips_path() {
        // If an intermediate segment is an array (not an object), the
        // path is silently skipped rather than panicking.
        let event = make_event(Some(serde_json::json!({
            "items": [{"email": "a@b.com"}]
        })));
        let desc = make_descriptor(vec!["items.email"], MaskStrategy::Redact);
        let result = apply_mask(event, &desc);
        assert_eq!(result, TransformResult::PassThrough);
    }

    #[test]
    fn multiple_fields_all_masked() {
        let event = make_event(Some(serde_json::json!({
            "email": "a@b.com",
            "phone": "555-0100",
            "name": "Alice"
        })));
        let desc = make_descriptor(vec!["email", "phone"], MaskStrategy::Redact);
        let event = expect_modified(apply_mask(event, &desc));
        assert_eq!(
            event.after.as_ref().and_then(|v| v.get("email")),
            Some(&serde_json::json!("***REDACTED***"))
        );
        assert_eq!(
            event.after.as_ref().and_then(|v| v.get("phone")),
            Some(&serde_json::json!("***REDACTED***"))
        );
        // name unchanged
        assert_eq!(
            event.after.as_ref().and_then(|v| v.get("name")),
            Some(&serde_json::json!("Alice"))
        );
    }

    #[test]
    fn hmac_key_is_valid() {
        // Verify the default HMAC key is accepted by Hmac::new_from_slice
        // (any non-empty key is valid for HMAC-SHA-256).
        assert!(
            Hmac::<Sha256>::new_from_slice(HMAC_DEFAULT_KEY).is_ok(),
            "HMAC_DEFAULT_KEY must be accepted by Hmac"
        );
    }

    // ── Integration: round-trip ────────────────────────────────────────

    #[test]
    fn mask_result_contains_original_event_fields() {
        let event = make_event(Some(serde_json::json!({
            "email": "a@b.com",
            "name": "Alice",
            "age": 30,
        })));
        let desc = make_descriptor(vec!["email"], MaskStrategy::Redact);
        let result = apply_mask(event, &desc);
        match result {
            TransformResult::Modified(ref e) => {
                assert_eq!(e.id, "test-id");
                assert_eq!(e.op, Operation::Create);
                assert_eq!(e.source.table, "users");
                // Unmasked fields survive
                let after = e.after.as_ref().unwrap();
                assert_eq!(after["name"], "Alice");
                assert_eq!(after["age"], 30);
            }
            other => panic!("expected Modified, got {other:?}"),
        }
    }
}

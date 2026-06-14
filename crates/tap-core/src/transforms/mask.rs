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
pub fn apply_mask(event: &mut ChangeEvent, descriptor: &TransformDescriptor) -> TransformResult {
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

    for field_path in fields {
        if let Some(ref mut after) = event.after {
            if traverse_and_mask(after, field_path, *strategy) {
                modified = true;
            }
        }
        if let Some(ref mut before) = event.before {
            if traverse_and_mask(before, field_path, *strategy) {
                modified = true;
            }
        }
    }

    if modified {
        TransformResult::Modified(Box::new(event.clone()))
    } else {
        TransformResult::PassThrough
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Walk a dot-separated path into a JSON value tree and mask the leaf.
///
/// Returns `true` if a value was actually modified.
fn traverse_and_mask(value: &mut serde_json::Value, path: &str, strategy: MaskStrategy) -> bool {
    let mut segments = path.split('.');
    // Peel the leaf segment off the end; remaining segments are parents.
    let leaf = segments.next_back();
    let mut current = value;

    for segment in segments {
        current = match current.get_mut(segment) {
            Some(v @ serde_json::Value::Object(_)) => v,
            _ => return false, // missing or non-object parent → skip
        };
    }

    // SAFETY: `str::split` on any string returns at least one element,
    // so `leaf` is always `Some`.
    match leaf.and_then(|key| current.get_mut(key)) {
        Some(val) => {
            *val = mask_value(std::mem::take(val), strategy);
            true
        }
        None => false,
    }
}

/// Produce the masked replacement for a single JSON value.
fn mask_value(value: serde_json::Value, strategy: MaskStrategy) -> serde_json::Value {
    match strategy {
        MaskStrategy::Redact => serde_json::Value::String("***REDACTED***".into()),
        MaskStrategy::Hash => {
            let input: Vec<u8> = match &value {
                serde_json::Value::String(s) => s.as_bytes().to_vec(),
                other => other.to_string().into_bytes(),
            };
            let mut mac = Hmac::<Sha256>::new_from_slice(HMAC_DEFAULT_KEY)
                .expect("HMAC default key is valid 32-byte key");
            mac.update(&input);
            let code = mac.finalize().into_bytes();
            serde_json::Value::String(hex::encode(code))
        }
        MaskStrategy::Null => serde_json::Value::Null,
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
        let mut event = make_event(Some(serde_json::json!({"email": "alice@example.com"})));
        let desc = make_descriptor(vec!["email"], MaskStrategy::Redact);
        let result = apply_mask(&mut event, &desc);
        assert!(matches!(result, TransformResult::Modified(_)));
        assert_eq!(
            event.after.as_ref().and_then(|v| v.get("email")),
            Some(&serde_json::json!("***REDACTED***"))
        );
    }

    #[test]
    fn redact_top_level_number() {
        let mut event = make_event(Some(serde_json::json!({"ssn": 123456789})));
        let desc = make_descriptor(vec!["ssn"], MaskStrategy::Redact);
        let result = apply_mask(&mut event, &desc);
        assert!(matches!(result, TransformResult::Modified(_)));
        assert_eq!(
            event.after.as_ref().and_then(|v| v.get("ssn")),
            Some(&serde_json::json!("***REDACTED***"))
        );
    }

    #[test]
    fn redact_nested_field() {
        let mut event = make_event(Some(serde_json::json!({
            "user": {"email": "bob@example.com", "name": "Bob"}
        })));
        let desc = make_descriptor(vec!["user.email"], MaskStrategy::Redact);
        let result = apply_mask(&mut event, &desc);
        assert!(matches!(result, TransformResult::Modified(_)));
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
        let mut event = ChangeEvent {
            before: Some(serde_json::json!({"email": "old@example.com"})),
            after: Some(serde_json::json!({"email": "new@example.com"})),
            ..make_event(None)
        };
        let desc = make_descriptor(vec!["email"], MaskStrategy::Redact);
        let result = apply_mask(&mut event, &desc);
        assert!(matches!(result, TransformResult::Modified(_)));
        assert_eq!(
            event.before.as_ref().and_then(|v| v.get("email")),
            Some(&serde_json::json!("***REDACTED***"))
        );
        assert_eq!(
            event.after.as_ref().and_then(|v| v.get("email")),
            Some(&serde_json::json!("***REDACTED***"))
        );
    }

    // ── Hash ───────────────────────────────────────────────────────────

    #[test]
    fn hash_top_level_string() {
        let mut event = make_event(Some(serde_json::json!({"email": "alice@example.com"})));
        let desc = make_descriptor(vec!["email"], MaskStrategy::Hash);
        let result = apply_mask(&mut event, &desc);
        assert!(matches!(result, TransformResult::Modified(_)));
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

        let mut event1 = make_event(Some(json.clone()));
        let desc = make_descriptor(vec!["field"], MaskStrategy::Hash);
        let _ = apply_mask(&mut event1, &desc);
        let h1 = event1
            .after
            .unwrap()
            .get("field")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();

        let mut event2 = make_event(Some(json));
        let _ = apply_mask(&mut event2, &desc);
        let h2 = event2
            .after
            .unwrap()
            .get("field")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();

        assert_eq!(h1, h2, "hash must be deterministic for same input");
    }

    #[test]
    fn hash_different_inputs_differ() {
        let mut event1 = make_event(Some(serde_json::json!({"field": "alice"})));
        let desc = make_descriptor(vec!["field"], MaskStrategy::Hash);
        let _ = apply_mask(&mut event1, &desc);
        let h1 = event1
            .after
            .unwrap()
            .get("field")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();

        let mut event2 = make_event(Some(serde_json::json!({"field": "bob"})));
        let _ = apply_mask(&mut event2, &desc);
        let h2 = event2
            .after
            .unwrap()
            .get("field")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();

        assert_ne!(h1, h2, "different inputs must produce different hashes");
    }

    #[test]
    fn hash_number_field() {
        let mut event = make_event(Some(serde_json::json!({"id": 42})));
        let desc = make_descriptor(vec!["id"], MaskStrategy::Hash);
        let result = apply_mask(&mut event, &desc);
        assert!(matches!(result, TransformResult::Modified(_)));
        let masked = event.after.as_ref().and_then(|v| v.get("id"));
        assert!(masked.unwrap().is_string());
        assert_eq!(masked.unwrap().as_str().unwrap().len(), 64);
    }

    #[test]
    fn hash_boolean_field() {
        let mut event = make_event(Some(serde_json::json!({"active": true})));
        let desc = make_descriptor(vec!["active"], MaskStrategy::Hash);
        apply_mask(&mut event, &desc);
        let masked = event.after.as_ref().and_then(|v| v.get("active"));
        assert!(masked.unwrap().is_string());
    }

    // ── Null ───────────────────────────────────────────────────────────

    #[test]
    fn null_top_level_string() {
        let mut event = make_event(Some(serde_json::json!({"email": "alice@example.com"})));
        let desc = make_descriptor(vec!["email"], MaskStrategy::Null);
        let result = apply_mask(&mut event, &desc);
        assert!(matches!(result, TransformResult::Modified(_)));
        assert_eq!(
            event.after.as_ref().and_then(|v| v.get("email")),
            Some(&serde_json::Value::Null)
        );
    }

    #[test]
    fn null_nested_field() {
        let mut event = make_event(Some(serde_json::json!({
            "user": {"ssn": "123-45-6789"}
        })));
        let desc = make_descriptor(vec!["user.ssn"], MaskStrategy::Null);
        apply_mask(&mut event, &desc);
        assert_eq!(
            event.after.as_ref().and_then(|v| v.pointer("/user/ssn")),
            Some(&serde_json::Value::Null)
        );
    }

    // ── Edge cases ─────────────────────────────────────────────────────

    #[test]
    fn non_mask_descriptor_returns_error() {
        let mut event = make_event(Some(serde_json::json!({"x": 1})));
        let desc = TransformDescriptor::Filter {
            script: "() => true".into(),
        };
        let result = apply_mask(&mut event, &desc);
        assert!(matches!(result, TransformResult::Error(_)));
    }

    #[test]
    fn empty_fields_passthrough() {
        let mut event = make_event(Some(serde_json::json!({"x": 1})));
        let desc = make_descriptor(vec![], MaskStrategy::Redact);
        let result = apply_mask(&mut event, &desc);
        assert_eq!(result, TransformResult::PassThrough);
    }

    #[test]
    fn non_existent_field_passthrough() {
        let mut event = make_event(Some(serde_json::json!({"x": 1})));
        let desc = make_descriptor(vec!["y"], MaskStrategy::Redact);
        let result = apply_mask(&mut event, &desc);
        assert_eq!(result, TransformResult::PassThrough);
    }

    #[test]
    fn non_existent_nested_field_passthrough() {
        let mut event = make_event(Some(serde_json::json!({"x": {"y": 1}})));
        let desc = make_descriptor(vec!["x.z"], MaskStrategy::Redact);
        let result = apply_mask(&mut event, &desc);
        assert_eq!(result, TransformResult::PassThrough);
    }

    #[test]
    fn missing_before_and_after_passthrough() {
        let mut event = ChangeEvent {
            before: None,
            after: None,
            ..make_event(None)
        };
        let desc = make_descriptor(vec!["email"], MaskStrategy::Redact);
        let result = apply_mask(&mut event, &desc);
        assert_eq!(result, TransformResult::PassThrough);
    }

    #[test]
    fn after_only_produces_modified() {
        let mut event = ChangeEvent {
            before: None,
            after: Some(serde_json::json!({"email": "alice@example.com"})),
            ..make_event(None)
        };
        let desc = make_descriptor(vec!["email"], MaskStrategy::Redact);
        let result = apply_mask(&mut event, &desc);
        assert!(matches!(result, TransformResult::Modified(_)));
    }

    #[test]
    fn intermediate_is_array_skips_path() {
        // If an intermediate segment is an array (not an object), the
        // path is silently skipped rather than panicking.
        let mut event = make_event(Some(serde_json::json!({
            "items": [{"email": "a@b.com"}]
        })));
        let desc = make_descriptor(vec!["items.email"], MaskStrategy::Redact);
        let result = apply_mask(&mut event, &desc);
        assert_eq!(result, TransformResult::PassThrough);
    }

    #[test]
    fn multiple_fields_all_masked() {
        let mut event = make_event(Some(serde_json::json!({
            "email": "a@b.com",
            "phone": "555-0100",
            "name": "Alice"
        })));
        let desc = make_descriptor(vec!["email", "phone"], MaskStrategy::Redact);
        let result = apply_mask(&mut event, &desc);
        assert!(matches!(result, TransformResult::Modified(_)));
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

    // ── Integration: round-trip ────────────────────────────────────────

    #[test]
    fn mask_result_contains_original_event_fields() {
        let mut event = make_event(Some(serde_json::json!({
            "email": "a@b.com",
            "name": "Alice",
            "age": 30,
        })));
        let desc = make_descriptor(vec!["email"], MaskStrategy::Redact);
        let result = apply_mask(&mut event, &desc);
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

//! Transform configuration types.
//!
//! Defines the types used to describe, configure, and report results of
//! data-transformation pipelines — filter, map, and mask — that run
//! inside the Transform Engine (Phase 5).
//!
//! # TOML example
//!
//! ```toml
//! failClosed = false
//!
//! [[transforms]]
//! type = "filter"
//! script = "function f(e) { return e.op !== 'd'; }"
//!
//! [[transforms]]
//! type = "map"
//! script = """
//!   function m(e) {
//!     e.after.full_name = e.after.first + ' ' + e.after.last;
//!     return e;
//!   }
//! """
//!
//! [[transforms]]
//! type = "mask"
//! fields = ["email", "phone"]
//! strategy = "hash"
//! ```

use serde::{Deserialize, Serialize};

use crate::event::ChangeEvent;

// ---------------------------------------------------------------------------
// TransformConfig
// ---------------------------------------------------------------------------

/// The `[transforms]` section of a Tap configuration file.
///
/// Contains an ordered pipeline of [`TransformDescriptor`] entries that are
/// applied to each `ChangeEvent` before it is delivered to downstream
/// consumers.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct TransformConfig {
    /// Ordered list of transform descriptors to apply to every event.
    #[serde(default)]
    pub transforms: Vec<TransformDescriptor>,

    /// Behaviour when a transform produces an error:
    ///
    /// - `false` (default) — **fail-open**: the event passes through
    ///   unchanged and the error is logged.
    /// - `true` — **fail-closed**: the event is dropped and the error is
    ///   surfaced (e.g. via a health-check endpoint).
    #[serde(default)]
    pub fail_closed: bool,
}

// ---------------------------------------------------------------------------
// TransformDescriptor
// ---------------------------------------------------------------------------

/// One step in a transform pipeline.
///
/// Each variant describes a different kind of transformation:
///
/// | Variant | Effect |
/// |---------|--------|
/// | `Filter` | Runs a JavaScript function; if it returns `false` the event is dropped. |
/// | `Map`    | Runs a JavaScript function that receives and returns a (possibly modified) event. |
/// | `Mask`   | Replaces designated field values using the chosen [`MaskStrategy`] (no JS needed). |
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TransformDescriptor {
    /// A JavaScript filter: `function(event): boolean`.
    ///
    /// Returning `false` causes the event to be dropped from the pipeline.
    /// Returning `true` passes the event through unchanged.
    Filter {
        /// Inline JavaScript source code, or a path to a `.js` / `.wasm` file
        /// containing the filter function.
        script: String,
    },

    /// A JavaScript map: `function(event): event`.
    ///
    /// Receives the full [`ChangeEvent`] and must return the (possibly
    /// modified) event.  The function may add, remove, or update any field.
    Map {
        /// Inline JavaScript source code, or a path to a `.js` / `.wasm` file
        /// containing the map function.
        script: String,
    },

    /// A declarative field-masking step.
    ///
    /// No JavaScript is required — the engine replaces each listed field's
    /// value according to the chosen [`MaskStrategy`].  Nested fields can be
    /// addressed with dot-separated paths (e.g. `"user.email"`).
    Mask {
        /// Fields to mask (dot-separated paths for nested access).
        fields: Vec<String>,

        /// Masking strategy to apply to each field.
        #[serde(default)]
        strategy: MaskStrategy,
    },
}

// ---------------------------------------------------------------------------
// MaskStrategy
// ---------------------------------------------------------------------------

/// Strategy for replacing sensitive field values.
///
/// | Strategy | TOML value | Behaviour |
/// |----------|------------|-----------|
/// | `Redact` | `"redact"` | Replace the value with the string `***REDACTED***`. |
/// | `Hash`   | `"hash"`   | Replace the value with a deterministic HMAC-SHA-256 digest (hex-encoded). |
/// | `Null`   | `"null"`   | Replace the value with JSON `null`. |
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaskStrategy {
    /// Replace with the literal string `***REDACTED***`.
    #[default]
    Redact,
    /// Replace with a deterministic HMAC-SHA-256 hex digest.
    Hash,
    /// Replace with JSON `null`.
    Null,
}

// ---------------------------------------------------------------------------
// TransformResult
// ---------------------------------------------------------------------------

/// The outcome of applying a [`TransformDescriptor`] to a [`ChangeEvent`].
///
/// This enum lets the engine decide how to route the event after each step.
#[derive(Debug, Clone, PartialEq)]
pub enum TransformResult {
    /// The event passed through this transform unchanged (filter returned
    /// `true`, or no matching fields for a mask).
    PassThrough,

    /// The event was dropped by a filter (filter returned `false`).
    Dropped,

    /// The event was modified by a map or mask transform.
    Modified(Box<ChangeEvent>),

    /// The transform encountered an error.
    ///
    /// Whether the error is fatal depends on the engine's `fail_closed`
    /// setting — if `fail_closed` is `false` (the default) the event is
    /// treated as `PassThrough` and the error is logged.
    Error(String),
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // TransformConfig
    // -----------------------------------------------------------------------

    #[test]
    fn test_transform_config_default() {
        let cfg = TransformConfig::default();
        assert!(cfg.transforms.is_empty());
        assert!(!cfg.fail_closed);
    }

    #[test]
    fn test_transform_config_empty_pipeline() {
        let cfg: TransformConfig = toml::from_str("").expect("empty transforms");
        assert!(cfg.transforms.is_empty());
        assert!(!cfg.fail_closed);
    }

    #[test]
    fn test_transform_config_fail_closed() {
        let toml_str = "failClosed = true";
        let cfg: TransformConfig = toml::from_str(toml_str).expect("fail_closed");
        assert!(cfg.fail_closed);
    }

    // -----------------------------------------------------------------------
    // Filter descriptor
    // -----------------------------------------------------------------------

    #[test]
    fn test_transform_descriptor_filter() {
        let toml_str = r#"
        [[transforms]]
        type = "filter"
        script = "function f(e) { return e.op !== 'd'; }"
        "#;
        let cfg: TransformConfig = toml::from_str(toml_str).expect("filter descriptor");
        assert_eq!(cfg.transforms.len(), 1);

        match &cfg.transforms[0] {
            TransformDescriptor::Filter { script } => {
                assert!(script.contains("function f"));
            }
            other => panic!("expected Filter, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Map descriptor
    // -----------------------------------------------------------------------

    #[test]
    fn test_transform_descriptor_map() {
        let toml_str = r#"
        [[transforms]]
        type = "map"
        script = "function m(e) { e.after.name = 'test'; return e; }"
        "#;
        let cfg: TransformConfig = toml::from_str(toml_str).expect("map descriptor");
        assert_eq!(cfg.transforms.len(), 1);

        match &cfg.transforms[0] {
            TransformDescriptor::Map { script } => {
                assert!(script.contains("function m"));
            }
            other => panic!("expected Map, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Mask descriptor
    // -----------------------------------------------------------------------

    #[test]
    fn test_transform_descriptor_mask_default_strategy() {
        let toml_str = r#"
        [[transforms]]
        type = "mask"
        fields = ["email"]
        "#;
        let cfg: TransformConfig = toml::from_str(toml_str).expect("mask default");
        assert_eq!(cfg.transforms.len(), 1);

        match &cfg.transforms[0] {
            TransformDescriptor::Mask { fields, strategy } => {
                assert_eq!(fields.len(), 1);
                assert_eq!(fields[0], "email");
                assert_eq!(*strategy, MaskStrategy::Redact);
            }
            other => panic!("expected Mask, got {other:?}"),
        }
    }

    #[test]
    fn test_transform_descriptor_mask_hash_strategy() {
        let toml_str = r#"
        [[transforms]]
        type = "mask"
        fields = ["email", "phone"]
        strategy = "hash"
        "#;
        let cfg: TransformConfig = toml::from_str(toml_str).expect("mask hash");
        assert_eq!(cfg.transforms.len(), 1);

        match &cfg.transforms[0] {
            TransformDescriptor::Mask { fields, strategy } => {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0], "email");
                assert_eq!(fields[1], "phone");
                assert_eq!(*strategy, MaskStrategy::Hash);
            }
            other => panic!("expected Mask, got {other:?}"),
        }
    }

    #[test]
    fn test_transform_descriptor_mask_null_strategy() {
        let toml_str = r#"
        [[transforms]]
        type = "mask"
        fields = ["ssn"]
        strategy = "null"
        "#;
        let cfg: TransformConfig = toml::from_str(toml_str).expect("mask null");
        assert_eq!(cfg.transforms.len(), 1);

        match &cfg.transforms[0] {
            TransformDescriptor::Mask { fields, strategy } => {
                assert_eq!(fields[0], "ssn");
                assert_eq!(*strategy, MaskStrategy::Null);
            }
            other => panic!("expected Mask, got {other:?}"),
        }
    }

    #[test]
    fn test_transform_descriptor_mask_redact_strategy() {
        let toml_str = r#"
        [[transforms]]
        type = "mask"
        fields = ["secret"]
        strategy = "redact"
        "#;
        let cfg: TransformConfig = toml::from_str(toml_str).expect("mask redact");
        assert_eq!(cfg.transforms.len(), 1);

        match &cfg.transforms[0] {
            TransformDescriptor::Mask { fields, strategy } => {
                assert_eq!(fields[0], "secret");
                assert_eq!(*strategy, MaskStrategy::Redact);
            }
            other => panic!("expected Mask, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Full pipeline
    // -----------------------------------------------------------------------

    #[test]
    fn test_transform_pipeline_all_three() {
        let toml_str = r#"
        failClosed = true

        [[transforms]]
        type = "filter"
        script = "function f(e) { return e.op !== 'd'; }"

        [[transforms]]
        type = "map"
        script = "function m(e) { e.after.ts = Date.now(); return e; }"

        [[transforms]]
        type = "mask"
        fields = ["email"]
        strategy = "hash"
        "#;
        let cfg: TransformConfig = toml::from_str(toml_str).expect("full pipeline");
        assert!(cfg.fail_closed);
        assert_eq!(cfg.transforms.len(), 3);

        assert!(matches!(
            cfg.transforms[0],
            TransformDescriptor::Filter { .. }
        ));
        assert!(matches!(cfg.transforms[1], TransformDescriptor::Map { .. }));
        assert!(matches!(
            cfg.transforms[2],
            TransformDescriptor::Mask { .. }
        ));
    }

    // -----------------------------------------------------------------------
    // MaskStrategy serde
    // -----------------------------------------------------------------------

    #[test]
    fn test_mask_strategy_default() {
        assert_eq!(MaskStrategy::default(), MaskStrategy::Redact);
    }

    #[test]
    fn test_mask_strategy_serde_roundtrip() {
        let cases = [
            (MaskStrategy::Redact, "\"redact\""),
            (MaskStrategy::Hash, "\"hash\""),
            (MaskStrategy::Null, "\"null\""),
        ];
        for (strategy, expected_json) in cases {
            let json = serde_json::to_string(&strategy).expect("serialize");
            assert_eq!(json, expected_json);

            let deserialized: MaskStrategy =
                serde_json::from_str(expected_json).expect("deserialize");
            assert_eq!(deserialized, strategy);
        }
    }

    #[test]
    fn test_mask_strategy_toml_embedded() {
        // A mask descriptor using each strategy — this exercises TOML
        // deserialization for MaskStrategy embedded in a struct
        let toml_cases = [
            (MaskStrategy::Redact, "redact"),
            (MaskStrategy::Hash, "hash"),
            (MaskStrategy::Null, "null"),
        ];
        for (expected, strategy_str) in toml_cases {
            let toml_str = format!(
                r#"
                [[transforms]]
                type = "mask"
                fields = ["x"]
                strategy = "{}"
                "#,
                strategy_str
            );
            let cfg: TransformConfig = toml::from_str(&toml_str).expect("deserialize mask");
            match &cfg.transforms[0] {
                TransformDescriptor::Mask { strategy, .. } => {
                    assert_eq!(*strategy, expected);
                }
                other => panic!("expected Mask, got {other:?}"),
            }
        }
    }

    // -----------------------------------------------------------------------
    // TransformResult
    // -----------------------------------------------------------------------

    #[test]
    fn test_transform_result_variants() {
        // Compile-time check that all variants exist
        let _pass = TransformResult::PassThrough;
        let _drop = TransformResult::Dropped;
        let _err = TransformResult::Error("oops".into());

        // Modified variant carries a ChangeEvent
        let source = crate::event::SourceMetadata {
            db: "d".into(),
            schema: "s".into(),
            table: "t".into(),
            ..Default::default()
        };
        let event = ChangeEvent {
            op: crate::event::Operation::Create,
            before: None,
            after: Some(serde_json::json!({"id": 1})),
            source,
            ts_ms: 0,
            id: "test".into(),
        };
        let modified = TransformResult::Modified(Box::new(event.clone()));
        match modified {
            TransformResult::Modified(ref e) => {
                assert_eq!(e.id, "test");
                assert_eq!(e.op, crate::event::Operation::Create);
            }
            other => panic!("expected Modified, got {other:?}"),
        }

        // Dropped variant
        assert_eq!(format!("{:?}", _drop), "Dropped");
    }

    // -----------------------------------------------------------------------
    // Error handling — unknown transform type
    // -----------------------------------------------------------------------

    #[test]
    fn test_transform_descriptor_unknown_type() {
        let toml_str = r#"
        [[transforms]]
        type = "unknown"
        "#;
        let result: Result<TransformConfig, toml::de::Error> = toml::from_str(toml_str);
        assert!(result.is_err(), "unknown transform type should fail");
    }

    // -----------------------------------------------------------------------
    // Error handling — missing required fields
    // -----------------------------------------------------------------------

    #[test]
    fn test_transform_descriptor_filter_missing_script() {
        let toml_str = r#"
        [[transforms]]
        type = "filter"
        "#;
        let result: Result<TransformConfig, toml::de::Error> = toml::from_str(toml_str);
        assert!(result.is_err(), "filter without script should fail");
    }

    #[test]
    fn test_transform_descriptor_map_missing_script() {
        let toml_str = r#"
        [[transforms]]
        type = "map"
        "#;
        let result: Result<TransformConfig, toml::de::Error> = toml::from_str(toml_str);
        assert!(result.is_err(), "map without script should fail");
    }

    #[test]
    fn test_transform_descriptor_mask_missing_fields() {
        let toml_str = r#"
        [[transforms]]
        type = "mask"
        "#;
        let result: Result<TransformConfig, toml::de::Error> = toml::from_str(toml_str);
        assert!(result.is_err(), "mask without fields should fail");
    }
}

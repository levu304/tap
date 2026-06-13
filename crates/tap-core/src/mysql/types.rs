//! MySQL value type mapping.
//!
//! Converts [`mysql_async::Value`] (the unified value type for both the
//! regular MySQL protocol and the binlog) into [`serde_json::Value`] for
//! downstream consumption.
//!
//! # Binlog values
//!
//! Row events carry [`mysql_async::binlog::row::BinlogRow`] instances whose
//! columns are accessible as [`mysql_async::binlog::value::BinlogValue`].
//! Each `BinlogValue` wraps either a regular `Value`, a JSONB blob, or a
//! JSON diff.  The conversion functions below handle the common `Value`
//! cases; JSONB / JSON-diff handling is deferred to the binlog-streaming
//! phase.

use mysql_async::Value;
use mysql_async::consts::ColumnType;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonValue};

/// Serialise [`ColumnType`] as its `u8` discrimant so the field can
/// participate in `#[derive(Serialize, Deserialize)]`.
pub mod col_type_serde {
    use mysql_async::consts::ColumnType;
    use serde::Deserialize;
    use serde::de;
    use serde::ser;

    pub fn serialize<S>(ct: &ColumnType, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        serializer.serialize_u8(*ct as u8)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<ColumnType, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let val = u8::deserialize(deserializer)?;
        ColumnType::try_from(val).map_err(de::Error::custom)
    }
}

/// Lightweight column metadata extracted from a MySQL result set or table
/// map event.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ColumnInfo {
    /// Column name.
    pub name: String,
    /// MySQL column type (e.g. `MYSQL_TYPE_LONG`, `MYSQL_TYPE_VARCHAR`).
    #[serde(with = "col_type_serde")]
    pub col_type: ColumnType,
    /// Whether the column is unsigned (relevant for integer types).
    pub is_unsigned: bool,
}

/// Convert a `mysql_async::Value` (regular protocol or binlog primitive) to
/// a `serde_json::Value`.
///
/// MySQL `NULL` is mapped to `JsonValue::Null`.  Other types are mapped to
/// the closest JSON representation:
///
/// | MySQL type          | JSON type       |
/// |---------------------|-----------------|
/// | `NULL`              | `Null`          |
/// | `Bytes`             | `String` (UTF-8) |
/// | `Int`               | `Number`        |
/// | `UInt`              | `Number`        |
/// | `Float`             | `Number`        |
/// | `Double`            | `Number`        |
/// | `Date`              | `String` (ISO-8601) |
/// | `Time`              | `String` (ISO-8601) |
pub fn mysql_value_to_json(value: &Value) -> JsonValue {
    match value {
        Value::NULL => JsonValue::Null,
        Value::Bytes(bytes) => {
            // Best-effort UTF-8 conversion.  Non-UTF-8 byte sequences are
            // hex-encoded so the pipeline never loses data.
            match String::from_utf8(bytes.clone()) {
                Ok(s) => JsonValue::String(s),
                Err(_) => {
                    // Non-UTF-8 bytes encoded as base64 (base64 is already a
                    // workspace dependency).
                    use base64::Engine;
                    JsonValue::String(base64::engine::general_purpose::STANDARD.encode(bytes))
                }
            }
        }
        Value::Int(i) => JsonValue::Number(serde_json::Number::from(*i)),
        Value::UInt(u) => JsonValue::Number(serde_json::Number::from(*u)),
        Value::Float(f) => {
            // serde_json does not support f32 directly — convert through f64.
            JsonValue::Number(
                serde_json::Number::from_f64(*f as f64).unwrap_or(serde_json::Number::from(0)),
            )
        }
        Value::Double(d) => JsonValue::Number(
            serde_json::Number::from_f64(*d).unwrap_or(serde_json::Number::from(0)),
        ),
        Value::Date(year, month, day, hour, min, sec, micros) => {
            // Format as ISO-8601-ish datetime.
            let date_str = format!(
                "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:06}",
                year, month, day, hour, min, sec, micros
            );
            JsonValue::String(date_str)
        }
        Value::Time(is_negative, days, hours, minutes, seconds, micros) => {
            let sign = if *is_negative { "-" } else { "" };
            let time_str = format!(
                "{}P{}DT{:02}:{:02}:{:02}.{:06}",
                sign, days, hours, minutes, seconds, micros
            );
            JsonValue::String(time_str)
        }
    }
}

/// Convert a slice of `mysql_async::Value` references into a
/// `serde_json::Value::Object` keyed by column name.
///
/// Returns `None` if `columns` and `values` have different lengths (callers
/// should treat this as a protocol-level inconsistency).
pub fn row_to_json_object(values: &[Value], columns: &[ColumnInfo]) -> Option<JsonValue> {
    if values.len() != columns.len() {
        return None;
    }

    let mut map = Map::with_capacity(values.len());
    for (value, col) in values.iter().zip(columns.iter()) {
        map.insert(col.name.clone(), mysql_value_to_json(value));
    }

    Some(JsonValue::Object(map))
}

// ──────────────────────────────────────────────
//  Schema-aware type mapping
// ──────────────────────────────────────────────

/// Target JSON type for a MySQL column, resolved from its schema metadata.
///
/// Some MySQL types cannot be faithfully represented as JSON numbers in
/// downstream consumers:
///
/// | MySQL type            | Problem                              | Target   |
/// |-----------------------|--------------------------------------|----------|
/// | `BIGINT UNSIGNED`     | u64 exceeds JS `Number.MAX_SAFE_INTEGER` | `String` |
/// | `DECIMAL`/`NUMERIC`   | Precision loss in IEEE 754           | `String` |
/// | `DATE`/`DATETIME`     | Must remain a formatted string       | `String` |
/// | `TIMESTAMP`           | Must remain a formatted string       | `String` |
///
/// The mapping is driven by the column's [`ColumnType`] and `is_unsigned`
/// flag from [`ColumnInfo`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum JsonTargetType {
    /// Serialize as a JSON `number`.
    Number,
    /// Serialize as a JSON `string` (e.g. `"12345678901234567"`).
    String,
    /// Serialize as a JSON `boolean`.
    Boolean,
    /// Use the default [`mysql_value_to_json`] behaviour (already returns
    /// strings for temporal types, JSON for JSON, etc.).
    Default,
}

/// Resolve the target JSON type for a column from its schema metadata.
///
/// This determines how the column's values will be serialised in the
/// ChangeEvent payload.
///
/// # Examples
///
/// ```
/// use tap_core::mysql::types::*;
/// use mysql_async::consts::ColumnType;
///
/// // BIGINT UNSIGNED → String (safe for JS consumers)
/// let col = ColumnInfo {
///     name: "id".into(),
///     col_type: ColumnType::MYSQL_TYPE_LONGLONG,
///     is_unsigned: true,
/// };
/// assert_eq!(resolve_json_target(&col), JsonTargetType::String);
///
/// // DECIMAL → String (preserve precision)
/// let col = ColumnInfo {
///     name: "price".into(),
///     col_type: ColumnType::MYSQL_TYPE_NEWDECIMAL,
///     is_unsigned: false,
/// };
/// assert_eq!(resolve_json_target(&col), JsonTargetType::String);
///
/// // INT → Number
/// let col = ColumnInfo {
///     name: "count".into(),
///     col_type: ColumnType::MYSQL_TYPE_LONG,
///     is_unsigned: false,
/// };
/// assert_eq!(resolve_json_target(&col), JsonTargetType::Number);
/// ```
pub fn resolve_json_target(col: &ColumnInfo) -> JsonTargetType {
    use ColumnType::*;
    match col.col_type {
        // Integers: unsigned longlong exceeds JS safe integer
        MYSQL_TYPE_TINY | MYSQL_TYPE_SHORT | MYSQL_TYPE_INT24 | MYSQL_TYPE_LONG
        | MYSQL_TYPE_YEAR => JsonTargetType::Number,
        MYSQL_TYPE_LONGLONG => {
            if col.is_unsigned {
                JsonTargetType::String
            } else {
                JsonTargetType::Number
            }
        }
        // Float / double — always number
        MYSQL_TYPE_FLOAT | MYSQL_TYPE_DOUBLE => JsonTargetType::Number,
        // Decimal — string to preserve precision
        MYSQL_TYPE_DECIMAL | MYSQL_TYPE_NEWDECIMAL => JsonTargetType::String,
        // Bit — number
        MYSQL_TYPE_BIT => JsonTargetType::Number,
        // Temporal — default (mysql_value_to_json already returns strings)
        MYSQL_TYPE_DATE
        | MYSQL_TYPE_TIME
        | MYSQL_TYPE_DATETIME
        | MYSQL_TYPE_TIMESTAMP
        | MYSQL_TYPE_NEWDATE
        | MYSQL_TYPE_TIMESTAMP2
        | MYSQL_TYPE_DATETIME2
        | MYSQL_TYPE_TIME2 => JsonTargetType::Default,
        // String / text / blob types — default (already strings)
        MYSQL_TYPE_STRING
        | MYSQL_TYPE_VAR_STRING
        | MYSQL_TYPE_VARCHAR
        | MYSQL_TYPE_TINY_BLOB
        | MYSQL_TYPE_BLOB
        | MYSQL_TYPE_MEDIUM_BLOB
        | MYSQL_TYPE_LONG_BLOB
        | MYSQL_TYPE_ENUM
        | MYSQL_TYPE_SET
        | MYSQL_TYPE_GEOMETRY => JsonTargetType::Default,
        // JSON — default (mysql_async already handles JSON/JSONB)
        MYSQL_TYPE_JSON => JsonTargetType::Default,
        // Null — default (passthrough)
        MYSQL_TYPE_NULL => JsonTargetType::Default,
        // Vector / typed array / unknown — default (fallback)
        MYSQL_TYPE_VECTOR | MYSQL_TYPE_TYPED_ARRAY | MYSQL_TYPE_UNKNOWN => JsonTargetType::Default,
    }
}

/// Convert a `mysql_async::Value` to `serde_json::Value` using a
/// schema-aware type mapping.
///
/// Unlike [`mysql_value_to_json`] which always maps `Int`/`UInt` to JSON
/// numbers, this function respects the target type:
///
/// * `JsonTargetType::String` — preserves numeric precision by emitting a
///   JSON *string* (e.g. `"12345678901234567890"`)
/// * `JsonTargetType::Number` — same as [`mysql_value_to_json`]
/// * `JsonTargetType::Boolean` — maps to JSON `true`/`false`
/// * `JsonTargetType::Default` — delegates to [`mysql_value_to_json`]
pub fn mysql_value_to_json_with_mapping(value: &Value, target: JsonTargetType) -> JsonValue {
    match target {
        JsonTargetType::Number => mysql_value_to_json(value),
        JsonTargetType::Default => mysql_value_to_json(value),
        JsonTargetType::String => value_to_json_string(value),
        JsonTargetType::Boolean => value_to_json_bool(value),
    }
}

/// Convert a value to its JSON string representation.
fn value_to_json_string(value: &Value) -> JsonValue {
    match value {
        Value::NULL => JsonValue::Null,
        Value::Bytes(bytes) => JsonValue::String(String::from_utf8_lossy(bytes).into_owned()),
        Value::Int(i) => JsonValue::String(i.to_string()),
        Value::UInt(u) => JsonValue::String(u.to_string()),
        Value::Float(f) => JsonValue::String(f.to_string()),
        Value::Double(d) => JsonValue::String(d.to_string()),
        // Temporal types already render as ISO-8601 strings.
        Value::Date(..) | Value::Time(..) => mysql_value_to_json(value),
    }
}

/// Convert a value to a JSON boolean.
fn value_to_json_bool(value: &Value) -> JsonValue {
    match value {
        Value::NULL => JsonValue::Null,
        Value::Int(i) => JsonValue::Bool(*i != 0),
        Value::UInt(u) => JsonValue::Bool(*u != 0),
        Value::Float(f) => JsonValue::Bool((*f).abs() > f32::EPSILON),
        Value::Double(d) => JsonValue::Bool((*d).abs() > f64::EPSILON),
        Value::Bytes(bytes) => {
            // Empty or "0"/"false" → false, everything else → true
            let s = String::from_utf8_lossy(bytes);
            JsonValue::Bool(!(s.is_empty() || s == "0" || s == "false"))
        }
        // Temporal types → truthy if non-zero timestamp
        Value::Date(..) | Value::Time(..) => JsonValue::Bool(true),
    }
}

/// Convert a slice of values to a JSON object, applying schema-aware type
/// mapping per column.
///
/// Returns `None` if lengths mismatch.
pub fn row_to_json_object_with_mapping(
    values: &[Value],
    columns: &[ColumnInfo],
) -> Option<JsonValue> {
    if values.len() != columns.len() {
        return None;
    }

    let mut map = Map::with_capacity(values.len());
    for (value, col) in values.iter().zip(columns.iter()) {
        let target = resolve_json_target(col);
        map.insert(
            col.name.clone(),
            mysql_value_to_json_with_mapping(value, target),
        );
    }

    Some(JsonValue::Object(map))
}

#[cfg(test)]
mod tests {
    use super::*;
    use mysql_async::Value;
    use mysql_async::consts::ColumnType;

    #[test]
    fn test_null_maps_to_null() {
        assert_eq!(mysql_value_to_json(&Value::NULL), JsonValue::Null);
    }

    #[test]
    fn test_bytes_to_string() {
        assert_eq!(
            mysql_value_to_json(&Value::Bytes(b"hello".to_vec())),
            JsonValue::String("hello".into())
        );
    }

    #[test]
    fn test_bytes_non_utf8_base64_encoded() {
        // 0xFF 0xFE is not valid UTF-8.
        let val = mysql_value_to_json(&Value::Bytes(vec![0xFF, 0xFE]));
        // Base64 of [0xFF, 0xFE] is "//4=" (with standard alphabet).
        assert_eq!(val, JsonValue::String("//4=".into()));
    }

    #[test]
    fn test_int_to_number() {
        assert_eq!(
            mysql_value_to_json(&Value::Int(-42)),
            JsonValue::Number(serde_json::Number::from(-42))
        );
    }

    #[test]
    fn test_uint_to_number() {
        assert_eq!(
            mysql_value_to_json(&Value::UInt(42)),
            JsonValue::Number(serde_json::Number::from(42u64))
        );
    }

    #[test]
    fn test_float_to_number() {
        let val = mysql_value_to_json(&Value::Float(3.14));
        if let JsonValue::Number(n) = val {
            let f: f64 = n.as_f64().unwrap();
            assert!((f - 3.14).abs() < 1e-6);
        } else {
            panic!("expected Number");
        }
    }

    #[test]
    fn test_double_to_number() {
        let val = mysql_value_to_json(&Value::Double(std::f64::consts::PI));
        if let JsonValue::Number(n) = val {
            let f: f64 = n.as_f64().unwrap();
            assert!((f - std::f64::consts::PI).abs() < 1e-12);
        } else {
            panic!("expected Number");
        }
    }

    #[test]
    fn test_date_to_iso_string() {
        let val = mysql_value_to_json(&Value::Date(2024, 3, 15, 10, 30, 0, 500_000));
        assert_eq!(val, JsonValue::String("2024-03-15T10:30:00.500000".into()));
    }

    #[test]
    fn test_time_to_duration_string() {
        let val = mysql_value_to_json(&Value::Time(false, 1, 12, 30, 0, 0));
        assert_eq!(val, JsonValue::String("P1DT12:30:00.000000".into()));
    }

    #[test]
    fn test_negative_time() {
        let val = mysql_value_to_json(&Value::Time(true, 0, 0, 5, 30, 0));
        assert_eq!(val, JsonValue::String("-P0DT00:05:30.000000".into()));
    }

    #[test]
    fn test_row_to_json_object() {
        let values = vec![Value::Int(1), Value::Bytes(b"alice".to_vec()), Value::NULL];
        let columns = vec![
            ColumnInfo {
                name: "id".into(),
                col_type: ColumnType::MYSQL_TYPE_LONG,
                is_unsigned: false,
            },
            ColumnInfo {
                name: "name".into(),
                col_type: ColumnType::MYSQL_TYPE_VARCHAR,
                is_unsigned: false,
            },
            ColumnInfo {
                name: "deleted_at".into(),
                col_type: ColumnType::MYSQL_TYPE_TIMESTAMP,
                is_unsigned: false,
            },
        ];

        let obj = row_to_json_object(&values, &columns).unwrap();
        let map = obj.as_object().unwrap();
        assert_eq!(map["id"], JsonValue::Number(serde_json::Number::from(1)));
        assert_eq!(map["name"], JsonValue::String("alice".into()));
        assert_eq!(map["deleted_at"], JsonValue::Null);
    }

    #[test]
    fn test_row_to_json_object_mismatched_lengths() {
        let values = vec![Value::Int(1)];
        let columns = vec![
            ColumnInfo {
                name: "id".into(),
                col_type: ColumnType::MYSQL_TYPE_LONG,
                is_unsigned: false,
            },
            ColumnInfo {
                name: "name".into(),
                col_type: ColumnType::MYSQL_TYPE_VARCHAR,
                is_unsigned: false,
            },
        ];
        assert!(row_to_json_object(&values, &columns).is_none());
    }

    // ── resolve_json_target ──────────────────────────────────────────

    #[test]
    fn test_resolve_target_bigint_unsigned_is_string() {
        let col = ColumnInfo {
            name: "id".into(),
            col_type: ColumnType::MYSQL_TYPE_LONGLONG,
            is_unsigned: true,
        };
        assert_eq!(resolve_json_target(&col), JsonTargetType::String);
    }

    #[test]
    fn test_resolve_target_bigint_signed_is_number() {
        let col = ColumnInfo {
            name: "id".into(),
            col_type: ColumnType::MYSQL_TYPE_LONGLONG,
            is_unsigned: false,
        };
        assert_eq!(resolve_json_target(&col), JsonTargetType::Number);
    }

    #[test]
    fn test_resolve_target_decimal_is_string() {
        for ct in [
            ColumnType::MYSQL_TYPE_DECIMAL,
            ColumnType::MYSQL_TYPE_NEWDECIMAL,
        ] {
            let col = ColumnInfo {
                name: "price".into(),
                col_type: ct,
                is_unsigned: false,
            };
            assert_eq!(resolve_json_target(&col), JsonTargetType::String);
        }
    }

    #[test]
    fn test_resolve_target_tinyint_is_number() {
        let col = ColumnInfo {
            name: "flag".into(),
            col_type: ColumnType::MYSQL_TYPE_TINY,
            is_unsigned: false,
        };
        assert_eq!(resolve_json_target(&col), JsonTargetType::Number);
    }

    #[test]
    fn test_resolve_target_float_and_double_are_number() {
        for ct in [ColumnType::MYSQL_TYPE_FLOAT, ColumnType::MYSQL_TYPE_DOUBLE] {
            let col = ColumnInfo {
                name: "val".into(),
                col_type: ct,
                is_unsigned: false,
            };
            assert_eq!(resolve_json_target(&col), JsonTargetType::Number);
        }
    }

    #[test]
    fn test_resolve_target_varchar_is_default() {
        let col = ColumnInfo {
            name: "name".into(),
            col_type: ColumnType::MYSQL_TYPE_VARCHAR,
            is_unsigned: false,
        };
        assert_eq!(resolve_json_target(&col), JsonTargetType::Default);
    }

    #[test]
    fn test_resolve_target_temporal_is_default() {
        for ct in [
            ColumnType::MYSQL_TYPE_DATE,
            ColumnType::MYSQL_TYPE_DATETIME,
            ColumnType::MYSQL_TYPE_TIMESTAMP,
            ColumnType::MYSQL_TYPE_DATETIME2,
            ColumnType::MYSQL_TYPE_TIMESTAMP2,
            ColumnType::MYSQL_TYPE_TIME,
            ColumnType::MYSQL_TYPE_TIME2,
        ] {
            let col = ColumnInfo {
                name: "ts".into(),
                col_type: ct,
                is_unsigned: false,
            };
            assert_eq!(resolve_json_target(&col), JsonTargetType::Default);
        }
    }

    #[test]
    fn test_resolve_target_json_is_default() {
        let col = ColumnInfo {
            name: "data".into(),
            col_type: ColumnType::MYSQL_TYPE_JSON,
            is_unsigned: false,
        };
        assert_eq!(resolve_json_target(&col), JsonTargetType::Default);
    }

    #[test]
    fn test_resolve_target_bit_is_number() {
        let col = ColumnInfo {
            name: "flags".into(),
            col_type: ColumnType::MYSQL_TYPE_BIT,
            is_unsigned: false,
        };
        assert_eq!(resolve_json_target(&col), JsonTargetType::Number);
    }

    // ── mysql_value_to_json_with_mapping ─────────────────────────────

    #[test]
    fn test_mapping_string_target_uint() {
        let val = Value::UInt(18_446_744_073_709_551_615u64);
        assert_eq!(
            mysql_value_to_json_with_mapping(&val, JsonTargetType::String),
            JsonValue::String("18446744073709551615".into())
        );
    }

    #[test]
    fn test_mapping_string_target_int() {
        let val = Value::Int(-1);
        assert_eq!(
            mysql_value_to_json_with_mapping(&val, JsonTargetType::String),
            JsonValue::String("-1".into())
        );
    }

    #[test]
    fn test_mapping_string_target_bytes() {
        let val = Value::Bytes(b"hello".to_vec());
        assert_eq!(
            mysql_value_to_json_with_mapping(&val, JsonTargetType::String),
            JsonValue::String("hello".into())
        );
    }

    #[test]
    fn test_mapping_string_target_float() {
        let val = Value::Float(3.14);
        assert_eq!(
            mysql_value_to_json_with_mapping(&val, JsonTargetType::String),
            JsonValue::String("3.14".into())
        );
    }

    #[test]
    fn test_mapping_string_target_double() {
        let val = Value::Double(1.23456789);
        assert_eq!(
            mysql_value_to_json_with_mapping(&val, JsonTargetType::String),
            JsonValue::String("1.23456789".into())
        );
    }

    #[test]
    fn test_mapping_string_target_null() {
        assert_eq!(
            mysql_value_to_json_with_mapping(&Value::NULL, JsonTargetType::String),
            JsonValue::Null
        );
    }

    #[test]
    fn test_mapping_boolean_target_int() {
        assert_eq!(
            mysql_value_to_json_with_mapping(&Value::Int(0), JsonTargetType::Boolean),
            JsonValue::Bool(false)
        );
        assert_eq!(
            mysql_value_to_json_with_mapping(&Value::Int(1), JsonTargetType::Boolean),
            JsonValue::Bool(true)
        );
        assert_eq!(
            mysql_value_to_json_with_mapping(&Value::Int(-1), JsonTargetType::Boolean),
            JsonValue::Bool(true)
        );
    }

    #[test]
    fn test_mapping_boolean_target_uint() {
        assert_eq!(
            mysql_value_to_json_with_mapping(&Value::UInt(0), JsonTargetType::Boolean),
            JsonValue::Bool(false)
        );
        assert_eq!(
            mysql_value_to_json_with_mapping(&Value::UInt(1), JsonTargetType::Boolean),
            JsonValue::Bool(true)
        );
    }

    #[test]
    fn test_mapping_boolean_target_null() {
        assert_eq!(
            mysql_value_to_json_with_mapping(&Value::NULL, JsonTargetType::Boolean),
            JsonValue::Null
        );
    }

    #[test]
    fn test_mapping_boolean_target_bytes() {
        assert_eq!(
            mysql_value_to_json_with_mapping(
                &Value::Bytes(b"true".to_vec()),
                JsonTargetType::Boolean
            ),
            JsonValue::Bool(true)
        );
        assert_eq!(
            mysql_value_to_json_with_mapping(
                &Value::Bytes(b"false".to_vec()),
                JsonTargetType::Boolean
            ),
            JsonValue::Bool(false)
        );
        assert_eq!(
            mysql_value_to_json_with_mapping(&Value::Bytes(b"".to_vec()), JsonTargetType::Boolean),
            JsonValue::Bool(false)
        );
    }

    #[test]
    fn test_mapping_number_target_passthrough() {
        let val = Value::Int(42);
        assert_eq!(
            mysql_value_to_json_with_mapping(&val, JsonTargetType::Number),
            mysql_value_to_json(&val)
        );
    }

    #[test]
    fn test_mapping_default_target_passthrough() {
        let val = Value::UInt(42);
        assert_eq!(
            mysql_value_to_json_with_mapping(&val, JsonTargetType::Default),
            mysql_value_to_json(&val)
        );
    }

    // ── row_to_json_object_with_mapping ──────────────────────────────

    #[test]
    fn test_row_to_json_object_with_mapping_bigint_unsigned() {
        let values = vec![Value::UInt(18_446_744_073_709_551_615u64), Value::Int(42)];
        let columns = vec![
            ColumnInfo {
                name: "big_unsigned".into(),
                col_type: ColumnType::MYSQL_TYPE_LONGLONG,
                is_unsigned: true,
            },
            ColumnInfo {
                name: "signed_int".into(),
                col_type: ColumnType::MYSQL_TYPE_LONG,
                is_unsigned: false,
            },
        ];

        let obj = row_to_json_object_with_mapping(&values, &columns).unwrap();
        let map = obj.as_object().unwrap();
        // BIGINT UNSIGNED → string
        assert_eq!(
            map["big_unsigned"],
            JsonValue::String("18446744073709551615".into())
        );
        // INT → number
        assert_eq!(map["signed_int"], serde_json::json!(42));
    }

    #[test]
    fn test_row_to_json_object_with_mapping_decimal() {
        let values = vec![Value::Bytes(b"123.456789".to_vec())];
        let columns = vec![ColumnInfo {
            name: "price".into(),
            col_type: ColumnType::MYSQL_TYPE_NEWDECIMAL,
            is_unsigned: false,
        }];

        let obj = row_to_json_object_with_mapping(&values, &columns).unwrap();
        let map = obj.as_object().unwrap();
        // DECIMAL → string (preserve precision)
        assert_eq!(map["price"], JsonValue::String("123.456789".into()));
    }

    #[test]
    fn test_row_to_json_object_with_mapping_mismatched_lengths() {
        let values = vec![Value::Int(1)];
        let columns = vec![
            ColumnInfo {
                name: "a".into(),
                col_type: ColumnType::MYSQL_TYPE_LONG,
                is_unsigned: false,
            },
            ColumnInfo {
                name: "b".into(),
                col_type: ColumnType::MYSQL_TYPE_LONG,
                is_unsigned: false,
            },
        ];
        assert!(row_to_json_object_with_mapping(&values, &columns).is_none());
    }

    #[test]
    fn test_row_to_json_object_with_mapping_mixed_types() {
        let values = vec![
            Value::NULL,
            Value::Int(100),
            Value::UInt(999),
            Value::Bytes(b"2024-01-15T10:00:00.000000".to_vec()),
        ];
        let columns = vec![
            ColumnInfo {
                name: "null_col".into(),
                col_type: ColumnType::MYSQL_TYPE_NULL,
                is_unsigned: false,
            },
            ColumnInfo {
                name: "count".into(),
                col_type: ColumnType::MYSQL_TYPE_TINY,
                is_unsigned: false,
            },
            ColumnInfo {
                name: "big_id".into(),
                col_type: ColumnType::MYSQL_TYPE_LONGLONG,
                is_unsigned: true,
            },
            ColumnInfo {
                name: "created".into(),
                col_type: ColumnType::MYSQL_TYPE_DATETIME,
                is_unsigned: false,
            },
        ];

        let obj = row_to_json_object_with_mapping(&values, &columns).unwrap();
        let map = obj.as_object().unwrap();
        assert_eq!(map["null_col"], JsonValue::Null);
        assert_eq!(map["count"], serde_json::json!(100));
        assert_eq!(map["big_id"], JsonValue::String("999".into()));
        assert_eq!(
            map["created"],
            JsonValue::String("2024-01-15T10:00:00.000000".into())
        );
    }
}

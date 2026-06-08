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
use serde_json::{Map, Value as JsonValue};

/// Lightweight column metadata extracted from a MySQL result set or table
/// map event.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    /// Column name.
    pub name: String,
    /// MySQL column type (e.g. `MYSQL_TYPE_LONG`, `MYSQL_TYPE_VARCHAR`).
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
}

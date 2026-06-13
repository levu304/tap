//! MySQL CDC adapter — re-exports from [`crate::mysql`].
//!
//! This module provides a stable public API surface for MySQL CDC
//! functionality, re-exporting types from the internal [`crate::mysql`]
//! module hierarchy.
//!
//! # Public API
//!
//! | Symbol | Source |
//! |--------|--------|
//! | [`MySqlConnection`] | connection lifecycle, pool, pre-flight checks |
//! | [`MySqlChangeEvent`] | decoded binlog change event |
//! | [`process_binlog_event`] | `MySqlBinlogEvent` → `Vec<ChangeEvent>` |
//! | [`parse_binlog_event`] | raw bytes → `MySqlBinlogEvent` |
//! | [`SchemaCache`] | lazy `information_schema.COLUMNS` resolution |
//! | [`ColumnInfo`] | column metadata for type mapping |
//! | [`run_mysql_parallel_snapshot`] | parallel snapshot engine |
//! | [`JsonTargetType`] | JSON-safe type classifier for value mapping |
//! | [`row_to_json_object_with_mapping`] | schema-aware JSON serialization |

pub use crate::mysql::connection::MySqlConnection;
pub use crate::mysql::events::{
    parse_binlog_event, process_binlog_event, MySqlBinlogEvent, MySqlChangeEvent,
};
pub use crate::mysql::schema::SchemaCache;
pub use crate::mysql::snapshot::run_mysql_parallel_snapshot;
pub use crate::mysql::types::{ColumnInfo, JsonTargetType, row_to_json_object_with_mapping};

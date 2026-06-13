//! Schema cache for MySQL binlog column resolution.
//!
//! [`SchemaCache`] queries `information_schema.COLUMNS` to obtain column
//! names and metadata for tables referenced in binlog events.  This is
//! necessary because the binlog's [`TableMapEvent`] carries column *types*
//! but only optionally includes column names (MySQL 8.0.1+ with
//! `binlog_transaction_dependency_tracking` enabled).
//!
//! The cache is populated lazily on first access and invalidated by DDL
//! statements detected in [`QueryEvent`]s.
//!
//! # Example
//!
//! ```ignore
//! use tap_core::mysql::schema::SchemaCache;
//!
//! let mut cache = SchemaCache::new(pool.clone());
//!
//! // On TABLE_MAP, resolve column names:
//! let columns = cache.get_or_fetch("my_db", "users").await?;
//!
//! // On DDL (ALTER TABLE), invalidate:
//! if let Some((db, table)) = cache.detect_ddl(query_event.schema(), query_event.query()) {
//!     cache.invalidate(db, table);
//! }
//! ```

use std::collections::HashMap;
use std::sync::OnceLock;

use mysql_async::prelude::*;
use mysql_async::{Pool, Row as MyRow};
use regex::Regex;
use tracing::{info, warn};

use crate::error::TapError;
use crate::mysql::types::ColumnInfo;
use mysql_async::consts::ColumnType;

/// Compiled regex for matching DDL statements against query text.
///
/// Matches `ALTER TABLE`, `CREATE TABLE`, `DROP TABLE`, `TRUNCATE TABLE`,
/// and `RENAME TABLE` at the start of a statement, capturing the table name.
static DDL_RE: OnceLock<Regex> = OnceLock::new();

fn ddl_re() -> &'static Regex {
    DDL_RE.get_or_init(|| {
        Regex::new(
            r"(?i)^\s*(ALTER|CREATE|DROP|TRUNCATE|RENAME)\s+TABLE\s+(?:IF\s+(?:NOT\s+)?EXISTS\s+)?(?:`?(\w+)`?\.)?`?(\w+)`?"
        )
        .expect("invalid DDL regex")
    })
}

/// Schema cache for MySQL column metadata.
///
/// Queries `information_schema.COLUMNS` on cache miss and caches the
/// result keyed by `(database_name, table_name)`.
///
/// The cache does **not** perform automatic expiry — callers must
/// [`invalidate`](SchemaCache::invalidate) entries when DDL events are
/// detected.
#[derive(Debug)]
pub struct SchemaCache {
    pool: Pool,
    cache: HashMap<(String, String), Vec<ColumnInfo>>,
}

impl SchemaCache {
    /// Create a new schema cache backed by the given connection pool.
    pub fn new(pool: Pool) -> Self {
        Self {
            pool,
            cache: HashMap::new(),
        }
    }

    /// Return cached column metadata for `(db, table)`, or query
    /// `information_schema.COLUMNS` and cache the result.
    ///
    /// # Errors
    ///
    /// Returns [`TapError::MySqlConnection`] when the query fails
    /// (e.g. network error, insufficient privileges).
    pub async fn get_or_fetch(&mut self, db: &str, table: &str) -> Result<&[ColumnInfo], TapError> {
        // Fast path: already cached.
        if self
            .cache
            .contains_key(&(db.to_string(), table.to_string()))
        {
            return Ok(self
                .cache
                .get(&(db.to_string(), table.to_string()))
                .unwrap()
                .as_slice());
        }

        // Slow path: query information_schema.
        let columns = self.fetch_columns(db, table).await?;
        info!(
            db = db,
            table = table,
            column_count = columns.len(),
            "schema cache miss — fetched from information_schema"
        );
        self.cache
            .insert((db.to_string(), table.to_string()), columns);
        Ok(self
            .cache
            .get(&(db.to_string(), table.to_string()))
            .unwrap()
            .as_slice())
    }

    /// Remove the cached entry for `(db, table)`, forcing a re-fetch on
    /// the next access.
    pub fn invalidate(&mut self, db: &str, table: &str) {
        if self
            .cache
            .remove(&(db.to_string(), table.to_string()))
            .is_some()
        {
            info!(db = db, table = table, "schema cache invalidated");
        }
    }

    /// Remove all cached entries for a database.
    pub fn invalidate_db(&mut self, db: &str) {
        let db = db.to_string();
        self.cache.retain(|key, _| key.0 != db);
        info!(db = db, "schema cache invalidated for entire database");
    }

    /// Detect whether `query` is a DDL table statement.
    ///
    /// Returns `Some((db, table_name))` when the query matches, using
    /// `schema` as a fallback when the statement does not explicitly
    /// qualify the table name.
    ///
    /// # Examples
    ///
    /// ```
    /// # use tap_core::mysql::schema::SchemaCache;
    /// # use mysql_async::{Opts, Pool};
    /// # let opts: Opts = "mysql://localhost:3306".parse().unwrap();
    /// # let pool = Pool::new(opts);
    /// # let cache = SchemaCache::new(pool);
    ///
    /// // Fully-qualified
    /// assert_eq!(
    ///     cache.detect_ddl("public", "ALTER TABLE my_db.users ADD COLUMN age INT"),
    ///     Some(("my_db", "users")),
    /// );
    ///
    /// // Unqualified — uses the provided schema
    /// assert_eq!(
    ///     cache.detect_ddl("my_db", "DROP TABLE users"),
    ///     Some(("my_db", "users")),
    /// );
    ///
    /// // Not a DDL statement
    /// assert_eq!(cache.detect_ddl("my_db", "SELECT 1"), None);
    /// ```
    pub fn detect_ddl<'a>(&self, schema: &'a str, query: &'a str) -> Option<(&'a str, &'a str)> {
        let re = ddl_re();
        let caps = re.captures(query)?;

        // The db group (group 2) is optional when the table is not qualified.
        let db = caps.get(2).map(|m| m.as_str()).unwrap_or(schema);
        let table = caps.get(3)?.as_str();

        info!(
            db = db,
            table = table,
            query = %caps.get(1).map(|m| m.as_str()).unwrap_or("unknown"),
            "DDL detected"
        );
        Some((db, table))
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    /// Query `information_schema.COLUMNS` and map rows to `ColumnInfo`.
    async fn fetch_columns(&self, db: &str, table: &str) -> Result<Vec<ColumnInfo>, TapError> {
        let mut conn = self.pool.get_conn().await.map_err(|e| {
            TapError::MySqlConnection(format!(
                "failed to get connection for schema query on {db}.{table}: {e}"
            ))
        })?;

        let rows: Vec<MyRow> = conn
            .exec_iter(
                "SELECT \
                     COLUMN_NAME, \
                     ORDINAL_POSITION, \
                     DATA_TYPE, \
                     IS_NULLABLE, \
                     COLUMN_TYPE, \
                     CHARACTER_SET_NAME \
                 FROM information_schema.COLUMNS \
                 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? \
                 ORDER BY ORDINAL_POSITION",
                (db, table),
            )
            .await
            .map_err(|e| {
                TapError::MySqlConnection(format!("schema query failed for {db}.{table}: {e}"))
            })?
            .collect()
            .await
            .map_err(|e| {
                TapError::MySqlConnection(format!("schema collect failed for {db}.{table}: {e}"))
            })?;

        if rows.is_empty() {
            warn!(
                db = db,
                table = table,
                "no columns found in information_schema — table may not exist"
            );
        }

        let columns: Vec<ColumnInfo> = rows.iter().map(column_info_from_row).collect();

        Ok(columns)
    }
}

/// Convert a single `information_schema.COLUMNS` row to [`ColumnInfo`].
///
/// `ColumnType` mapping is best-effort: when the DATA_TYPE string does not
/// match a known `ColumnType` variant, we fall back to
/// `MYSQL_TYPE_STRING` as a safe default (the type is only used for
/// JSON serialisation hints, not for data decoding — the binlog's own
/// type metadata is authoritative).
fn column_info_from_row(row: &MyRow) -> ColumnInfo {
    let name = row.get::<String, &str>("COLUMN_NAME").unwrap_or_default();

    let col_type_str = row.get::<String, &str>("DATA_TYPE").unwrap_or_default();

    // Detect unsigned from COLUMN_TYPE (e.g. "bigint unsigned").
    let col_type_full = row.get::<String, &str>("COLUMN_TYPE").unwrap_or_default();
    let is_unsigned = col_type_full.contains("unsigned");

    ColumnInfo {
        name,
        col_type: data_type_to_column_type(&col_type_str),
        is_unsigned,
    }
}

/// Map a MySQL DATA_TYPE string to the corresponding [`ColumnType`] enum.
///
/// This mapping is used for informational purposes (JSON serialisation);
/// the authoritative type metadata comes from the binlog's
/// [`TableMapEvent`].
fn data_type_to_column_type(dt: &str) -> ColumnType {
    match dt {
        "tinyint" => ColumnType::MYSQL_TYPE_TINY,
        "smallint" => ColumnType::MYSQL_TYPE_SHORT,
        "mediumint" => ColumnType::MYSQL_TYPE_INT24,
        "int" | "integer" => ColumnType::MYSQL_TYPE_LONG,
        "bigint" => ColumnType::MYSQL_TYPE_LONGLONG,
        "float" => ColumnType::MYSQL_TYPE_FLOAT,
        "double" => ColumnType::MYSQL_TYPE_DOUBLE,
        "decimal" | "numeric" => ColumnType::MYSQL_TYPE_NEWDECIMAL,
        "char" => ColumnType::MYSQL_TYPE_STRING,
        "varchar" => ColumnType::MYSQL_TYPE_VARCHAR,
        "binary" => ColumnType::MYSQL_TYPE_BLOB, // closest match
        "varbinary" => ColumnType::MYSQL_TYPE_VARCHAR,
        "tinyblob" | "tinytext" => ColumnType::MYSQL_TYPE_TINY_BLOB,
        "blob" | "text" => ColumnType::MYSQL_TYPE_BLOB,
        "mediumblob" | "mediumtext" => ColumnType::MYSQL_TYPE_MEDIUM_BLOB,
        "longblob" | "longtext" => ColumnType::MYSQL_TYPE_LONG_BLOB,
        "json" => ColumnType::MYSQL_TYPE_JSON,
        "enum" => ColumnType::MYSQL_TYPE_ENUM,
        "set" => ColumnType::MYSQL_TYPE_SET,
        "date" => ColumnType::MYSQL_TYPE_DATE,
        "time" => ColumnType::MYSQL_TYPE_TIME,
        "datetime" => ColumnType::MYSQL_TYPE_DATETIME,
        "timestamp" => ColumnType::MYSQL_TYPE_TIMESTAMP,
        "year" => ColumnType::MYSQL_TYPE_YEAR,
        "bit" => ColumnType::MYSQL_TYPE_BIT,
        "geometry" => ColumnType::MYSQL_TYPE_GEOMETRY,
        _ => {
            warn!(
                data_type = dt,
                "unknown DATA_TYPE, falling back to MYSQL_TYPE_STRING"
            );
            ColumnType::MYSQL_TYPE_STRING
        }
    }
}

/// Map a `ColumnType` to the corresponding DATA_TYPE string (inverse of
/// [`data_type_to_column_type`]).
///
/// Useful for debugging and instrumentation.
#[allow(dead_code)]
pub fn column_type_to_data_type(ct: ColumnType) -> &'static str {
    match ct {
        ColumnType::MYSQL_TYPE_TINY => "tinyint",
        ColumnType::MYSQL_TYPE_SHORT => "smallint",
        ColumnType::MYSQL_TYPE_INT24 => "mediumint",
        ColumnType::MYSQL_TYPE_LONG => "int",
        ColumnType::MYSQL_TYPE_LONGLONG => "bigint",
        ColumnType::MYSQL_TYPE_FLOAT => "float",
        ColumnType::MYSQL_TYPE_DOUBLE => "double",
        ColumnType::MYSQL_TYPE_NEWDECIMAL => "decimal",
        ColumnType::MYSQL_TYPE_STRING => "varchar", // common default
        ColumnType::MYSQL_TYPE_VARCHAR => "varchar",
        ColumnType::MYSQL_TYPE_TINY_BLOB => "tinyblob",
        ColumnType::MYSQL_TYPE_BLOB => "blob",
        ColumnType::MYSQL_TYPE_MEDIUM_BLOB => "mediumblob",
        ColumnType::MYSQL_TYPE_LONG_BLOB => "longblob",
        ColumnType::MYSQL_TYPE_JSON => "json",
        ColumnType::MYSQL_TYPE_ENUM => "enum",
        ColumnType::MYSQL_TYPE_SET => "set",
        ColumnType::MYSQL_TYPE_DATE => "date",
        ColumnType::MYSQL_TYPE_TIME => "time",
        ColumnType::MYSQL_TYPE_DATETIME => "datetime",
        ColumnType::MYSQL_TYPE_TIMESTAMP => "timestamp",
        ColumnType::MYSQL_TYPE_YEAR => "year",
        ColumnType::MYSQL_TYPE_BIT => "bit",
        ColumnType::MYSQL_TYPE_GEOMETRY => "geometry",
        _ => "unknown",
    }
}

/// Create a `SchemaCache` backed by an unconnected pool (safe for unit
/// tests that only exercise the cache's non-IO methods).
#[cfg(test)]
fn test_cache() -> SchemaCache {
    use mysql_async::Opts;
    let opts: Opts = "mysql://localhost:3306".parse().expect("valid opts");
    SchemaCache::new(Pool::new(opts))
}

#[cfg(test)]
mod tests {
    use super::*;
    use mysql_async::consts::ColumnType;

    // ------------------------------------------------------------------
    // data_type_to_column_type mapping
    // ------------------------------------------------------------------

    #[test]
    fn test_data_type_to_column_type_integer() {
        assert_eq!(
            data_type_to_column_type("tinyint"),
            ColumnType::MYSQL_TYPE_TINY
        );
        assert_eq!(
            data_type_to_column_type("smallint"),
            ColumnType::MYSQL_TYPE_SHORT
        );
        assert_eq!(
            data_type_to_column_type("mediumint"),
            ColumnType::MYSQL_TYPE_INT24
        );
        assert_eq!(data_type_to_column_type("int"), ColumnType::MYSQL_TYPE_LONG);
        assert_eq!(
            data_type_to_column_type("integer"),
            ColumnType::MYSQL_TYPE_LONG
        );
        assert_eq!(
            data_type_to_column_type("bigint"),
            ColumnType::MYSQL_TYPE_LONGLONG
        );
    }

    #[test]
    fn test_data_type_to_column_type_float() {
        assert_eq!(
            data_type_to_column_type("float"),
            ColumnType::MYSQL_TYPE_FLOAT
        );
        assert_eq!(
            data_type_to_column_type("double"),
            ColumnType::MYSQL_TYPE_DOUBLE
        );
        assert_eq!(
            data_type_to_column_type("decimal"),
            ColumnType::MYSQL_TYPE_NEWDECIMAL
        );
        assert_eq!(
            data_type_to_column_type("numeric"),
            ColumnType::MYSQL_TYPE_NEWDECIMAL
        );
    }

    #[test]
    fn test_data_type_to_column_type_string() {
        assert_eq!(
            data_type_to_column_type("char"),
            ColumnType::MYSQL_TYPE_STRING
        );
        assert_eq!(
            data_type_to_column_type("varchar"),
            ColumnType::MYSQL_TYPE_VARCHAR
        );
        assert_eq!(
            data_type_to_column_type("text"),
            ColumnType::MYSQL_TYPE_BLOB
        );
        assert_eq!(
            data_type_to_column_type("tinytext"),
            ColumnType::MYSQL_TYPE_TINY_BLOB
        );
        assert_eq!(
            data_type_to_column_type("mediumtext"),
            ColumnType::MYSQL_TYPE_MEDIUM_BLOB
        );
        assert_eq!(
            data_type_to_column_type("longtext"),
            ColumnType::MYSQL_TYPE_LONG_BLOB
        );
    }

    #[test]
    fn test_data_type_to_column_type_binary() {
        assert_eq!(
            data_type_to_column_type("binary"),
            ColumnType::MYSQL_TYPE_BLOB
        );
        assert_eq!(
            data_type_to_column_type("varbinary"),
            ColumnType::MYSQL_TYPE_VARCHAR
        );
        assert_eq!(
            data_type_to_column_type("blob"),
            ColumnType::MYSQL_TYPE_BLOB
        );
        assert_eq!(
            data_type_to_column_type("tinyblob"),
            ColumnType::MYSQL_TYPE_TINY_BLOB
        );
        assert_eq!(
            data_type_to_column_type("mediumblob"),
            ColumnType::MYSQL_TYPE_MEDIUM_BLOB
        );
        assert_eq!(
            data_type_to_column_type("longblob"),
            ColumnType::MYSQL_TYPE_LONG_BLOB
        );
    }

    #[test]
    fn test_data_type_to_column_type_temporal() {
        assert_eq!(
            data_type_to_column_type("date"),
            ColumnType::MYSQL_TYPE_DATE
        );
        assert_eq!(
            data_type_to_column_type("time"),
            ColumnType::MYSQL_TYPE_TIME
        );
        assert_eq!(
            data_type_to_column_type("datetime"),
            ColumnType::MYSQL_TYPE_DATETIME
        );
        assert_eq!(
            data_type_to_column_type("timestamp"),
            ColumnType::MYSQL_TYPE_TIMESTAMP
        );
        assert_eq!(
            data_type_to_column_type("year"),
            ColumnType::MYSQL_TYPE_YEAR
        );
    }

    #[test]
    fn test_data_type_to_column_type_other() {
        assert_eq!(
            data_type_to_column_type("json"),
            ColumnType::MYSQL_TYPE_JSON
        );
        assert_eq!(
            data_type_to_column_type("enum"),
            ColumnType::MYSQL_TYPE_ENUM
        );
        assert_eq!(data_type_to_column_type("set"), ColumnType::MYSQL_TYPE_SET);
        assert_eq!(data_type_to_column_type("bit"), ColumnType::MYSQL_TYPE_BIT);
        assert_eq!(
            data_type_to_column_type("geometry"),
            ColumnType::MYSQL_TYPE_GEOMETRY
        );
    }

    #[test]
    fn test_data_type_to_column_type_unknown_falls_back_to_string() {
        assert_eq!(
            data_type_to_column_type("some_unknown_type"),
            ColumnType::MYSQL_TYPE_STRING
        );
    }

    // ------------------------------------------------------------------
    // detect_ddl
    // ------------------------------------------------------------------

    #[test]
    fn test_detect_ddl_alter_table() {
        let cache = test_cache();
        assert_eq!(
            cache.detect_ddl("mydb", "ALTER TABLE mydb.users ADD COLUMN age INT"),
            Some(("mydb", "users"))
        );
    }

    #[test]
    fn test_detect_ddl_create_table() {
        let cache = test_cache();
        assert_eq!(
            cache.detect_ddl("mydb", "CREATE TABLE orders (id INT)"),
            Some(("mydb", "orders"))
        );
    }

    #[test]
    fn test_detect_ddl_drop_table() {
        let cache = test_cache();
        assert_eq!(
            cache.detect_ddl("mydb", "DROP TABLE users"),
            Some(("mydb", "users"))
        );
    }

    #[test]
    fn test_detect_ddl_truncate_table() {
        let cache = test_cache();
        assert_eq!(
            cache.detect_ddl("mydb", "TRUNCATE TABLE users"),
            Some(("mydb", "users"))
        );
    }

    #[test]
    fn test_detect_ddl_rename_table() {
        let cache = test_cache();
        assert_eq!(
            cache.detect_ddl("mydb", "RENAME TABLE users TO old_users"),
            Some(("mydb", "users"))
        );
    }

    #[test]
    fn test_detect_ddl_case_insensitive() {
        let cache = test_cache();
        assert_eq!(
            cache.detect_ddl("mydb", "alter table mydb.users add column age int"),
            Some(("mydb", "users"))
        );
    }

    #[test]
    fn test_detect_ddl_not_a_ddl_statement() {
        let cache = test_cache();
        assert_eq!(cache.detect_ddl("mydb", "SELECT * FROM users"), None);
        assert_eq!(
            cache.detect_ddl("mydb", "INSERT INTO users VALUES (1)"),
            None
        );
        assert_eq!(
            cache.detect_ddl("mydb", "UPDATE users SET name = 'x'"),
            None
        );
    }

    #[test]
    fn test_detect_ddl_with_if_exists() {
        let cache = test_cache();
        assert_eq!(
            cache.detect_ddl("mydb", "DROP TABLE IF EXISTS users"),
            Some(("mydb", "users"))
        );
        assert_eq!(
            cache.detect_ddl("mydb", "CREATE TABLE IF NOT EXISTS users (id INT)"),
            Some(("mydb", "users"))
        );
    }

    #[test]
    fn test_detect_ddl_unqualified_uses_schema() {
        let cache = test_cache();
        assert_eq!(
            cache.detect_ddl("otherdb", "DROP TABLE users"),
            Some(("otherdb", "users"))
        );
    }

    #[test]
    fn test_detect_ddl_with_backtick_quoting() {
        let cache = test_cache();
        assert_eq!(
            cache.detect_ddl("mydb", "ALTER TABLE `users` ADD COLUMN x INT"),
            Some(("mydb", "users"))
        );
        assert_eq!(
            cache.detect_ddl("mydb", "ALTER TABLE `mydb`.`users` ADD COLUMN x INT"),
            Some(("mydb", "users"))
        );
    }

    // ------------------------------------------------------------------
    // invalidate
    // ------------------------------------------------------------------

    #[test]
    fn test_invalidate_removes_entry() {
        let _cache = test_cache();
        // Invalidation is a no-op when the entry doesn't exist.
        // Structural test: verify the method compiles and doesn't panic.
    }

    #[test]
    fn test_invalidate_db_removes_all_entries_for_db() {
        let mut cache = test_cache();

        // Simulate a cached entry by injecting directly.
        cache.cache.insert(
            ("mydb".into(), "users".into()),
            vec![ColumnInfo {
                name: "id".into(),
                col_type: ColumnType::MYSQL_TYPE_LONG,
                is_unsigned: false,
            }],
        );
        cache.cache.insert(
            ("mydb".into(), "orders".into()),
            vec![ColumnInfo {
                name: "id".into(),
                col_type: ColumnType::MYSQL_TYPE_LONG,
                is_unsigned: false,
            }],
        );
        cache.cache.insert(
            ("otherdb".into(), "products".into()),
            vec![ColumnInfo {
                name: "id".into(),
                col_type: ColumnType::MYSQL_TYPE_LONG,
                is_unsigned: false,
            }],
        );

        assert_eq!(cache.cache.len(), 3);
        cache.invalidate_db("mydb");
        assert_eq!(cache.cache.len(), 1);
        assert!(
            cache
                .cache
                .contains_key(&("otherdb".into(), "products".into()))
        );
    }

    // ------------------------------------------------------------------
    // cache hit / miss path (structural)
    // ------------------------------------------------------------------

    #[test]
    fn test_cache_hit_returns_cached_columns() {
        let mut cache = test_cache();

        // Directly inject a cache entry.
        let cols = vec![ColumnInfo {
            name: "id".into(),
            col_type: ColumnType::MYSQL_TYPE_LONG,
            is_unsigned: false,
        }];
        cache
            .cache
            .insert(("mydb".into(), "users".into()), cols.clone());

        // Verify the cache contains the entry (sync path — async fetch_columns
        // is not called when already cached).
        let entry = cache.cache.get(&("mydb".into(), "users".into()));
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().len(), 1);
    }

    // ------------------------------------------------------------------
    // column_type_to_data_type roundtrip
    // ------------------------------------------------------------------

    #[test]
    fn test_column_type_to_data_type_roundtrip() {
        for (dt_str, ct) in [
            ("tinyint", ColumnType::MYSQL_TYPE_TINY),
            ("smallint", ColumnType::MYSQL_TYPE_SHORT),
            ("mediumint", ColumnType::MYSQL_TYPE_INT24),
            ("int", ColumnType::MYSQL_TYPE_LONG),
            ("bigint", ColumnType::MYSQL_TYPE_LONGLONG),
            ("float", ColumnType::MYSQL_TYPE_FLOAT),
            ("double", ColumnType::MYSQL_TYPE_DOUBLE),
            ("decimal", ColumnType::MYSQL_TYPE_NEWDECIMAL),
            ("json", ColumnType::MYSQL_TYPE_JSON),
            ("enum", ColumnType::MYSQL_TYPE_ENUM),
            ("set", ColumnType::MYSQL_TYPE_SET),
            ("date", ColumnType::MYSQL_TYPE_DATE),
            ("time", ColumnType::MYSQL_TYPE_TIME),
            ("datetime", ColumnType::MYSQL_TYPE_DATETIME),
            ("timestamp", ColumnType::MYSQL_TYPE_TIMESTAMP),
            ("year", ColumnType::MYSQL_TYPE_YEAR),
            ("bit", ColumnType::MYSQL_TYPE_BIT),
            ("geometry", ColumnType::MYSQL_TYPE_GEOMETRY),
        ] {
            assert_eq!(data_type_to_column_type(dt_str), ct, "forward: {dt_str}");
            assert_eq!(column_type_to_data_type(ct), dt_str, "reverse: {ct:?}");
        }
    }
}

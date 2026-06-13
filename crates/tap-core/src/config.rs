//! Configuration types for Tap capture sessions.
//!
//! Deserialized from TOML configuration files.  The top-level
//! [`TapConfig`] bundles sub-configurations for the Postgres source,
//! SSE sink, capture behaviour, snapshotting, SQLite-backed state,
//! and structured logging.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

use crate::error::TapError;

// ---------------------------------------------------------------------------
// Top-level config
// ---------------------------------------------------------------------------

/// Root configuration for a Tap replication session.
///
/// # TOML example
///
/// ```toml
/// [source]
/// host = "localhost"
/// port = 5432
/// dbname = "myapp"
/// user = "replicator"
/// password = "secret"
/// slotName = "tap_slot"
/// publication = "tap_pub"
/// tables = ["public.users", "public.orders"]
/// plugin = "pgoutput"
///
/// [sink]
/// host = "0.0.0.0"
/// port = 8080
/// maxBufferSize = 1000
/// heartbeatIntervalMs = 30_000
///
/// [capture]
/// snapshot = true
/// maxBatchSize = 100
/// flushIntervalMs = 1_000
///
/// [snapshot]
/// batchSize = 1000
/// numWorkers = 4
///
/// [state]
/// path = ".tap/state.db"
/// maxBackupSizeKb = 10_240
///
/// [logging]
/// format = "json"
/// level = "info"
/// ```
#[derive(Clone, Serialize, Deserialize)]
pub struct TapConfig {
    /// Postgres source configuration.
    ///
    /// When using MySQL as the source, leave this as the default and provide
    /// [`mysql_source`] instead.
    #[serde(default)]
    pub source: SourceConfig,
    /// MySQL source configuration (optional).
    ///
    /// If set, Postgres replication will not be used — tap connects to MySQL
    /// and reads its binary log instead.
    #[serde(default)]
    pub mysql_source: Option<MySqlSourceConfig>,
    pub sink: SinkConfig,
    pub capture: CaptureConfig,
    pub snapshot: SnapshotConfig,
    pub state: StateConfig,
    pub logging: LoggingConfig,
}

impl fmt::Debug for TapConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_struct("TapConfig");
        d.field("source", &self.source)
            .field("sink", &self.sink)
            .field("capture", &self.capture)
            .field("snapshot", &self.snapshot)
            .field("state", &self.state)
            .field("logging", &self.logging);
        if let Some(ref ms) = self.mysql_source {
            d.field("mysql_source", ms);
        }
        d.finish()
    }
}

impl TapConfig {
    /// Reads and parses a TOML configuration file from the given path.
    ///
    /// # Errors
    ///
    /// Returns [`TapError::Io`] if the file cannot be read, or
    /// [`TapError::Config`] if the TOML content is malformed.
    pub fn from_path(path: &str) -> Result<Self, TapError> {
        let content = std::fs::read_to_string(path)?;
        let mut config = tap_config_from_toml(&content)?;
        config.validate()?;
        Ok(config)
    }

    /// Validates all sub-configurations.
    fn validate(&mut self) -> Result<(), TapError> {
        if let Some(ref ms) = self.mysql_source {
            ms.validate()?;
        } else {
            self.source.validate()?;
        }
        self.snapshot.validate()?;
        Ok(())
    }
}

/// Parse a TOML string into a [`TapConfig`].
fn tap_config_from_toml(content: &str) -> Result<TapConfig, TapError> {
    toml::from_str(content).map_err(|e| {
        // The error's Display already includes line/column position
        TapError::Config(format!("Failed to parse config: {e}"))
    })
}

// ---------------------------------------------------------------------------
// Source
// ---------------------------------------------------------------------------

/// Whether and how to encrypt the Postgres connection with TLS.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SslMode {
    /// Do not use TLS (default).  Credentials and data are sent in cleartext.
    #[default]
    Disable,
    /// Require a TLS connection.  The server certificate is not verified.
    Require,
    /// Require TLS and verify the server certificate against the system CA
    /// store.
    VerifyCa,
    /// Require TLS, verify the server certificate, and verify the hostname
    /// matches.
    VerifyFull,
}

impl fmt::Display for SslMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Disable => write!(f, "disable"),
            Self::Require => write!(f, "require"),
            Self::VerifyCa => write!(f, "verify-ca"),
            Self::VerifyFull => write!(f, "verify-full"),
        }
    }
}

impl FromStr for SslMode {
    type Err = TapError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "disable" => Ok(Self::Disable),
            "require" => Ok(Self::Require),
            "verify-ca" => Ok(Self::VerifyCa),
            "verify-full" => Ok(Self::VerifyFull),
            _ => Err(TapError::Config(format!(
                "invalid ssl_mode '{s}': expected disable, require, verify-ca, or verify-full"
            ))),
        }
    }
}

/// Postgres source connection and replication settings.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct SourceConfig {
    /// Postgres server hostname.
    pub host: String,
    /// Postgres server port.
    pub port: u16,
    /// Database name to connect to.
    pub dbname: String,
    /// Replication user name.
    pub user: String,
    /// Replication user password.
    pub password: String,
    /// Logical replication slot name.
    pub slot_name: String,
    /// Publication name for filtered replication.
    pub publication: String,
    /// Tables to capture (e.g. `["public.users", "public.orders"]`).
    /// Empty means all tables in the publication.
    #[serde(default)]
    pub tables: Vec<String>,
    /// Output plugin (`pgoutput` for Postgres 10+).
    pub plugin: String,
    /// TLS encryption mode for the Postgres connection.
    #[serde(default)]
    pub ssl_mode: SslMode,
}

impl fmt::Debug for SourceConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SourceConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("dbname", &self.dbname)
            .field("user", &self.user)
            .field("password", &"<REDACTED>")
            .field("slot_name", &self.slot_name)
            .field("publication", &self.publication)
            .field("tables", &self.tables)
            .field("plugin", &self.plugin)
            .field("ssl_mode", &self.ssl_mode)
            .finish()
    }
}

impl Default for SourceConfig {
    fn default() -> Self {
        Self {
            host: "localhost".into(),
            port: 5432,
            dbname: String::new(),
            user: String::new(),
            password: String::new(),
            slot_name: "tap_slot".into(),
            publication: "tap_publication".into(),
            tables: Vec::new(),
            plugin: "pgoutput".into(),
            ssl_mode: SslMode::Disable,
        }
    }
}

/// Validate a Postgres identifier for safe use in DDL.
///
/// Only allows alphanumeric characters, underscores, and dots (for
/// schema-qualified names like `"public.users"`).  Returns an error if
/// the identifier is empty or contains disallowed characters.
pub fn validate_identifier(name: &str, field_name: &str) -> Result<(), TapError> {
    if name.is_empty() {
        return Err(TapError::Config(format!("{field_name} must not be empty")));
    }
    if !name
        .chars()
        .all(|c| c.is_alphanumeric() || c == '_' || c == '.')
    {
        return Err(TapError::Config(format!(
            "{field_name} '{name}' contains invalid characters: \
             only alphanumeric, underscore, and dot are allowed"
        )));
    }
    Ok(())
}

impl SourceConfig {
    /// Checks that all required fields are non-empty and validates
    /// replication identifiers.
    pub fn validate(&self) -> Result<(), TapError> {
        let mut errors: Vec<String> = Vec::new();
        if self.dbname.is_empty() {
            errors.push("source.dbname is required".into());
        }
        if self.dbname.contains('\0') {
            errors.push("source.dbname must not contain null bytes".into());
        }
        if self.user.is_empty() {
            errors.push("source.user is required".into());
        }
        if self.user.contains('\0') {
            errors.push("source.user must not contain null bytes".into());
        }
        if self.password.is_empty() {
            errors.push("source.password is required".into());
        }

        // Validate replication identifiers (alphanumeric + underscore + dot)
        if let Err(e) = validate_identifier(&self.slot_name, "source.slotName") {
            errors.push(e.to_string());
        }
        if let Err(e) = validate_identifier(&self.publication, "source.publication") {
            errors.push(e.to_string());
        }
        for (i, table) in self.tables.iter().enumerate() {
            if let Err(e) = validate_identifier(table, &format!("source.tables[{i}]")) {
                errors.push(e.to_string());
            }
        }
        if let Err(e) = validate_identifier(&self.plugin, "source.plugin") {
            errors.push(e.to_string());
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(TapError::Config(format!(
                "config validation failed: {}",
                errors.join("; ")
            )))
        }
    }
}

// ---------------------------------------------------------------------------
// MySQL source
// ---------------------------------------------------------------------------

/// MySQL binlog source connection and replication settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct MySqlSourceConfig {
    /// MySQL server hostname.
    pub host: String,
    /// MySQL server port.
    pub port: u16,
    /// Database name to connect to.
    pub dbname: String,
    /// Replication user name.
    pub user: String,
    /// Replication user password.
    pub password: String,
    /// Tables to capture (e.g. `["mydb.users", "mydb.orders"]`).
    /// Empty means all tables.
    #[serde(default)]
    pub tables: Vec<String>,
    /// Unique server ID for this replication client (must be > 0).
    pub server_id: u32,
    /// Resume from a specific binlog file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binlog_file: Option<String>,
    /// Resume from a specific binlog offset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binlog_offset: Option<u64>,
    /// Binlog retention warning threshold in seconds (default: 86400 = 24h).
    ///
    /// A warning is emitted during pre-flight checks when
    /// `binlog_expire_logs_seconds` or `expire_logs_days` is below this
    /// threshold.  Set to 0 to disable the warning.
    #[serde(default = "default_binlog_retention_threshold")]
    pub binlog_retention_warning_threshold: u64,
}

const fn default_binlog_retention_threshold() -> u64 {
    86_400 // 24 hours
}

impl Default for MySqlSourceConfig {
    fn default() -> Self {
        Self {
            host: "localhost".into(),
            port: 3306,
            dbname: String::new(),
            user: String::new(),
            password: String::new(),
            tables: Vec::new(),
            server_id: 12345,
            binlog_file: None,
            binlog_offset: None,
            binlog_retention_warning_threshold: default_binlog_retention_threshold(),
        }
    }
}

impl fmt::Display for MySqlSourceConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "mysql://{}:****@{}:{}/{}",
            self.user, self.host, self.port, self.dbname,
        )
    }
}

impl MySqlSourceConfig {
    /// Build an `OptsBuilder` for `mysql_async::Pool`.
    ///
    /// Each field is set separately, avoiding URL encoding issues with credentials
    /// that contain metacharacters (`@`, `:`, `/`).
    pub fn opts(&self) -> mysql_async::OptsBuilder {
        mysql_async::OptsBuilder::default()
            .ip_or_hostname(self.host.clone())
            .tcp_port(self.port)
            .user(Some(self.user.clone()))
            .pass(Some(self.password.clone()))
            .db_name(Some(self.dbname.clone()))
    }

    /// Build a connection URL with the password redacted for logging.
    pub fn redacted_url(&self) -> String {
        format!(
            "mysql://{}:****@{}:{}/{}",
            self.user, self.host, self.port, self.dbname,
        )
    }

    /// Validate that all required fields are non-empty and server_id > 0.
    pub fn validate(&self) -> Result<(), TapError> {
        let mut errors: Vec<String> = Vec::new();

        if self.host.is_empty() {
            errors.push("mysqlSource.host is required".into());
        }
        if self.dbname.is_empty() {
            errors.push("mysqlSource.dbname is required".into());
        }
        if self.user.is_empty() {
            errors.push("mysqlSource.user is required".into());
        }
        if self.password.is_empty() {
            errors.push("mysqlSource.password is required".into());
        }
        if self.server_id == 0 {
            errors.push("mysqlSource.serverId must be > 0".into());
        }

        for (i, table) in self.tables.iter().enumerate() {
            if let Err(e) = validate_identifier(table, &format!("mysqlSource.tables[{i}]")) {
                errors.push(e.to_string());
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(TapError::Config(format!(
                "config validation failed: {}",
                errors.join("; ")
            )))
        }
    }
}

// ---------------------------------------------------------------------------
// Sink
// ---------------------------------------------------------------------------

/// SSE sink (HTTP server) configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct SinkConfig {
    /// Address the SSE server binds to.
    pub host: String,
    /// Port the SSE server listens on.
    pub port: u16,
    /// Maximum number of buffered events before blocking.
    pub max_buffer_size: usize,
    /// SSE heartbeat interval in milliseconds.
    pub heartbeat_interval_ms: u64,
    /// Optional API key for authenticating SSE client connections.
    /// Clients provide it via `Authorization: Bearer <key>` or
    /// `X-Api-Key: <key>`.  When `None`, auth is disabled.
    #[serde(default)]
    pub api_key: Option<String>,
}

impl Default for SinkConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".into(),
            port: 8080,
            max_buffer_size: 1000,
            heartbeat_interval_ms: 30_000,
            api_key: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Capture
// ---------------------------------------------------------------------------

/// Capture-engine run-time configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct CaptureConfig {
    /// Optional LSN to start replication from.  Empty / absent means
    /// use the publication's current position.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_lsn: Option<String>,
    /// Whether to run an initial snapshot before starting streaming.
    pub snapshot: bool,
    /// Maximum number of events to batch per flush.
    pub max_batch_size: usize,
    /// Flush interval in milliseconds.
    pub flush_interval_ms: u64,
}

impl Default for CaptureConfig {
    fn default() -> Self {
        Self {
            from_lsn: None,
            snapshot: true,
            max_batch_size: 100,
            flush_interval_ms: 1_000,
        }
    }
}

// ---------------------------------------------------------------------------
// Snapshot
// ---------------------------------------------------------------------------

/// Snapshot-phase configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct SnapshotConfig {
    /// Number of rows to fetch per snapshot query batch.
    pub batch_size: u64,
    /// Number of parallel worker threads for snapshotting.
    pub num_workers: u32,
    /// Tables to include in the snapshot.  Empty means all captured tables.
    #[serde(default)]
    pub tables: Vec<String>,
}

impl SnapshotConfig {
    /// Validate snapshot configuration.
    pub fn validate(&self) -> Result<(), TapError> {
        let mut errors: Vec<String> = Vec::new();

        if self.batch_size == 0 {
            errors.push("snapshot.batchSize must be > 0".into());
        }

        for (i, table) in self.tables.iter().enumerate() {
            if let Err(e) = validate_identifier(table, &format!("snapshot.tables[{i}]")) {
                errors.push(e.to_string());
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(TapError::Config(format!(
                "config validation failed: {}",
                errors.join("; ")
            )))
        }
    }
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            batch_size: 1000,
            num_workers: 4,
            tables: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

/// SQLite-backed state-store configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct StateConfig {
    /// Path to the SQLite database file.
    pub path: String,
    /// Maximum size (in kilobytes) for automatic state backups.
    pub max_backup_size_kb: u64,
}

impl Default for StateConfig {
    fn default() -> Self {
        Self {
            path: ".tap/state.db".into(),
            max_backup_size_kb: 10_240,
        }
    }
}

// ---------------------------------------------------------------------------
// Logging
// ---------------------------------------------------------------------------

/// Structured-logging configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct LoggingConfig {
    /// Output format: `"json"` or `"text"`.
    pub format: String,
    /// Log level: `"trace"`, `"debug"`, `"info"`, `"warn"`, `"error"`.
    pub level: String,
    /// Optional file path to write logs to (writes to stderr when absent).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            format: "json".into(),
            level: "info".into(),
            file: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Canonical TOML example covering every section.
    const CANONICAL_TOML: &str = r#"
[source]
host = "pg.example.com"
port = 5432
dbname = "myapp"
user = "replicator"
password = "s3cret"
slotName = "tap_slot"
publication = "tap_pub"
tables = ["public.users", "public.orders"]
plugin = "pgoutput"

[sink]
host = "0.0.0.0"
port = 8080
maxBufferSize = 500
heartbeatIntervalMs = 15_000

[capture]
snapshot = true
maxBatchSize = 200
flushIntervalMs = 500

[snapshot]
batchSize = 5000
numWorkers = 8

[state]
path = "/data/tap/state.db"
maxBackupSizeKb = 20_480

[logging]
format = "text"
level = "debug"
file = "/var/log/tap.log"
"#;

    #[test]
    fn test_config_from_toml() {
        let config: TapConfig = toml::from_str(CANONICAL_TOML).expect("parse canonical TOML");

        // Source
        assert_eq!(config.source.host, "pg.example.com");
        assert_eq!(config.source.port, 5432);
        assert_eq!(config.source.dbname, "myapp");
        assert_eq!(config.source.user, "replicator");
        assert_eq!(config.source.password, "s3cret");
        assert_eq!(config.source.slot_name, "tap_slot");
        assert_eq!(config.source.publication, "tap_pub");
        assert_eq!(config.source.tables, vec!["public.users", "public.orders"]);
        assert_eq!(config.source.plugin, "pgoutput");

        // Sink
        assert_eq!(config.sink.host, "0.0.0.0");
        assert_eq!(config.sink.port, 8080);
        assert_eq!(config.sink.max_buffer_size, 500);
        assert_eq!(config.sink.heartbeat_interval_ms, 15_000);

        // Capture
        assert!(config.capture.snapshot);
        assert_eq!(config.capture.max_batch_size, 200);
        assert_eq!(config.capture.flush_interval_ms, 500);
        assert!(config.capture.from_lsn.is_none());

        // Snapshot
        assert_eq!(config.snapshot.batch_size, 5_000);
        assert_eq!(config.snapshot.num_workers, 8);

        // State
        assert_eq!(config.state.path, "/data/tap/state.db");
        assert_eq!(config.state.max_backup_size_kb, 20_480);

        // Logging
        assert_eq!(config.logging.format, "text");
        assert_eq!(config.logging.level, "debug");
        assert_eq!(config.logging.file, Some("/var/log/tap.log".into()));
    }

    #[test]
    fn test_config_defaults() {
        // Sections should have sensible defaults
        let source = SourceConfig::default();
        assert_eq!(source.host, "localhost");
        assert_eq!(source.port, 5432);

        let sink = SinkConfig::default();
        assert_eq!(sink.port, 8080);

        let capture = CaptureConfig::default();
        assert!(capture.snapshot);
        assert_eq!(capture.max_batch_size, 100);

        let snapshot = SnapshotConfig::default();
        assert_eq!(snapshot.batch_size, 1_000);
        assert_eq!(snapshot.num_workers, 4);

        let state = StateConfig::default();
        assert_eq!(state.path, ".tap/state.db");

        let logging = LoggingConfig::default();
        assert_eq!(logging.format, "json");
        assert_eq!(logging.level, "info");
        assert!(logging.file.is_none());
    }

    #[test]
    fn test_config_minimal_toml() {
        // Minimal TOML should parse with defaults for anything unspecified
        let toml_str = r#"
[source]
host = "localhost"
port = 5432
dbname = "test"
user = "u"
password = "p"

[sink]
host = "0.0.0.0"
port = 8080

[capture]
snapshot = true

[snapshot]
batchSize = 1000
numWorkers = 4

[state]
path = "state.db"

[logging]
format = "json"
level = "info"
"#;

        let config: TapConfig = toml::from_str(toml_str).expect("minimal TOML");
        assert_eq!(config.source.host, "localhost");
        assert_eq!(config.sink.port, 8080);
        assert_eq!(config.state.path, "state.db");
    }

    #[test]
    fn test_config_from_path_invalid() {
        let result = TapConfig::from_path("/tmp/nonexistent/tap-config.toml");
        assert!(result.is_err());
    }

    #[test]
    fn test_config_debug_redacts_password() {
        let s = SourceConfig {
            password: "s3cret".into(),
            ..SourceConfig::default()
        };
        let debug_str = format!("{s:?}");
        // The password value must NOT appear in debug output
        assert!(
            !debug_str.contains("s3cret"),
            "password leaked: {debug_str}"
        );
        // Must show redaction marker instead
        assert!(debug_str.contains("<REDACTED>"));
    }

    #[test]
    fn test_config_validation_empty_dbname() {
        let config = SourceConfig {
            dbname: String::new(),
            user: "u".into(),
            password: "p".into(),
            ..SourceConfig::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("dbname"));
    }

    #[test]
    fn test_config_validation_empty_user() {
        let config = SourceConfig {
            dbname: "d".into(),
            user: String::new(),
            password: "p".into(),
            ..SourceConfig::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("user"));
    }

    #[test]
    fn test_config_validation_empty_password() {
        let config = SourceConfig {
            dbname: "d".into(),
            user: "u".into(),
            password: String::new(),
            ..SourceConfig::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("password"));
    }

    #[test]
    fn test_config_validation_all_required_ok() {
        let config = SourceConfig {
            dbname: "d".into(),
            user: "u".into(),
            password: "p".into(),
            ..SourceConfig::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validation_null_byte_in_user() {
        let config = SourceConfig {
            dbname: "d".into(),
            user: "u\0options=-clog_statement=all".into(),
            password: "p".into(),
            ..SourceConfig::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("null byte"));
    }

    #[test]
    fn test_config_validation_null_byte_in_dbname() {
        let config = SourceConfig {
            dbname: "d\0extra".into(),
            user: "u".into(),
            password: "p".into(),
            ..SourceConfig::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("null byte"));
    }

    #[test]
    fn test_config_validation_from_path_rejects_missing_required() {
        let dir = std::env::temp_dir();
        let path = dir.join("test_tap_invalid_config.toml");
        let toml_str = r#"
[source]
host = "localhost"
port = 5432
user = "u"
password = "p"

[sink]
host = "0.0.0.0"
port = 8080

[capture]
snapshot = true

[snapshot]
batchSize = 1000
numWorkers = 4

[state]
path = "state.db"

[logging]
format = "json"
level = "info"
"#;
        std::fs::write(&path, toml_str).unwrap();

        let result = TapConfig::from_path(path.to_str().unwrap());
        let _ = std::fs::remove_file(&path);

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("dbname"), "expected 'dbname' in error: {err}");
    }

    #[test]
    fn test_snapshot_config_validates_batch_size_zero() {
        let config = SnapshotConfig {
            batch_size: 0,
            ..SnapshotConfig::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("batchSize"));
    }

    #[test]
    fn test_snapshot_config_validates_tables() {
        let config = SnapshotConfig {
            tables: vec!["public.users".into(), "evil; DROP TABLE users; --".into()],
            ..SnapshotConfig::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("invalid characters"));
    }

    #[test]
    fn test_snapshot_config_validates_ok() {
        let config = SnapshotConfig {
            batch_size: 1000,
            tables: vec!["public.users".into(), "public.orders".into()],
            ..SnapshotConfig::default()
        };
        assert!(config.validate().is_ok());
    }
}

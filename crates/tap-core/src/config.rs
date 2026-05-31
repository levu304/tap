//! Configuration types for Tap capture sessions.
//!
//! Deserialized from TOML configuration files.  The top-level
//! [`TapConfig`] bundles sub-configurations for the Postgres source,
//! SSE sink, capture behaviour, snapshotting, SQLite-backed state,
//! and structured logging.

use serde::{Deserialize, Serialize};
use std::fmt;

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
    pub source: SourceConfig,
    pub sink: SinkConfig,
    pub capture: CaptureConfig,
    pub snapshot: SnapshotConfig,
    pub state: StateConfig,
    pub logging: LoggingConfig,
}

impl fmt::Debug for TapConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TapConfig")
            .field("source", &self.source)
            .field("sink", &self.sink)
            .field("capture", &self.capture)
            .field("snapshot", &self.snapshot)
            .field("state", &self.state)
            .field("logging", &self.logging)
            .finish()
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
        self.source.validate()?;
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
        }
    }
}

impl SourceConfig {
    /// Checks that all required fields are non-empty.
    fn validate(&self) -> Result<(), TapError> {
        let mut errors: Vec<String> = Vec::new();
        if self.dbname.is_empty() {
            errors.push("source.dbname is required".into());
        }
        if self.user.is_empty() {
            errors.push("source.user is required".into());
        }
        if self.password.is_empty() {
            errors.push("source.password is required".into());
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
}

impl Default for SinkConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".into(),
            port: 8080,
            max_buffer_size: 1000,
            heartbeat_interval_ms: 30_000,
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
}

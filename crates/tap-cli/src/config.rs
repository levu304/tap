//! Configuration loading helpers for the Tap CLI.
//!
//! Wraps [`tap_core::config::TapConfig`] with CLI-specific convenience
//! functions for loading from a file path or building from [`InitArgs`].

use crate::commands::init::InitArgs;
use tap_core::config::{
    CaptureConfig, LoggingConfig, SinkConfig, SnapshotConfig, SourceConfig, SslMode, StateConfig,
    TapConfig,
};
use tap_core::error::TapError;

/// The default configuration file path, used by all CLI commands.
pub const DEFAULT_CONFIG_PATH: &str = ".tap/config.toml";

/// Load a [`TapConfig`] from a TOML file at the given path.
///
/// Delegates to [`TapConfig::from_path`].
///
/// # Errors
///
/// Returns [`TapError::Io`] if the file cannot be read, or
/// [`TapError::Config`] if the TOML content is malformed or invalid.
pub fn load_config(path: &str) -> Result<TapConfig, TapError> {
    TapConfig::from_path(path)
}

/// Build a [`TapConfig`] from [`InitArgs`], using defaults for unspecified
/// sub-configs.  This is used by `tap init` before a TOML file exists.
pub fn config_from_init_args(args: &InitArgs) -> TapConfig {
    TapConfig {
        source: SourceConfig {
            host: args.host.clone(),
            port: args.port,
            dbname: args.db.clone(),
            user: args.user.clone(),
            password: args.password.clone(),
            slot_name: args.slot.clone(),
            publication: args.publication.clone(),
            tables: args.tables.clone(),
            plugin: args.plugin.clone(),
            ssl_mode: SslMode::Require,
        },
        sink: SinkConfig::default(),
        capture: CaptureConfig::default(),
        snapshot: SnapshotConfig::default(),
        state: StateConfig::default(),
        logging: LoggingConfig::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Cli, Command};
    use clap::Parser;

    #[test]
    fn test_config_from_init_args_roundtrip() {
        let cli = Cli::parse_from([
            "tap",
            "init",
            "--db",
            "testdb",
            "--user",
            "replicator",
            "--password",
            "s3cret",
            "--host",
            "pg.example.com",
            "--port",
            "5432",
            "--slot",
            "my_slot",
            "--publication",
            "my_pub",
            "--table",
            "public.users",
            "--table",
            "public.orders",
            "--plugin",
            "pgoutput",
            "--output",
            "/tmp/tap_test",
            "--force",
        ]);
        let args = match cli.command {
            Command::Init(args) => args,
            _ => panic!("expected Init command"),
        };

        let config = config_from_init_args(&args);

        assert_eq!(config.source.host, "pg.example.com");
        assert_eq!(config.source.port, 5432);
        assert_eq!(config.source.dbname, "testdb");
        assert_eq!(config.source.user, "replicator");
        assert_eq!(config.source.password, "s3cret");
        assert_eq!(config.source.slot_name, "my_slot");
        assert_eq!(config.source.publication, "my_pub");
        assert_eq!(config.source.tables, vec!["public.users", "public.orders"]);
        assert_eq!(config.source.plugin, "pgoutput");
    }

    #[test]
    fn test_load_config_returns_error_for_nonexistent() {
        let result = load_config("/tmp/nonexistent_tap_config_dir/file.toml");
        assert!(result.is_err());
    }
}

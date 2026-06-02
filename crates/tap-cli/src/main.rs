//! Tap CLI — Postgres Change Data Capture platform.
//!
//! Binary entry point with clap derive CLI.  Supports five subcommands:
//!
//! * `tap init`     — scaffold a new Tap project
//! * `tap capture`  — start a capture session
//! * `tap inspect`  — inspect database schema, generate types
//! * `tap dev`      — dev mode with enhanced output
//! * `tap test`     — validate fixture files

#![deny(clippy::all)]
#![deny(clippy::pedantic)]
#![allow(
    clippy::module_name_repetitions,
    clippy::missing_errors_doc,
    clippy::must_use_candidate,
    clippy::doc_markdown,
    clippy::default_trait_access,
    clippy::too_many_lines,
    clippy::similar_names
)]

use std::process::ExitCode;

use clap::Parser;

use tap_core::error::TapError;

mod commands;
mod config;

/// Tap — Change Data Capture platform.
///
/// Captures row-level changes from Postgres logical replication
/// and streams them via Server-Sent Events (SSE).
#[derive(Parser)]
#[command(
    name = "tap",
    version,
    about = "Postgres Change Data Capture platform",
    long_about = "\
Tap captures row-level changes from Postgres logical replication \
and streams them via Server-Sent Events (SSE).

SUPPORTED COMMANDS:
  init      Scaffold a new Tap project in the current directory
  capture   Start a capture session (snapshot + streaming)
  inspect   Inspect database schema and generate type definitions
  dev       Start a capture session with enhanced output (dev mode)
  test      Validate fixture files

EXAMPLE:
  tap init --db myapp --table public.users --table public.orders
  tap capture
  tap inspect --json
"
)]
struct Cli {
    /// Path to the TOML configuration file.
    #[arg(
        short = 'c',
        long = "config",
        default_value_t = String::from(crate::config::DEFAULT_CONFIG_PATH),
        global = true,
        help = "Path to TOML configuration file"
    )]
    config: String,

    /// Log level filter (trace, debug, info, warn, error).
    #[arg(
        long = "log-level",
        default_value = "info",
        global = true,
        help = "Log level filter"
    )]
    log_level: String,

    /// Log output format (text or json).
    #[arg(
        long = "log-format",
        default_value = "text",
        global = true,
        help = "Log format (text or json)"
    )]
    log_format: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Parser)]
enum Command {
    /// Initialize a new Tap project in the current directory.
    Init(commands::init::InitArgs),
    /// Start a capture session (snapshot + streaming).
    Capture(commands::capture::CaptureArgs),
    /// Inspect database schema and generate type definitions.
    Inspect(commands::inspect::InspectArgs),
    /// Start a capture session with enhanced output (dev mode).
    Dev(commands::dev::DevArgs),
    /// Validate fixture files.
    Test(commands::test::TestArgs),
}

/// Entry point.
#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();

    init_tracing(&cli.log_level, &cli.log_format);

    let result = run_command(cli).await;

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            tracing::error!("{err}");
            ExitCode::from(exit_code_for_error(&err))
        }
    }
}

/// Initialize tracing/logging based on the provided level and format.
fn init_tracing(level: &str, format: &str) {
    let filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
        .with_env_var("TAP_LOG")
        .parse_lossy(level);

    match format {
        "json" => {
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .json()
                .with_target(true)
                .init();
        }
        _ => {
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .with_target(true)
                .init();
        }
    }
}

/// Dispatch to the appropriate command handler.
async fn run_command(cli: Cli) -> Result<(), TapError> {
    match cli.command {
        Command::Init(args) => commands::init::run(args).await,
        Command::Capture(args) => commands::capture::run(args).await,
        Command::Inspect(args) => commands::inspect::run(args).await,
        Command::Dev(args) => commands::dev::run(args).await,
        Command::Test(args) => commands::test::run(&args),
    }
}

/// Map [`TapError`] variants to POSIX exit codes.
///
/// | Code | Condition                      |
/// |------|--------------------------------|
/// | 0    | Success                        |
/// | 1    | Configuration error            |
/// | 2    | Postgres connection error      |
/// | 3    | Permission error (reserved)    |
/// | 4    | I/O error                      |
/// | 5    | Internal / data decode error   |
/// | 6    | Replication slot error         |
/// | 7    | State store corruption         |
/// | 8    | Fatal / unknown error          |
fn exit_code_for_error(err: &TapError) -> u8 {
    match err {
        TapError::Config(_) => 1,
        TapError::PostgresConnection(_) | TapError::PostgresConnectionRedacted(_) => 2,
        TapError::ReplicationSlot(_) => 6,
        TapError::Io(_) => 4,
        TapError::Decode(_) | TapError::Snapshot(_) => 5,
        TapError::Sqlite(_) | TapError::StateCorruption(_) => 7,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn test_cli_version() {
        // Verify the CLI can parse --version (just check it doesn't panic)
        let _cli = Cli::try_parse_from(["tap", "--version"]);
    }

    #[test]
    fn test_cli_help() {
        let _cli = Cli::try_parse_from(["tap", "--help"]);
    }

    #[test]
    fn test_cli_init_subcommand() {
        let cli = Cli::try_parse_from([
            "tap",
            "init",
            "--db",
            "testdb",
            "--user",
            "postgres",
            "--password",
            "secret",
        ])
        .expect("should parse init command");
        assert!(matches!(cli.command, Command::Init(_)));
    }

    #[test]
    fn test_cli_capture_subcommand() {
        let cli = Cli::try_parse_from(["tap", "capture"]).expect("should parse capture command");
        assert!(matches!(cli.command, Command::Capture(_)));
    }

    #[test]
    fn test_cli_inspect_subcommand() {
        let cli = Cli::try_parse_from(["tap", "inspect"]).expect("should parse inspect command");
        assert!(matches!(cli.command, Command::Inspect(_)));
    }

    #[test]
    fn test_cli_dev_subcommand() {
        let cli = Cli::try_parse_from(["tap", "dev"]).expect("should parse dev command");
        assert!(matches!(cli.command, Command::Dev(_)));
    }

    #[test]
    fn test_cli_test_subcommand() {
        let cli = Cli::try_parse_from(["tap", "test"]).expect("should parse test command");
        assert!(matches!(cli.command, Command::Test(_)));
    }

    #[test]
    fn test_cli_global_config_flag() {
        let cli = Cli::try_parse_from(["tap", "--config", "/custom/path.toml", "capture"])
            .expect("should parse with global config flag");
        assert_eq!(cli.config, "/custom/path.toml");
    }

    #[test]
    fn test_cli_global_log_level() {
        let cli = Cli::try_parse_from(["tap", "--log-level", "debug", "capture"])
            .expect("should parse with log-level flag");
        assert_eq!(cli.log_level, "debug");
    }

    #[test]
    fn test_cli_global_log_format() {
        let cli = Cli::try_parse_from(["tap", "--log-format", "json", "capture"])
            .expect("should parse with log-format flag");
        assert_eq!(cli.log_format, "json");
    }

    #[test]
    fn test_exit_codes() {
        assert_eq!(exit_code_for_error(&TapError::Config("bad".into())), 1);
        assert_eq!(
            exit_code_for_error(&TapError::PostgresConnectionRedacted("fail".into())),
            2
        );
        assert_eq!(
            exit_code_for_error(&TapError::ReplicationSlot("fail".into())),
            6
        );
        assert_eq!(
            exit_code_for_error(&TapError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "io"
            ))),
            4
        );
        assert_eq!(exit_code_for_error(&TapError::Decode("bad data".into())), 5);
        assert_eq!(
            exit_code_for_error(&TapError::StateCorruption("corrupt".into())),
            7
        );
    }

    fn extract_init(cli: Cli) -> commands::init::InitArgs {
        match cli.command {
            Command::Init(args) => args,
            _ => panic!("expected Init command"),
        }
    }

    fn extract_capture(cli: Cli) -> commands::capture::CaptureArgs {
        match cli.command {
            Command::Capture(args) => args,
            _ => panic!("expected Capture command"),
        }
    }

    fn extract_inspect(cli: Cli) -> commands::inspect::InspectArgs {
        match cli.command {
            Command::Inspect(args) => args,
            _ => panic!("expected Inspect command"),
        }
    }

    fn extract_dev(cli: Cli) -> commands::dev::DevArgs {
        match cli.command {
            Command::Dev(args) => args,
            _ => panic!("expected Dev command"),
        }
    }

    fn extract_test(cli: Cli) -> commands::test::TestArgs {
        match cli.command {
            Command::Test(args) => args,
            _ => panic!("expected Test command"),
        }
    }

    #[test]
    fn test_init_args_defaults() {
        let args = extract_init(
            Cli::try_parse_from([
                "tap",
                "init",
                "--db",
                "mydb",
                "--user",
                "u",
                "--password",
                "p",
            ])
            .expect("should parse"),
        );

        assert_eq!(args.db, "mydb");
        assert_eq!(args.slot, "tap_slot");
        assert_eq!(args.publication, "tap_publication");
        assert_eq!(args.plugin, "pgoutput");
        assert_eq!(args.output, ".");
        assert!(!args.force);
    }

    #[test]
    fn test_capture_args_defaults() {
        let args = extract_capture(Cli::try_parse_from(["tap", "capture"]).expect("should parse"));

        assert_eq!(args.config, ".tap/config.toml");
        assert!(args.from_lsn.is_none());
        assert!(!args.snapshot);
        assert!(args.tables.is_empty());
    }

    #[test]
    fn test_inspect_args_defaults() {
        let args = extract_inspect(Cli::try_parse_from(["tap", "inspect"]).expect("should parse"));

        assert_eq!(args.config, ".tap/config.toml");
        assert!(!args.json);
        assert!(args.output.is_none());
    }

    #[test]
    fn test_dev_args_defaults() {
        let args = extract_dev(Cli::try_parse_from(["tap", "dev"]).expect("should parse"));

        assert_eq!(args.config, ".tap/config.toml");
        assert!(args.from_lsn.is_none());
        assert!(!args.snapshot);
    }

    #[test]
    fn test_test_args_defaults() {
        let args = extract_test(Cli::try_parse_from(["tap", "test"]).expect("should parse"));

        assert_eq!(args.config, ".tap/config.toml");
        assert!(!args.list);
        assert!(args.file.is_none());
    }

    #[test]
    fn test_capture_with_tables() {
        let args = extract_capture(
            Cli::try_parse_from([
                "tap",
                "capture",
                "--table",
                "public.users",
                "--table",
                "public.orders",
            ])
            .expect("should parse"),
        );

        assert_eq!(args.tables, vec!["public.users", "public.orders"]);
    }
}

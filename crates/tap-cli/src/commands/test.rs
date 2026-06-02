//! `tap test` — validate fixture files and schema definitions.
//!
//! In v0.1.0 this is a placeholder.  It lists fixture files from
//! `.tap/fixtures/` and validates that individual fixture files
//! contain valid [`ChangeEvent`] JSON.

use std::path::Path;

use clap::Args;
use tap_core::error::TapError;
use tap_core::event::ChangeEvent;

/// Arguments for the `tap test` command.
#[derive(Args, Debug, Clone)]
pub struct TestArgs {
    /// Path to the TOML configuration file.
    #[arg(
        short = 'c',
        long = "config",
        default_value_t = String::from(crate::config::DEFAULT_CONFIG_PATH)
    )]
    pub config: String,

    /// List available fixture files from `.tap/fixtures/`.
    #[arg(short = 'l', long = "list")]
    pub list: bool,

    /// Path to a single fixture file to validate.
    #[arg(short = 'f', long = "file")]
    pub file: Option<String>,
}

/// The default fixture directory, relative to the project root.
const FIXTURE_DIR: &str = ".tap/fixtures";

/// Run `tap test`.
pub fn run(args: &TestArgs) -> Result<(), TapError> {
    if args.list {
        list_fixtures()?;
        return Ok(());
    }

    if let Some(file_path) = &args.file {
        validate_fixture(file_path)?;
        return Ok(());
    }

    // No flags set — print brief usage hint
    println!("tap test: validate fixture files");
    println!();
    println!("Usage:");
    println!("  tap test --list              List fixture files");
    println!("  tap test --file <path>       Validate a fixture file");
    println!();
    println!("Fixture files are JSON files containing a ChangeEvent payload.");
    println!("They live in the {FIXTURE_DIR}/ directory by default.");

    Ok(())
}

/// List `.json` fixture files from the default fixture directory.
fn list_fixtures() -> Result<(), TapError> {
    let dir = Path::new(FIXTURE_DIR);

    if !dir.exists() {
        println!("No fixture directory found at {FIXTURE_DIR}");
        println!("Create one with `mkdir -p {FIXTURE_DIR}` and add `.json` files.");
        return Ok(());
    }

    let mut entries: Vec<_> = std::fs::read_dir(dir)?
        .filter_map(Result::ok)
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "json"))
        .collect();

    entries.sort_by_key(std::fs::DirEntry::file_name);

    if entries.is_empty() {
        println!("No `.json` fixture files found in {FIXTURE_DIR}/");
        return Ok(());
    }

    println!("Fixture files in {FIXTURE_DIR}/:");
    for entry in &entries {
        let meta = entry.metadata().ok();
        let size = meta.as_ref().map_or(0, std::fs::Metadata::len);
        println!("  {} ({} bytes)", entry.file_name().to_string_lossy(), size);
    }

    Ok(())
}

/// Validate that a single JSON file is a valid [`ChangeEvent`].
fn validate_fixture(path: &str) -> Result<(), TapError> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| TapError::Io(std::io::Error::new(e.kind(), format!("{path}: {e}"))))?;

    // Try to parse as a ChangeEvent
    let event: ChangeEvent = serde_json::from_str(&content)
        .map_err(|e| TapError::Decode(format!("{path}: invalid ChangeEvent JSON: {e}")))?;

    println!("✓ {path} — valid ChangeEvent");
    println!(
        "   op={op}, table={schema}.{table}, lsn={lsn}",
        op = event.op.as_str(),
        schema = event.source.schema,
        table = event.source.table,
        lsn = event.source.lsn,
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_valid_change_event() {
        let json = r#"{
            "op": "c",
            "before": null,
            "after": {"id": 1, "name": "test"},
            "source": {
                "db": "testdb",
                "schema": "public",
                "table": "users",
                "lsn": "0/ABCDEF",
                "tx_id": "42",
                "ts_ms": 1700000000000
            },
            "ts_ms": 1700000000001,
            "id": "0/ABCDEF:42"
        }"#;

        let event: ChangeEvent = serde_json::from_str(json).expect("should parse");
        assert_eq!(event.op.as_str(), "c");
        assert_eq!(event.source.table, "users");
    }

    #[test]
    fn test_validate_invalid_change_event() {
        let json = r#"{"not": "a change event"}"#;
        let result: Result<ChangeEvent, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_missing_op_field() {
        let json = r#"{
            "before": null,
            "after": {"id": 1},
            "source": {
                "db": "test",
                "schema": "public",
                "table": "t",
                "lsn": "0/1",
                "txId": "1",
                "tsMs": 0
            },
            "tsMs": 0,
            "id": "test"
        }"#;

        let result: Result<ChangeEvent, _> = serde_json::from_str(json);
        assert!(result.is_err(), "missing op field should fail");
    }
}

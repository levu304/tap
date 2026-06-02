//! `tap dev` — development mode capture session.
//!
//! In v0.1.0 this is functionally equivalent to `tap capture` with
//! enhanced terminal output formatting and a minimal HTML status page.

use clap::Args;
use tap_core::error::TapError;
use tracing::info;

/// Arguments for the `tap dev` command.
#[derive(Args, Debug, Clone)]
pub struct DevArgs {
    /// Path to the TOML configuration file.
    #[arg(short = 'c', long = "config", default_value = ".tap/config.toml")]
    pub config: String,

    /// Start replication from a specific LSN (overrides saved checkpoint).
    #[arg(long = "from-lsn")]
    pub from_lsn: Option<String>,

    /// Force a full snapshot before starting streaming.
    #[arg(short = 's', long = "snapshot")]
    pub snapshot: bool,
}

/// Run `tap dev`.
///
/// In v0.1.0, delegates to `tap capture` with enhanced output.  Future
/// versions will add a `/status` HTML page and richer terminal dashboards.
pub async fn run(args: DevArgs) -> Result<(), TapError> {
    info!("Starting dev mode — enhanced output enabled");

    // Build capture args from dev args
    let capture_args = super::capture::CaptureArgs {
        config: args.config,
        from_lsn: args.from_lsn,
        snapshot: args.snapshot,
        tables: Vec::new(),
    };

    // The dev command currently reuses the capture logic.
    // Future enhancement: add a separate HTML status page route
    // to the SSE server for `tap dev`.
    println!("╔══════════════════════════════════════════════╗");
    println!(
        "║     Tap Dev Mode — v{}              ║",
        env!("CARGO_PKG_VERSION")
    );
    println!("╚══════════════════════════════════════════════╝");
    println!();

    // Delegate to capture's run function
    super::capture::run(capture_args).await
}

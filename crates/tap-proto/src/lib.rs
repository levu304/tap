//! Generated protobuf types for the Tap CDC protocol.
//!
//! This crate contains the protobuf definitions compiled via `tonic-build`.
//! The generated types are re-exported for use by `tap-core` and `tap-sidecar`.

tonic::include_proto!("tap.v1");

// Re-export the package version
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

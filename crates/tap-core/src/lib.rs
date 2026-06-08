//! tap-core — Change Data Capture engine
//!
//! Core library for Tap, a PostgreSQL Change Data Capture platform.
//! Handles logical replication connections, WAL decoding, snapshotting,
//! state management, and SSE event delivery.

pub mod config;
pub mod error;
pub mod event;
pub mod mysql;
pub mod postgres;
pub mod replication;
pub mod snapshot;
pub mod sse;
pub mod state;

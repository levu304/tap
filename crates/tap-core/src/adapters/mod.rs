//! Database adapter implementations.
//!
//! Each adapter (e.g. `mysql`, `postgres`) implements the CDC connector
//! trait for a specific database engine, handling connection lifecycle,
//! event stream decoding, and offset tracking.

pub mod mysql;

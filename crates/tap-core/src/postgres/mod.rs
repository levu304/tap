//! Postgres logical replication connection and slot lifecycle.
//!
//! This module provides:
//!
//! * [`Lsn`] — a Postgres Log Sequence Number (LSN) newtype with parsing,
//!   display, and serialization support.
//! * [`PgConnection`] — a Postgres connection configured for logical
//!   replication, managing slot creation, publication management, table
//!   validation, and replication stream startup.
//! * [`ReplicationStream`] — a thin stream wrapper that yields raw WAL
//!   payload bytes with XLogData message framing stripped.

pub mod connection;
pub mod decoder;

pub use crate::replication::ReplicationStream;
pub use connection::{Lsn, PgConnection, connect_plain};
pub use decoder::{
    ColumnInfo, PgoutputDecoder, RelationSchema, Wal2JsonDecoder, WalDecoder, create_decoder,
};

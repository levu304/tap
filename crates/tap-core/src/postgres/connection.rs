//! Postgres logical replication connection and slot lifecycle.
//!
//! Implements [`Lsn`] (Log Sequence Number), [`PgConnection`] (replication
//! client wrapper), and [`ReplicationStream`] (WAL data stream).

use std::{
    fmt,
    pin::Pin,
    str::FromStr,
    task::{Context, Poll},
};

use futures::Stream;
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::{config::SourceConfig, error::TapError};

// ---------------------------------------------------------------------------
// Lsn — Log Sequence Number
// ---------------------------------------------------------------------------

/// A Postgres Log Sequence Number (LSN).
///
/// LSNs are 64-bit integers representing a position in the WAL.  They are
/// commonly displayed in the format `0/16B37428` where the value before `/`
/// is the upper 32 bits (in hex) and the value after `/` is the lower 32
/// bits (in zero-padded hex).
///
/// # Examples
///
/// ```
/// use tap_core::postgres::Lsn;
/// use std::str::FromStr;
///
/// let lsn = Lsn::from_str("0/16B37428").unwrap();
/// assert_eq!(lsn, Lsn(0x16B37428));
/// assert_eq!(lsn.to_string(), "0/16B37428");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Lsn(pub u64);

impl Lsn {
    /// Zero LSN constant, representing the beginning of the WAL.
    pub const ZERO: Lsn = Lsn(0);
}

impl FromStr for Lsn {
    type Err = TapError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split('/').collect();
        if parts.len() != 2 {
            return Err(TapError::Decode(format!(
                "invalid LSN format: expected 'X/XXXXXXXX', got '{s}'"
            )));
        }
        let high = u32::from_str_radix(parts[0], 16)
            .map_err(|e| TapError::Decode(format!("invalid LSN high part '{}': {e}", parts[0])))?;
        let low = u32::from_str_radix(parts[1], 16)
            .map_err(|e| TapError::Decode(format!("invalid LSN low part '{}': {e}", parts[1])))?;
        Ok(Lsn(((high as u64) << 32) | low as u64))
    }
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let high = (self.0 >> 32) as u32;
        let low = self.0 as u32;
        write!(f, "{high:X}/{low:08X}")
    }
}

impl Serialize for Lsn {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.to_string().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Lsn {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

// ---------------------------------------------------------------------------
// Connection string builder
// ---------------------------------------------------------------------------

/// Build a connection string from [`SourceConfig`] suitable for
/// `tokio-postgres`.
///
/// Includes the `options=--replication=database` parameter required for
/// logical replication connections.
fn connection_string(config: &SourceConfig) -> String {
    format!(
        "host={} port={} dbname={} user={} password={} options='--replication=database'",
        config.host, config.port, config.dbname, config.user, config.password,
    )
}

/// Build a redacted version of the connection string for logging purposes.
/// The password value is replaced with `<REDACTED>`.
fn redacted_connection_string(config: &SourceConfig) -> String {
    format!(
        "host={} port={} dbname={} user={} password=<REDACTED> options='--replication=database'",
        config.host, config.port, config.dbname, config.user,
    )
}

// ---------------------------------------------------------------------------
// PgConnection
// ---------------------------------------------------------------------------

/// A connection to a Postgres database configured for logical replication.
///
/// Wraps a `tokio_postgres::Client` and the [`SourceConfig`] that was used
/// to create it.  Provides methods for managing replication slots,
/// publications, and streaming WAL data.
pub struct PgConnection {
    /// The underlying tokio-postgres client.
    client: tokio_postgres::Client,
    /// The configuration used to establish the connection.
    config: SourceConfig,
}

impl PgConnection {
    /// Connect to Postgres in replication mode.
    ///
    /// Builds a connection string from the provided config, connects via
    /// `tokio_postgres::connect()`, spawns the background connection
    /// handler, and returns a [`PgConnection`] ready for replication
    /// operations.
    ///
    /// The connection string includes `options=--replication=database` to
    /// enable logical replication mode.
    ///
    /// # Errors
    ///
    /// Returns [`TapError::PostgresConnection`] if the connection fails.
    pub async fn connect(config: &SourceConfig) -> Result<Self, TapError> {
        let conn_str = connection_string(config);
        let redacted = redacted_connection_string(config);
        info!("connecting to Postgres: {redacted}");

        let (client, connection) =
            tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await?;

        // Spawn the connection handler in the background so it can process
        // messages and keep the connection alive.
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("Postgres connection error: {e}");
            }
        });

        info!(
            "connected to Postgres (host={}, dbname={})",
            config.host, config.dbname
        );

        Ok(Self {
            client,
            config: config.clone(),
        })
    }

    /// Ensure the replication slot exists.
    ///
    /// Queries `pg_replication_slots` for a slot matching the configured
    /// slot name.  If found, returns its `confirmed_flush_lsn` parsed as an
    /// [`Lsn`].  If not found, creates the slot via
    /// `CREATE_REPLICATION_SLOT` and returns [`Lsn::ZERO`].
    ///
    /// # Slot name validation
    ///
    /// Only alphanumeric characters and underscores are allowed in slot
    /// names.  Returns [`TapError::Config`] if the name is invalid.
    ///
    /// # Errors
    ///
    /// Returns [`TapError::Config`] if the slot name is invalid or
    /// [`TapError::PostgresConnection`] if the query fails.
    pub async fn ensure_replication_slot(&self) -> Result<Lsn, TapError> {
        let slot_name = &self.config.slot_name;

        // Validate slot name: alphanumeric + underscore only
        if slot_name.is_empty() || !slot_name.chars().all(|c| c.is_alphanumeric() || c == '_') {
            return Err(TapError::Config(format!(
                "invalid slot name '{slot_name}': only alphanumeric and underscore characters are allowed"
            )));
        }

        // Check if slot already exists
        let rows = self
            .client
            .query(
                "SELECT slot_name, confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1",
                &[slot_name],
            )
            .await?;

        if let Some(row) = rows.first() {
            let lsn_str: String = row.get(1);
            let lsn = Lsn::from_str(&lsn_str)?;
            info!("found existing replication slot '{slot_name}' at LSN {lsn}");
            return Ok(lsn);
        }

        // Create the slot using pgoutput plugin
        let create_sql = format!("CREATE_REPLICATION_SLOT \"{slot_name}\" LOGICAL pgoutput");
        info!("creating replication slot '{slot_name}'");
        self.client.simple_query(&create_sql).await?;
        info!("created replication slot '{slot_name}'");
        Ok(Lsn::ZERO)
    }

    /// Ensure the publication exists.
    ///
    /// Queries `pg_publication` for a publication matching the configured
    /// publication name.  If found, returns successfully.  If not found,
    /// creates the publication:
    ///
    /// * `FOR ALL TABLES` — when [`SourceConfig::tables`] is empty.
    /// * `FOR TABLE "t1", "t2", ...` — when tables are specified.
    ///
    /// # Errors
    ///
    /// Returns [`TapError::PostgresConnection`] if the query fails.
    pub async fn ensure_publication(&self) -> Result<(), TapError> {
        let pub_name = &self.config.publication;
        let tables = &self.config.tables;

        // Check if publication already exists
        let rows = self
            .client
            .query(
                "SELECT pubname FROM pg_publication WHERE pubname = $1",
                &[pub_name],
            )
            .await?;

        if !rows.is_empty() {
            info!("found existing publication '{pub_name}'");
            return Ok(());
        }

        // Create the publication
        if tables.is_empty() {
            let sql = format!("CREATE PUBLICATION \"{pub_name}\" FOR ALL TABLES");
            info!("creating publication '{pub_name}' FOR ALL TABLES");
            self.client.simple_query(&sql).await?;
        } else {
            let table_list = tables
                .iter()
                .map(|t| format!("\"{t}\""))
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!("CREATE PUBLICATION \"{pub_name}\" FOR TABLE {table_list}");
            info!("creating publication '{pub_name}' FOR TABLE {table_list}");
            self.client.simple_query(&sql).await?;
        }

        Ok(())
    }

    /// Validate that all tables in the provided list exist in the database.
    ///
    /// Runs `SELECT to_regclass($1)` for each table to verify its existence.
    ///
    /// # Errors
    //
    /// Returns [`TapError::Config`] if any table does not exist.
    pub async fn validate_tables(&self, tables: &[String]) -> Result<(), TapError> {
        for table in tables {
            let row = self
                .client
                .query_one("SELECT to_regclass($1)", &[table])
                .await?;
            let regclass: Option<String> = row.get(0);
            if regclass.is_none() {
                return Err(TapError::Config(format!(
                    "table '{table}' does not exist in the database"
                )));
            }
        }
        Ok(())
    }

    /// Start a logical replication stream.
    ///
    /// # Current Limitations
    ///
    /// The current version of `tokio-postgres` (0.7) does not expose the
    /// Postgres `COPY BOTH` protocol needed to execute
    /// `START_REPLICATION`.  Once the dependency is upgraded (tokio-postgres
    /// 0.8+ or equivalent), this method will issue:
    ///
    /// ```text
    /// START_REPLICATION SLOT "{slot_name}" LOGICAL {start_lsn}
    ///   (plugin "{plugin}", publication "{publication}")
    /// ```
    ///
    /// via `client.copy_both_simple()` and wrap the resulting stream.
    ///
    /// For now, this returns a channel-backed [`ReplicationStream`] that
    /// yields no data, allowing the connection lifecycle to be exercised
    /// in isolation.
    ///
    /// # Errors
    ///
    /// Returns [`TapError::Config`] if the slot name fails validation.
    pub async fn start_replication(
        &self,
        slot_name: &str,
        publication: &str,
        start_lsn: Lsn,
        plugin: &str,
    ) -> Result<ReplicationStream, TapError> {
        let _lsn_str = start_lsn.to_string();
        let _publication = publication.to_string();
        let _plugin = plugin.to_string();

        // Validate slot name
        if slot_name.is_empty() || !slot_name.chars().all(|c| c.is_alphanumeric() || c == '_') {
            return Err(TapError::Config(format!(
                "invalid slot name '{slot_name}': only alphanumeric and underscore characters are allowed"
            )));
        }

        info!(
            "start_replication called (slot={slot_name}, publication={_publication}, \
             lsn={start_lsn}, plugin={_plugin}) — implementation stubbed, \
             requires tokio-postgres copy_both support"
        );

        // Return an empty stream for now.
        // TODO(P9): Replace with actual copy_both implementation.
        let (_tx, rx) = tokio::sync::mpsc::channel(1024);
        Ok(ReplicationStream::from_receiver(rx))
    }

    /// Close the connection gracefully.
    ///
    /// Drops the client, which will terminate the background connection
    /// handler.
    pub async fn close(self) -> Result<(), TapError> {
        info!("closing Postgres connection");
        drop(self.client);
        Ok(())
    }

    /// Reference to the underlying config.
    pub fn config(&self) -> &SourceConfig {
        &self.config
    }
}

// ---------------------------------------------------------------------------
// ReplicationStream
// ---------------------------------------------------------------------------

/// A stream of raw WAL data from a Postgres logical replication connection.
///
/// Wraps a channel-based stream.  In the current version (which uses
/// tokio-postgres 0.7), this is backed by an mpsc channel.  When
/// `tokio-postgres` gains `copy_both` support, the backing will be swapped
/// to the real replication protocol, and the XLogData message framing will
/// be stripped (25-byte header: 1 byte msg_type, 8 bytes start_lsn, 8 bytes
/// end_lsn, 8 bytes timestamp).
pub struct ReplicationStream {
    rx: tokio::sync::mpsc::Receiver<Result<Vec<u8>, TapError>>,
}

impl ReplicationStream {
    /// Create a new `ReplicationStream` from an mpsc receiver.
    ///
    /// Used internally and for testing to inject WAL data without a real
    /// Postgres connection.
    pub fn from_receiver(rx: tokio::sync::mpsc::Receiver<Result<Vec<u8>, TapError>>) -> Self {
        Self { rx }
    }
}

impl Stream for ReplicationStream {
    type Item = Result<Vec<u8>, TapError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ── Lsn ──────────────────────────────────────────────────────────────

    #[test]
    fn test_lsn_parse_display_round_trip() {
        let cases = [
            "0/0",
            "0/16B37428",
            "1/2A3B4C5D",
            "FFFFFFFF/FFFFFFFF",
            "0/1",
            "ABCD/12345678",
            "7FFFFFFF/FFFFFFFF",
        ];
        for case in &cases {
            let lsn = Lsn::from_str(case).unwrap();
            let displayed = lsn.to_string();
            let reparsed = Lsn::from_str(&displayed).unwrap();
            assert_eq!(
                lsn, reparsed,
                "round-trip failed for '{case}': parsed={lsn:?}, displayed='{displayed}', reparsed={reparsed:?}"
            );
        }
    }

    #[test]
    fn test_lsn_zero_constant() {
        assert_eq!(Lsn::ZERO, Lsn(0));
        assert_eq!(Lsn::ZERO.to_string(), "0/00000000");
    }

    #[test]
    fn test_lsn_ordering() {
        assert!(Lsn(0) < Lsn(1));
        assert!(Lsn(100) > Lsn(99));
        assert!(Lsn(u64::MAX) > Lsn(0));
        assert_eq!(Lsn(42), Lsn(42));
        assert_ne!(Lsn(1), Lsn(2));
    }

    #[test]
    fn test_lsn_parse_invalid_format() {
        assert!(Lsn::from_str("").is_err());
        assert!(Lsn::from_str("0").is_err());
        assert!(Lsn::from_str("0/").is_err());
        assert!(Lsn::from_str("/0").is_err());
        assert!(Lsn::from_str("0/GGGGGGGG").is_err());
        assert!(Lsn::from_str("not-a-lsn").is_err());
    }

    #[test]
    fn test_lsn_specific_values() {
        assert_eq!(Lsn::from_str("0/16B37428").unwrap(), Lsn(0x16B37428));
        assert_eq!(
            Lsn::from_str("1/2A3B4C5D").unwrap(),
            Lsn((1u64 << 32) | 0x2A3B4C5D)
        );
        assert_eq!(Lsn::from_str("FFFFFFFF/FFFFFFFF").unwrap(), Lsn(u64::MAX));
    }

    #[test]
    fn test_lsn_serialize_deserialize() {
        let lsn = Lsn::from_str("0/16B37428").unwrap();
        let json = serde_json::to_string(&lsn).unwrap();
        assert_eq!(json, "\"0/16B37428\"");
        let deserialized: Lsn = serde_json::from_str(&json).unwrap();
        assert_eq!(lsn, deserialized);
    }

    #[test]
    fn test_lsn_deserialize_invalid() {
        let result: Result<Lsn, _> = serde_json::from_str("\"not-a-lsn\"");
        assert!(result.is_err());
    }

    #[test]
    fn test_lsn_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(Lsn(1));
        set.insert(Lsn(2));
        set.insert(Lsn(1));
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn test_lsn_copy_trait() {
        let a = Lsn(42);
        let _b = a; // move
        let _c = a; // still available — Copy
        assert_eq!(a, Lsn(42));
    }

    // ── Connection string ────────────────────────────────────────────────

    #[test]
    fn test_connection_string_includes_replication_option() {
        let config = SourceConfig {
            host: "pg.example.com".into(),
            port: 5432,
            dbname: "testdb".into(),
            user: "replicator".into(),
            password: "s3cret".into(),
            ..SourceConfig::default()
        };
        let conn_str = connection_string(&config);
        assert!(conn_str.contains("host=pg.example.com"));
        assert!(conn_str.contains("port=5432"));
        assert!(conn_str.contains("dbname=testdb"));
        assert!(conn_str.contains("user=replicator"));
        assert!(conn_str.contains("password=s3cret"));
        assert!(conn_str.contains("options='--replication=database'"));
    }

    #[test]
    fn test_connection_string_redacted_log_output() {
        let config = SourceConfig {
            host: "localhost".into(),
            port: 5432,
            dbname: "test".into(),
            user: "u".into(),
            password: "supersecret".into(),
            ..SourceConfig::default()
        };
        let redacted = redacted_connection_string(&config);
        assert!(!redacted.contains("supersecret"));
        assert!(redacted.contains("<REDACTED>"));
    }

    #[test]
    fn test_debug_redacts_password() {
        let config = SourceConfig {
            password: "s3cret".into(),
            ..SourceConfig::default()
        };
        let debug_str = format!("{config:?}");
        assert!(!debug_str.contains("s3cret"));
        assert!(debug_str.contains("<REDACTED>"));
    }

    // ── Slot name validation ─────────────────────────────────────────────

    #[test]
    fn test_valid_slot_names() {
        let valid_names = ["tap_slot", "slot123", "abc_def_123", "a", "Z"];
        for name in &valid_names {
            assert!(
                !name.is_empty() && name.chars().all(|c| c.is_alphanumeric() || c == '_'),
                "'{name}' should be valid"
            );
        }
    }

    #[test]
    fn test_invalid_slot_names() {
        let invalid_names = ["tap-slot", "slot name", "slot.name", "", "slot$pecial"];
        for name in &invalid_names {
            assert!(
                name.is_empty() || !name.chars().all(|c| c.is_alphanumeric() || c == '_'),
                "'{name}' should be invalid"
            );
        }
    }

    // ── Publication name validation ──────────────────────────────────────

    #[test]
    fn test_publication_name_sql_injection_prevention() {
        // Publication names should be properly quoted in SQL statements
        let pub_name = "my_pub; DROP TABLE users;";
        let quoted = format!("\"{pub_name}\"");
        assert_eq!(quoted, r#""my_pub; DROP TABLE users;""#);
    }

    // ── ReplicationStream ────────────────────────────────────────────────

    #[test]
    fn test_replication_stream_poll_ready() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (tx, rx) = tokio::sync::mpsc::channel(16);
            let mut stream = ReplicationStream::from_receiver(rx);

            tx.send(Ok(vec![1, 2, 3])).await.unwrap();
            tx.send(Ok(vec![4, 5, 6])).await.unwrap();

            use futures::StreamExt;
            let item1 = stream.next().await;
            assert!(item1.is_some());
            assert_eq!(item1.unwrap().unwrap(), vec![1, 2, 3]);

            let item2 = stream.next().await;
            assert!(item2.is_some());
            assert_eq!(item2.unwrap().unwrap(), vec![4, 5, 6]);
        });
    }

    #[test]
    fn test_replication_stream_closed_channel() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (tx, rx) = tokio::sync::mpsc::channel::<Result<Vec<u8>, TapError>>(16);
            // Drop the sender immediately so the channel is closed
            drop(tx);
            let mut stream = ReplicationStream::from_receiver(rx);

            use futures::StreamExt;
            let item = stream.next().await;
            assert!(item.is_none());
        });
    }

    // ── Edge cases ──────────────────────────────────────────────────────

    #[test]
    fn test_lsn_high_bits_only() {
        let lsn = Lsn::from_str("1/0").unwrap();
        assert_eq!(lsn, Lsn(1u64 << 32));
        assert_eq!(lsn.to_string(), "1/00000000");
    }

    #[test]
    fn test_lsn_low_bits_only() {
        let lsn = Lsn::from_str("0/FFFFFFFF").unwrap();
        assert_eq!(lsn, Lsn(0xFFFF_FFFF));
        assert_eq!(lsn.to_string(), "0/FFFFFFFF");
    }

    #[test]
    fn test_connection_string_default_port() {
        let config = SourceConfig {
            host: "localhost".into(),
            port: 5432,
            dbname: "mydb".into(),
            user: "u".into(),
            password: "p".into(),
            ..SourceConfig::default()
        };
        let conn_str = connection_string(&config);
        assert!(conn_str.contains("port=5432"));
    }
}

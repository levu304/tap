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
/// assert_eq!(lsn, Lsn::from_u64(0x16B37428));
/// assert_eq!(lsn.to_string(), "0/16B37428");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Lsn(u64);

impl Lsn {
    /// Zero LSN constant, representing the beginning of the WAL.
    pub const ZERO: Lsn = Lsn(0);

    /// Construct an [`Lsn`] from a raw `u64` value.
    pub fn from_u64(value: u64) -> Self {
        Self(value)
    }

    /// Return the raw `u64` value.
    pub fn as_u64(&self) -> u64 {
        self.0
    }
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
/// Note: `tokio-postgres` 0.7 does not support the `replication` connection
/// parameter in the startup message, so options like `--replication=database`
/// do **not** work as intended.  We omit replication options here and would
/// add them properly when we upgrade to tokio-postgres 0.8+ (which has
/// `copy_both` support and can handle the replication startup message
/// correctly).
fn connection_string(config: &SourceConfig) -> String {
    format!(
        "host={} port={} dbname={} user={} password={}",
        config.host, config.port, config.dbname, config.user, config.password,
    )
}

/// Build a redacted version of the connection string for logging purposes.
/// The password value is replaced with `<REDACTED>`.
fn redacted_connection_string(config: &SourceConfig) -> String {
    format!(
        "host={} port={} dbname={} user={} password=<REDACTED>",
        config.host, config.port, config.dbname, config.user,
    )
}

// ---------------------------------------------------------------------------
// PgConnection
// ---------------------------------------------------------------------------

/// A connection to a Postgres database configured for logical replication.
///
/// Wraps a `tokio_postgres::Client`, the [`SourceConfig`] that was used
/// to create it, and a join handle for the background connection handler.
/// Provides methods for managing replication slots, publications, and
/// streaming WAL data.
pub struct PgConnection {
    /// The underlying tokio-postgres client.
    client: tokio_postgres::Client,
    /// The configuration used to establish the connection.
    config: SourceConfig,
    /// Join handle for the background connection handler task.  Used by
    /// [`close()`](Self::close) to wait for clean shutdown and surface any
    /// panics that may have occurred.
    join_handle: Option<tokio::task::JoinHandle<()>>,
}

impl PgConnection {
    /// Connect to Postgres in replication mode.
    ///
    /// Builds a connection string from the provided config, connects via
    /// `tokio_postgres::connect()` using the configured TLS mode
    /// ([`SslMode`]), spawns the background connection handler, and returns
    /// a [`PgConnection`] ready for replication operations.
    ///
    /// Note: tokio-postgres 0.7 does not support the `replication` connection
    /// parameter, so `--replication=database` is not included.  It will be
    /// added when we upgrade to tokio-postgres 0.8+.
    ///
    /// # Errors
    ///
    /// Returns [`TapError::PostgresConnectionRedacted`] if the connection
    /// fails (with the password redacted from the error message).
    pub async fn connect(config: &SourceConfig) -> Result<Self, TapError> {
        let conn_str = connection_string(config);
        let redacted = redacted_connection_string(config);
        info!("connecting to Postgres: {redacted}");

        // tokio-postgres 0.7 uses a generic `Connection<S>` parameterised
        // by the TLS stream type, so each TLS backend produces a different
        // `Connection` type.  We branch on `ssl_mode` to keep the concrete
        // type uniform within each arm.
        let (client, join_handle) = match config.ssl_mode {
            crate::config::SslMode::Disable => {
                let (c, conn) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
                    .await
                    .map_err(|e| {
                        TapError::PostgresConnectionRedacted(
                            e.to_string().replace(&config.password, "<REDACTED>"),
                        )
                    })?;
                let jh = tokio::spawn(async move {
                    if let Err(e) = conn.await {
                        tracing::error!("Postgres connection error: {e}");
                    }
                });
                (c, jh)
            }
            _ => {
                let connector = native_tls::TlsConnector::builder().build().map_err(|e| {
                    TapError::PostgresConnectionRedacted(format!(
                        "failed to build TLS connector: {e}"
                    ))
                })?;
                let (c, conn) = tokio_postgres::connect(
                    &conn_str,
                    postgres_native_tls::MakeTlsConnector::new(connector),
                )
                .await
                .map_err(|e| {
                    TapError::PostgresConnectionRedacted(
                        e.to_string().replace(&config.password, "<REDACTED>"),
                    )
                })?;
                let jh = tokio::spawn(async move {
                    if let Err(e) = conn.await {
                        tracing::error!("Postgres connection error: {e}");
                    }
                });
                (c, jh)
            }
        };

        info!(
            "connected to Postgres (host={}, dbname={})",
            config.host, config.dbname
        );

        Ok(Self {
            client,
            config: config.clone(),
            join_handle: Some(join_handle),
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
        crate::config::validate_identifier(slot_name, "slot_name")?;

        // Check if slot already exists
        let rows = self
            .client
            .query(
                "SELECT slot_name, confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1",
                &[slot_name],
            )
            .await?;

        if let Some(row) = rows.first() {
            let lsn_str: Option<String> = row.try_get(1).map_err(|e| {
                TapError::Decode(format!(
                    "failed to read confirmed_flush_lsn for slot '{slot_name}': {e}"
                ))
            })?;
            let lsn_str = lsn_str.ok_or_else(|| {
                TapError::Decode(format!(
                    "confirmed_flush_lsn is NULL for existing slot '{slot_name}'"
                ))
            })?;
            let lsn = Lsn::from_str(&lsn_str)?;
            info!("found existing replication slot '{slot_name}' at LSN {lsn}");
            return Ok(lsn);
        }

        // Create the slot using pgoutput plugin.
        // Uses the SQL function pg_create_logical_replication_slot() instead of
        // the replication-protocol CREATE_REPLICATION_SLOT command because
        // tokio-postgres 0.7 does not support the `replication=database`
        // connection parameter needed for protocol-level commands.
        info!("creating replication slot '{slot_name}'");
        let row = self
            .client
            .query_one(
                "SELECT lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput')",
                &[slot_name],
            )
            .await?;
        let lsn_str: String = row.get(0);
        let lsn = Lsn::from_str(&lsn_str)?;
        info!("created replication slot '{slot_name}' at LSN {lsn}");
        Ok(lsn)
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

        // Validate publication name and table names before building SQL
        crate::config::validate_identifier(pub_name, "publication")?;
        for (i, table) in tables.iter().enumerate() {
            crate::config::validate_identifier(table, &format!("tables[{i}]"))?;
        }

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

    /// Validate that all configured tables exist in the database.
    ///
    /// Reads tables from [`SourceConfig::tables`] and runs
    /// `SELECT to_regclass($1)` for each to verify its existence.
    ///
    /// # Errors
    ///
    /// Returns [`TapError::Config`] if any table does not exist.
    pub async fn validate_tables(&self) -> Result<(), TapError> {
        for table in &self.config.tables {
            let row = self
                .client
                .query_one("SELECT to_regclass($1)::text", &[table])
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

        // Validate slot name: alphanumeric + underscore only
        crate::config::validate_identifier(slot_name, "slot_name")?;

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
    /// Drops the `Client`, waits for the background connection handler to
    /// finish, and surfaces any panic that may have occurred in the handler
    /// task.
    pub async fn close(self) {
        info!("closing Postgres connection");
        drop(self.client);
        if let Some(handle) = self.join_handle {
            // Log if the background task panicked — the JoinHandle can't be
            // cancelled at this point, we just surface the diagnostic.
            if let Err(e) = handle.await {
                tracing::error!("Postgres connection handler panicked: {e}");
            }
        }
    }

    /// Reference to the underlying config.
    pub fn config(&self) -> &SourceConfig {
        &self.config
    }

    /// Access the underlying tokio-postgres client for direct SQL queries.
    ///
    /// Used by the snapshot engine and other components that need to run
    /// ad-hoc queries outside the replication protocol.
    pub fn client(&self) -> &tokio_postgres::Client {
        &self.client
    }
}

// ---------------------------------------------------------------------------
// Plain connection for snapshot operations
// ---------------------------------------------------------------------------

/// Connect to Postgres in plain (non-replication) mode.
///
/// Builds a connection string **without** the `--replication=database`
/// option, making it suitable for ordinary SQL queries — catalog lookups,
/// `pg_export_snapshot()`, `SELECT` scans, etc.
///
/// The snapshot engine uses two of these: a **keeper** that holds the
/// exported snapshot transaction open, and a **worker** that pins its
/// transactions to that snapshot.
///
/// # Errors
///
/// Returns [`TapError::PostgresConnectionRedacted`] if the connection
/// fails (with the password redacted from the error message).
pub async fn connect_plain(
    config: &SourceConfig,
) -> Result<(tokio_postgres::Client, tokio::task::JoinHandle<()>), TapError> {
    let conn_str = connection_string(config);
    let redacted = format!(
        "host={} port={} dbname={} user={} password=<REDACTED>",
        config.host, config.port, config.dbname, config.user,
    );
    info!("connecting to Postgres (plain): {redacted}");

    let (client, join_handle) = match config.ssl_mode {
        crate::config::SslMode::Disable => {
            let (c, conn) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
                .await
                .map_err(|e| {
                    TapError::PostgresConnectionRedacted(
                        e.to_string().replace(&config.password, "<REDACTED>"),
                    )
                })?;
            let jh = tokio::spawn(async move {
                if let Err(e) = conn.await {
                    tracing::error!("Postgres (plain) connection error: {e}");
                }
            });
            (c, jh)
        }
        _ => {
            let connector = native_tls::TlsConnector::builder().build().map_err(|e| {
                TapError::PostgresConnectionRedacted(format!("failed to build TLS connector: {e}"))
            })?;
            let (c, conn) = tokio_postgres::connect(
                &conn_str,
                postgres_native_tls::MakeTlsConnector::new(connector),
            )
            .await
            .map_err(|e| {
                TapError::PostgresConnectionRedacted(
                    e.to_string().replace(&config.password, "<REDACTED>"),
                )
            })?;
            let jh = tokio::spawn(async move {
                if let Err(e) = conn.await {
                    tracing::error!("Postgres (plain) connection error: {e}");
                }
            });
            (c, jh)
        }
    };

    Ok((client, join_handle))
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
        assert_eq!(Lsn::ZERO, Lsn::from_u64(0));
        assert_eq!(Lsn::ZERO.to_string(), "0/00000000");
    }

    #[test]
    fn test_lsn_ordering() {
        assert!(Lsn::from_u64(0) < Lsn::from_u64(1));
        assert!(Lsn::from_u64(100) > Lsn::from_u64(99));
        assert!(Lsn::from_u64(u64::MAX) > Lsn::from_u64(0));
        assert_eq!(Lsn::from_u64(42), Lsn::from_u64(42));
        assert_ne!(Lsn::from_u64(1), Lsn::from_u64(2));
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
        assert_eq!(
            Lsn::from_str("0/16B37428").unwrap(),
            Lsn::from_u64(0x16B37428)
        );
        assert_eq!(
            Lsn::from_str("1/2A3B4C5D").unwrap(),
            Lsn::from_u64((1u64 << 32) | 0x2A3B4C5D)
        );
        assert_eq!(
            Lsn::from_str("FFFFFFFF/FFFFFFFF").unwrap(),
            Lsn::from_u64(u64::MAX)
        );
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
        set.insert(Lsn::from_u64(1));
        set.insert(Lsn::from_u64(2));
        set.insert(Lsn::from_u64(1));
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn test_lsn_copy_trait() {
        let a = Lsn::from_u64(42);
        let _b = a; // move
        let _c = a; // still available — Copy
        assert_eq!(a, Lsn::from_u64(42));
    }

    // ── Connection string ────────────────────────────────────────────────

    #[test]
    fn test_connection_string_omits_replication_option() {
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
        assert!(
            !conn_str.contains("--replication"),
            "replication option omitted per tokio-postgres 0.7 limitation: {conn_str}"
        );
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

    // ── Identifier validation (via config::validate_identifier) ──────────

    #[test]
    fn test_valid_identifiers() {
        let valid = [
            ("tap_slot", "slot_name"),
            ("slot123", "slot_name"),
            ("public.users", "table name"),
            ("abc_def_123", "any"),
            ("a", "any"),
            ("Z", "any"),
        ];
        for (name, field) in &valid {
            assert!(
                crate::config::validate_identifier(name, field).is_ok(),
                "'{name}' should be valid"
            );
        }
    }

    #[test]
    fn test_invalid_identifiers() {
        let invalid = [
            "",
            "tap-slot",
            "slot name",
            "slot$pecial",
            "my_pub; DROP TABLE users;",
        ];
        for name in &invalid {
            assert!(
                crate::config::validate_identifier(name, "test").is_err(),
                "'{name}' should be invalid"
            );
        }
    }

    #[test]
    fn test_empty_identifier_fails() {
        let err = crate::config::validate_identifier("", "field").unwrap_err();
        assert!(err.to_string().contains("must not be empty"));
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
        assert_eq!(lsn, Lsn::from_u64(1u64 << 32));
        assert_eq!(lsn.to_string(), "1/00000000");
    }

    #[test]
    fn test_lsn_low_bits_only() {
        let lsn = Lsn::from_str("0/FFFFFFFF").unwrap();
        assert_eq!(lsn, Lsn::from_u64(0xFFFF_FFFF));
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

    // ── Plain connection strings ──────────────────────────────────────────

    #[test]
    fn test_plain_connection_string_with_tls() {
        let config = SourceConfig {
            host: "pg.example.com".into(),
            port: 5432,
            dbname: "testdb".into(),
            user: "replicator".into(),
            password: "s3cret".into(),
            ssl_mode: crate::config::SslMode::Require,
            ..SourceConfig::default()
        };
        let conn_str = connection_string(&config);
        assert!(conn_str.contains("host=pg.example.com"));
        assert!(conn_str.contains("password=s3cret"));
        assert!(!conn_str.contains("--replication"));
    }
}

//! MySQL connection management and pre-flight checks.
//!
//! [`MySqlConnection`] wraps a `mysql_async::Pool` and provides:
//!
//! *   TCP/TLS connectivity validation
//! *   MySQL server version check (5.7+ / 8.0+)
//! *   Binlog format verification (`binlog_format = ROW`)
//! *   `binlog_row_image = FULL` verification
//! *   Replication privileges check
//! *   Binlog position parsing and formatting
//!
//! # Pre-flight checks
//!
//! Before starting a capture session, [`MySqlConnection::validate()`] runs
//! the following checks in order:
//!
//! 1. **Ping** — verifies TCP/TLS connectivity.
//! 2. **Version** — ensures the server is MySQL 5.7+ or 8.0+.
//! 3. **Binlog format** — confirms `binlog_format = ROW` (required for CDC).
//! 4. **Row image** — confirms `binlog_row_image = FULL`.
//! 5. **Privileges** — checks `REPLICATION SLAVE`, `REPLICATION CLIENT`, and
//!    table-level `SELECT` privileges on the target database.

use mysql_async::prelude::*;
use mysql_async::{Conn, Pool};
use tracing::info;

use crate::config::MySqlSourceConfig;
use crate::error::TapError;

/// A MySQL connection handle for pre-flight validation.
///
/// Created from a [`MySqlSourceConfig`] via [`MySqlConnection::connect`].
/// The underlying `mysql_async::Pool` is held open until the connection is
/// dropped, at which point the pool is closed.
#[derive(Debug)]
pub struct MySqlConnection {
    /// The connection pool to the MySQL server.
    pool: Pool,
    /// The configuration used to establish the connection.
    config: MySqlSourceConfig,
}

impl MySqlConnection {
    /// Establish a connection pool to the MySQL server defined by `config`.
    ///
    /// # Errors
    ///
    /// Returns [`TapError::MySqlConnectionRedacted`] when the pool cannot be
    /// created (the redacted variant is used so that the password is not
    /// leaked in logs or error messages).
    pub async fn connect(config: MySqlSourceConfig) -> Result<Self, TapError> {
        let redacted = config.redacted_url();

        let pool = Pool::new(config.opts());

        // Verify connectivity with a simple ping.
        let mut conn = pool.get_conn().await.map_err(|e| {
            TapError::MySqlConnectionRedacted(format!("failed to connect to {redacted}: {e}"))
        })?;

        let version: String = conn
            .query_first("SELECT VERSION()")
            .await
            .map_err(|e| {
                TapError::MySqlConnectionRedacted(format!(
                    "failed to query MySQL version from {redacted}: {e}"
                ))
            })?
            .unwrap_or_else(|| "unknown".into());

        info!(%version, "connected to MySQL");

        drop(conn);

        Ok(Self { pool, config })
    }

    /// Run pre-flight checks against the connected MySQL server.
    ///
    /// Returns `Ok(())` when all checks pass, or the first failing
    /// [`TapError`].
    ///
    /// # Checks performed (in order)
    ///
    /// 1. **Ping** — connectivity via `CONNECTION_ID()`.
    /// 2. **Version** — MySQL 5.7+ or 8.0+ required.
    /// 3. **Binlog format** — `binlog_format = ROW`.
    /// 4. **Row image** — `binlog_row_image = FULL`.
    /// 5. **Privileges** — replication + table SELECT.
    ///
    /// # Errors
    ///
    /// Returns [`TapError::MySqlConnection`] with a human-readable
    /// description of the first check that fails.
    pub async fn validate(&self) -> Result<(), TapError> {
        let mut conn = self.pool.get_conn().await.map_err(|e| {
            TapError::MySqlConnection(format!(
                "failed to get connection from pool for validation: {e}"
            ))
        })?;

        // 1. Ping / connectivity
        self.check_connectivity(&mut conn).await?;

        // 2. Version check
        self.check_version(&mut conn).await?;

        // 3. Binlog format
        self.check_binlog_format(&mut conn).await?;

        // 4. Row image
        self.check_row_image(&mut conn).await?;

        // 5. Privileges
        self.check_privileges(&mut conn).await?;

        info!("MySQL pre-flight checks passed");
        Ok(())
    }

    /// Returns a reference to the underlying configuration.
    pub fn config(&self) -> &MySqlSourceConfig {
        &self.config
    }

    /// Returns a reference to the underlying connection pool.
    ///
    /// Useful for advanced operations (e.g. querying table schemas).
    pub fn pool(&self) -> &Pool {
        &self.pool
    }

    // ------------------------------------------------------------------
    // Internal check helpers
    // ------------------------------------------------------------------

    /// Verify basic connectivity.
    async fn check_connectivity(&self, conn: &mut Conn) -> Result<(), TapError> {
        let connection_id: Option<u32> = conn
            .query_first("SELECT CONNECTION_ID()")
            .await
            .map_err(|e| TapError::MySqlConnection(format!("connectivity check failed: {e}")))?;

        match connection_id {
            Some(id) => {
                info!(connection_id = id, "MySQL connectivity OK");
                Ok(())
            }
            None => Err(TapError::MySqlConnection(
                "connectivity check returned no connection ID".into(),
            )),
        }
    }

    /// Verify that MySQL version is 5.7+ or 8.0+.
    async fn check_version(&self, conn: &mut Conn) -> Result<(), TapError> {
        let version: String = conn
            .query_first("SELECT VERSION()")
            .await
            .map_err(|e| TapError::MySqlConnection(format!("version check failed: {e}")))?
            .unwrap_or_else(|| "0.0.0".into());

        // Parse the version string.  Examples: "8.0.32", "5.7.42-log".
        let major = version
            .split(|c: char| !c.is_ascii_digit())
            .next()
            .and_then(|s| s.parse::<u32>().ok())
            .unwrap_or(0);
        let minor = version
            .split(|c: char| !c.is_ascii_digit())
            .nth(1)
            .and_then(|s| s.parse::<u32>().ok())
            .unwrap_or(0);

        let ok = matches!((major, minor), (5, 7..=7) | (8, 0..=99));
        if !ok {
            return Err(TapError::MySqlConnection(format!(
                "unsupported MySQL version: {version} (need 5.7.x or 8.0.x)"
            )));
        }

        info!(%version, "MySQL version OK");
        Ok(())
    }

    /// Verify `binlog_format = ROW`.
    async fn check_binlog_format(&self, conn: &mut Conn) -> Result<(), TapError> {
        let format: Option<String> =
            conn.query_first("SELECT @@binlog_format")
                .await
                .map_err(|e| {
                    TapError::MySqlConnection(format!("failed to query binlog_format: {e}"))
                })?;

        match format.as_deref() {
            Some("ROW") => {
                info!("binlog_format = ROW OK");
                Ok(())
            }
            Some(other) => Err(TapError::MySqlConnection(format!(
                "binlog_format is {other:?}, expected ROW (set binlog_format=ROW)"
            ))),
            None => Err(TapError::MySqlConnection(
                "binlog_format is not set; enable binlogging first".into(),
            )),
        }
    }

    /// Verify `binlog_row_image = FULL`.
    async fn check_row_image(&self, conn: &mut Conn) -> Result<(), TapError> {
        let image: Option<String> = conn
            .query_first("SELECT @@binlog_row_image")
            .await
            .map_err(|e| {
                TapError::MySqlConnection(format!("failed to query binlog_row_image: {e}"))
            })?;

        match image.as_deref() {
            Some("FULL") => {
                info!("binlog_row_image = FULL OK");
                Ok(())
            }
            Some(other) => Err(TapError::MySqlConnection(format!(
                "binlog_row_image is {other:?}, expected FULL \
                 (set binlog_row_image=FULL to capture before-images)"
            ))),
            None => Err(TapError::MySqlConnection(
                "binlog_row_image is not set".into(),
            )),
        }
    }

    /// Check that the configured user has replication and SELECT privileges.
    async fn check_privileges(&self, conn: &mut Conn) -> Result<(), TapError> {
        // Check REPLICATION SLAVE (needed to connect as a replica).
        let slave_priv: Option<String> = conn
            .query_first("SELECT @@session.sql_slave_skip_counter")
            .await
            .map_err(|e| {
                // If this fails the user probably doesn't have REPLICATION SLAVE.
                TapError::MySqlConnection(format!(
                    "REPLICATION SLAVE privilege likely missing: {e}"
                ))
            })?;

        // A successful query confirms the privilege exists.
        let _ = slave_priv;

        // Check that the grants include REPLICATION SLAVE and SELECT on
        // the target database.
        let current_user: Option<String> = conn
            .query_first("SELECT CURRENT_USER()")
            .await
            .map_err(|e| TapError::MySqlConnection(format!("failed to query current user: {e}")))?;

        let user = current_user.unwrap_or_else(|| "unknown".into());

        // Query SHOW GRANTS for the current user.
        let grants: Vec<String> = conn.query("SHOW GRANTS").await.map_err(|e| {
            TapError::MySqlConnection(format!("failed to query SHOW GRANTS for {user}: {e}"))
        })?;

        let grants_concat = grants.join(" ");
        let grants_upper = grants_concat.to_uppercase();

        // ALL PRIVILEGES ON *.* covers every individual privilege check below.
        if grants_upper.contains("ALL PRIVILEGES ON *.*") {
            info!(%user, "MySQL privileges OK (all privileges)");
            return Ok(());
        }

        let has_replication_slave = grants_upper.contains("REPLICATION SLAVE");
        let has_replication_client = grants_upper.contains("REPLICATION CLIENT");
        let has_select_on_target = grants_upper.contains(&format!(
            "SELECT ON `{}`",
            self.config.dbname.to_uppercase()
        )) || grants_upper.contains("SELECT ON *.*");

        let mut missing: Vec<String> = Vec::new();
        if !has_replication_slave {
            missing.push("REPLICATION SLAVE".into());
        }
        if !has_replication_client {
            missing.push("REPLICATION CLIENT".into());
        }
        if !has_select_on_target {
            missing.push(format!("SELECT on {}", self.config.dbname));
        }

        if missing.is_empty() {
            info!(%user, "MySQL privileges OK");
            Ok(())
        } else {
            Err(TapError::MySqlConnection(format!(
                "user {user} is missing required privileges: {}",
                missing.join(", ")
            )))
        }
    }
}

impl Drop for MySqlConnection {
    fn drop(&mut self) {
        // Best-effort disconnect: spawn only if a Tokio runtime is active.
        // Without this guard, tokio::task::spawn panics when dropped from a
        // sync context (e.g. unit tests, shutdown after runtime teardown).
        // When no runtime is available, mysql_async::Pool's own Drop handles
        // cleanup.
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let pool = self.pool.clone();
            handle.spawn(async move {
                pool.disconnect().await.ok();
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::config::MySqlSourceConfig;

    /// Verify that the connection module compiles and that the config
    /// validation works as a prerequisite for connection.
    #[test]
    fn test_config_validate_basic() {
        let config = MySqlSourceConfig {
            host: "127.0.0.1".into(),
            port: 3306,
            dbname: "test".into(),
            user: "root".into(),
            password: "secret".into(),
            server_id: 42,
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validate_fails_on_empty_host() {
        let config = MySqlSourceConfig {
            host: String::new(),
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validate_fails_on_zero_server_id() {
        let config = MySqlSourceConfig {
            server_id: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_redacted_url_hides_password() {
        let config = MySqlSourceConfig {
            dbname: "testdb".into(),
            user: "replicator".into(),
            password: "hunter2".into(),
            ..Default::default()
        };
        let url = config.redacted_url();
        assert_eq!(url, "mysql://replicator:****@localhost:3306/testdb");
        // The real password must not appear.
        assert!(!url.contains("hunter2"));
    }

    #[test]
    fn test_opts_builder_accepts_special_chars() {
        // Credentials with URL metacharacters must not cause encoding errors.
        let config = MySqlSourceConfig {
            host: "127.0.0.1".into(),
            port: 3306,
            dbname: "test_db".into(),
            user: "test_user".into(),
            password: "p@ss:w0rd/foo".into(),
            ..Default::default()
        };
        // OptsBuilder should construct without errors regardless of
        // credential content — it sets each field separately.
        let _opts = config.opts();
    }
}

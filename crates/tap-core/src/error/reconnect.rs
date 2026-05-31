//! Reconnection configuration and exponential-backoff calculation.

use std::time::Duration;

use crate::error::TapError;

/// Configuration for reconnection behaviour.
///
/// Controls how many times a Postgres connection is retried and how long
/// to wait between attempts, with optional jitter to avoid thundering-herd
/// effects.
///
/// # Examples
///
/// ```
/// use tap_core::error::ReconnectConfig;
///
/// let cfg = ReconnectConfig {
///     max_retries: 5,
///     initial_backoff_ms: 100,
///     max_backoff_ms: 5_000,
///     jitter: true,
/// };
///
/// let delay = cfg.backoff(0).unwrap();
/// assert!(delay.as_millis() >= 80);   // 100 - 20 %
/// assert!(delay.as_millis() <= 120);  // 100 + 20 %
/// ```
#[derive(Debug, Clone)]
pub struct ReconnectConfig {
    /// Maximum number of retry attempts before giving up.
    pub max_retries: u32,
    /// Initial backoff duration in milliseconds (doubles each attempt).
    pub initial_backoff_ms: u64,
    /// Maximum backoff duration in milliseconds (hard cap).
    pub max_backoff_ms: u64,
    /// Whether to apply ±20 % random jitter to each backoff value.
    pub jitter: bool,
}

impl ReconnectConfig {
    /// Returns the backoff [`Duration`] for the given attempt number.
    ///
    /// Returns `None` when `attempt >= max_retries` (exhausted retries).
    ///
    /// `attempt` is zero-indexed (the first retry is attempt 0, the
    /// second is attempt 1, etc.).
    ///
    /// The formula is:
    ///
    /// ```text
    /// raw = initial * 2^attempt       // exponential growth
    /// capped = min(raw, max_backoff)  // hard cap
    ///                                     |
    /// if jitter: capped * (1 + j)          | — j ∈ [-0.2, +0.2]
    /// else:      capped                    |
    /// ```
    ///
    /// The jitter is deterministic per attempt number and stable across
    /// Rust versions (uses SplitMix64 internally).
    pub fn backoff(&self, attempt: u32) -> Option<Duration> {
        if attempt >= self.max_retries {
            return None;
        }
        let exponential = self
            .initial_backoff_ms
            .saturating_mul(2u64.saturating_pow(attempt));
        let capped = exponential.min(self.max_backoff_ms);

        if self.jitter {
            let factor = Self::jitter_factor(attempt);
            let jittered = ((capped as f64) * (1.0 + factor)) as u64;
            Some(Duration::from_millis(jittered.min(self.max_backoff_ms)))
        } else {
            Some(Duration::from_millis(capped))
        }
    }

    /// Returns a backoff or an error when retries are exhausted.
    ///
    /// Convenience wrapper that converts `None` into a `TapError`.
    pub fn backoff_or_err(&self, attempt: u32) -> Result<Duration, TapError> {
        self.backoff(attempt).ok_or_else(|| {
            TapError::Config(format!(
                "retries exhausted after {} attempt(s)",
                self.max_retries
            ))
        })
    }

    /// Deterministic pseudo-random factor in `[-0.2, +0.2]` derived from
    /// `attempt` using SplitMix64.
    ///
    /// Stable across Rust versions and platforms.
    fn jitter_factor(attempt: u32) -> f64 {
        // SplitMix64 hash — deterministic, cross-version stable
        let mut z = attempt as u64;
        z = z.wrapping_add(0x9e3779b97f4a7c15);
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^= z >> 31;

        (z as f64 / u64::MAX as f64) * 0.4 - 0.2
    }
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            max_retries: 10,
            initial_backoff_ms: 500,
            max_backoff_ms: 30_000,
            jitter: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backoff_calculation_no_jitter() {
        let cfg = ReconnectConfig {
            max_retries: 5,
            initial_backoff_ms: 100,
            max_backoff_ms: 10_000,
            jitter: false,
        };

        assert_eq!(cfg.backoff(0).unwrap(), Duration::from_millis(100));
        assert_eq!(cfg.backoff(1).unwrap(), Duration::from_millis(200));
        assert_eq!(cfg.backoff(2).unwrap(), Duration::from_millis(400));
        assert_eq!(cfg.backoff(3).unwrap(), Duration::from_millis(800));
    }

    #[test]
    fn test_backoff_max_cap() {
        let cfg = ReconnectConfig {
            max_retries: 10,
            initial_backoff_ms: 1_000,
            max_backoff_ms: 3_000,
            jitter: false,
        };

        assert_eq!(cfg.backoff(2).unwrap(), Duration::from_millis(3_000));
        assert_eq!(cfg.backoff(9).unwrap(), Duration::from_millis(3_000));
    }

    #[test]
    fn test_backoff_exhausted_returns_none() {
        let cfg = ReconnectConfig {
            max_retries: 3,
            initial_backoff_ms: 100,
            max_backoff_ms: 10_000,
            jitter: false,
        };

        assert!(cfg.backoff(0).is_some());
        assert!(cfg.backoff(1).is_some());
        assert!(cfg.backoff(2).is_some());
        assert!(cfg.backoff(3).is_none());
        assert!(cfg.backoff(99).is_none());
    }

    #[test]
    fn test_backoff_jitter_range() {
        let cfg = ReconnectConfig {
            max_retries: 5,
            initial_backoff_ms: 1_000,
            max_backoff_ms: 10_000,
            jitter: true,
        };

        let d = cfg.backoff(0).unwrap();
        let ms = d.as_millis();
        assert!(
            ms >= 800 && ms <= 1200,
            "jitter out of range: {ms} (expected 800..1200)"
        );

        // Deterministic jitter: same attempt → same value
        assert_eq!(cfg.backoff(0), cfg.backoff(0));

        // Different attempts produce different jitter
        let values: Vec<u128> = (0..cfg.max_retries)
            .map(|a| cfg.backoff(a).unwrap().as_millis())
            .collect();
        let unique = values
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len();
        assert!(unique > 1, "all backoff values were identical: {values:?}");
    }

    #[test]
    fn test_backoff_jitter_capped() {
        let cfg = ReconnectConfig {
            max_retries: 5,
            initial_backoff_ms: 1_000,
            max_backoff_ms: 500,
            jitter: true,
        };

        let d = cfg.backoff(2).unwrap();
        assert!(
            d.as_millis() <= 500,
            "jitter should not exceed max_backoff: {}",
            d.as_millis()
        );
    }

    #[test]
    fn test_backoff_zero_initial() {
        let cfg = ReconnectConfig {
            max_retries: 3,
            initial_backoff_ms: 0,
            max_backoff_ms: 1_000,
            jitter: false,
        };

        assert_eq!(cfg.backoff(0).unwrap(), Duration::from_millis(0));
        assert_eq!(cfg.backoff(1).unwrap(), Duration::from_millis(0));
    }

    #[test]
    fn test_backoff_saturating_mul() {
        let cfg = ReconnectConfig {
            max_retries: 100,
            initial_backoff_ms: u64::MAX,
            max_backoff_ms: u64::MAX,
            jitter: false,
        };

        assert_eq!(cfg.backoff(0).unwrap(), Duration::from_millis(u64::MAX));
    }

    #[test]
    fn test_backoff_jitter_factor_range() {
        // Jitter factor must stay within [-0.2, +0.2] for a range of attempts
        for attempt in 0..100 {
            let factor = ReconnectConfig::jitter_factor(attempt);
            assert!(
                factor >= -0.2 && factor <= 0.2,
                "jitter_factor({attempt}) out of range: {factor}"
            );
        }
    }

    #[test]
    fn test_reconnect_defaults() {
        let cfg = ReconnectConfig::default();
        assert_eq!(cfg.max_retries, 10);
        assert_eq!(cfg.initial_backoff_ms, 500);
        assert_eq!(cfg.max_backoff_ms, 30_000);
        assert!(cfg.jitter);
    }
}

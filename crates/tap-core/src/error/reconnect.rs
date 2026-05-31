//! Reconnection configuration and exponential-backoff calculation.

use std::time::Duration;

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
/// let delay = cfg.backoff(0);
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
    /// The jitter is deterministic per attempt number: the same
    /// `ReconnectConfig` and same `attempt` always produce the same
    /// value, which makes tests reproducible.
    pub fn backoff(&self, attempt: u32) -> Duration {
        let exponential = self
            .initial_backoff_ms
            .saturating_mul(2u64.saturating_pow(attempt));
        let capped = exponential.min(self.max_backoff_ms);

        if self.jitter {
            let factor = Self::jitter_factor(attempt);
            let jittered = ((capped as f64) * (1.0 + factor)) as u64;
            Duration::from_millis(jittered.min(self.max_backoff_ms))
        } else {
            Duration::from_millis(capped)
        }
    }

    /// Deterministic pseudo-random factor in `[-0.2, +0.2]` derived from
    /// `attempt`.  Uses [`std::hash::DefaultHasher`] so results are
    /// consistent for the same input within the same process.
    fn jitter_factor(attempt: u32) -> f64 {
        use std::hash::{Hash, Hasher};

        let mut hasher = std::hash::DefaultHasher::new();
        attempt.hash(&mut hasher);
        let hash = hasher.finish();

        // Map the full u64 range onto [-0.2, +0.2]
        (hash as f64 / u64::MAX as f64) * 0.4 - 0.2
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

        // attempt 0: 100 * 2^0 = 100
        assert_eq!(cfg.backoff(0), Duration::from_millis(100));
        // attempt 1: 100 * 2^1 = 200
        assert_eq!(cfg.backoff(1), Duration::from_millis(200));
        // attempt 2: 100 * 2^2 = 400
        assert_eq!(cfg.backoff(2), Duration::from_millis(400));
        // attempt 3: 100 * 2^3 = 800
        assert_eq!(cfg.backoff(3), Duration::from_millis(800));
    }

    #[test]
    fn test_backoff_max_cap() {
        let cfg = ReconnectConfig {
            max_retries: 10,
            initial_backoff_ms: 1_000,
            max_backoff_ms: 3_000,
            jitter: false,
        };

        // attempt 2: 1000 * 4 = 4000, capped at 3000
        assert_eq!(cfg.backoff(2), Duration::from_millis(3_000));
        // Higher attempts also stay at the cap
        assert_eq!(cfg.backoff(10), Duration::from_millis(3_000));
    }

    #[test]
    fn test_backoff_jitter_range() {
        let cfg = ReconnectConfig {
            max_retries: 5,
            initial_backoff_ms: 1_000,
            max_backoff_ms: 10_000,
            jitter: true,
        };

        // Attempt 0: base of 1000, jitter should stay in [800, 1200]
        let d = cfg.backoff(0);
        let ms = d.as_millis();
        assert!(
            ms >= 800 && ms <= 1200,
            "jitter out of range: {ms} (expected 800..1200)"
        );

        // Multiple calls to the same attempt produce the same result
        // (deterministic jitter)
        assert_eq!(cfg.backoff(0), cfg.backoff(0));
    }

    #[test]
    fn test_backoff_jitter_capped() {
        // With a low max and high jitter, the value should still be capped
        let cfg = ReconnectConfig {
            max_retries: 5,
            initial_backoff_ms: 1_000,
            max_backoff_ms: 500,
            jitter: true,
        };

        let d = cfg.backoff(2);
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

        assert_eq!(cfg.backoff(0), Duration::from_millis(0));
        assert_eq!(cfg.backoff(1), Duration::from_millis(0));
    }

    #[test]
    fn test_backoff_saturating_mul() {
        // Very large initial * 2^attempt should not panic via overflow
        let cfg = ReconnectConfig {
            max_retries: 100,
            initial_backoff_ms: u64::MAX,
            max_backoff_ms: u64::MAX,
            jitter: false,
        };

        // Should return the cap without panicking
        let d = cfg.backoff(100);
        assert_eq!(d, Duration::from_millis(u64::MAX));
    }
}

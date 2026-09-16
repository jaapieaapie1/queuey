//! Retry configuration: [`RetryPolicy`], [`Backoff`] and the backoff math.

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// How long to wait between attempts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum Backoff {
    /// Retry immediately.
    None,
    /// Constant delay between attempts.
    Fixed(Duration),
    /// `delay = min(max, base * factor^(attempt-1))`, optionally with full jitter.
    Exponential {
        /// Delay used for the first retry (after attempt 1 failed).
        base: Duration,
        /// Multiplier applied once per previous attempt.
        factor: f64,
        /// Upper bound of the computed delay, jitter included.
        max: Duration,
        /// When `true`, the delay is drawn uniformly from `[0, computed]`.
        jitter: bool,
    },
}

impl Backoff {
    /// Sensible exponential default: 1s base, x2, capped at 5 minutes, jittered.
    pub fn exponential() -> Self {
        Self::Exponential {
            base: Duration::from_secs(1),
            factor: 2.0,
            max: Duration::from_secs(300),
            jitter: true,
        }
    }

    /// Delay before the *next* attempt, given the attempt that just failed (1-based).
    /// Must never panic and must return a value <= `max` for the exponential variant.
    /// Jitter, when enabled, picks uniformly in `[0, computed]`.
    pub fn delay_for(&self, failed_attempt: u32) -> Duration {
        crate::retry::compute_delay(self, failed_attempt)
    }
}

/// Retry configuration attached to a queue or a job.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct RetryPolicy {
    /// Total attempts including the first. `1` means no retries.
    pub max_attempts: u32,
    /// How long to wait before each retry.
    pub backoff: Backoff,
}

impl Default for RetryPolicy {
    /// No retries.
    fn default() -> Self {
        Self {
            max_attempts: 1,
            backoff: Backoff::None,
        }
    }
}

/// Outcome of consulting a policy after a failure.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum RetryDecision {
    /// Re-publish after `delay`.
    Retry {
        /// How long to wait before the next attempt.
        delay: Duration,
    },
    /// Attempts exhausted; dead-letter.
    GiveUp,
}

impl RetryPolicy {
    /// A policy allowing `max_attempts` total attempts with the given backoff.
    pub fn new(max_attempts: u32, backoff: Backoff) -> Self {
        Self {
            max_attempts,
            backoff,
        }
    }
    /// No retries: a single attempt, no delay.
    pub fn none() -> Self {
        Self::default()
    }
    /// `max_attempts` attempts with the default exponential backoff.
    pub fn exponential(max_attempts: u32) -> Self {
        Self {
            max_attempts,
            backoff: Backoff::exponential(),
        }
    }
    /// `max_attempts` attempts with a constant `delay` between them.
    pub fn fixed(max_attempts: u32, delay: Duration) -> Self {
        Self {
            max_attempts,
            backoff: Backoff::Fixed(delay),
        }
    }

    /// Decide what to do after `failed_attempt` (1-based) failed.
    pub fn decide(&self, failed_attempt: u32) -> RetryDecision {
        if failed_attempt >= self.max_attempts {
            RetryDecision::GiveUp
        } else {
            RetryDecision::Retry {
                delay: self.backoff.delay_for(failed_attempt),
            }
        }
    }
}

/// Pure backoff math shared by [`Backoff::delay_for`].
///
/// Never panics: `failed_attempt` is clamped to at least 1, the exponentiation is
/// done in `f64` (saturating to `max` on overflow) and any non-finite intermediate
/// result falls back to `max`.
pub(crate) fn compute_delay(backoff: &Backoff, failed_attempt: u32) -> Duration {
    match backoff {
        Backoff::None => Duration::ZERO,
        Backoff::Fixed(delay) => *delay,
        Backoff::Exponential {
            base,
            factor,
            max,
            jitter,
        } => {
            let computed = exponential_delay(*base, *factor, *max, failed_attempt);
            if *jitter {
                full_jitter(computed)
            } else {
                computed
            }
        }
    }
}

/// `min(max, base * factor^(failed_attempt - 1))`, overflow-safe.
fn exponential_delay(base: Duration, factor: f64, max: Duration, failed_attempt: u32) -> Duration {
    if base.is_zero() || max.is_zero() {
        return Duration::ZERO;
    }
    // Attempt 0 is nonsensical but must not underflow; treat it like attempt 1.
    let exponent = f64::from(failed_attempt.max(1) - 1);
    // A non-finite or non-positive factor degenerates to a constant `base`.
    let factor = if factor.is_finite() && factor > 0.0 {
        factor
    } else {
        1.0
    };

    let secs = base.as_secs_f64() * factor.powf(exponent);
    if !secs.is_finite() {
        // Overflowed to +inf (or produced a NaN); the cap is the only sane answer.
        return max;
    }
    let capped = secs.clamp(0.0, max.as_secs_f64());
    Duration::try_from_secs_f64(capped).unwrap_or(max).min(max)
}

/// Full jitter: uniformly distributed in `[0, delay]`.
fn full_jitter(delay: Duration) -> Duration {
    use rand::RngExt;

    let secs = delay.as_secs_f64();
    if !secs.is_finite() || secs <= 0.0 {
        return Duration::ZERO;
    }
    let sampled = rand::rng().random_range(0.0..=secs);
    Duration::try_from_secs_f64(sampled)
        .unwrap_or(delay)
        .min(delay)
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: fn(u64) -> Duration = Duration::from_millis;

    #[test]
    fn none_is_zero() {
        assert_eq!(Backoff::None.delay_for(1), Duration::ZERO);
        assert_eq!(Backoff::None.delay_for(0), Duration::ZERO);
        assert_eq!(Backoff::None.delay_for(u32::MAX), Duration::ZERO);
    }

    #[test]
    fn fixed_is_constant() {
        let b = Backoff::Fixed(MS(250));
        for attempt in [0, 1, 2, 7, u32::MAX] {
            assert_eq!(b.delay_for(attempt), MS(250));
        }
    }

    #[test]
    fn exponential_first_attempt_is_base() {
        let b = Backoff::Exponential {
            base: MS(500),
            factor: 3.0,
            max: Duration::from_secs(600),
            jitter: false,
        };
        assert_eq!(b.delay_for(1), MS(500));
    }

    #[test]
    fn exponential_grows_by_factor() {
        let b = Backoff::Exponential {
            base: Duration::from_secs(1),
            factor: 2.0,
            max: Duration::from_secs(600),
            jitter: false,
        };
        assert_eq!(b.delay_for(1), Duration::from_secs(1));
        assert_eq!(b.delay_for(2), Duration::from_secs(2));
        assert_eq!(b.delay_for(3), Duration::from_secs(4));
        assert_eq!(b.delay_for(4), Duration::from_secs(8));
    }

    #[test]
    fn exponential_respects_cap() {
        let max = Duration::from_secs(10);
        let b = Backoff::Exponential {
            base: Duration::from_secs(1),
            factor: 2.0,
            max,
            jitter: false,
        };
        assert_eq!(b.delay_for(5), Duration::from_secs(10));
        assert_eq!(b.delay_for(50), max);
        assert_eq!(b.delay_for(u32::MAX), max);
    }

    #[test]
    fn exponential_attempt_zero_is_base() {
        let b = Backoff::Exponential {
            base: MS(120),
            factor: 2.0,
            max: Duration::from_secs(60),
            jitter: false,
        };
        assert_eq!(b.delay_for(0), MS(120));
    }

    #[test]
    fn exponential_handles_degenerate_factors() {
        let max = Duration::from_secs(60);
        for factor in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 0.0, -2.0] {
            let b = Backoff::Exponential {
                base: MS(100),
                factor,
                max,
                jitter: false,
            };
            for attempt in [0, 1, 2, 1000, u32::MAX] {
                let d = b.delay_for(attempt);
                assert!(d <= max, "factor {factor} attempt {attempt} gave {d:?}");
            }
            // Degenerate factors collapse to a constant `base`.
            assert_eq!(b.delay_for(3), MS(100), "factor {factor}");
        }
    }

    #[test]
    fn exponential_shrinking_factor_never_underflows() {
        let b = Backoff::Exponential {
            base: Duration::from_secs(1),
            factor: 0.5,
            max: Duration::from_secs(60),
            jitter: false,
        };
        assert_eq!(b.delay_for(2), MS(500));
        assert_eq!(b.delay_for(u32::MAX), Duration::ZERO);
    }

    #[test]
    fn exponential_zero_base_or_max_is_zero() {
        let a = Backoff::Exponential {
            base: Duration::ZERO,
            factor: 2.0,
            max: Duration::from_secs(60),
            jitter: true,
        };
        let b = Backoff::Exponential {
            base: Duration::from_secs(1),
            factor: 2.0,
            max: Duration::ZERO,
            jitter: false,
        };
        for attempt in [0, 1, 9, u32::MAX] {
            assert_eq!(a.delay_for(attempt), Duration::ZERO);
            assert_eq!(b.delay_for(attempt), Duration::ZERO);
        }
    }

    #[test]
    fn jitter_stays_within_bounds() {
        let max = Duration::from_secs(30);
        let b = Backoff::Exponential {
            base: Duration::from_secs(1),
            factor: 2.0,
            max,
            jitter: true,
        };
        let mut saw_below_cap = false;
        for _ in 0..2_000 {
            let d = b.delay_for(3);
            assert!(d <= Duration::from_secs(4), "{d:?} exceeded uncapped value");
            let capped = b.delay_for(99);
            assert!(capped <= max, "{capped:?} exceeded max");
            if d < Duration::from_secs(4) {
                saw_below_cap = true;
            }
        }
        assert!(
            saw_below_cap,
            "jitter never produced a value below the computed delay"
        );
    }

    #[test]
    fn jitter_is_not_constant() {
        let b = Backoff::Exponential {
            base: Duration::from_secs(10),
            factor: 2.0,
            max: Duration::from_secs(600),
            jitter: true,
        };
        let first = b.delay_for(4);
        let differs = (0..100).any(|_| b.delay_for(4) != first);
        assert!(differs, "jitter produced the same value 100 times");
    }

    #[test]
    fn huge_attempts_do_not_panic_with_jitter() {
        let b = Backoff::exponential();
        for attempt in [0, 1, u32::MAX / 2, u32::MAX] {
            let d = b.delay_for(attempt);
            assert!(d <= Duration::from_secs(300));
        }
    }

    #[test]
    fn decide_single_attempt_gives_up_immediately() {
        let p = RetryPolicy::new(1, Backoff::Fixed(Duration::from_secs(1)));
        assert_eq!(p.decide(1), RetryDecision::GiveUp);
    }

    #[test]
    fn decide_boundaries_for_three_attempts() {
        let p = RetryPolicy::new(3, Backoff::Fixed(Duration::from_secs(2)));
        assert_eq!(
            p.decide(1),
            RetryDecision::Retry {
                delay: Duration::from_secs(2)
            }
        );
        assert_eq!(
            p.decide(2),
            RetryDecision::Retry {
                delay: Duration::from_secs(2)
            }
        );
        assert_eq!(p.decide(3), RetryDecision::GiveUp);
        assert_eq!(p.decide(4), RetryDecision::GiveUp);
        assert_eq!(p.decide(u32::MAX), RetryDecision::GiveUp);
    }

    #[test]
    fn default_policy_has_no_retries() {
        let p = RetryPolicy::default();
        assert_eq!(p.max_attempts, 1);
        assert_eq!(p.backoff, Backoff::None);
        assert_eq!(p.decide(1), RetryDecision::GiveUp);
    }

    #[test]
    fn decide_uses_the_failed_attempt_for_the_delay() {
        let p = RetryPolicy::new(
            5,
            Backoff::Exponential {
                base: Duration::from_secs(1),
                factor: 2.0,
                max: Duration::from_secs(600),
                jitter: false,
            },
        );
        assert_eq!(
            p.decide(1),
            RetryDecision::Retry {
                delay: Duration::from_secs(1)
            }
        );
        assert_eq!(
            p.decide(3),
            RetryDecision::Retry {
                delay: Duration::from_secs(4)
            }
        );
    }
}

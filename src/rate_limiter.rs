//! Token-bucket rate limiter for controlling compaction write throughput.

use parking_lot::Mutex;
use std::time::{Duration, Instant};

/// A token-bucket rate limiter.
///
/// Controls the rate at which bytes can be written during compaction
/// to reduce impact on foreground read/write operations.
pub struct RateLimiter {
    inner: Mutex<RateLimiterInner>,
}

struct RateLimiterInner {
    /// Maximum bytes per second.
    rate_bytes_per_sec: u64,
    /// Available tokens (bytes).
    available: f64,
    /// Last time tokens were refilled.
    last_refill: Instant,
}

impl RateLimiter {
    /// Create a new rate limiter with the given bytes-per-second limit.
    /// If `rate_bytes_per_sec` is 0, the limiter is effectively disabled.
    pub fn new(rate_bytes_per_sec: u64) -> Self {
        Self {
            inner: Mutex::new(RateLimiterInner {
                rate_bytes_per_sec,
                available: rate_bytes_per_sec as f64,
                last_refill: Instant::now(),
            }),
        }
    }

    /// Request `bytes` tokens. Blocks until enough tokens are available.
    /// Returns immediately if the rate limiter is disabled (rate = 0).
    pub fn request(&self, bytes: usize) {
        let mut inner = self.inner.lock();
        if inner.rate_bytes_per_sec == 0 {
            return;
        }

        // Refill tokens
        let now = Instant::now();
        let elapsed = now.duration_since(inner.last_refill).as_secs_f64();
        inner.available += elapsed * inner.rate_bytes_per_sec as f64;
        inner.available = inner.available.min(inner.rate_bytes_per_sec as f64 * 2.0); // cap at 2x burst
        inner.last_refill = now;

        let needed = bytes as f64;
        if inner.available >= needed {
            inner.available -= needed;
            return;
        }

        // Compute deficit including accumulated debt from prior callers so
        // concurrent threads collectively respect the configured rate.
        let deficit = needed - inner.available;
        inner.available -= needed;
        let wait_secs = deficit / inner.rate_bytes_per_sec as f64;
        drop(inner);

        std::thread::sleep(Duration::from_secs_f64(wait_secs));
    }

    /// Check if the rate limiter is enabled.
    pub fn is_enabled(&self) -> bool {
        self.inner.lock().rate_bytes_per_sec > 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limiter_disabled() {
        let rl = RateLimiter::new(0);
        assert!(!rl.is_enabled());
        // Should return immediately
        rl.request(1_000_000);
    }

    #[test]
    fn test_rate_limiter_basic() {
        let rl = RateLimiter::new(1_000_000); // 1 MB/s
        assert!(rl.is_enabled());
        // Small request should succeed immediately
        rl.request(100);
    }
}

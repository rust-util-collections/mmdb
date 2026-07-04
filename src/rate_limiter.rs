//! Token-bucket rate limiter for controlling compaction write throughput.

use parking_lot::Mutex;
use std::time::{Duration, Instant};

/// Upper bound on a single `request()` sleep. Requests larger than
/// `rate_bytes_per_sec * MAX_SLEEP_SECS` are internally split into multiple
/// chunks (see `next_chunk`), each individually capped at this sleep, so a
/// single pathologically large request cannot overflow
/// `Duration::from_secs_f64` and so that a *sustained* stream of oversized
/// requests still converges to the configured rate instead of being capped
/// at `bytes / MAX_SLEEP_SECS` regardless of `rate_bytes_per_sec`.
const MAX_SLEEP_SECS: f64 = 60.0;

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
    /// Available tokens (bytes). May go negative to represent accumulated
    /// debt from a chunk whose full cost couldn't be slept off in one
    /// `MAX_SLEEP_SECS`-capped sleep; a chunk's debt never exceeds one
    /// capped sleep's worth (see `next_chunk`), so it always fully refills
    /// on the next call's elapsed-time-based refill.
    available: f64,
    /// Last time tokens were refilled.
    last_refill: Instant,
}

/// Result of accounting for one chunk of a `request()` call against the
/// current token balance. Pure function (no I/O, no sleeping) so the exact
/// accounting math can be unit-tested without real wall-clock delays.
struct ChunkPlan {
    /// Token balance after debiting this chunk (may be negative debt).
    new_available: f64,
    /// How long to sleep for this chunk, in seconds (0.0 if no wait needed).
    wait_secs: f64,
}

/// Plan one chunk of `remaining` bytes against `available` tokens at
/// `rate_bytes_per_sec`, bytes/sec, never accounting for more than
/// `rate_bytes_per_sec * MAX_SLEEP_SECS` bytes in this single chunk (the
/// most a `MAX_SLEEP_SECS`-capped sleep can fully pay for). Returns the
/// chunk size actually accounted for and the resulting `ChunkPlan`.
fn next_chunk(available: f64, remaining: f64, rate_bytes_per_sec: u64) -> (f64, ChunkPlan) {
    let rate = rate_bytes_per_sec as f64;
    let max_chunk = rate * MAX_SLEEP_SECS;
    let chunk = remaining.min(max_chunk);

    if available >= chunk {
        return (
            chunk,
            ChunkPlan {
                new_available: available - chunk,
                wait_secs: 0.0,
            },
        );
    }

    let deficit = chunk - available;
    // deficit <= max_chunk (since available >= 0 is the common case, and
    // even when available is already negative debt, chunk's own cost is
    // capped at max_chunk), so wait_secs <= MAX_SLEEP_SECS always; the
    // final `.min()` is a defensive clamp against floating-point edge cases.
    let wait_secs = (deficit / rate).min(MAX_SLEEP_SECS);
    (
        chunk,
        ChunkPlan {
            new_available: available - chunk,
            wait_secs,
        },
    )
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
    /// Large requests are internally chunked (see `next_chunk`) so no
    /// single sleep needs to exceed `MAX_SLEEP_SECS`, while still fully and
    /// proportionally accounting for every byte — this keeps a sustained
    /// stream of large requests converging to the configured rate instead
    /// of being capped at `bytes / MAX_SLEEP_SECS` regardless of
    /// `rate_bytes_per_sec`.
    /// Returns immediately if the rate limiter is disabled (rate = 0).
    pub fn request(&self, bytes: usize) {
        let mut remaining = bytes as f64;
        while remaining > 0.0 {
            let mut inner = self.inner.lock();
            if inner.rate_bytes_per_sec == 0 {
                return;
            }

            // Refill tokens.
            let now = Instant::now();
            let elapsed = now.duration_since(inner.last_refill).as_secs_f64();
            inner.available += elapsed * inner.rate_bytes_per_sec as f64;
            inner.available = inner.available.min(inner.rate_bytes_per_sec as f64 * 2.0); // cap at 2x burst
            inner.last_refill = now;

            let (chunk, plan) = next_chunk(inner.available, remaining, inner.rate_bytes_per_sec);
            inner.available = plan.new_available;
            remaining -= chunk;
            drop(inner);

            if plan.wait_secs > 0.0 {
                std::thread::sleep(Duration::from_secs_f64(plan.wait_secs));
            }
        }
    }
}

impl RateLimiter {
    /// Whether a rate limit is configured (test helper).
    #[cfg(test)]
    pub(crate) fn is_enabled(&self) -> bool {
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

    /// Regression test for unbounded debt accumulation: simulates a
    /// SUSTAINED stream of oversized requests (each exceeding
    /// `rate * MAX_SLEEP_SECS`) purely through the `next_chunk` accounting
    /// function — no real sleeping — and asserts the *total* wait time
    /// converges to `total_bytes / rate` (the configured rate), not to
    /// `total_bytes / MAX_SLEEP_SECS` (which would be 60x+ the configured
    /// rate for the numbers used here, matching the bug this test guards
    /// against).
    #[test]
    fn test_rate_limiter_oversized_requests_converge_to_configured_rate() {
        let rate: u64 = 1000; // 1000 B/s
        let one_request_bytes = 1_000_000.0; // >> rate * MAX_SLEEP_SECS (60_000)
        let num_requests = 5;

        let mut available = rate as f64; // matches RateLimiter::new's initial state
        let mut total_wait_secs = 0.0;
        for _ in 0..num_requests {
            // Simulate the refill a real caller would get from having
            // waited `total_wait_secs` worth of wall-clock time so far,
            // capped at the 2x burst ceiling exactly like `request()`.
            let mut remaining = one_request_bytes;
            while remaining > 0.0 {
                let (chunk, plan) = next_chunk(available, remaining, rate);
                assert!(
                    plan.wait_secs <= MAX_SLEEP_SECS,
                    "a single chunk must never require more than MAX_SLEEP_SECS"
                );
                available = plan.new_available;
                remaining -= chunk;
                total_wait_secs += plan.wait_secs;
                // Refill exactly as much as the simulated sleep represents,
                // mirroring what the next loop iteration in `request()`
                // would observe from real elapsed time.
                available += plan.wait_secs * rate as f64;
                available = available.min(rate as f64 * 2.0);
            }
        }

        let total_bytes = one_request_bytes * num_requests as f64;
        let expected_secs = total_bytes / rate as f64;
        // Allow generous slack for the burst-cap head start, but the whole
        // point of this test is that actual time must track the
        // configured rate (~1000s here), not `total_bytes / MAX_SLEEP_SECS`
        // (~16.7s) which is what the pre-fix code converged to.
        assert!(
            total_wait_secs >= expected_secs * 0.9 && total_wait_secs <= expected_secs * 1.1,
            "expected total wait ~{expected_secs}s (configured rate), got {total_wait_secs}s"
        );
    }

    /// The un-chunked accounting a single oversized request would have
    /// produced pre-fix: one MAX_SLEEP_SECS-capped sleep regardless of size.
    /// Kept as a direct demonstration that `next_chunk` no longer allows a
    /// single request to compress its whole deficit into one capped sleep.
    #[test]
    fn test_rate_limiter_single_oversized_request_needs_multiple_chunks() {
        let rate: u64 = 1000;
        let bytes = 1_000_000.0; // needs 1_000_000 / (1000 * 60) ≈ 17 chunks
        let mut available = rate as f64;
        let mut remaining = bytes;
        let mut chunks = 0;
        while remaining > 0.0 {
            let (chunk, plan) = next_chunk(available, remaining, rate);
            available = plan.new_available;
            remaining -= chunk;
            chunks += 1;
            assert!(
                chunks < 1000,
                "should converge in a bounded number of chunks"
            );
        }
        assert!(
            chunks > 1,
            "an oversized request must be split into multiple chunks, not resolved in one capped sleep"
        );
    }
}

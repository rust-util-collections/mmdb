//! Token-bucket rate limiter for controlling compaction write throughput.

use parking_lot::Mutex;
use std::time::{Duration, Instant};

/// Upper bound on a single `thread::sleep` call issued by `request()`.
/// Requests larger than `rate_bytes_per_sec * MAX_SLEEP_SECS` are
/// internally split into multiple chunks (see `next_chunk`). A chunk's own
/// true wait can still exceed `MAX_SLEEP_SECS` when concurrent callers
/// share the same `available` counter (see `RateLimiterInner::available`);
/// `request` sleeps that off in several `MAX_SLEEP_SECS`-capped increments
/// rather than issuing one giant sleep. This bounds every individual
/// `Duration::from_secs_f64` call and keeps a *sustained* stream of
/// oversized or concurrent requests converging to the configured rate
/// instead of being capped at `bytes / MAX_SLEEP_SECS` regardless of
/// `rate_bytes_per_sec`.
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
    /// debt: a chunk's full cost is always debited immediately, even when
    /// it can't be slept off in one `max_sleep_secs`-capped sleep. Under
    /// concurrent `request()` callers sharing this counter, a chunk's debt
    /// can legitimately require several capped sleeps to repay (see
    /// `next_chunk` and `request`); it is always fully repaid by the
    /// thread that incurred it before that thread returns, so debt never
    /// silently accumulates beyond what is currently in flight.
    available: f64,
    /// Last time tokens were refilled.
    last_refill: Instant,
    /// Per-chunk sleep cap. Always `MAX_SLEEP_SECS` in production (see
    /// `RateLimiter::new`); overridable in tests so the multi-chunk
    /// wait-loop in `request` can be exercised in milliseconds instead of
    /// minutes without changing the accounting logic under test.
    max_sleep_secs: f64,
}

/// Result of accounting for one chunk of a `request()` call against the
/// current token balance. Pure function (no I/O, no sleeping) so the exact
/// accounting math can be unit-tested without real wall-clock delays.
struct ChunkPlan {
    /// Token balance after debiting this chunk (may be negative debt).
    new_available: f64,
    /// This chunk's true, uncapped wait, in seconds (0.0 if no wait is
    /// needed). May exceed `max_sleep_secs` when concurrent callers have
    /// pushed `available` deeply negative; `request` sleeps this off in
    /// `max_sleep_secs`-capped increments rather than discarding the
    /// excess, which is what previously let aggregate throughput exceed
    /// the configured rate under concurrency.
    wait_secs: f64,
}

/// Plan one chunk of `remaining` bytes against `available` tokens at
/// `rate_bytes_per_sec` bytes/sec, never accounting for more than
/// `rate_bytes_per_sec * max_sleep_secs` bytes in this single chunk (the
/// most a single `max_sleep_secs`-capped sleep can fully pay for from a
/// non-negative balance). Returns the chunk size actually accounted for
/// and the resulting `ChunkPlan`.
///
/// `wait_secs` is intentionally NOT capped at `max_sleep_secs` here: under
/// concurrent callers sharing `available`, one chunk's true deficit can
/// exceed `max_sleep_secs` worth of tokens even though the chunk itself
/// never does (see `RateLimiterInner::available`). Capping it in this
/// function would silently forgive real debt instead of deferring it,
/// letting aggregate throughput exceed the configured rate under
/// concurrency — `request` is responsible for honoring the full,
/// uncapped `wait_secs` by sleeping it off in bounded increments instead.
fn next_chunk(
    available: f64,
    remaining: f64,
    rate_bytes_per_sec: u64,
    max_sleep_secs: f64,
) -> (f64, ChunkPlan) {
    let rate = rate_bytes_per_sec as f64;
    let max_chunk = rate * max_sleep_secs;
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
    let wait_secs = deficit / rate;
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
                max_sleep_secs: MAX_SLEEP_SECS,
            }),
        }
    }

    /// Request `bytes` tokens. Blocks until enough tokens are available.
    /// Large requests are internally chunked (see `next_chunk`) so no
    /// single chunk accounts for more than `rate * MAX_SLEEP_SECS` bytes at
    /// once, while still fully and proportionally accounting for every
    /// byte — this keeps a sustained stream of large requests converging
    /// to the configured rate instead of being capped at
    /// `bytes / MAX_SLEEP_SECS` regardless of `rate_bytes_per_sec`.
    ///
    /// Each chunk's own true wait (`ChunkPlan::wait_secs`) is slept off in
    /// full, in increments of at most `MAX_SLEEP_SECS` per `thread::sleep`
    /// call. The remaining wait for the current chunk is tracked in a
    /// plain local variable rather than being recomputed from the shared
    /// `available` counter between increments, so other threads
    /// concurrently debiting `available` in the meantime cannot shrink
    /// (or inflate) what this chunk still owes. This keeps every caller's
    /// wait proportional to its own fair share of the configured rate even
    /// under concurrency, instead of every caller being independently
    /// capped at `MAX_SLEEP_SECS` regardless of how deep the shared debt
    /// already is — which previously let aggregate throughput across N
    /// concurrent callers reach ~N times the configured rate.
    ///
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

            let (chunk, plan) = next_chunk(
                inner.available,
                remaining,
                inner.rate_bytes_per_sec,
                inner.max_sleep_secs,
            );
            inner.available = plan.new_available;
            let max_sleep_secs = inner.max_sleep_secs;
            remaining -= chunk;
            drop(inner);

            let mut remaining_wait = plan.wait_secs;
            while remaining_wait > 0.0 {
                let sleep_secs = remaining_wait.min(max_sleep_secs);
                std::thread::sleep(Duration::from_secs_f64(sleep_secs));
                remaining_wait -= sleep_secs;
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
    use std::sync::Arc;
    use std::thread;

    /// Builds a `RateLimiter` with a small `max_sleep_secs` override so
    /// concurrency tests can drive `request`'s multi-chunk wait-loop (and
    /// the bug it guards against) in milliseconds instead of minutes.
    /// Production code always goes through `RateLimiter::new`, which
    /// hardcodes `MAX_SLEEP_SECS`; overriding it only rescales the "unit"
    /// a chunk is measured against, not the accounting logic under test.
    fn rate_limiter_for_test(rate_bytes_per_sec: u64, max_sleep_secs: f64) -> RateLimiter {
        RateLimiter {
            inner: Mutex::new(RateLimiterInner {
                rate_bytes_per_sec,
                available: rate_bytes_per_sec as f64,
                last_refill: Instant::now(),
                max_sleep_secs,
            }),
        }
    }

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
                let (chunk, plan) = next_chunk(available, remaining, rate, MAX_SLEEP_SECS);
                assert!(
                    plan.wait_secs <= MAX_SLEEP_SECS,
                    "a single chunk must never require more than MAX_SLEEP_SECS \
                     in single-threaded use (no concurrent debt to compound)"
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
            let (chunk, plan) = next_chunk(available, remaining, rate, MAX_SLEEP_SECS);
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

    /// Regression test for the concurrent debt-accounting bug: N threads
    /// share one `Arc<RateLimiter>` and each request `max_chunk`-sized
    /// bytes in a tight loop. Before the fix, `next_chunk` capped
    /// `wait_secs` at `max_sleep_secs` based only on the calling thread's
    /// own read of `available`, taken while holding the lock — but the
    /// actual sleep happens after the lock is dropped. A second concurrent
    /// thread could then lock, read the still-unpaid (deeply negative)
    /// `available` left by the first thread's debit, and independently
    /// compute its OWN `max_sleep_secs`-capped wait against an
    /// already-deep deficit, so each of N concurrent threads ended up
    /// granted a full chunk within roughly the same time window —
    /// aggregate throughput of ~N times the configured rate instead of
    /// the configured rate.
    ///
    /// Uses `rate_limiter_for_test`'s small `max_sleep_secs` override so
    /// the same multi-chunk wait-loop that would take minutes to exercise
    /// at the production `MAX_SLEEP_SECS = 60.0` completes in a fraction
    /// of a second here, keeping this test fast and non-flaky while still
    /// exercising the exact same accounting code as production.
    #[test]
    fn test_rate_limiter_concurrent_requests_converge_to_configured_rate() {
        const NUM_THREADS: u64 = 4;
        let rate: u64 = 200_000; // 200 KB/s
        let max_sleep_secs = 0.01; // 10ms -> max_chunk = 2,000 bytes/chunk
        let bytes_per_request: usize = 2_000; // == max_chunk, maximizes per-chunk debt
        let requests_per_thread: u64 = 50; // 100_000 bytes/thread

        let rl = Arc::new(rate_limiter_for_test(rate, max_sleep_secs));

        // Drain the constructor's initial burst allowance (`available`
        // starts at `rate_bytes_per_sec` tokens, see `RateLimiter::new`)
        // up front, untimed, so the timed section below measures only
        // steady-state rate-limited throughput. Without this, a one-time
        // burst credit large relative to this short test's total bytes
        // would inflate the measured aggregate rate independent of
        // whether concurrent debt accounting is correct.
        rl.request(rate as usize);

        let start = Instant::now();
        let handles: Vec<_> = (0..NUM_THREADS)
            .map(|_| {
                let rl = Arc::clone(&rl);
                thread::spawn(move || {
                    for _ in 0..requests_per_thread {
                        rl.request(bytes_per_request);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().expect("rate limiter thread panicked");
        }
        let elapsed = start.elapsed().as_secs_f64();

        let total_bytes = (NUM_THREADS * requests_per_thread * bytes_per_request as u64) as f64;
        let aggregate_rate = total_bytes / elapsed;

        // Pre-fix, each of the NUM_THREADS threads was independently
        // capped at max_sleep_secs regardless of the shared debt's true
        // depth, so aggregate throughput converged to ~NUM_THREADS times
        // the configured rate. Post-fix it must stay close to the
        // configured rate instead. Generous asymmetric tolerance avoids
        // CI flakiness while still failing loudly on an ~Nx regression
        // (for NUM_THREADS=4, Nx would be 800_000 B/s, far above 1.3x;
        // empirically the pre-fix code measures ~2.7x on this scenario).
        assert!(
            aggregate_rate <= rate as f64 * 1.3,
            "aggregate throughput {aggregate_rate:.0} B/s across {NUM_THREADS} \
             concurrent threads should stay close to the configured rate of \
             {rate} B/s, not far exceed it (elapsed={elapsed:.3}s, \
             total_bytes={total_bytes})"
        );
        assert!(
            aggregate_rate >= rate as f64 * 0.5,
            "aggregate throughput {aggregate_rate:.0} B/s dropped implausibly \
             far below the configured rate of {rate} B/s (elapsed={elapsed:.3}s, \
             total_bytes={total_bytes})"
        );
    }
}

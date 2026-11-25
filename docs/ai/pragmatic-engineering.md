# Pragmatic Engineering

Skills and reviews serve correct, durable MMDB behavior. Ceremony, metrics, and
process artifacts are not goals.

Simplicity = stable, deterministic execution — not shorter text.

- **Keep signal**: explicit names, file:line evidence, safety steps that remove guesswork.
- **Cut variance**: duplicate truth, discretionary branches, rework without new evidence, speculative features, risk-free ceremony.
- **Keep controls**: one-issue-one-commit, worktree ownership, independent checks of findings against existing guards, interrupted-write durability evidence, final gates.
- **Admit only improvements that name the ambiguity or failure they remove.**

## Performance priorities

Protect MVCC visibility, ordering, and durability while optimizing the point
read, scan, and write paths. Compare the same workload before and after a
change. Include background flush or compaction when its throughput limits
admission or causes foreground stalls.

For a measured optimization, identify the control, operation class, offered
load, durability and memory/CPU budget. Keep arrival-to-ack and service
latency distinct. Separate per-thread diagnostic timing (`bench-metrics`)
from uninstrumented trials and report the actual worker or shard scope.
Compare paired changes against baseline-to-baseline variation. A promising
primary metric leaves the candidate provisional when a protected read,
write or resource budget remains unresolved. Raw paired runs and deferred
prototypes belong with their [benchmark evidence](../benchmarks/).

An occasional cleanup pass or a one-time format conversion can take longer
when correctness or a simpler implementation requires it. Keep tool-specific
optimizations in the tool. Before adding state, locking, or branches to the
engine for maintenance work, establish a measurable benefit to normal
operations. If cleanup holds a writer lock or retains a growing queue,
measure that foreground impact as part of the core path.

For bounded point reads, keep the scalar and grouped controls on the same snapshot and charge the same lifetime for returned pins. Compare full-request p99 across the same input trace and qualify device traffic separately. [Grouped-read evidence](../benchmarks/grouped-reads/README.md)

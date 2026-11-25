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

An occasional cleanup pass or a one-time format conversion can take longer
when correctness or a simpler implementation requires it. Keep tool-specific
optimizations in the tool. Before adding state, locking, or branches to the
engine for maintenance work, establish a measurable benefit to normal
operations. If cleanup holds a writer lock or retains a growing queue,
measure that foreground impact as part of the core path.

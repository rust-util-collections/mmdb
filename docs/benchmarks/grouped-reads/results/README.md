# Raw grouped-read observations

- `prototype/` records the single-cursor experiment at `b1a7d22`. The first
  primary run used 50,000 requests per fresh process and could not resolve the
  singleton guardrail. `extended.csv` repeated the complete paired experiment
  at 300,000 requests. `warm.csv` and `page.csv` are the other completed matrix
  cells. The device series was interrupted between processes, then superseded
  by the review requirement for per-request storage coverage. Its partial
  rows are retained as `device-incomplete.csv` for diagnostic reference.
- `public-before-correction/` records the default-feature scalar guard and
  the interrupted primary run at `09b8e29`. Review identified premature
  returned-byte accounting on an error and unnecessary cache churn on seeks
  past a third file's last partition. These observations precede both fixes.
- `final/` contains measurements of the corrected runtime at
  `fca4802`. Instrumented trials use that same release executable for the scalar
  and grouped arms. Separate production-build guards compare normal MMDB
  dependencies on the parent revision `72d1dd9` and the corrected candidate.
  The integrated production check uses `277ef08` and `736b87e`.

Each measured duration uses one snapshot. The CSVs record A/A and A/B
pair identity, order, trace seed and request count. The per-matrix JSON
records the binary digest, fixture and host configuration. Only complete
paired series for the corrected runtime enter the final decision.

For a device cell, the printed `p99_us` includes every completed request in
each A/B arm. `io_positive_only_p99_us` uses the requests with at least 4 KiB
of observed Linux storage reads. `min_io_qualified` is the lowest qualifying
fraction in any A/A or A/B process. `linux_read_bytes_per_request` divides
the process's Linux `read_bytes` by all completed requests, then reports the
median of each A/B arm. The raw `peak_pin_bytes` column measures the maximum
capacity of distinct SST data allocations retained by one request.

## Check out the measured source

The trials used detached development commits. The [Git bundle](measured-revisions.bundle)
contains the eight prototype and candidate commits ending at `fca4802` and
the two integration commits ending at `736b87e`. Their prerequisites
`72d1dd9` and `277ef08` are in the main history. From a full MMDB clone
containing this directory, restore the benchmark commits locally:

```sh
bundle=docs/benchmarks/grouped-reads/results/measured-revisions.bundle
git bundle verify "$bundle"
git fetch "$bundle" refs/heads/grouped-candidate:refs/heads/grouped-candidate \
    refs/heads/grouped-integration:refs/heads/grouped-integration
git worktree add --detach ../mmdb-read-trial fca4802
```

In a fresh clone, these steps also recover the exact source trees at
`b1a7d22`, `09b8e29` and `736b87e`. Their raw measurements retain the
build and fixture identifiers from the original runs.

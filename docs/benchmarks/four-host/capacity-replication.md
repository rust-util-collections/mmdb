# Cache-capacity replication

The primary capacity experiment uses one server with 377 GiB of RAM. This
supplement was specified on 2026-11-26 before its own samples or the EPYC
formal 64 GiB results were available. Repeat the same relative cache fit
on the 9950X/ext4 host with a one-eighth-scale dataset. This tests the
role of working-set fit across two CPUs, kernels and storage stacks.

Use the same external driver, dependency versions, latest engine revision,
8 KiB values, 100,000 uniform random reads, five A/A and five independent
A/B pairs as on EPYC. Build the fixture anew on the ext4 scratch path.
Map the 64 GiB value payload to 8 GiB (1,048,576 rows), 4 GiB of cache to
512 MiB, and 96 GiB to 12 GiB per weighted store. Require a measured
large-arm charge of at least 6 GiB and small-arm charge of at most
640 MiB. Use CPU 4 and preserve the 32 GiB host memory reserve.

Report warmed charges, process RSS, file residency and per-arm hits, misses
and physical read counters. The hit-ratio gate for the large arm is 95%;
all measured arms require ≥99% SST residency before and after random
reads and zero measured process disk reads. Use the primary rule of five
favorable paired p99 reductions with their median above both the A/A
spread and 10%. A cross-host recipe requires the evidence from both
fixture sizes. [Primary protocol](protocol.md) · [Capacity driver](../cache-capacity/README.md)

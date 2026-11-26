# Matched disk comparison

This supplementary comparison was specified before its first sample. It
answers a question the main tuning matrix leaves open when the two
volumes are measured in different hours. On each Linux host, alternate
reference/reference and reference/candidate volumes, running the same
binary, configuration, trace and CPU allocation for both. Each comparison
uses five A/A pairs and five independent A/B pairs.

The reference is the single-disk Btrfs scratch on the 5900X and EPYC,
and ext4 on the 9950X. The candidate is respectively the dual-drive home
Btrfs RAID0 or the dedicated ZFS dataset. The two workloads are 10,000
WalSync puts and uncompressed SST ingestion/scan/point reads on the
64 MiB high-entropy input. Use the same per-process workload verification
and primary metrics as the main [protocol](protocol.md). Report storage
bytes and buffered-read status together with latency. Only the
deployment's whole storage stack is eligible for comparison.

The measured WAL policies remain identical across volumes. The final
fence and reopen verify the acknowledged rows. This comparison excludes
the Apple host, which contributed one physical storage stack.

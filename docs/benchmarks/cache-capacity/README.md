# Engine cache capacity with a resident SST working set

The Linux driver compares block-cache sizes against the same immutable MMDB files. `prepare` ingests sorted eight-byte keys with deterministic 8 KiB values straight into the bottom level, using 64 KiB data blocks, 256 MiB target SSTs and no compression. Filesystem compression is controlled separately on the scratch directory. Each fresh read-only process opens one snapshot, reads every fourth key to populate the caches and then times random `get_pinned` calls. Both the warm-up reads and timed results are checked byte for byte. Verification runs after each lookup timer stops.

Before warming, after warming and after the random reads, the driver records the two MMDB cache charges, database-local buffers, Linux RSS and high-water RSS, host memory availability and page-cache bytes. `fincore` measures residency for the fixture SSTs in whole pages. A resident last page can include unused bytes, so the reported residency fraction is capped at one. `/proc/self/io` supplies the process's disk read bytes during the random requests. Hit and miss counts include probes into cached SST index and data blocks. `service_ns` sums only time spent in the measured lookups.

Build the standalone binary from this directory. It links MMDB with its default features. Set `probe` to its release executable and `fixture` to an absent path on the test volume. A default run needs 64 GiB for the value payload, additional space for SST metadata and enough RAM for both application and kernel caches. The runner reserves 32 GiB of available host memory and checks it before each arm and during warm-up.

```sh
cargo +stable build --release
"$probe" prepare "$fixture" 8388608
python3 run.py "$probe" "$fixture" capacity.jsonl --cpu 4
```

The default matrix uses five independent small-cache/small-cache controls and five 4 GiB/96 GiB pairs with the same seeds. Each fresh process makes 100,000 uniform pseudorandom requests and holds its pin through validation. Report both capacities' warm charges, measured hits and misses, process memory and SST residency. For the 64 GiB fixture the intended contrast requires at least 48 GiB charged in the large cache and at most 5 GiB in the small cache. Both arms must have at least 99% SST residency at warm-up completion and after the random reads, plus zero measured disk read bytes to support a page-cache comparison. Report a tail-latency benefit when all five paired gains are positive and the median gain clears both 10% and the largest absolute A/A change.

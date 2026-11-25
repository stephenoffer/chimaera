# Shuffle phases, 2 peers (one machine, 2026-11-24)

300 maps of 200,000 pairs into 256 partitions, 4,000,037 keys; median of 1 runs.

Held to 1073741824 bytes (cgroup memory.max); memory.peak 1,073,811,456 (page cache included); OOM kills 0; /dev/shm 2,684,354,560 bytes; each peer's memory pool 320MiB.

## uniform: 47.474 s, digest matches the serial reference, largest partition 10,031 keys, 256 partitions over 2 cores

| Phase | Seconds (sum over calls) | CPU seconds (sum) | Per call, median (s) | Slowest call (s) | Bytes |
|---|---:|---:|---:|---:|---:|
| plan | 0.040 |  | 0.0404 | 0.040 | 0 |
| map | 8.523 | 7.232 | 0.0258 | 0.088 | 0 |
| split | 12.050 | 11.303 | 0.0368 | 0.173 | 0 |
| scatter_encode | 2.695 | 2.493 | 0.0069 | 0.128 | 425,534,264 |
| fetch | 0.595 | 0.543 | 0.0023 | 0.009 |  |
| read | 30.623 | 17.717 | 0.0855 | 0.567 | 425,534,264 |
| reduce | 12.772 | 12.381 | 0.0448 | 0.259 | 0 |
| gather_encode | 0.118 | 0.115 | 0.0003 | 0.006 | 18,008,387 |
| final | 3.016 |  | 3.0158 | 3.016 | 0 |
| network (the peers' shuffle sends) | | | | | 0 |
| spill (the peers' stores) | 0.418 | | | | 221,625,025 |
| most a gather grew its process | | | | | 0 |

| Boundary (ms after the driver's start) | First | Median | Last |
|---|---:|---:|---:|
| submit_start | 0.9 | 0.9 | 0.9 |
| submitted | 41.3 | 41.3 | 41.3 |
| scatter_start | 39.6 | 8524.6 | 18151.2 |
| scatter_mapped | 61.7 | 8548.3 | 18175.4 |
| scatter_split | 120.4 | 8591.1 | 18205.3 |
| scatter_end | 134.0 | 8596.9 | 18210.3 |
| manifest_start | 17993.2 | 17993.2 | 17993.2 |
| manifest_waited | 18243.1 | 18243.1 | 18243.1 |
| manifest_end | 18243.2 | 18243.2 | 18243.2 |
| gather_start | 18299.5 | 29353.2 | 43866.1 |
| gather_fetched | 18303.5 | 29354.8 | 43868.4 |
| gather_merged | 18594.0 | 29436.7 | 44219.1 |
| gather_end | 18595.5 | 29437.2 | 44219.8 |
| driver_done | 44243.1 | 44243.1 | 44243.1 |
| driver_collected | 47258.9 | 47258.9 | 47258.9 |

# Shuffle phases, 2 peers (one machine, 2026-11-24)

2400 maps of 200,000 pairs into 256 partitions, 4,000,037 keys; median of 1 runs.

Held to 1073741824 bytes (cgroup memory.max); memory.peak 1,074,200,576 (page cache included); OOM kills 0; /dev/shm 67,108,864 bytes; each peer's memory pool auto.

## uniform: 1220.723 s, digest matches the serial reference, largest partition 15,626 keys, 256 partitions over 2 cores

| Phase | Seconds (sum over calls) | CPU seconds (sum) | Per call, median (s) | Slowest call (s) | Bytes |
|---|---:|---:|---:|---:|---:|
| plan | 0.116 |  | 0.1160 | 0.116 | 0 |
| map | 72.263 | 59.994 | 0.0262 | 0.822 | 0 |
| split | 103.676 | 94.642 | 0.0364 | 0.948 | 0 |
| scatter_encode | 24.187 | 22.300 | 0.0081 | 0.256 | 3,395,277,019 |
| fetch | 8.094 | 6.898 | 0.0220 | 0.141 |  |
| read | 1845.865 | 691.118 | 6.4067 | 19.140 | 3,395,277,019 |
| reduce | 164.160 | 152.381 | 0.6268 | 1.120 | 0 |
| gather_encode | 0.998 | 0.883 | 0.0022 | 0.033 | 28,484,674 |
| final | 4.595 |  | 4.5952 | 4.595 | 0 |
| network (the peers' shuffle sends) | | | | | 0 |
| spill (the peers' stores) | 0.000 | | | | 0 |
| most a gather grew its process | | | | | 0 |

| Boundary (ms after the driver's start) | First | Median | Last |
|---|---:|---:|---:|
| submit_start | 0.3 | 0.3 | 0.3 |
| submitted | 116.3 | 116.3 | 116.3 |
| scatter_start | 21.3 | 65254.2 | 154583.4 |
| scatter_mapped | 43.5 | 65283.7 | 154616.5 |
| scatter_split | 64.9 | 65324.4 | 154674.1 |
| scatter_end | 89.7 | 65332.9 | 154687.6 |
| manifest_start | 119322.5 | 119322.5 | 119322.5 |
| manifest_waited | 154757.0 | 154757.0 | 154757.0 |
| manifest_end | 154759.8 | 154759.8 | 154759.8 |
| gather_start | 154846.9 | 745787.1 | 1199960.2 |
| gather_fetched | 154878.7 | 745820.9 | 1199971.9 |
| gather_merged | 159259.3 | 754412.1 | 1204085.9 |
| gather_end | 159261.4 | 754417.0 | 1204086.6 |
| driver_done | 1204099.9 | 1204099.9 | 1204099.9 |
| driver_collected | 1208695.1 | 1208695.1 | 1208695.1 |

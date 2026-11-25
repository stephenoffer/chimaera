# Shuffle phases, 3 peers on one machine (2026-11-24)

64 maps of 50,000 pairs into 64 partitions, 100,003 keys; median of 3 runs.

## uniform: 0.665 s, digest matches the serial reference, largest partition 1,563 keys

| Phase | Seconds (sum over calls) | Per call, median (s) | Slowest call (s) | Bytes |
|---|---:|---:|---:|---:|
| plan | 0.004 | 0.0041 | 0.004 | 0 |
| map | 0.215 | 0.0025 | 0.011 | 0 |
| split | 0.278 | 0.0035 | 0.041 | 0 |
| scatter_encode | 0.145 | 0.0012 | 0.027 | 18,496,024 |
| fetch | 0.826 | 0.0005 | 0.086 |  |
| read | 0.839 | 0.0071 | 0.072 | 18,496,024 |
| reduce | 0.743 | 0.0064 | 0.067 | 0 |
| gather_encode | 0.016 | 0.0001 | 0.010 | 707,701 |
| final | 0.088 | 0.0878 | 0.088 | 0 |
| network (the peers' shuffle sends) | | | | 21,389,062 |
| spill (the peers' stores) | 0.000 | | | 0 |
| most a gather grew its process | | | | 147,456 |

| Boundary (ms after the driver's start) | First | Median | Last |
|---|---:|---:|---:|
| submit_start | 0.2 | 0.2 | 0.2 |
| submitted | 4.4 | 4.4 | 4.4 |
| scatter_start | 3.5 | 84.5 | 245.4 |
| scatter_mapped | 5.9 | 88.4 | 247.8 |
| scatter_split | 9.3 | 101.0 | 250.7 |
| scatter_end | 11.9 | 102.0 | 251.7 |
| manifest_start | 147.9 | 147.9 | 147.9 |
| manifest_waited | 259.8 | 259.8 | 259.8 |
| manifest_end | 259.9 | 259.9 | 259.9 |
| gather_start | 272.9 | 409.5 | 540.9 |
| gather_fetched | 333.4 | 414.6 | 541.3 |
| gather_merged | 342.9 | 438.2 | 573.6 |
| gather_end | 343.0 | 438.3 | 573.7 |
| driver_done | 576.8 | 576.8 | 576.8 |
| driver_collected | 664.6 | 664.6 | 664.6 |

## zipf: 0.529 s, digest matches the serial reference, largest partition 1,548 keys

| Phase | Seconds (sum over calls) | Per call, median (s) | Slowest call (s) | Bytes |
|---|---:|---:|---:|---:|
| plan | 0.004 | 0.0041 | 0.004 | 0 |
| map | 0.650 | 0.0085 | 0.047 | 0 |
| split | 0.594 | 0.0037 | 0.051 | 0 |
| scatter_encode | 0.079 | 0.0003 | 0.025 | 3,912,224 |
| fetch | 0.172 | 0.0001 | 0.021 |  |
| read | 0.416 | 0.0020 | 0.070 | 3,912,224 |
| reduce | 0.194 | 0.0014 | 0.031 | 0 |
| gather_encode | 0.013 | 0.0001 | 0.005 | 699,244 |
| final | 0.076 | 0.0762 | 0.076 | 0 |
| network (the peers' shuffle sends) | | | | 1,382,337 |
| spill (the peers' stores) | 0.000 | | | 0 |
| most a gather grew its process | | | | 81,920 |

| Boundary (ms after the driver's start) | First | Median | Last |
|---|---:|---:|---:|
| submit_start | 0.1 | 0.1 | 0.1 |
| submitted | 4.3 | 4.3 | 4.3 |
| scatter_start | 3.1 | 87.4 | 267.7 |
| scatter_mapped | 9.8 | 96.3 | 274.5 |
| scatter_split | 12.5 | 113.0 | 277.4 |
| scatter_end | 12.9 | 117.9 | 277.8 |
| manifest_start | 183.9 | 183.9 | 183.9 |
| manifest_waited | 283.0 | 283.0 | 283.0 |
| manifest_end | 283.0 | 283.0 | 283.0 |
| gather_start | 299.6 | 368.2 | 439.3 |
| gather_fetched | 317.7 | 368.3 | 439.4 |
| gather_merged | 324.5 | 381.0 | 445.1 |
| gather_end | 324.6 | 381.1 | 445.2 |
| driver_done | 448.4 | 448.4 | 448.4 |
| driver_collected | 529.0 | 529.0 | 529.0 |

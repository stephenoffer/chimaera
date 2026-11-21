# Ray microbenchmarks vs Chimaera (2026-11-21)

Local measurements on a shared laptop: other sessions were building and testing Chimaera on the same machine during the runs (see the load averages). Not a cluster claim.

- Machine: Apple M3 Pro, hw.ncpu=12, 36.0 GiB, macOS 14.5 (arm64)
- Ray 2.60.0 on Python 3.12.13; Chimaera 22cbcd2 (working tree with 128 changed files) on Python 3.14.4; peer binary sha256 99f83ae2892e4d6e
- Method: Ray's `ray_microbenchmark_helpers.timeit` (1 s warmup, mean of 4 windows of 2 s) for both; pause before each benchmark 3 s (Ray's default is 10 s); each run is a fresh process, alternating which runtime goes first; completed runs: Ray 3, Chimaera 3 (failed runs are listed below and excluded); cells show the median of the per-run means, with the min–max across rounds in parentheses.
- Ratio = Chimaera / Ray; every metric is a rate, so above 1 means Chimaera is faster.
- Disk: the local peer never deletes superseded raft snapshots (one full state-machine copy every 50k log entries), so the harness deletes all but the newest during Chimaera runs to protect the shared disk. Building the snapshots still happens and is measured. Ray runs with RAY_local_fs_capacity_threshold=0.99 because the disk was over Ray's default 95% spill cutoff.

| metric | Ray 2.60 | Chimaera | ratio | note |
|---|---:|---:|---:|---|
| single client get calls (Plasma Store) | 16,358 (15,609–17,483) | 7,672,149 (5,208,467–8,018,036) | 469.01x | ch.put(0) is inline in the handle; .result() returns the value cached on the future after the first read (no IPC per call) |
| single client put calls (Plasma Store) | 7,902 (5,544–9,015) | 274,640 (265,703–277,770) | 34.76x | ch.put(0) pickles and hashes in the driver; small values stay inline, no IPC |
| multi client put calls (Plasma Store) | 21,785 (13,164–24,811) | 165,417 (151,596–170,391) | 7.59x | same as single put: inline, no IPC, inside 10 tasks |
| single client put gigabytes | 54.01 GB/s (49.31 GB/s–54.17 GB/s) | 13.05 GB/s (12.03 GB/s–15.21 GB/s) | 0.24x | content-addressed: the same array is blake3-hashed every put but written once per run |
| single client tasks and get batch | 10.5 (8.5–11.6) | 12.2 (11.1–12.4) | 1.16x |  |
| multi client put gigabytes | 13.07 GB/s (12.07 GB/s–24.30 GB/s) | 8.51 GB/s (8.38 GB/s–13.29 GB/s) | 0.65x | identical zero arrays: hashed every put, written once per run |
| single client get object containing 10k refs | 24.5 (23.8–28.4) | 5,991,246 (4,997,525–6,698,972) | 244646.19x | .result() decodes once and caches on the future; see the uncached row |
| single client get object containing 10k refs (uncached handle) | - | 110 (106–134) | - | supplemental: a fresh handle per get (private ch._future._future_from_key) so each get asks the peer and decodes 10k handles |
| single client wait 1k refs | 5.8 (5.2–7.6) | 13.0 (8.4–18.2) | 2.24x | no ch.wait: 1000 next() calls on one ch.as_completed over the shrinking set |
| single client tasks sync | 2,238 (2,120–2,384) | 1,419 (1,077–1,448) | 0.63x |  |
| single client tasks async | 10,416 (8,215–12,227) | 15,362 (9,830–15,747) | 1.47x |  |
| multi client tasks async | 13,552 (12,564–14,971) | 13,364 (5,282–14,758) | 0.99x |  |
| 1:1 actor calls sync | 4,968 (4,324–5,593) | 1,154 (518–1,260) | 0.23x |  |
| 1:1 actor calls async | 16,573 (12,610–17,306) | 5,471 (3,359–10,840) | 0.33x |  |
| 1:1 actor calls concurrent | 16,513 (12,085–16,551) | 3,891 (3,111–7,331) | 0.24x |  |
| 1:n actor calls async | 15,087 (12,834–19,388) | 7,507 (6,190–8,779) | 0.50x |  |
| n:n actor calls async | 24,681 (20,294–27,642) | 10,489 (4,344–10,989) | 0.43x |  |
| n:n actor calls with arg async | 9,167 (6,381–13,021) | 7,608 (7,348–9,378) | 0.83x |  |
| 1:1 async-actor calls sync | 2,997 (2,441–3,896) | 1,012 (475–1,151) | 0.34x | concurrency=1000, Ray's async-actor default |
| 1:1 async-actor calls async | 9,865 (4,424–11,485) | 4,496 (3,310–5,642) | 0.46x | concurrency=1000, Ray's async-actor default |
| 1:1 async-actor calls with args async | 6,429 (3,622–6,657) | 4,041 (2,900–6,750) | 0.63x | concurrency=1000, Ray's async-actor default |
| 1:n async-actor calls async | 13,305 (13,211–16,090) | 3,247 (2,881–3,678) | 0.24x | concurrency=1000, Ray's async-actor default |
| n:n async-actor calls async | 17,576 (16,825–17,872) | 5,273 (4,162–7,710) | 0.30x | concurrency=1000, Ray's async-actor default |
| placement group create/removal | 516 (445–678) | - | - | unsupported: Chimaera has no placement groups or reservations (placement= is P3, reservations P5) |
| client: get calls | 2,905 (1,627–3,363) | 7,619,805 (6,278,495–7,983,445) | 2623.33x | remote driver over TCP (--client-listen); value cached on the future |
| client: put calls | 1,800 (1,464–2,694) | 281,583 (141,348–286,571) | 156.41x | remote driver: small values inline, no IPC |
| client: put gigabytes | 0.35 GB/s (0.34 GB/s–0.37 GB/s) | 13.31 GB/s (11.66 GB/s–15.39 GB/s) | 38.51x | remote driver: content-addressed, uploaded once, hashed every put |
| client: tasks and put batch | 23,214 (16,661–25,835) | 87,195 (75,151–108,126) | 3.76x | remote driver: small values inline, no IPC |
| client: 1:1 actor calls sync | 1,439 (1,056–1,646) | 869 (663–890) | 0.60x |  |
| client: 1:1 actor calls async | 2,014 (1,885–2,895) | 9,150 (7,031–9,441) | 4.54x |  |
| client: 1:1 actor calls concurrent | 2,753 (2,727–2,805) | 7,131 (4,009–7,171) | 2.59x |  |
| client: tasks and get batch | 2.1 (1.6–2.7) | 11.9 (6.8–13.2) | 5.80x |  |

## Runs

| runtime | round | load avg before (1/5/15 min) | after | seconds | exit | snapshots pruned |
|---|---:|---|---|---:|---|---:|
| ray | 0 | 19.45 26.81 24.28 | 10.17 14.43 18.83 | 522.7 | 0 | - |
| chimaera | 0 | 10.17 14.43 18.83 | 7.09 11.19 15.67 | 470.4 | 0 | 4.7 GiB |
| chimaera | 1 | 6.84 11.07 15.60 | 7.75 12.37 14.80 | 512.8 | 0 | 3.7 GiB |
| ray | 1 | 7.37 12.22 14.73 | 4.04 10.80 13.14 | 822.2 | 1 (ConnectionError: Failed to connect to Ray cluster at 127.0.0.1:59639) | - |
| ray | 2 | 4.04 10.80 13.14 | 11.16 11.15 12.31 | 496.7 | 0 | - |
| chimaera | 2 | 11.16 11.15 12.31 | 17.45 12.97 11.95 | 540.5 | 0 | 5.5 GiB |
| ray | 3 | 13.99 12.49 11.80 | 7.76 9.83 10.89 | 504.2 | 0 | - |

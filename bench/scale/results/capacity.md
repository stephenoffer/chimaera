Assumptions: cell_size=512, slots_per_peer=32, occupancy=0.7, service_coarse_s=0.25, service_fine_s=0.005, service_tiny_s=0.001, agent_us_per_call=30.0, journal_bytes_per_call=256, journal_backups=2, journal_retain_s=60.0, swim_period_s=1.0, swim_msgs_per_period=2.2, swim_msg_bytes=512, swim_suspicion_mult=4.0, member_record_bytes=96, stream_silence_s=1.0, confirm_probe_s=0.5, summary_bytes=4096, summary_period_s=2.0, metadata_ops_per_peer_s=5.0, group_ops_budget_s=2000.0, min_groups_per_cell=8, voters=3, raft_tick_s=0.1, active_group_fraction=0.25, global_lookups_per_peer_s=0.05, global_group_ops_budget_s=2000.0, attempt_record_bytes=160, record_sample_fraction=0.02, sketch_bytes_per_fn_minute=2048, functions_per_peer=200, telemetry_window_s=900.0, agent_core_budget=0.5, peer_net_budget_Bps=31250000.0, peer_mem_budget_B=536870912, ambassador_net_budget_Bps=10485760

### coarse: 250 ms calls, 32 slots per peer, 70% busy

| peers | cells | fleet calls/s | calls/s per peer | agent cores | control MiB/s per peer | control MiB per peer | owner loss detected s | SWIM suspicion s | metadata groups per cell | summaries MiB/s per ambassador | global directory ops/s | over budget |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 1 | 89.6 | 89.6 | 0.003 | 0.00 | 7.4 | 0.0 | 0.0 | 1 | 0.00 | 0 | none |
| 64 | 1 | 5.73k | 89.6 | 0.003 | 0.09 | 10.0 | 1.5 | 7.2 | 8 | 0.00 | 0 | none |
| 1,024 | 2 | 91.75k | 89.6 | 0.003 | 0.09 | 10.1 | 1.5 | 10.8 | 8 | 0.00 | 51.2 | none |
| 10,240 | 20 | 917.50k | 89.6 | 0.003 | 0.09 | 10.1 | 1.5 | 10.8 | 8 | 0.04 | 512 | none |
| 102,400 | 200 | 9.18M | 89.6 | 0.003 | 0.09 | 10.1 | 1.5 | 10.8 | 8 | 0.39 | 5.12k | none |
| 262,144 | 512 | 23.49M | 89.6 | 0.003 | 0.09 | 10.1 | 1.5 | 10.8 | 8 | 1.00 | 13.11k | none |

### fine: 5 ms calls, 32 slots per peer, 70% busy

| peers | cells | fleet calls/s | calls/s per peer | agent cores | control MiB/s per peer | control MiB per peer | owner loss detected s | SWIM suspicion s | metadata groups per cell | summaries MiB/s per ambassador | global directory ops/s | over budget |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 1 | 4.48k | 4.48k | 0.134 | 0.00 | 83.8 | 0.0 | 0.0 | 1 | 0.00 | 0 | none |
| 64 | 1 | 286.72k | 4.48k | 0.134 | 4.38 | 215.0 | 1.5 | 7.2 | 8 | 0.00 | 0 | none |
| 1,024 | 2 | 4.59M | 4.48k | 0.134 | 4.38 | 215.1 | 1.5 | 10.8 | 8 | 0.00 | 51.2 | none |
| 10,240 | 20 | 45.88M | 4.48k | 0.134 | 4.38 | 215.1 | 1.5 | 10.8 | 8 | 0.04 | 512 | none |
| 102,400 | 200 | 458.75M | 4.48k | 0.134 | 4.38 | 215.1 | 1.5 | 10.8 | 8 | 0.39 | 5.12k | none |
| 262,144 | 512 | 1.17G | 4.48k | 0.134 | 4.38 | 215.1 | 1.5 | 10.8 | 8 | 1.00 | 13.11k | none |

### tiny: 1 ms calls, 32 slots per peer, 70% busy

| peers | cells | fleet calls/s | calls/s per peer | agent cores | control MiB/s per peer | control MiB per peer | owner loss detected s | SWIM suspicion s | metadata groups per cell | summaries MiB/s per ambassador | global directory ops/s | over budget |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 1 | 22.40k | 22.40k | 0.672 | 0.00 | 395.5 | 0.0 | 0.0 | 1 | 0.00 | 0 | agent CPU 0.67 cores |
| 64 | 1 | 1.43M | 22.40k | 0.672 | 21.88 | 1051.8 | 1.5 | 7.2 | 8 | 0.00 | 0 | agent CPU 0.67 cores; peer control memory 1052 MiB |
| 1,024 | 2 | 22.94M | 22.40k | 0.672 | 21.88 | 1051.8 | 1.5 | 10.8 | 8 | 0.00 | 51.2 | agent CPU 0.67 cores; peer control memory 1052 MiB |
| 10,240 | 20 | 229.38M | 22.40k | 0.672 | 21.88 | 1051.8 | 1.5 | 10.8 | 8 | 0.04 | 512 | agent CPU 0.67 cores; peer control memory 1052 MiB |
| 102,400 | 200 | 2.29G | 22.40k | 0.672 | 21.88 | 1051.8 | 1.5 | 10.8 | 8 | 0.39 | 5.12k | agent CPU 0.67 cores; peer control memory 1052 MiB |
| 262,144 | 512 | 5.87G | 22.40k | 0.672 | 21.88 | 1051.8 | 1.5 | 10.8 | 8 | 1.00 | 13.11k | agent CPU 0.67 cores; peer control memory 1052 MiB |

| p | reps | TMs | input achieved el/s | throughput (source out) el/s | sink in el/s | latency p50 ms (max / median over ops) | latency p99 ms (max / median) | TM CPU cores | CPU util of used TMs | Interpolate busy max ms/s | cost $/h |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 3 | 1 | 2734 ± 6 | 1337 ± 12 | 1338 ± 11 | 112 ± 10 / 104 ± 2 | 849 ± 141 / 367 ± 152 | 1.10 ± 0.00 | 1.10 ± 0.00 | 1000 ± 0 | 0.192 |
| 2 | 3 | 1 | 2733 ± 8 | 1296 ± 14 | 1297 ± 12 | 136 ± 15 / 79 ± 3 | 4243 ± 2077 / 525 ± 508 | 1.12 ± 0.00 | 1.12 ± 0.00 | 1000 ± 0 | 0.192 |
| 4 | 3 | 2 | 2735 ± 4 | 1528 ± 365 | 1530 ± 367 | 177 ± 68 / 43 ± 24 | 4034 ± 1698 / 98 ± 3 | 1.28 ± 0.28 | 0.64 ± 0.14 | 1000 ± 1 | 0.384 |
| 8 | 3 | 4 | 2729 ± 4 | 1959 ± 16 | 1961 ± 17 | 2760 ± 1670 / 59 ± 1 | 5847 ± 1667 / 98 ± 1 | 1.62 ± 0.01 | 0.40 ± 0.00 | 999 ± 1 | 0.768 |

FD (p=1) vs FMAX (p=8): throughput -31.7% (paper FIT: FD 34% lower than STREAMLINE, FMAX within 1% of STREAMLINE => FD ≈ -34% vs FMAX); cost FMAX/FD = 4.0x; CPU utilisation of used TMs FD 1.10 vs FMAX 0.40.

### Placement of the two hot subtasks (active source subtask, busiest Interpolate subtask)

| run | p | hot subtasks on same TM | throughput el/s | busiest Interpolate subtask ms/s | TMs used |
|---|---|---|---|---|---|
| p1_r3500_rep1 | 1 | yes | 1325 | 1000 | 1 |
| p1_r3500_rep2 | 1 | yes | 1337 | 1000 | 1 |
| p1_r3500_rep3 | 1 | yes | 1349 | 1000 | 1 |
| p2_r3500_rep1 | 2 | yes | 1295 | 1000 | 1 |
| p2_r3500_rep2 | 2 | yes | 1283 | 1000 | 1 |
| p2_r3500_rep3 | 2 | yes | 1311 | 1000 | 1 |
| p4_r3500_rep1 | 4 | yes | 1288 | 1000 | 2 |
| p4_r3500_rep2 | 4 | yes | 1348 | 1000 | 2 |
| p4_r3500_rep3 | 4 | no | 1948 | 999 | 2 |
| p8_r3500_rep1 | 8 | no | 1969 | 1000 | 4 |
| p8_r3500_rep2 | 8 | no | 1967 | 1000 | 4 |
| p8_r3500_rep3 | 8 | no | 1940 | 1000 | 4 |

p>2, hot subtasks co-located: throughput 1318 el/s (n=2)
p>2, hot subtasks separated: throughput 1956 el/s (n=4)

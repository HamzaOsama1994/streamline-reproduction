| p | reps | TMs | input achieved el/s | throughput (source out) el/s | sink in el/s | latency p50 ms (max / median over ops) | latency p99 ms (max / median) | TM CPU cores | CPU util of used TMs | Interpolate busy max ms/s | cost $/h |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 3 | 1 | 2636 ± 12 | 1115 ± 32 | 1115 ± 32 | 103 ± 4 / 98 ± 1 | 1401 ± 1319 / 636 ± 825 | 1.06 ± 0.02 | 1.06 ± 0.02 | 1000 ± 0 | 0.192 |
| 2 | 3 | 1 | 2641 ± 6 | 1100 ± 30 | 1100 ± 30 | 108 ± 6 / 53 ± 13 | 2317 ± 422 / 106 ± 10 | 1.06 ± 0.03 | 1.06 ± 0.03 | 1000 ± 0 | 0.192 |
| 4 | 3 | 2 | 2631 ± 7 | 1478 ± 344 | 1479 ± 344 | 153 ± 31 / 45 ± 2 | 2518 ± 374 / 101 ± 12 | 1.43 ± 0.29 | 0.71 ± 0.15 | 1000 ± 0 | 0.384 |
| 8 | 3 | 4 | 2620 ± 15 | 1473 ± 314 | 1474 ± 314 | 378 ± 105 / 47 ± 6 | 2736 ± 377 / 88 ± 10 | 1.45 ± 0.31 | 0.36 ± 0.08 | 1000 ± 0 | 0.768 |

FD (p=1) vs FMAX (p=8): throughput -24.3% (paper FIT: FD 34% lower than STREAMLINE, FMAX within 1% of STREAMLINE => FD ≈ -34% vs FMAX); cost FMAX/FD = 4.0x; CPU utilisation of used TMs FD 1.06 vs FMAX 0.36.

### Placement of the two hot subtasks (active source subtask, busiest Interpolate subtask)

| run | p | hot subtasks on same TM | throughput el/s | busiest Interpolate subtask ms/s | TMs used |
|---|---|---|---|---|---|
| p1_r4000_rep1 | 1 | yes | 1079 | 1000 | 1 |
| p1_r4000_rep2 | 1 | yes | 1141 | 1000 | 1 |
| p1_r4000_rep3 | 1 | yes | 1125 | 1000 | 1 |
| p2_r4000_rep1 | 2 | yes | 1067 | 1000 | 1 |
| p2_r4000_rep2 | 2 | yes | 1126 | 1000 | 1 |
| p2_r4000_rep3 | 2 | yes | 1105 | 1000 | 1 |
| p4_r4000_rep1 | 4 | yes | 1081 | 1000 | 2 |
| p4_r4000_rep2 | 4 | no | 1684 | 1000 | 2 |
| p4_r4000_rep3 | 4 | no | 1669 | 1000 | 2 |
| p8_r4000_rep1 | 8 | no | 1630 | 1000 | 4 |
| p8_r4000_rep2 | 8 | yes | 1111 | 1000 | 4 |
| p8_r4000_rep3 | 8 | no | 1677 | 1000 | 4 |

p>2, hot subtasks co-located: throughput 1096 el/s (n=2)
p>2, hot subtasks separated: throughput 1665 el/s (n=4)

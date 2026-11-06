| p | reps | TMs | input achieved el/s | throughput (source out) el/s | sink in el/s | latency p50 ms (max / median over ops) | latency p99 ms (max / median) | TM CPU cores | CPU util of used TMs | Interpolate busy max ms/s | cost $/h |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 3 | 1 | 756 ± 3 | 688 ± 12 | 697 ± 9 | 181 ± 26 / 158 ± 20 | 1705 ± 261 / 401 ± 109 | 1.06 ± 0.01 | 1.06 ± 0.01 | 987 ± 5 | 0.192 |
| 2 | 3 | 1 | 758 ± 5 | 684 ± 7 | 692 ± 9 | 217 ± 92 / 39 ± 10 | 2499 ± 203 / 603 ± 560 | 1.06 ± 0.01 | 1.06 ± 0.01 | 986 ± 5 | 0.192 |
| 4 | 4 | 2 | 754 ± 5 | 785 ± 85 | 792 ± 89 | 392 ± 152 / 51 ± 13 | 4526 ± 2854 / 84 ± 11 | 1.24 ± 0.13 | 0.62 ± 0.06 | 744 ± 170 | 0.384 |
| 8 | 2 | 4 | 747 ± 7 | 834 ± 14 | 833 ± 17 | 6066 ± 2082 / 52 ± 15 | 14887 ± 1837 / 90 ± 3 | 1.36 ± 0.09 | 0.34 ± 0.02 | 695 ± 71 | 0.768 |

FD (p=1) vs FMAX (p=8): throughput -17.5% (paper FIT: FD 34% lower than STREAMLINE, FMAX within 1% of STREAMLINE => FD ≈ -34% vs FMAX); cost FMAX/FD = 4.0x; CPU utilisation of used TMs FD 1.06 vs FMAX 0.34.

### Placement of the two hot subtasks (active source subtask, busiest Interpolate subtask)

| run | p | hot subtasks on same TM | throughput el/s | busiest Interpolate subtask ms/s | TMs used |
|---|---|---|---|---|---|
| p4_r1000_rep4 | 4 | no | 826 | 507 | 2 |
| p8_r1000_rep3 | 8 | no | 824 | 450 | 4 |

p>2, hot subtasks separated: throughput 825 el/s (n=2)

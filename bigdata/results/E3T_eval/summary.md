## Per configuration (true end-to-end latency of records entering in the horizon)

| config | runs | TMs | cost $/h | e2e p50 ms | p95 ms | p99 ms | mean ms | max ms | TM CPU cores | records processed / produced |
|---|---|---|---|---|---|---|---|---|---|---|
| p1_tm1_seg32768 | 3 | 1 | 0.192 | 8383 ± 836 | 18839 ± 1219 | 19099 ± 1297 | 8404 ± 681 | 19242 ± 1308 | 0.96 ± 0.00 | 1.000 ± 0.000 |
| p6_tm3_seg16384 | 3 | 3 | 0.576 | 54 ± 1 | 152 ± 5 | 432 ± 105 | 69 ± 3 | 691 ± 82 | 1.09 ± 0.01 | 1.000 ± 0.000 |
| p7_tm4_seg16384 | 3 | 4 | 0.768 | 54 ± 2 | 152 ± 17 | 504 ± 283 | 71 ± 10 | 758 ± 328 | 1.12 ± 0.01 | 1.000 ± 0.000 |
| p8_tm4_seg32768 | 3 | 4 | 0.768 | 7671 ± 6847 | 14917 ± 12957 | 15113 ± 12921 | 7265 ± 6414 | 15307 ± 12829 | 1.07 ± 0.02 | 1.000 ± 0.000 |

Per run:

| run | e2e p50 | p95 | p99 | max | order match (3 slices) |
|---|---|---|---|---|---|
| p1_tm1_seg32768_rep1 | 9198 | 20161 | 20517 | 20679 | [1.0, 1.0, 1.0] |
| p1_tm1_seg32768_rep2 | 7527 | 17760 | 17971 | 18122 | [1.0, 1.0, 1.0] |
| p1_tm1_seg32768_rep3 | 8423 | 18596 | 18810 | 18924 | [1.0, 1.0, 1.0] |
| p6_tm3_seg16384_rep1 | 55 | 157 | 518 | 729 | [1.0, 1.0, 1.0] |
| p6_tm3_seg16384_rep2 | 53 | 147 | 462 | 747 | [1.0, 1.0, 1.0] |
| p6_tm3_seg16384_rep3 | 55 | 152 | 315 | 596 | [1.0, 1.0, 1.0] |
| p7_tm4_seg16384_rep1 | 56 | 166 | 666 | 900 | [1.0, 1.0, 1.0] |
| p7_tm4_seg16384_rep2 | 52 | 134 | 177 | 383 | [1.0, 1.0, 1.0] |
| p7_tm4_seg16384_rep3 | 54 | 157 | 669 | 991 | [1.0, 1.0, 1.0] |
| p8_tm4_seg32768_rep1 | 13318 | 24422 | 24602 | 24743 | [1.0, 1.0, 1.0] |
| p8_tm4_seg32768_rep2 | 56 | 158 | 397 | 699 | [1.0, 1.0, 1.0] |
| p8_tm4_seg32768_rep3 | 9640 | 20171 | 20340 | 20478 | [1.0, 1.0, 1.0] |

## Variants (expected outcome over seeds = frequency-weighted mean of the measured configurations)

| variant | seeds ok | configurations chosen (count) | E[cost $/h] | E[e2e p50 ms] | E[p95 ms] | E[p99 ms] | E[mean ms] |
|---|---|---|---|---|---|---|---|
| V0_original | 11/11 | {'p7_tm4_seg16384': 8, 'p6_tm3_seg16384': 3} | 0.716 | 54 | 152 | 484 | 71 |
| V4_corrected | 11/11 | {'p6_tm3_seg16384': 11} | 0.576 | 54 | 152 | 432 | 69 |

measured ingestion (broker timestamps, mean of runs): mean 1270 el/s vs schedule x 0.8: 1278 el/s

## Workload forecast vs actual workload (horizon 150-400 s, 50 x 5 s; actual = replay schedule x 0.8)

actual: mean 1278, min 279, max 1752 el/s

| variant | seed | forecast mean | MAE el/s | MAPE % | bias el/s | negative values |
|---|---|---|---|---|---|---|
| V0_original | orig | 1042 | 288 | 24.3 | -236 | 1 |
| V0_original | 0 | 1032 | 301 | 25.3 | -246 | 0 |
| V0_original | 1 | 1057 | 261 | 21.3 | -221 | 0 |
| V0_original | 2 | 1033 | 303 | 24.1 | -245 | 0 |
| V0_original | 3 | 1051 | 280 | 22.4 | -228 | 0 |
| V0_original | 4 | 981 | 345 | 28.4 | -297 | 0 |
| V0_original | 5 | 1040 | 307 | 24.5 | -238 | 0 |
| V0_original | 6 | 1032 | 289 | 23.2 | -246 | 0 |
| V0_original | 7 | 1050 | 286 | 23.1 | -228 | 0 |
| V0_original | 8 | 1005 | 312 | 26.5 | -273 | 1 |
| V0_original | 9 | 1018 | 301 | 25.1 | -261 | 0 |
| V4_corrected | orig | 1090 | 210 | 20.6 | -188 | 0 |
| V4_corrected | 0 | 1090 | 210 | 20.6 | -188 | 0 |
| V4_corrected | 1 | 1090 | 210 | 20.6 | -188 | 0 |
| V4_corrected | 2 | 1090 | 210 | 20.6 | -188 | 0 |
| V4_corrected | 3 | 1090 | 210 | 20.6 | -188 | 0 |
| V4_corrected | 4 | 1090 | 210 | 20.6 | -188 | 0 |
| V4_corrected | 5 | 1090 | 210 | 20.6 | -188 | 0 |
| V4_corrected | 6 | 1090 | 210 | 20.6 | -188 | 0 |
| V4_corrected | 7 | 1090 | 210 | 20.6 | -188 | 0 |
| V4_corrected | 8 | 1090 | 210 | 20.6 | -188 | 0 |
| V4_corrected | 9 | 1090 | 210 | 20.6 | -188 | 0 |

| variant | MAE mean ± sd over seeds | MAPE % | bias |
|---|---|---|---|
| V0_original | 297 ± 21 | 24.4 ± 2.0 | -247 ± 22 |
| V4_corrected | 210 ± 0 | 20.6 ± 0.0 | -188 ± 0 |

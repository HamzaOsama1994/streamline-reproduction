## Per configuration (true end-to-end latency of records entering in the horizon)

| config | runs | TMs | cost $/h | e2e p50 ms | p95 ms | p99 ms | mean ms | max ms | TM CPU cores | records processed / produced |
|---|---|---|---|---|---|---|---|---|---|---|
| p1_tm1_seg32768 | 3 | 1 | 0.192 | 18624 ± 2793 | 35144 ± 4301 | 35342 ± 4303 | 17865 ± 2540 | 35497 ± 4242 | 0.92 ± 0.02 | 1.000 ± 0.000 |
| p7_tm4_seg65536 | 3 | 4 | 0.768 | 99 ± 31 | 1468 ± 1674 | 1705 ± 1752 | 364 ± 345 | 1875 ± 1735 | 1.16 ± 0.06 | 1.000 ± 0.000 |
| p8_tm4_seg32768 | 3 | 4 | 0.768 | 7066 ± 12131 | 13688 ± 22848 | 13813 ± 22890 | 6893 ± 11732 | 13903 ± 22888 | 1.09 ± 0.03 | 1.000 ± 0.000 |
| p8_tm4_seg65536 | 3 | 4 | 0.768 | 176 ± 194 | 1405 ± 1969 | 2057 ± 2426 | 419 ± 563 | 2378 ± 2356 | 1.10 ± 0.02 | 1.000 ± 0.000 |

Per run:

| run | e2e p50 | p95 | p99 | max | order match (3 slices) |
|---|---|---|---|---|---|
| p1_tm1_seg32768_rep1 | 17228 | 33109 | 33262 | 33477 | [1.0, 1.0, 1.0] |
| p1_tm1_seg32768_rep2 | 21839 | 40085 | 40290 | 40372 | [1.0, 1.0, 1.0] |
| p1_tm1_seg32768_rep3 | 16804 | 32239 | 32474 | 32642 | [1.0, 1.0, 1.0] |
| p7_tm4_seg65536_rep1 | 117 | 809 | 1023 | 1302 | [1.0, 1.0, 1.0] |
| p7_tm4_seg65536_rep2 | 118 | 3372 | 3695 | 3824 | [1.0, 1.0, 1.0] |
| p7_tm4_seg65536_rep3 | 63 | 224 | 397 | 498 | [1.0, 1.0, 1.0] |
| p8_tm4_seg32768_rep1 | 62 | 637 | 750 | 847 | [1.0, 1.0, 1.0] |
| p8_tm4_seg32768_rep2 | 63 | 356 | 446 | 530 | [1.0, 1.0, 1.0] |
| p8_tm4_seg32768_rep3 | 21074 | 40070 | 40244 | 40331 | [1.0, 1.0, 1.0] |
| p8_tm4_seg65536_rep1 | 61 | 167 | 330 | 643 | [1.0, 1.0, 1.0] |
| p8_tm4_seg65536_rep2 | 400 | 3676 | 4831 | 5060 | [1.0, 1.0, 1.0] |
| p8_tm4_seg65536_rep3 | 66 | 373 | 1010 | 1430 | [1.0, 1.0, 1.0] |

## Variants (expected outcome over seeds = frequency-weighted mean of the measured configurations)

| variant | seeds ok | configurations chosen (count) | E[cost $/h] | E[e2e p50 ms] | E[p95 ms] | E[p99 ms] | E[mean ms] |
|---|---|---|---|---|---|---|---|
| V0_original | 11/11 | {'p8_tm4_seg65536': 1, 'p7_tm4_seg65536': 10} | 0.768 | 106 | 1463 | 1737 | 369 |
| V4_corrected | 11/11 | {'p8_tm4_seg65536': 11} | 0.768 | 176 | 1405 | 2057 | 419 |

measured ingestion (broker timestamps, mean of runs): mean 960 el/s vs schedule x 0.8: 967 el/s

## Workload forecast vs actual workload (horizon 150-400 s, 50 x 5 s; actual = replay schedule x 0.8)

actual: mean 967, min 201, max 1599 el/s

| variant | seed | forecast mean | MAE el/s | MAPE % | bias el/s | negative values |
|---|---|---|---|---|---|---|
| V0_original | orig | 919 | 115 | 21.9 | -48 | 0 |
| V0_original | 0 | 931 | 106 | 19.4 | -36 | 2 |
| V0_original | 1 | 916 | 137 | 21.0 | -51 | 1 |
| V0_original | 2 | 918 | 105 | 17.5 | -49 | 1 |
| V0_original | 3 | 948 | 103 | 17.5 | -18 | 0 |
| V0_original | 4 | 911 | 105 | 21.0 | -56 | 2 |
| V0_original | 5 | 926 | 113 | 18.9 | -41 | 1 |
| V0_original | 6 | 924 | 119 | 21.6 | -43 | 0 |
| V0_original | 7 | 929 | 132 | 24.9 | -38 | 1 |
| V0_original | 8 | 919 | 116 | 18.6 | -48 | 0 |
| V0_original | 9 | 938 | 111 | 17.6 | -29 | 0 |
| V4_corrected | orig | 977 | 41 | 6.2 | +10 | 0 |
| V4_corrected | 0 | 977 | 41 | 6.2 | +10 | 0 |
| V4_corrected | 1 | 977 | 41 | 6.2 | +10 | 0 |
| V4_corrected | 2 | 977 | 41 | 6.2 | +10 | 0 |
| V4_corrected | 3 | 977 | 41 | 6.2 | +10 | 0 |
| V4_corrected | 4 | 977 | 41 | 6.2 | +10 | 0 |
| V4_corrected | 5 | 977 | 41 | 6.2 | +10 | 0 |
| V4_corrected | 6 | 977 | 41 | 6.2 | +10 | 0 |
| V4_corrected | 7 | 977 | 41 | 6.2 | +10 | 0 |
| V4_corrected | 8 | 977 | 41 | 6.2 | +10 | 0 |
| V4_corrected | 9 | 977 | 41 | 6.2 | +10 | 0 |

| variant | MAE mean ± sd over seeds | MAPE % | bias |
|---|---|---|---|
| V0_original | 115 ± 11 | 20.0 ± 2.3 | -42 ± 11 |
| V4_corrected | 41 ± 0 | 6.2 ± 0.0 | 10 ± 0 |

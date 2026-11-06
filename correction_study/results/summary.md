# Correction study — results

Runs: 88 ({'V0_original': 11, 'V1_key': 11, 'V2_eval': 11, 'V3_scaling': 11, 'V4_all': 11, 'V5_key+eval': 11, 'V6_key+scaling': 11, 'V7_eval+scaling': 11}). Values are mean ± SD across seeds (seed 'orig' + seeds 0–9). Same checkpoints, workloads, resources and seed set for every variant.

## 1. Forecast accuracy (source operators, horizon 50 steps, events/s)

| Variant | K D S | MAE init p1 | MAE init p2 | MAE periodic p1 | MAE periodic p2 | mean MAE | mean RMSE | negative share | out-of-range share |
|---|---|---|---|---|---|---|---|---|---|
| V0_original | 0 0 0 | 122.6 ± 16.8 | 112.5 ± 19.0 | 151.4 ± 21.7 | 885.1 ± 30.5 | 317.9 ± 12.0 | 364.9 ± 12.7 | 0.115 ± 0.017 | 0.267 ± 0.034 |
| V1_key | 1 0 0 | 122.6 ± 16.8 | 112.5 ± 19.0 | 151.4 ± 21.7 | 128.1 ± 15.4 | 128.6 ± 9.1 | 157.5 ± 10.1 | 0.145 ± 0.023 | 0.281 ± 0.025 |
| V2_eval | 0 1 0 | 96.0 ± 0.0 | 111.9 ± 0.0 | 151.3 ± 0.0 | 900.7 ± 0.0 | 314.9 ± 0.0 | 354.7 ± 0.0 | 0.130 ± 0.000 | 0.315 ± 0.000 |
| V3_scaling | 0 0 1 | 68.7 ± 8.8 | 70.0 ± 12.2 | 67.2 ± 9.2 | 879.4 ± 31.5 | 271.3 ± 9.4 | 307.5 ± 9.5 | 0.021 ± 0.010 | 0.099 ± 0.029 |
| V4_all | 1 1 1 | 27.6 ± 0.0 | 24.5 ± 0.0 | 22.9 ± 0.0 | 18.6 ± 0.0 | 23.4 ± 0.0 | 27.8 ± 0.0 | 0.000 ± 0.000 | 0.150 ± 0.000 |
| V5_key+eval | 1 1 0 | 96.0 ± 0.0 | 111.9 ± 0.0 | 151.3 ± 0.0 | 121.6 ± 0.0 | 120.2 ± 0.0 | 140.2 ± 0.0 | 0.170 ± 0.000 | 0.325 ± 0.000 |
| V6_key+scaling | 1 0 1 | 68.7 ± 8.8 | 70.0 ± 12.2 | 67.2 ± 9.2 | 69.8 ± 11.1 | 68.9 ± 5.2 | 84.9 ± 5.9 | 0.025 ± 0.010 | 0.100 ± 0.026 |
| V7_eval+scaling | 0 1 1 | 27.6 ± 0.0 | 24.5 ± 0.0 | 22.9 ± 0.0 | 900.5 ± 0.0 | 243.9 ± 0.0 | 271.5 ± 0.0 | 0.000 ± 0.000 | 0.165 ± 0.000 |

Pipeline-2 periodic input window mean (shows whether the new workload reached the predictor): V0_original: 1026.9, V1_key: 1025.0, V2_eval: 1026.9, V3_scaling: 1026.9, V4_all: 1025.0, V5_key+eval: 1025.0, V6_key+scaling: 1025.0, V7_eval+scaling: 1026.9

## 2. Decisions: parallelism, placement, cost

Parallelism vector order: p1 v_0,v_1,v_2,v_3 | p2 v_5,v_6,v_7. Cost = sum of $/h of resources with load > 0 (examples/utils.py prices).

| Variant | initial parallelism (mode) | periodic parallelism (mode) | segment sizes | resources after periodic | distinct periodic placements | cost initial $/h | cost periodic $/h | reschedules |
|---|---|---|---|---|---|---|---|---|
| V0_original | 4-4-4-4-5-5-5 (7/11); 3 distinct | 2-1-2-4-5-5-5 (5/11); 5 distinct | 16384 (11/11) | 2.18 ± 0.60 | 11 | 0.890 ± 0.097 | 0.733 ± 0.168 | 0.00 ± 0.00 |
| V1_key | 4-4-4-4-5-5-5 (7/11); 3 distinct | 2-1-2-4-2-2-2 (6/11); 3 distinct | 16384 (11/11) | 1.55 ± 0.93 | 11 | 0.890 ± 0.097 | 0.489 ± 0.179 | 0.00 ± 0.00 |
| V2_eval | 4-4-4-4-5-5-5 (11/11) | 2-1-2-4-5-5-5 (11/11) | 16384 (11/11) | 2.00 ± 0.00 | 1 | 0.960 ± 0.000 | 0.768 ± 0.000 | 0.00 ± 0.00 |
| V3_scaling | 3-2-2-2-5-5-5 (5/11); 4 distinct | 2-1-1-4-5-5-5 (7/11); 3 distinct | 16384 (11/11) | 2.18 ± 0.40 | 11 | 0.751 ± 0.135 | 0.698 ± 0.155 | 0.00 ± 0.00 |
| V4_all | 3-3-2-2-5-5-5 (11/11) | 2-1-1-4-2-2-2 (11/11) | 16384 (11/11) | 1.00 ± 0.00 | 1 | 0.768 ± 0.000 | 0.384 ± 0.000 | 0.00 ± 0.00 |
| V5_key+eval | 4-4-4-4-5-5-5 (11/11) | 2-1-2-4-2-2-2 (11/11) | 16384 (11/11) | 1.00 ± 0.00 | 1 | 0.960 ± 0.000 | 0.384 ± 0.000 | 0.00 ± 0.00 |
| V6_key+scaling | 3-2-2-2-5-5-5 (5/11); 4 distinct | 2-1-1-4-2-2-2 (11/11) | 16384 (11/11) | 1.27 ± 0.47 | 11 | 0.751 ± 0.135 | 0.436 ± 0.090 | 0.00 ± 0.00 |
| V7_eval+scaling | 3-3-2-2-5-5-5 (11/11) | 2-1-1-4-5-5-5 (11/11) | 16384 (11/11) | 2.00 ± 0.00 | 1 | 0.768 ± 0.000 | 0.576 ± 0.000 | 0.00 ± 0.00 |

## 3. Optimizer execution time

Uncontended sequential pass (seed 'orig', 1 process, all threads), NSGA-II `solve()` wall time summed over the 14 operator optimisations:

| Variant | optimizer s (total) | per-operator s (mean ± SD) | tuning total s |
|---|---|---|---|
| V0_original | 152.9 | 10.92 ± 0.87 | 153.4 |
| V1_key | 160.0 | 11.43 ± 0.91 | 160.4 |
| V2_eval | 150.1 | 10.72 ± 0.24 | 150.5 |
| V3_scaling | 143.0 | 10.21 ± 0.12 | 143.4 |
| V4_all | 145.9 | 10.42 ± 0.25 | 146.3 |
| V5_key+eval | 144.6 | 10.33 ± 0.13 | 145.0 |
| V6_key+scaling | 144.9 | 10.35 ± 0.30 | 145.3 |
| V7_eval+scaling | 145.8 | 10.42 ± 0.20 | 146.2 |

Contended batch (8 parallel processes × 2 threads; comparable between variants only), median optimizer seconds per run (runs whose timing spans the laptop's sleep period are excluded: V7_eval+scaling/3, V7_eval+scaling/4, V7_eval+scaling/5, V7_eval+scaling/6, V7_eval+scaling/7, V7_eval+scaling/8, V7_eval+scaling/9): V0_original: 171s (n=11), V1_key: 171s (n=11), V2_eval: 172s (n=11), V3_scaling: 159s (n=11), V4_all: 142s (n=11), V5_key+eval: 150s (n=11), V6_key+scaling: 146s (n=11), V7_eval+scaling: 146s (n=4)

## 4. Factorial effects (2³, paired by seed)

Effect = mean(outcome | factor on) − mean(outcome | factor off), averaged over the other factors; interactions use the standard ±1 contrast. Computed per seed, then mean ± 95% CI over 11 seeds. An interaction whose CI excludes 0 means the fixes are **not additive**.

**MAE periodic p2:** K -806.868 [-816.856, -796.880] *; D -5.279 [-14.057, +3.499]; S -41.787 [-45.083, -38.492] *; KxD -23.635 [-33.623, -13.648] *; KxS -38.834 [-42.260, -35.407] *; DxS -9.789 [-13.085, -6.494] *; KxDxS -12.565 [-15.991, -9.138] *

**mean MAE (4 forecasts):** K -201.717 [-204.214, -199.220] *; D -21.093 [-24.399, -17.786] *; S -68.520 [-71.980, -65.059] *; KxD -5.909 [-8.406, -3.412] *; KxS -9.708 [-10.565, -8.852] *; DxS -15.396 [-18.857, -11.936] *; KxDxS -3.141 [-3.998, -2.285] *

**mean RMSE:** K -222.043 [-224.658, -219.428] *; D -30.155 [-33.899, -26.412] *; S -81.424 [-84.907, -77.941] *; KxD -7.071 [-9.686, -4.456] *; KxS -11.131 [-12.190, -10.071] *; DxS -16.406 [-19.889, -12.923] *; KxDxS -3.507 [-4.566, -2.448] *

**negative-forecast share:** K +0.019 [+0.017, +0.021] *; D -0.002 [-0.009, +0.006]; S -0.128 [-0.133, -0.124] *; KxD +0.001 [-0.001, +0.003]; KxS -0.017 [-0.019, -0.015] *; DxS -0.022 [-0.026, -0.017] *; KxDxS -0.003 [-0.005, -0.001] *

**periodic cost $/h:** K -0.271 [-0.329, -0.212] *; D -0.061 [-0.099, -0.023] *; S -0.070 [-0.102, -0.038] *; KxD -0.017 [-0.076, +0.041]; KxS +0.044 [-0.003, +0.090]; DxS -0.026 [-0.058, +0.006]; KxDxS +0.052 [+0.006, +0.099] *

**operator instances (periodic):** K -7.841 [-8.859, -6.823] *; D +1.295 [+0.281, +2.310] *; S -1.205 [-1.897, -0.512] *; KxD -1.159 [-2.177, -0.141] *; KxS +0.341 [-0.232, +0.914]; DxS +0.205 [-0.488, +0.897]; KxDxS -0.341 [-0.914, +0.232]

`*` = 95% CI excludes zero.

## 5. Additivity check (pre-registered V0–V4)

Predicted V4 = V0 + (V1−V0) + (V2−V0) + (V3−V0), from variant means. A large gap between predicted and observed V4 shows the single-fix contrasts cannot be summed.

| Outcome | V0 | V1 | V2 | V3 | V4 observed | V4 additive prediction | gap |
|---|---|---|---|---|---|---|---|
| MAE periodic p2 | 885.080 | 128.116 | 900.661 | 879.350 | 18.581 | 137.968 | -119.387 |
| mean MAE (4 forecasts) | 317.878 | 128.637 | 314.949 | 271.322 | 23.408 | 79.152 | -55.745 |
| mean RMSE | 364.894 | 157.545 | 354.709 | 307.500 | 27.764 | 89.966 | -62.201 |
| negative-forecast share | 0.115 | 0.145 | 0.130 | 0.021 | 0.000 | 0.068 | -0.068 |
| periodic cost $/h | 0.733 | 0.489 | 0.768 | 0.698 | 0.384 | 0.489 | -0.105 |
| operator instances (periodic) | 22.091 | 14.727 | 24.000 | 20.000 | 14.000 | 14.545 | -0.545 |

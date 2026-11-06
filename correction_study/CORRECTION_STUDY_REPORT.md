# STREAMLINE — Checkpoint audit and controlled correction study

Date: 2026-11-04/05 · Upstream code: `UIBK-DPS-DC/Streamline` @ `d65baec` (never edited; all fixes are applied in our scripts).
Scope: the decision layer only (forecasting → NSGA-II configuration → scheduling) on the authors' example workloads. These are synthetic sine and cosine waves, with surrogate models trained on the two example CSVs. The results show *mechanisms and their direction*. They are not estimates of the effect inside a running Flink cluster; that is experiment E3 in `../bigdata/BIGDATA_REPRODUCTION_PLAN.md`.

## Part A — Does `best_model = model` affect validation performance?

**Code fact.** `WorkloadPredictor.train_()` sets `best_model = model`. That is an alias, so the returned weights are always the last epoch's, whatever the best-loss bookkeeping says. Selection uses **training** loss; there is no validation split. (The surrogate `NeuralNetwork` does no checkpoint selection at all and always uses the final epoch.)

**Test 1: authors' examples** (`repro_scripts/train_checkpoints.py`, log `logs/train_checkpoints.log`). We repeated `ensemble.py`'s training exactly; every logged loss matches upstream. A deep copy was taken whenever upstream's own best-loss condition fired. For both predictors, the best training-loss epoch **is** the last epoch (200/200: p1 loss 0.005212, p2 0.005576), and the returned weights equal the true best snapshot. **No effect** in the released examples.

**Test 2: latent impact** (`repro_scripts/best_model_probe.py`, `../best_model_probe/best_model_probe.json`). 5 seeds × 2 settings: the examples' settings (200 epochs, horizon 50) and the paper's Table 2 settings (500 epochs, horizon 30). Training loss rose in 59–65 of 200 epochs (example) and 191–211 of 500 (paper), so the last epoch is usually *not* the best.

| Setting | Last epoch = best epoch | Best epochs | Held-out MAE difference, best − last (mean ± SD; events/s) |
|---|---|---|---|
| Example (200 ep) | 1/5 seeds | 197, 197, 200, 195, 199 | continuation −3.8 ± 6.9 · shifted workload −0.9 ± 4.2 (corrected inference) |
| Paper Table 2 (500 ep) | 0/5 seeds | 492, 498, 493, 492, 489 | continuation +1.7 ± 14.2 · shifted workload −1.0 ± 7.8 (corrected inference) |

The true best checkpoint was better on held-out data in only 1–3 of 5 seeds per condition, with differences inside seed-to-seed noise. Upstream (stochastic) inference shows the same picture: −9.2 to +6.3 mean, SD 7–15.

**Verdict.** This is a **real implementation defect**: the code does not keep the epoch it claims to keep, and under the paper's own settings it returns a different checkpoint in 5/5 seeds. But it has **no measurable systematic effect on held-out forecast accuracy** at n = 5 per condition. We classify it as a *latent defect* and do not count it as a fourth result-changing defect. A related methodological point: selecting a checkpoint on training loss with no validation split would not reliably pick a better forecaster even if the copy were made correctly.

## Part B — Controlled correction study (2³ full factorial)

### Design

| Factor | Level 0 (upstream) | Level 1 (corrected) |
|---|---|---|
| K: workload key | pipeline 2's periodic update written to `"v_0"` (ignored) | written to `"v_5"` |
| D: dropout at inference | model left in `train()` mode | `model.eval()` before `predict()` |
| S: input scaling | raw window (50–2000 events/s) fed to the network | window MinMax-scaled with the training scaler |

Pre-registered variants V0 (000), V1 (K), V2 (D), V3 (S), V4 (KDS). The remaining combinations V5 (KD), V6 (KS) and V7 (DS) were run as well, because the single-fix contrasts turned out not to be additive (§B.5).

**Controls:**
- All 88 runs load the **same checkpoints** (trained once, exactly as `ensemble.py`).
- They use the **same workload sequences**: initial sin (p1) and cos (p2), then −sin for both at the periodic step.
- They use the **same resource specification** (`examples/utils.py`) and the **same 11 seeds**: "orig", which restores the exact post-training RNG state, plus 0–9.
- **Validation:** V0 with seed "orig" reproduces the unmodified `ensemble.py` run bit for bit (R11 = 0.3514294578600675, R12 = 0.6767815603315829).

**Ground truth:**
- Forecasts are scored against the analytic continuation (50 steps) of the workload that is *actually* arriving. For pipeline 2 at the periodic step this is the −sin workload, even when the bug hides it from the predictor.
- "Invalid" means non-finite, negative, or outside the generator's physical range [50, 2000].
- Cost is the summed $/h of resources carrying load, using the example prices.

### B.1 Forecast accuracy (events/s; mean ± SD over 11 seeds)

| Variant | K D S | MAE init p1 | MAE init p2 | MAE periodic p1 | MAE periodic p2 | mean MAE | mean RMSE | negative share | out-of-range share |
|---|---|---|---|---|---|---|---|---|---|
| V0 original | 000 | 122.6 ± 16.8 | 112.5 ± 19.0 | 151.4 ± 21.7 | 885.1 ± 30.5 | 317.9 ± 12.0 | 364.9 ± 12.7 | 0.115 | 0.267 |
| V1 key | 100 | 122.6 ± 16.8 | 112.5 ± 19.0 | 151.4 ± 21.7 | 128.1 ± 15.4 | 128.6 ± 9.1 | 157.5 ± 10.1 | 0.145 | 0.281 |
| V2 eval | 010 | 96.0 | 111.9 | 151.3 | 900.7 | 314.9 | 354.7 | 0.130 | 0.315 |
| V3 scaling | 001 | 68.7 ± 8.8 | 70.0 ± 12.2 | 67.2 ± 9.2 | 879.4 ± 31.5 | 271.3 ± 9.4 | 307.5 ± 9.5 | 0.021 | 0.099 |
| **V4 all** | 111 | **27.6** | **24.5** | **22.9** | **18.6** | **23.4** | **27.8** | **0.000** | 0.150 |
| V5 key+eval | 110 | 96.0 | 111.9 | 151.3 | 121.6 | 120.2 | 140.2 | 0.170 | 0.325 |
| V6 key+scaling | 101 | 68.7 ± 8.8 | 70.0 ± 12.2 | 67.2 ± 9.2 | 69.8 ± 11.1 | 68.9 ± 5.2 | 84.9 ± 5.9 | 0.025 | 0.100 |
| V7 eval+scaling | 011 | 27.6 | 24.5 | 22.9 | 900.5 | 243.9 | 271.5 | 0.000 | 0.165 |

Variants with D = 1 have SD 0: they were identical across all 11 seeds.

### B.2 Decisions and cost

Parallelism vector order: p1 v_0, v_1, v_2, v_3 | p2 v_5, v_6, v_7. The segment size was 16384 in every run.

| Variant | Initial parallelism (mode) | Periodic parallelism (mode) | Resources after periodic | Distinct periodic placements | Cost initial $/h | Cost periodic $/h |
|---|---|---|---|---|---|---|
| V0 original | 4-4-4-4-5-5-5 (7/11) | 2-1-2-4-5-5-5 (5/11; 5 distinct) | 2.18 ± 0.60 | 11 | 0.890 ± 0.097 | 0.733 ± 0.168 |
| V1 key | 4-4-4-4-5-5-5 (7/11) | 2-1-2-4-2-2-2 (6/11; 3 distinct) | 1.55 ± 0.93 | 11 | 0.890 ± 0.097 | 0.489 ± 0.179 |
| V2 eval | 4-4-4-4-5-5-5 (11/11) | 2-1-2-4-5-5-5 (11/11) | 2.00 | 1 | 0.960 | 0.768 |
| V3 scaling | 3-2-2-2-5-5-5 (5/11) | 2-1-1-4-5-5-5 (7/11) | 2.18 ± 0.40 | 11 | 0.751 ± 0.135 | 0.698 ± 0.155 |
| **V4 all** | 3-3-2-2-5-5-5 (11/11) | 2-1-1-4-2-2-2 (11/11) | **1.00** | 1 | **0.768** | **0.384** |
| V5 key+eval | 4-4-4-4-5-5-5 (11/11) | 2-1-2-4-2-2-2 (11/11) | 1.00 | 1 | 0.960 | 0.384 |
| V6 key+scaling | 3-2-2-2-5-5-5 (5/11) | 2-1-1-4-2-2-2 (11/11) | 1.27 ± 0.47 | 11 | 0.751 ± 0.135 | 0.436 ± 0.090 |
| V7 eval+scaling | 3-3-2-2-5-5-5 (11/11) | 2-1-1-4-5-5-5 (11/11) | 2.00 | 1 | 0.768 | 0.576 |

No run triggered the α-rescheduling path.

### B.3 Optimizer execution time

Measured in an uncontended sequential pass: seed "orig", one process, 20 torch threads, laptop otherwise idle. The time is NSGA-II `solve()` wall time summed over the 14 operator optimisations of one experiment (7 initial + 7 periodic).

| Variant | Optimizer s (total) | Per operator s (mean ± SD) |
|---|---|---|
| V0 original | 152.9 | 10.92 ± 0.87 |
| V1 key | 160.0 | 11.43 ± 0.91 |
| V2 eval | 150.1 | 10.72 ± 0.24 |
| V3 scaling | 143.0 | 10.21 ± 0.12 |
| V4 all | 145.9 | 10.42 ± 0.25 |
| V5 key+eval | 144.6 | 10.33 ± 0.13 |
| V6 key+scaling | 144.9 | 10.35 ± 0.30 |
| V7 eval+scaling | 145.8 | 10.42 ± 0.20 |

The contended batch (8 processes × 2 threads) shows the same ordering: medians 171 s for V0–V2 vs 142–159 s for the variants with scaling. The fixes change optimizer time by at most about 10%. The cost is dominated by NSGA-II evaluating the surrogate network one configuration at a time (pop 20 × 20 generations + 99 initial individuals), at **~10–11 s per operator** whatever the forecast.

For comparison, the paper re-tunes every 5 minutes. At this rate, a 25-pipeline ensemble (~150 operators) would need about 26 min per round on this CPU. This is an extrapolation, not a measurement, and it is a scalability question to examine in the Big Data stage.

Consistency check: the timing-pass outputs (forecasts, configurations, placements) are **identical** to the batch's seed-"orig" runs for all 8 variants, despite 20 vs 2 threads.

### B.4 Factorial effects (paired by seed, mean and 95% CI over 11 seeds; `*` = CI excludes 0)

| Outcome | K | D | S | K×D | K×S | D×S | K×D×S |
|---|---|---|---|---|---|---|---|
| MAE periodic p2 | −806.9* | −5.3 | −41.8* | −23.6* | −38.8* | −9.8* | −12.6* |
| mean MAE | −201.7* | −21.1* | −68.5* | −5.9* | −9.7* | −15.4* | −3.1* |
| mean RMSE | −222.0* | −30.2* | −81.4* | −7.1* | −11.1* | −16.4* | −3.5* |
| negative-forecast share | +0.019* | −0.002 | −0.128* | +0.001 | −0.017* | −0.022* | −0.003* |
| periodic cost $/h | −0.271* | −0.061* | −0.070* | −0.017 | +0.044 | −0.026 | +0.052* |
| operator instances (periodic) | −7.84* | +1.30* | −1.21* | −1.16* | +0.34 | +0.21 | −0.34 |

Effects are coded as in a 2³ design: main effect = mean(on) − mean(off), averaged over the other factors. Full table with CIs: `results/effects.csv`.

### B.5 Additivity check (pre-registered V0–V4)

| Outcome | V0 | V4 observed | V4 predicted by summing V1, V2, V3 single-fix effects | Gap |
|---|---|---|---|---|
| MAE periodic p2 | 885.1 | 18.6 | 138.0 | −119.4 |
| mean MAE | 317.9 | 23.4 | 79.2 | −55.7 |
| negative share | 0.115 | 0.000 | 0.068 | −0.068 |
| periodic cost $/h | 0.733 | 0.384 | 0.489 | −0.105 |

The fixes **interact**. The single-fix contrasts under-predict the combined benefit by a factor of 3–6 on forecast error, so no single fix should be credited with the full improvement.

### B.6 Interpretation

1. **The key bug (K) dominates periodic behaviour for pipeline 2.** While it is present, pipeline 2 is re-tuned on a stale window (mean 1026.9 instead of 1025.0, ending at 2000 instead of 1025). Periodic MAE stays at 880–900 whatever the other fixes, and pipeline 2 keeps parallelism 5-5-5. Fixing K alone cuts periodic cost by 33% (0.733 → 0.489 $/h on average).
2. **Eval mode (D) and scaling (S) are synergistic, not independent.** With raw input, switching off dropout barely changes accuracy (V2 vs V0: mean MAE 315 vs 318), because the out-of-range input dominates the error. Once the input is scaled, dropout becomes the main remaining error source (V3 → V7: initial p1 MAE 68.7 → 27.6). This is why the D×S interaction is significant.
3. **Scaling removes most invalid forecasts.** Under V0, 11.5% of forecast points are negative, which is physically impossible. S cuts this to 2.1%, and S + D to 0%. Fixing K alone *raises* the negative share (11.5% → 14.5%), because the new −sin window, fed unscaled, produces more negatives. The ~15% "out of range" left in V4 consists of small overshoots near the bounds 50 and 2000, which the truth itself touches.
4. **Dropout is the only source of randomness in decisions.** Every D = 1 variant produced identical parallelism, placements and cost across all 11 seeds. Every D = 0 variant produced 11 distinct periodic placements, with periodic cost SD up to $0.18/h. So upstream STREAMLINE's scheduling decisions are not repeatable run to run unless the RNG state happens to match.
5. **Better forecasts do not monotonically lower cost.** Eval mode alone raises the expected cost (V2 0.768 vs V0 0.733), and scaling alone changes initial parallelism (4-4-4-4 → 3-2-2-2) more than periodic cost. Cost is driven by whether pipeline 2 sees its real workload (K), then by consolidation once forecasts are stable (K + D reach the same $0.384/h as all three fixes).
6. **Validity limits.** The workloads are synthetic sine waves, the models are trained on 349- and 259-row example CSVs, and placements use the example resource list. The study isolates code-level effects on the *decision layer*. Whether they change throughput or latency in a running system is tested in E3.

## Files

```
correction_study/
  checkpoints/models.pkl, transformer_*_best_state.pt   fixed checkpoints (+ post-training RNG state)
  runs/*.json                                           88 runs (8 variants x 11 seeds): forecasts, configs, placements, costs, solve times
  runs_timing/*.json                                    uncontended timing pass (seed 'orig')
  results/summary.md, per_run.csv, effects.csv          generated by repro_scripts/analyse_correction_study.py
  logs/                                                 training, batch and timing logs
  run_batch.sh, tasks.txt                               batch driver
repro_scripts/train_checkpoints.py, correction_study.py, analyse_correction_study.py, best_model_probe.py
```

**Incident note:** the laptop slept from 2026-11-04 ~23:59 to 2026-11-05 ~07:03 during the batch. Windows' performance counter keeps running through sleep, so the timings of V7 seeds 3–9 are invalid and excluded from the timing statistics. Their forecasts and decisions are unaffected (deterministic computation), and they are identical to V7's other seeds.

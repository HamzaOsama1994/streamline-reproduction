# E3: STREAMLINE V0 (as released) vs V4 (corrected), trained on our measurements and applied in the running Flink cluster

**Question (ours, not in the paper):** do the defects in STREAMLINE's released workload-prediction path change what it decides
and how the running system behaves?
**Code:** STREAMLINE `d65baec`, unmodified and imported as a library (`../repro_scripts/e3_decide.py`). Only
`WorkloadPredictor.predict` is wrapped to select the variant, exactly as in the correction study. The authors' ETL benchmark
`57524e2` is unmodified. Environment deviations are listed in `ENVIRONMENT_AND_MODIFICATIONS.md` (§7, §9–§13).
**Data:**
- profiling: `results/E3_profile/`
- training table: `results/E3_work/historical_data_etl.csv` (994 rows)
- final models and decisions: `results/E3_work/models_main/`
- evaluation runs: `results/E3_eval/*.json`
- generated summary: `results/E3_eval/summary.md`, from `scripts/analyse_e3.py`
- superseded model sets, kept for comparison (§3): `models_superseded_untrimmed_history/`, `models_battery_superseded/`
- excluded runs: `results/battery_or_standby/`, `results/contaminated_hostload/`

All numbers below are **measured in this reproduction**. Paper values appear only in §5 and are labelled as such.

## 1. Design

| Step | What was done |
|---|---|
| Variants | **V0** = as released: the predictor is left in train mode, so dropout is active, and the input window is unscaled. **V4** = `model.eval()` plus the input window MinMax-scaled with the predictor's own scaler. The third defect (`v_0`/`v_5` key) exists only in the two-pipeline example, so for one pipeline V4 = eval + scaling. Both variants load the **same** trained checkpoint and differ only in the predict path and the RNG seed. 11 seeds per variant (`orig` + 0–9) |
| Pipeline | The authors' ETL on FIT (Beam, FlinkRunner). It runs as 2 Flink vertices, modelled as 2 STREAMLINE operators: v_0 = source chain (Kafka read → parse → range filter → key), v_1 = Interpolate → annotate → serialise → sinks |
| Profiling (training data) | 12 runs: p ∈ {1, 2, 4, 8} × segment size ∈ {16, 32, 64 KiB}, stepped load of 300 to 2000 el/s effective, 4 TaskManagers (1 CPU, 2 GiB, 2 slots each). This gives 994 rows (497 five-second intervals × 2 operators). All 12 runs were on AC power; 4 were re-measured after the battery finding (ENVIRONMENT §13). Table builder rules: ENVIRONMENT §10 |
| Workload history | A sine replay with nominal rate 1125 + 875·sin(2πt/300 s), measured over 0–1800 s in 5 s bins. That is 359 values of 199 to 1618 el/s effective. The authors' producer sends 4 of every 5 lines (§9), so effective = 0.8 × nominal. The last bin was dropped as an end-of-file artefact |
| Training | `TrainingManager` as in `examples/single_pipeline.py`: WorkloadPredictor (input 100, output 50, 200 epochs), NeuralNetwork surrogate (200 epochs, R² train 0.88 / val 0.95), cubic PolynomialRegression capacity model. Run with the cluster idle |
| Decision | NSGA-II as in the authors' examples (weights 0.5/0.25/0.25, α = 0.2, Δt = 100). Resources = our 4 TaskManagers at $0.192/h each. About 21 s per decision |
| Mapping to Flink | Beam exposes only a **job-wide** parallelism, so the per-operator plan becomes job parallelism = max over operators. The chosen segment size becomes `taskmanager.memory.segment-size`. A job with p = 7 or 8 needs 4 TaskManagers (2 slots each). Flink cannot pin subtasks to named TaskManagers, so STREAMLINE's placement is **not** applied |
| Evaluation | Continuation of the same sine. The run starts at t = 1650 s of the sine and lasts 400 s. The first 150 s are JIT warm-up, so the **horizon is sine t = 1800 to 2050 s, the 50 × 5 s that STREAMLINE forecast**. Each distinct chosen configuration plus the paper's baselines (FD: p = 1, 1 TM, 32 KiB default; FMAX: p = 8, 4 TMs, 32 KiB) was run 3 times |
| Latency | **True end-to-end latency**: broker LogAppendTime of the i-th output record minus that of the i-th input record. The order check is 1.0 in all 12 runs. It is measured for records entering in the horizon. Latency markers are not used for conclusions (L1_RESULTS §3) |

**Deviation from the plan:** the plan put E3 on TAXI. TAXI was not available when E3 started, so E3 uses FIT, the same
pipeline as E1 and L1. TAXI is now converted and checked (ENVIRONMENT §5).

## 2. Workload forecast vs the actual workload (horizon, 50 × 5 s)

The actual workload is the replay schedule × 0.8, with mean 967 and range 201 to 1599 el/s. Ingestion measured from broker
timestamps agrees with the schedule (mean 960 el/s over the runs).

| Forecast | MAE (el/s) | MAPE | Bias (el/s) | Forecast peak (el/s) | Negative values |
|---|---|---|---|---|---|
| **V0 as released** (11 seeds) | **115 ± 11** | 20.0 ± 2.3% | −42 ± 11 | 1629–1831 (10 of 11 seeds ≥ 1672) | in 7 of 11 seeds (down to −177) |
| **V4 corrected** (11 seeds, identical output) | **41** | 6.2% | +10 | 1630 | none |
| Seasonal naive (value one period, 300 s, earlier) | 19 | 2.6% | −5 | — | — |
| Naive last value | 489 | 85.5% | −161 | — | — |
| Mean of the last 100 values | 490 | 85.3% | −163 | — | — |

- **The fix cuts forecast error by 64%.** It also makes the forecast deterministic (V0 changes with the seed because dropout is active) and removes physically impossible negative rates. This confirms the correction study (`../correction_study/`) in a real system.
- **The seasonal-naive forecast beats both variants** on this perfectly periodic workload. STREAMLINE's Transformer forecaster adds nothing over a trivial seasonal model here; our workload is easier than the paper's.

## 3. Decisions: the defects change the decision

| Variant | Plan per operator (STREAMLINE) | Seeds | Applied in Flink | Cost: STREAMLINE's plan / applied in Flink |
|---|---|---|---|---|
| V0 as released | v_0: p = 1 · v_1: **p = 7**, 64 KiB | **10 of 11** | p = 7, 4 TMs, 64 KiB | $0.384 / **$0.768** per h |
| V0 as released | v_0: p = 1 · v_1: p = 8, 64 KiB | 1 of 11 (`orig`) | p = 8, 4 TMs, 64 KiB | $0.384 / $0.768 per h |
| V4 corrected | v_0: p = 1 · v_1: **p = 8**, 64 KiB | **11 of 11** | p = 8, 4 TMs, 64 KiB | $0.384 / $0.768 per h |

- **The decision follows the forecast peak.** Every forecast that peaks at ≤ 1630 el/s leads to v_1 = 8; every forecast that peaks at ≥ 1672 el/s leads to v_1 = 7. The actual peak was 1599 el/s. V4's peak forecast (1630) is close to it; V0's overshoots by 5–15% in 10 of 11 seeds, and that overshoot flips the choice.
- **The capacity model drives this.** The cubic capacity fit for v_1 peaks at p = 6–7: p = 4: 1503, p = 6: 1940, p = 7: 1908, p = 8: 1583 el/s. A higher forecast peak therefore favours p = 7 over p = 8. The fall from p = 7 to p = 8 is an artefact of fitting a cubic to 4 measured p values, as the authors' `PolynomialRegression` does. No p = 7 was ever measured.
- **Mapping to Flink doubles the cost** for both variants. STREAMLINE packs the tasks onto 2 resources ($0.384/h). Flink with 2 slots per TaskManager needs 4 TaskManagers for p = 7 or 8 ($0.768/h).

**The decision depends on the training data, including placement luck.** Two earlier model sets were trained before the final one:

| Model set | Training table | Decision V0 / V4 |
|---|---|---|
| `models_superseded_untrimmed_history` | history with an end-of-file artefact | p = 8 in 21 of 22 decisions |
| `models_battery_superseded` | 978 rows, 4 profiling runs on battery (all p = 4 runs) | p = 8 in 22 of 22: **no difference between variants** |
| `models_main` (final) | 994 rows, all AC | V0 p = 7 (10/11), V4 p = 8 (11/11): **variants differ** |

In the battery table, two of the three p = 4 runs also happened to be co-located: the source and the hot keyed subtask
shared one TaskManager (§4). Their re-runs were not co-located. That changed the learned p = 4 capacity from 859 to
1503 el/s and reshaped the whole curve. This is how the variants came to differ. So whether the released defects change
STREAMLINE's decision depends on where Flink happened to place tasks during profiling. Its features cannot record that.

## 4. Measured outcomes (3 runs per configuration, horizon only)

| Configuration | Chosen by | Cost ($/h) | e2e p50 per run (ms) | p99 per run (ms) | Runs with hot subtasks on one TM |
|---|---|---|---|---|---|
| FD: p = 1, 1 TM, 32 KiB | baseline | 0.192 | 17,228 / 21,839 / 16,804 | 33,262 / 40,290 / 32,474 | 3 of 3 (p = 1 always) |
| FMAX: p = 8, 4 TMs, 32 KiB | baseline | 0.768 | 62 / 63 / **21,074** | 750 / 446 / **40,244** | **1 of 3** (rep 3) |
| p = 8, 4 TMs, 64 KiB | **V4** (11/11), V0 (1/11) | 0.768 | 61 / 400 / 66 | 330 / 4,831 / 1,010 | 0 of 3 |
| p = 7, 4 TMs, 64 KiB | **V0** (10/11) | 0.768 | 117 / 118 / 63 | 1,023 / 3,695 / 397 | 0 of 3 |

Expected outcome per variant (frequency-weighted over seeds, mean of runs):

| Variant | Cost ($/h) | e2e p50 (ms) | p99 (ms) |
|---|---|---|---|
| V0 as released | 0.768 | 106 | 1,737 |
| V4 corrected | 0.768 | 176 | 2,057 |

**V0 vs V4 in the running system: different decisions, indistinguishable outcomes.** Both cost the same. Their latencies
overlap run by run: p50 63–118 vs 61–400 ms, p99 397–3,695 vs 330–4,831 ms. The difference in expected values comes from
one run each and is within run-to-run variation. With 3 runs per configuration, we cannot claim that either variant performs
better.

**Why the tails vary.** The worst 25 s windows fall at the sine peak (t ≈ 225–250 s, 1599 el/s). Without co-location, p = 8
sustains about 1415–1726 el/s (profiling and E1; p = 7 was never profiled), so the peak sits at its capacity. A small slowdown at the
peak then means seconds of queueing. The five runs measured on 11-06 also recorded higher host load from other processes (3.8–5.2 cores vs
3.0–3.6 in the earlier evaluation runs). It is recorded per sample and could not be controlled.

**FD is overloaded by design.** The workload peaks at 1599 el/s, and p = 1 sustains ~1100 el/s (E1). Records wait in Kafka
for up to 32–40 s during each peak, and p50 is 17–22 s.

**Placement decides the extremes.** The ETL job keys every record with the same key (`WithKeys.of("")`, ENVIRONMENT §8). All
Interpolate work therefore lands on one keyed subtask: subtask 7 at p = 8, subtask 6 at p = 7, the same in every run. Only
one source subtask (0) is active, because there is 1 Kafka partition. If Flink places these two on the same 1-CPU
TaskManager, the job behaves like FD (FMAX rep 3: p50 21 s).
- Across all p = 8 runs with recorded placement (E1 3, L1 3, profiling 3, E3 6), the two subtasks shared a TaskManager in **3 of 15**. The 3 p = 7 runs had 0.
- Segment size does not influence slot placement, so the difference between STREAMLINE's configurations and FMAX in this respect is chance.

**Deployment failures (reported, not counted as outcomes):** two submissions of the p = 8 / 64 KiB configuration failed at
start-up (RPC timeout, ENVIRONMENT §12) and were re-run. Two later runs overlapped Modern Standby and were re-measured
(§13). All 12 runs used here ran on AC power with no sleep event; this is checked automatically for the 5 runs measured
after 11-06 06:50.

## 5. Comparison with the paper (relative outcomes; ETL/FIT, §5.1.1)

| Statement in the paper | Paper | This reproduction | Agreement |
|---|---|---|---|
| STREAMLINE has lower end-to-end latency than FD | −76% | p50 106–176 ms vs 18,624 ms (**−99%**). Expected p99 1.7–2.1 s vs 35.3 s (−94 to −95%) | Same direction. The size is not comparable: our FD is overloaded at the peaks by design, so the gap is queueing time. Below capacity there is no latency gain at all (L1_RESULTS) |
| STREAMLINE costs less than the baselines | −63% (FIT, average vs baselines) | **+300% vs FD, 0% vs FMAX** in Flink. STREAMLINE's own plan ($0.384/h) would be −50% vs FMAX and +100% vs FD | **Not reproduced.** STREAMLINE meets the peak only with p = 7–8, and on Flink with 2 slots per TM this needs every TaskManager. The paper's per-operator placement on larger nodes is not available here (Beam job-wide parallelism, no pinning) |
| STREAMLINE reaches FMAX-level performance | ≈ FMAX throughput | Latency comparable with FMAX runs that had the same placement | Consistent: STREAMLINE chose FMAX-like configurations |

## 6. Critical analysis

1. **The released defects change the decision, but only sometimes.** With the final training table, V0's inflated forecast peaks moved v_1 from 8 to 7 in 10 of 11 seeds. With an earlier table (4 battery runs, 2 of them co-located), both variants chose p = 8. Whether forecast error matters depends on how close the forecast peak sits to a point where the capacity model's ranking of configurations flips. That point is itself an artefact of a cubic fit to 4 measured parallelism levels.
2. **The decision changed, the outcome did not (measurably).** p = 7 and p = 8 cost the same in Flink. Their latency distributions overlap within the run-to-run variation of 3 runs. In this setup, fixing the defects improves the forecast by 64% without a measurable operational benefit.
3. **The biggest source of variance is not modelled.** STREAMLINE's features (TP_in, parallelism, segment size, operator type) cannot represent hot-key slot co-location. It shifts both the learned capacity (§3) and the measured outcome (FMAX rep 3). A co-located deployment performs like FD at 4× the cost; this happened in 3 of 15 p = 8 runs. Neither the paper nor STREAMLINE's code mentions key skew or placement.
4. **The mapping loses STREAMLINE's cost advantage.** Its per-operator plan (v_0 = 1, v_1 = 7–8) cannot be expressed in Beam's FlinkRunner. Job-wide p = 7–8 occupies all four 2-slot TaskManagers. Native Flink operators with per-operator `setParallelism` and slot-sharing groups would allow 2 TaskManagers, but the hot key would still sit on one subtask.
5. **The seasonal-naive forecast beats STREAMLINE's Transformer forecaster** on this periodic workload. The forecasting component should be benchmarked against simple baselines, which the paper does not report.

## 7. Threats to validity

- **Small samples:** 3 runs per configuration, and co-location is a ~20% event. Differences between STREAMLINE's two configurations and FMAX's non-co-located runs are within run-to-run variation.
- **The training data depends on one placement draw per profiling run (§3).** A second set of 12 profiling runs could give a different decision. We did not repeat the profiling grid.
- **One laptop:** k3d in Docker Desktop's WSL2 VM. Host load is recorded per sample, and runs that overlapped host-side jobs, battery power or standby were excluded and re-measured (ENVIRONMENT §7, §13). The final 4 profiling runs used 1.18–1.21 ms CPU per record, against 1.05–1.15 for the earlier AC runs (higher host load) and 1.41–1.56 on battery.
- **One pipeline (ETL/FIT) and one synthetic periodic workload.** The forecast results depend on the trained model: MAE V4 22–41 and V0 115–138 el/s across the three model sets.
- **The authors' producer sends 0.8 × nominal (§9).** All rates above are effective rates.

# E3-T — STREAMLINE in the loop on TAXI's real arrival pattern (measured 2026-11-06/07)

Question: on the paper's fluctuating TAXI regime (§5.1.1), what do STREAMLINE's decisions (V0 as released, V4 corrected) deliver
against FD and FMAX, and do the paper's TAXI statements hold (FD −14% average throughput; FD vs FMAX latency < 4%; STREAMLINE
cost −51% / −67% vs FD / FMAX on one node)?

Everything below is measured in this reproduction unless marked "paper".

## Setup

| Item | Value |
|---|---|
| Workload history | real trips Mon 14 Jan 00:00 – Fri 18 Jan 14:00, 2013, replayed 220× faster (`e3t_taxi_hist`, 1,800 s); broker ingestion in 5 s bins (359 bins, mean 979, max 1,726 el/s) → `results/E3T_work/workload_history.json` |
| Profiling | 12 runs (p = 1, 2, 4, 8 × 16/32/64 KiB), stepped TAXI load `e3t_profile_steps` (510 s) → 1,002 training rows |
| STREAMLINE | train + decide with `repro_scripts/e3_decide.py` (released example hyperparameters), V0 and V4, 11 seeds each |
| Evaluation | real trips Fri 05:50 – Sat 05:17 (`e3t_taxi_eval`, 400 s); first 150 s warm-up; horizon = 150–400 s = Fri 14:00 – Sat 05:17; effective 279–1,752 el/s, mean 1,278 |
| Runs | FD, FMAX and every chosen configuration, 3 reps, interleaved; `--e2e --drain 300 --trace-full`; power guard |
| Per run | result JSON, complete per-record trace `*.e2e_trace.csv.gz`, placement log `*.placement.log` |

Capacities for reference (E1-TAXI): FD 1,337 el/s; FMAX 1,959 el/s with the hot subtasks separated, 1,283–1,349 when co-located.

## Validity

- 12 of 12 runs valid: AC power, no standby event, host load 1.4–2.4 other cores, order match 1.0 on all three slices,
  input 479,539 / output 479,535 (4 records held by `GroupIntoBatches(5)`), drain ≤ 2.4 s.
- One run (V4 p = 6, rep 1) was invalidated by the power guard (Modern Standby at 22:22) and repeated automatically:
  `results/E3T_eval/power_invalid/`.
- FD rep 3 was re-measured: in the first measurement the job ran normally, but reading the output topic back returned only
  469,538 of 479,535 timestamps, so its trace and order check were unusable. Archived with an explanation in
  `results/E3T_eval/e2e_dump_truncated/`.
- Profiling: 2 of the 6 multi-TaskManager runs were co-located (p = 4 / 64 KiB, p = 8 / 16 KiB), so the learned capacity at
  those points is placement-depressed (see E1 for the mechanism).

## Forecasts (horizon, 50 × 5 s; actual = replay schedule × 0.8)

| Forecaster | MAE el/s | MAPE % | bias el/s | forecast peak |
|---|---|---|---|---|
| V0 as released (11 seeds) | 297 ± 21 | 24.4 ± 2.0 | −247 ± 22 | 1,829 (orig seed) |
| V4 corrected (deterministic) | 210 | 20.6 | −188 | 1,716 |
| Seasonal naive (same hours one day earlier, from the measured history) | 236 | 22.7 | −208 | 1,704 |
| Naive (last history value, 1,276) | 314 | | | |

Both Transformer variants underestimate the mean but reach the actual peak (1,752). V4 beats the seasonal-naive baseline; V0 does not.

## Decisions

| Variant | Decision (count over 11 seeds) | STREAMLINE's own plan | In Flink | E[cost in Flink] |
|---|---|---|---|---|
| V0 | v_1 = 7 (8), v_1 = 6 (3); v_0 = 1; 16 KiB | 1 TM, $0.192/h (8); 2 TMs, $0.384/h (3) | p = 7 / 4 TMs (8); p = 6 / 3 TMs (3) | $0.716/h |
| V4 | v_1 = 6 (11); v_0 = 1; 16 KiB | 2 TMs, $0.384/h (11) | p = 6 / 3 TMs (11) | $0.576/h |

The correction changes the decision and lowers the expected deployed cost by 19%.

## Outcomes (records entering in the horizon; per run)

| Configuration | TMs | $/h | hot subtasks separated | e2e p50 ms | e2e p99 ms | output el/s, 60 s around the peak |
|---|---|---|---|---|---|---|
| FD p1 / 32 KiB | 1 | 0.192 | — (one TM) | 9,198 / 7,527 / 8,423 | 20,517 / 17,971 / 18,810 | 1,375 / 1,406 / 1,389 |
| p6 / 16 KiB (V4; V0 ×3) | 3 | 0.576 | 3 of 3 | 55 / 53 / 55 | 518 / 462 / 315 | 1,630 / 1,629 / 1,629 |
| p7 / 16 KiB (V0 ×8) | 4 | 0.768 | 3 of 3 | 56 / 52 / 54 | 666 / 177 / 669 | 1,630 / 1,629 / 1,627 |
| FMAX p8 / 32 KiB | 4 | 0.768 | 1 of 3 (rep 2) | 13,318 / 56 / 9,640 | 24,602 / 397 / 20,340 | 1,351 / 1,629 / 1,366 |

Input in the 60 s around the peak: 1,628–1,634 el/s. Averaged over the whole horizon, every run delivered 1,270–1,271 el/s, because
the backlog drains in the night-time trough before the run ends.

Expected outcome per variant (frequency-weighted over seeds): V0 p50 54 ms, p99 484 ms, $0.716/h; V4 p50 54 ms, p99 432 ms, $0.576/h.

Unexplained: every unsaturated run (6 STREAMLINE runs, FMAX rep 2) has a 0.4–1.0 s latency spike 290–293 s into the run. It is
not in the input schedule (checked at 1 s resolution) and its cause was not identified; it sets most of the p99 values above.

## Against the paper (§5.1.1, TAXI)

| Statement (paper) | Measured here | Verdict |
|---|---|---|
| FD 14% lower average throughput | 0% over the horizon; −15% in the 60 s around the peak; −32% at saturation (E1-TAXI) | Reproduced in direction; the size depends on the averaging window |
| FD and FMAX average latency differ by < 4% | mean p50 7,671 vs 8,383 ms (−8%), but per run FMAX is FD-like when co-located (2 runs) and −99% when separated (1 run) | Matches on average only; set by placement |
| STREAMLINE cost −51% vs FD, −67% vs FMAX, one type-2 node | STREAMLINE's own plan: one node in 8 of 11 V0 seeds. Deployed through Beam: V4 −25% vs FMAX, +200% vs FD | Plan-level reproduced; not deployable through Beam's job-wide parallelism |
| STREAMLINE latency −46% vs ContTune | ContTune not reproduced (out of scope) | — |

## Files

`results/E3T_history_taxi.json`, `results/E3T_profile/`, `results/E3T_work/` (dataset, models, `decisions.json`),
`results/E3T_eval/` (results, traces, placement logs, `summary.md`, `horizon_actual_ingestion.json`),
`scripts/chain_e3_taxi.sh`, `scripts/e3t_*_spec.json`, logs `logs/*E3T*`. Figure: report Fig. 3 (`scripts/make_report_figures.py fig3`).

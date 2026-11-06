# L1: Latency below capacity: ETL/FIT at ~780 el/s, FD vs p = 4 vs FMAX

**Question:** E1 ran every configuration saturated, so its latency could not be compared. When the input rate is below
every configuration's capacity, does extra parallelism buy lower latency?
**Code:** the authors' benchmark `57524e2`, unmodified. Environment deviations are listed in ENVIRONMENT §7 and §9–§13.
**Data:**
- `results/L1_fit_r1000/` (L1, latency markers) and `results/L1b_e2e_r1000/` (L1b, true end-to-end latency)
- generated summary `results/L1_fit_r1000/summary.md`, from `scripts/analyse_l1.py`
- excluded runs: `results/contaminated_hostload/` and `results/battery_or_standby/` (§7, §13)

All numbers are measured in this reproduction.

## 1. Setup

| Item | Value |
|---|---|
| Workload | FIT, nominal 1000 el/s. The authors' producer sends 4 of every 5 lines (§9), so the achieved input is 779–784 el/s (one run: 738). This is below the capacity of every configuration: p = 1 sustains ~1100 el/s (E1) |
| Configurations | FD (p = 1, 1 TM, $0.192/h) · p = 4 (2 TMs, $0.384/h) · FMAX (p = 8, 4 TMs, $0.768/h) |
| Protocol | 360 s per run, first 150 s discarded (JIT warm-up), fresh TaskManagers per run, 3 repetitions per configuration, interleaved |
| L1 latency | Flink latency markers, 1 s interval, 60-marker history (§7) |
| L1b latency | **True end-to-end latency** of records entering after the warm-up: broker LogAppendTime of the i-th output record minus that of the i-th input record. Order check 0.98–1.0 in every run; every 10th record is stored |

## 2. Results

**True end-to-end latency (L1b), per run, ms:**

| Config | Cost ($/h) | Rep 1 p50 / p95 / p99 | Rep 2 | Rep 3 |
|---|---|---|---|---|
| FD (p = 1) | 0.192 | **4,313 / 22,414 / 23,991** (stall, see below) | 39 / 98 / 134 | 39 / 103 / 160 |
| p = 4 | 0.384 | 33 / 110 / 146 | 36 / 105 / 132 | 35 / 108 / 142 (max 23,913: end-of-run stall) |
| FMAX (p = 8) | 0.768 | 35 / 109 / 140 | 36 / 115 / 402 | 35 / 100 / 130 |

**Latency markers (L1, separate runs), mean ± SD over 3 runs, ms:**

| Config | Marker p50 | Marker p95 | Marker p99 |
|---|---|---|---|
| FD (p = 1) | 14.5 ± 2.8 | 76 ± 5 | 97 ± 6 |
| p = 4 | 62.9 ± 8.0 | 1,260 ± 593 | 2,644 ± 59 |
| FMAX (p = 8) | 74.3 ± 12.4 | 3,385 ± 886 | 3,406 ± 887 |

## 3. Findings

1. **Below capacity, parallelism buys no latency.** In 8 of 9 runs, true end-to-end p50 was 33–39 ms, p95 98–115 ms and p99 130–160 ms (one FMAX run: 402 ms), whatever the configuration. **FD gives the same latency at ¼ of FMAX's cost.**
2. **The markers' multi-second tails at p = 4 and 8 are a measurement artefact.** Each Flink latency marker goes to one random downstream channel. Each of the p keyed subtasks therefore receives ~1/p of the markers, and its 60-marker window spans ~60·p s (480 s at p = 8). This is longer than the run, so the window still contains warm-up markers (ENVIRONMENT §10.5). True latency for the same configurations shows no such tail. Marker quantiles at p > 1 should not be used to compare configurations, yet STREAMLINE trains on them.
3. **FD has little headroom, and rare stalls cost it more.** FD rep 1's source throughput dropped to 362 el/s for ~30 s at t ≈ 180–210 s. Host load (3.5 cores) and power (AC) were normal. At ~20% headroom the backlog took ~90 s to clear, so the run's p50 was 4.3 s. A similar one-off stall hit p = 4 rep 3 at t ≈ 338 s (≈ 70 records delayed ~24 s); its p50 and p99 are unaffected because it occurred at the end of the run. We did not identify the cause. Candidates include JVM garbage collection, a Kafka log-segment roll and the MongoDB sink. Stalls were seen in 2 of 9 runs.

## 4. Comparison with the paper

The paper reports STREAMLINE at **−76% end-to-end latency vs FD** on ETL/FIT (§5.1.1). L1 shows that **below capacity, no
configuration has lower latency than FD** (findings 1 and 3). The paper's latency gain can therefore only arise where FD is
overloaded and records queue. E3 tests that regime with a workload that peaks above FD's capacity (`E3_RESULTS.md`).
The paper's latency metric is not specified at the level of marker vs end-to-end. If it used Flink's latency markers, finding 2 shows they
can mislead at p > 1.

## 5. Threats to validity

- 3 runs per configuration and one input rate (~780 el/s ≈ 70% of FD's capacity).
- One laptop: host load is recorded per sample (other processes 3.1–3.8 cores in L1b), and runs on battery or in standby were excluded and re-measured (ENVIRONMENT §13).
- L1 and L1b are separate runs, so marker and true latency come from different repetitions of the same configurations.

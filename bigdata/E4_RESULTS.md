# E4 results: STREAMLINE's own TAXI plans deployed with per-vertex parallelism

Measured 2026-11-13, 15:45–17:19 local time. All values are measured in this reproduction. The original study's numbers are not
used here except where a row says "paper". Method and environment are described in `ENVIRONMENT_AND_MODIFICATIONS.md` §16.
Per-run table: `results/E4_eval/summary.md` (written by `scripts/analyse_e4.py`).

## Question

In E3-T, STREAMLINE's per-operator TAXI plans could only be applied with job-wide parallelism (Beam's FlinkRunner). V0's dominant
plan (v_0 = 1, v_1 = 7, one TaskManager, $0.192/h emulated) therefore became four TaskManagers ($0.768/h), and V4's plan
(v_0 = 1, v_1 = 6, two TaskManagers, $0.384/h) became three ($0.576/h). E4 asks what STREAMLINE's plans deliver when they are
deployed as planned.

## Mechanism (no change to the jar, Beam or the authors' code)

* Flink 1.18 cluster option `pipeline.jobvertex-parallelism-overrides=<vertexId>:<p>`, set as a JobManager dynamic property. The
  Dispatcher rewrites the vertex parallelism at submission (JobManager log: "Changing job vertex 1f9ad02c… parallelism from 7 to 1").
* The job is submitted with `--parallelism` = v_1's parallelism; the override sets v_0 (source chain, vertex id
  `1f9ad02cd246b23e2f077255eeddb60e`, stable across all runs) to 1.
* TaskManager slot count (`taskmanager.numberOfTaskSlots`) set so that the job needs exactly the TaskManagers STREAMLINE planned.
* The harness checks after submission that REST reports the requested per-vertex parallelism and aborts the run otherwise
  (`run_pv_experiment.py`). All 9 kept runs passed this check.
* Flink, not STREAMLINE, chooses which slot each subtask gets. STREAMLINE's subtask-to-node mapping is therefore not enforced.

## Design

Same workload, warm-up, duration, drain and analysis as E3-T (`e3t_taxi_eval`, 400 s, horizon from 150 s, peak 1,752 el/s
schedule; measured peak-window input 1,624–1,630 el/s). 3 repetitions, interleaved. Spec: `scripts/e4_eval_spec.json`.

| Configuration | What it is | Vertex parallelism | TaskManagers × slots | Emulated cost |
|---|---|---|---|---|
| V0 plan | STREAMLINE V0's plan in 8 of 11 seeds | v_0 = 1, v_1 = 7 | 1 × 7 | $0.192/h |
| V4 plan | STREAMLINE V4's plan in 11 of 11 seeds (V0: 3 of 11) | v_0 = 1, v_1 = 6 | 2 × 3 | $0.384/h |
| Control | **Our design, not a STREAMLINE decision.** Puts the source and the busy Interpolate subtask on different TaskManagers by construction | v_0 = 1, v_1 = 2 | 2 × 1 | $0.384/h |

Why the control separates by construction: in all 100 runs we inspected (p = 1, 2, 4, 6, 7, 8) the busy Interpolate subtask was index p − 1, and with
slot sharing v_0's only subtask shares slot 0 with v_1 subtask 0. With one slot per TaskManager and v_1 = 2, subtask 1 (the busy
one) is on the other TaskManager. This uses knowledge of the single-key structure that STREAMLINE's models do not have.

## Results (measured, 3 runs each)

| Configuration | TMs; $/h | Hot subtasks separated | e2e p50 (ms) per run | e2e p99 (ms) per run | Peak-window output (el/s) | Host other cores (mean) |
|---|---|---|---|---|---|---|
| V0 plan (per-vertex) | 1; 0.192 | 0 of 3 (one TM) | 8,963 / 11,915 / 17,145 | 20,581 / 25,324 / 33,390 | 1,382 / 1,328 / 1,241 | 3.3 / 3.3 / 6.6 |
| V4 plan (per-vertex) | 2; 0.384 | 0 of 3 | 8,336 / 10,012 / 14,053 | 18,983 / 21,995 / 29,198 | 1,390 / 1,361 / 1,290 | 3.5 / 3.7 / 6.6 |
| Control (ours) | 2; 0.384 | 3 of 3 | 44 / 44 / 46 | 296 / 254 / 474 | 1,631 / 1,628 / 1,627 | 3.3 / 4.7 / 4.9 |
| *E3-T, for reference:* FD | 1; 0.192 | – | 9,198 / 7,527 / 8,423 | 20,517 / 17,971 / 18,810 | 1,375 / 1,406 / 1,389 | 1.4 / 1.4 / 1.5 |
| *E3-T:* job-wide p = 6 (V4) | 3; 0.576 | 3 of 3 | 55 / 53 / 55 | 518 / 462 / 315 | 1,630 / 1,629 / 1,629 | 1.6 / 2.1 / 1.6 |
| *E3-T:* job-wide p = 7 (V0) | 4; 0.768 | 3 of 3 | 56 / 52 / 54 | 666 / 177 / 669 | 1,630 / 1,629 / 1,627 | 1.7 / 2.2 / 1.6 |
| *E3-T:* FMAX (p = 8) | 4; 0.768 | 1 of 3 | 13,318 / 56 / 9,640 | 24,602 / 397 / 20,340 | 1,351 / 1,629 / 1,366 | 2.4 / 1.8 / 1.5 |

Means: V0 plan p50 12,674 ± 4,144 ms; V4 plan 10,800 ± 2,939 ms; control 45 ± 1 ms; E3-T FD 8,383 ± 836 ms.
All runs: order match 1.00 in all three slices, AC power at start and end, no power events, 0–4 records unprocessed at the end
(the night-time trough let every queue drain).

### Placement (from the per-subtask REST metrics; busy ms/s at the last sample)

* V0 plan: everything on one TaskManager; source subtask 0 at 670–680 busy ms/s, v_1 subtask 6 at 876–1,000; subtasks 0–5 idle.
  The TaskManager used 0.97–0.99 of its 1-core limit.
* V4 plan: in all 3 runs Flink put the source and the busy v_1 subtask 5 on the same TaskManager (844–1,000 busy ms/s); the other
  TaskManager held only idle subtasks. STREAMLINE's own mapping for this plan also puts `v_0_0` and `v_1_5` together on TM1
  (`decisions.json`), so enforcing STREAMLINE's placement would not have separated them either.
* Control: source on one TaskManager (621–656 busy ms/s), v_1 subtask 1 on the other (278–294 busy ms/s).

### Expected outcome over the 11 seeds (frequency-weighted, as in E3-T)

| Variant | Deployed job-wide (E3-T) | Deployed per-vertex (E4) |
|---|---|---|
| V0 (as released) | $0.716/h, p50 ≈ 54 ms | $0.244/h, p50 ≈ 12.2 s (8 × V0 plan, 3 × V4 plan) |
| V4 (corrected) | $0.576/h, p50 ≈ 54 ms | $0.384/h, p50 ≈ 10.8 s |

## Interpretation

1. **Per-vertex deployment works without modifying the authors' code.** STREAMLINE's plans now run on the number of TaskManagers
   STREAMLINE chose, so the cost side of the plan is reproduced: V0's plan costs the same as FD and 75% less than FMAX, V4's plan
   100% more than FD and 50% less than FMAX.
2. **But STREAMLINE's plans do not carry the TAXI peak.** Both plans queued like FD (peak output 1,241–1,390 vs input ~1,629 el/s;
   p50 8–17 s). The paper's claim is a cost saving at equal or better performance; deployed as planned on our cluster, the saving
   comes with FD's latency. Not reproduced.
3. **Why:** (a) the single key means one v_1 subtask does all the work, so v_1 = 6 or 7 adds no capacity over v_1 = 1;
   (b) STREAMLINE's capacity for high parallelism was learned from profiling runs that spread the job over ⌈p/2⌉ one-core
   TaskManagers (and separated the hot subtasks in most runs), and its features (input rate, parallelism, segment size, operator
   type) cannot tell that packing the same parallelism onto fewer cores removes that capacity; (c) for V4, STREAMLINE's own
   mapping co-locates the source and the hot subtask.
4. **The saving exists on this cluster, but STREAMLINE does not find it.** The control (our design) carried the peak with p50
   44–46 ms on two TaskManagers: the same latency as STREAMLINE's job-wide E3-T configurations at 50% of FMAX's cost and 33% below
   V4's job-wide cost. It needs to know which subtask is hot, which STREAMLINE's models cannot represent.
5. **Forecaster fix (O3) under per-vertex deployment:** V4's expected cost is higher than V0's ($0.384 vs $0.244/h) and both queue.
   The 19% saving reported for E3-T holds for job-wide deployment only.

## Validity notes

* **Host load was higher than in E3-T.** The Notion desktop app used about 1.5 cores in the background; mean "other cores" was
  3.3–6.6 in E4 against 1.4–2.4 in E3-T (most other experiments: 3–5; E1-TAXI 4.0–5.1, E3 3.0–5.2). It was not a CPU-heavy job, so the runs were kept and the load is reported per run. The
  slowest V0 and V4 runs (rep 3) coincide with the highest load (6.6 cores), so the *size* of their queueing is load-sensitive.
  The *outcome* is not: V0 and V4 already fell behind the input at 3.3–3.5 cores (peak output 1,382 / 1,390 el/s, the same as FD
  at 1.4 cores), while the control kept up at 4.7–4.9 cores.
* **One failed attempt.** The first attempt of `v4plan_…_rep2` failed 23 s after submission: a task failed during deployment
  (`Execution.deploy`, "Recovery is suppressed by NoRestartBackoffTimeStrategy"); the stored stack trace is truncated at 2,000
  characters, so the root cause was not retained. The harness re-ran it automatically; the failed record is kept in
  `results/E4_eval/failed/`.
* **Cost** is the emulated proxy of the report (§3.2): $0.192/h per one-core TaskManager hosting at least one task.
* **Smoke tests** (`results/E4_smoke/`, constant TAXI overload 3,500 el/s target, 240 s, one run each, not used as results):
  V0 plan 1,303 el/s on one TaskManager (E1 FD: 1,337 ± 12); control 1,959 el/s on two (E1 FMAX: 1,959 ± 16 on four).
* After the grid the harness restored the JobManager args (`["jobmanager"]`), TaskManager args (`["taskmanager"]`) and 4 TaskManager
  replicas (checked with kubectl).

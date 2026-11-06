# Big Data stage — environment record and modification log

Rule followed: **the authors' benchmark source is not modified.**
`beam-applications-java` @ `57524e284c4393db0840a32731ffa7e52c854cc2` (2024-08-08): `git status` shows no changes to tracked files. Untracked items are only the build outputs and data files listed in §3, which the repo's own `.gitignore` partly covers.
Every deviation from the authors' setup is listed below with its reason.

## 1. Host and toolchain

| Item | Value | Note |
|---|---|---|
| Host | Intel Core Ultra 7 255HX, 20 threads, 31.4 GB RAM, Windows 11 (26200) | |
| Docker | Docker Desktop, engine 29.1.3, WSL2 backend: 20 CPUs, 16.4 GB visible (WSL default limit) | `.wslconfig` not changed |
| Kubernetes | k3d v5.9.0 → k3s v1.35.5+k3s1, 1 server node, Traefik disabled | `bigdata/tools/k3d.exe` (portable) |
| Helm | v4.3.1 (portable, not used so far) | |
| JDK (build, producer) | Temurin 11.0.32.1+1, portable in `bigdata/tools/` | Repo requires JDK 11; system Java (8) untouched |
| Gradle | 8.6 via the repo's wrapper | |

## 2. Environment workarounds (no source changes)

| # | Problem | Workaround | Effect on results |
|---|---|---|---|
| W1 | Gradle dependency downloads failed intermittently with `javax.crypto.AEADBadTagException: Tag mismatch!` on random artifacts (2 failed builds, logs `logs/gradle_etl_build*.log`). `curl` fetched the same URLs fine. | Build JVM run with `JAVA_TOOL_OPTIONS="-XX:+UnlockDiagnosticVMOptions -XX:-UseAESIntrinsics -XX:-UseGHASHIntrinsics"`, which disables HotSpot's AES/GHASH CPU intrinsics. Diagnosis: JVM AES-GCM intrinsics misbehaving on this recent CPU. | Build only. **Not** applied to the producer or Flink. |
| W2 | k3d wrote kubeconfig server `https://host.docker.internal:<port>`, which resolves to the LAN IP and is unreachable | `kubectl config set-cluster k3d-streamline --server=https://127.0.0.1:<port>` | None |
| W3 | Image pulls inside k3s were very slow (>15 min) | `docker pull` on the host, then `k3d image import` | None (same image tags) |
| W4 | Git Bash rewrites absolute API paths (`/apis/...`) and `pod:/path` arguments | `MSYS_NO_PATHCONV=1` for manual kubectl calls; the harness calls kubectl from Python, which does no rewriting | None |

## 3. Infrastructure substitutions (authors' Helm chart → lean manifests in `bigdata/k8s/`)

| Authors' chart (`helm-charts/riot-applications`) | Ours | Why |
|---|---|---|
| Strimzi 0.41 operator, Kafka 3.7.0, **3 brokers**, `min.insync.replicas: 2` | `apache/kafka:3.7.0`, **1 KRaft broker**. The Service is named `kafka-cluster-kafka-bootstrap` because the benchmark hard-codes `kafka-cluster-kafka-bootstrap:9092`. A second listener, `localhost:9093`, serves the host producer, which hard-codes `localhost:9093`. | Fits the laptop budget, and the code stays unmodified |
| Topics `senml-source`, `senml-cleaned`: 1 partition, 1 replica, `retention.ms=7200000` | **Identical** (recreated by the harness before every run) | Fidelity. Note: **1 partition caps effective source parallelism at 1** |
| MongoDB chart (PV/PVC, NodePort, secret) | `mongo:7.0` Deployment (WiredTiger cache 0.5 GB, no PV). The Service is named `mongodb-nodeport` with the credentials hard-coded in `StoreStringInDBSink.java` | Code unmodified; persistence not needed |
| Flink Kubernetes Operator 1.8, `FlinkDeployment` `flink:1.18`, JM/TM 1 CPU / 2048m, 2 slots, Prometheus reporter on 9249 | Plain standalone session cluster, `flink:1.18-java11` (Flink **1.18.1**). JM 1 CPU / 1600Mi. **4 TaskManagers × (1 CPU, 2 GiB, 2 slots) = 8 slots**. Same Prometheus reporter. | Avoids the operator. TM size equals the authors' chart, and equals paper node type 1 (4 cores) scaled ÷4 |
| (none) | Flink `metrics.latency.interval: 5000` | The paper measured end-to-end latency with latency markers |
| Prometheus port written `"9249"` (quoted) | Written `9249` unquoted | Flink 1.18's `flink-conf.yaml` parser keeps the quotes, so the reporter never bound the port. The operator rewrites the config, which is why the quotes worked there. |
| kube-prometheus-stack | Not deployed. Metrics are read directly: Flink REST, TM Prometheus endpoints through the API-server pod proxy, and pod CPU through k3s metrics-server | Lighter |

**Paper vs ours:** the paper used Flink 1.19.1; the benchmark repo pins Flink 1.18 (Beam 2.57.0 `beam-runners-flink-1.18`), so we use 1.18.1.

## 4. Job submission options (Beam pipeline options, passed on the command line, no code change)

| Option | Why |
|---|---|
| `--experiRunId=FIT` | Selects the FIT ETL pipeline (the authors' own CLI option) |
| `--parallelism=P` | **The only parallelism control Beam's FlinkRunner exposes: job-wide.** STREAMLINE's per-operator parallelism cannot be applied through it (plan, option B) |
| `--attachedMode=false` | Without it, `flink run -d` submits the job and then Beam throws `InvalidProgramException: Job was submitted in detached mode…` while reading the runtime of a detached job. (The job does start; the client just fails.) |
| `--latencyTrackingInterval=5000` | Beam's `FlinkPipelineOptions.latencyTrackingInterval` defaults to 0 and **overrides** the cluster's `metrics.latency.interval`. Without it no latency-marker metrics exist (verified: none were emitted). |

## 5. Data preparation

| Step | Detail |
|---|---|
| Raw FIT data | UCI MHEALTH #319 zip, 75,567,983 bytes, SHA-256 `16ad0ce7…f02b8e0`; 10 subject logs copied into `beam-applications-java/data/` (git-ignored) |
| Conversion | Authors' `senMlConverter` Docker image built unmodified from `senMlConverter/Dockerfile`; run with `DATASET=FIT SCALING=1`, giving `riot_events_FIT_scale1.csv`: 1,215,745 events, 2 ms apart (500 el/s), 84 s conversion time |
| Rate files | `scripts/make_rate_files.py` rescales **only** the delay column (`delay·500/R`) to R ∈ {1000, 2000, 4000, 7000} el/s; payload unchanged. This is equivalent to the converter with `SCALING = 500/R`. |
| Header line | The converter writes no header, but `KafkaProducer` skips the first line (`withSkipLines(1)`), so the first event would be lost. The rate files prepend `delay|string`, the same header as the repo's `kafkaProducer/test_FIT.csv`. |
| Replay | Authors' `KafkaProducer.jar`, unmodified, 4 threads, on the host through `kubectl port-forward` 9093. It sleeps in 100 ms steps, so events arrive in 100 ms bursts. |
| TAXI source | The authors' source (Illinois Databank `FOIL2013.zip`) refuses scripted downloads (HTTP 403) and also failed for a manual browser download. Used instead: the archive.org copy of the same 2013 FOIL release (`nycTaxiTripData2013`: `trip_data.7z`, MD5 `f03d0a7749f44db2a8999cc592e2c828`; `trip_fare.7z`, MD5 `52cf3fdfc2af2705db40fc1cd5d6b079`; both verified against archive.org's checksums). Only `trip_data_1.csv` / `trip_fare_1.csv` (January 2013) are used, as the converter README prescribes |
| TAXI preparation (`scripts/prepare_taxi.py`, log `logs/prepare_taxi.log`) | (1) **IDs:** this copy has MD5-hash `medallion`/`hack_license` (as RIoTBench's own `TAXI_sample_data_senml.csv` does); the converter reads them as int64 (Illinois copy). Each hash → integer by first appearance (13,426 medallions, 32,224 licences); the converter writes IDs as strings, so only the identifier text changes. (2) **Order:** the file is not chronological (47% backward steps in the first 2M rows); the converter computes delays relative to the first row, so rows are stably sorted by `pickup_datetime`. Data/fare rows are paired by position, as in the converter; pairing verified on medallion, licence, vendor and pickup time: 14,776,615 pairs, 0 mismatches, 0 unpaired |
| TAXI week | Converter README: trip_data_1/trip_fare_1 "will contain data from 2013-01-14 to 2013-01-21" (RIoTBench's TAXI week; RIoTBench's `inputFileForTimerSpout-TAXI.csv` starts at 1358101800000 = 2013-01-14 00:00 IST). The converter has no date filter, so the input is restricted to pickups in [2013-01-14 00:00, 2013-01-21 00:00): 3,466,804 trips, in `data/taxi_week/` |
| TAXI conversion | Authors' `senMlConverter` image, unmodified: `DATASET=TAXI SCALING=1` (real-time ms delays) on `data/taxi_week/` → `riot_events_TAXI_scale1.csv`; replay files are derived from it as for FIT |
| TAXI parser check | `scripts/TaxiParseCheck.java` (offline, no cluster): first 300,000 lines read exactly as the authors' producer reads them (OpenCSV, separator `|`) and parsed by the authors' `TaxiSenMLParserJSON` + `RangeFilterFunction` from the unmodified ETL jar: 300,000 parsed, 0 exceptions, no empty field before the range filter. The converter's quirks are harmless: the Kafka value keeps a leading space (`" [{…"`), which org.json skips; the pickup datetime keeps a double space (`2013-01-14  00:00:00`), but no ETL stage parses it, and the authors' own parser test `parseSenMLPack_allExpectedRecords_2` uses exactly this format. The authors' range filter nulls many TAXI values (in this sample: trip_distance 41%, fare 25%, tip 50%, tolls 94%; its distance range 1.37–29.86 is flagged "Strange" in their own comment) — authors' behaviour, unchanged |
| TAXI replay for capacity runs (E1-TAXI, 2026-11-06) | `scripts/make_taxi_rate_file.py 8000 3200000` → `data/riot_events_TAXI_r8000.csv`: the first 3.2M events of the TAXI week, in pickup order, payloads unchanged; only the delay column becomes a constant-rate schedule (i × 1000 / 8000 ms). This discards the real arrival pattern. That is acceptable for saturated runs, where throughput measures capacity, and matches how the FIT rate files were made. Job: the authors' `FlinkJob` with `--experiRunId=TAXI` (`PipelineBuilder.buildTAXIPipeline`, unmodified); harness option `--dataset TAXI` (default FIT, so FIT runs are unchanged). TAXI lines are ~756 bytes vs ~380 for FIT. **Rate choice:** a p = 8 pilot at 8000 el/s nominal (`results/E1_taxi_pilot/`, pilot only) was saturated (input 6240 el/s, throughput 1639 el/s, Interpolate 100% busy). But the host-side producer and `kubectl port-forward` moving ~4.7 MB/s raised host load from other processes to 9.3 cores (FIT runs: ~3.5). The grid therefore uses 3500 el/s nominal (`riot_events_TAXI_r3500.csv`, 1.5M events). That offers ~1.6× the highest capacity, like E1 on FIT (2630 offered vs ≤1684 processed) |
| Concurrent host activity | TAXI preparation (19:31–19:42) and conversion ran on the host/Docker VM while E3 profiling runs were being measured; TaskManagers are cgroup-limited to 1 CPU each on a 20-thread host. The affected profiling runs are identified by timestamp in `results/E3_profile/` |

## 6. Measurement definitions (`scripts/run_fit_experiment.py`)

| Metric | Definition | Paper equivalent / deviation |
|---|---|---|
| Throughput | Flink REST `numRecordsOutPerSecond` summed over subtasks of the source vertex (records entering the pipeline); the sink vertex's `numRecordsInPerSecond` is reported too. Sampled every 5 s; mean over the post-warm-up window | Paper: Flink REST throughput every 5 s |
| Latency | Flink latency markers (source → operator), Prometheus histogram p50 and p99. The max over operators and subtasks is taken per sample, then averaged. **Excludes time spent waiting in Kafka before the source reads it.** | Paper: latency markers |
| CPU | Pod CPU (cores) from metrics.k8s.io, summed over TaskManager pods; utilisation = cores ÷ number of TaskManagers hosting the job's tasks. Flink busy and backpressure ms/s per vertex as a diagnostic. | Paper: per-operator CPU time via ThreadMXBean. **Deviation:** Beam fuses operators into chains, so per-operator CPU is not separable without code changes |
| Cost | (TaskManagers hosting at least one task) × $0.192/h, the paper's Table 1 type-1 price | Paper: AWS EC2-equivalent price per used node |
| Warm-up | The first 60 s after the producer starts are discarded | — |
| Placement | From the run that started 2026-11-05 ~07:40 on: the TaskManager of each subtask, plus per-subtask busy time and records in/out, sampled once before the run stops | Needed to explain the run-to-run variance at p = 4 |

## 7. Harness incidents and fixes

| Date | Incident | Fix | Data impact |
|---|---|---|---|
| 2026-11-05 | p = 8 repetitions 2 and 3 failed. The previous job's slots were still being released (Beam tasks close their Kafka consumers slowly), so the new job was scheduled on slots being freed: `TaskSubmissionException: No task slot allocated…`. With no restart strategy (as in the authors' setup), the job FAILED, and a 10 s REST timeout then aborted the harness. | Before submitting, wait until `slots-available == slots-total` and no job is running. Retry a failed deployment once (`deploy_attempts` is recorded). REST timeout raised to 30 s. | The two failed runs produced no result files and were rerun. The completed runs are unaffected: the fix only touches what happens before submission. |

| 2026-11-05 | Even with all slots free, some p = 4 and p = 8 deployments failed with `TaskExecutor … has no more allocated slots` / `No task slot allocated`. **Root cause:** each submission re-ships the 206 MB Beam fat jar through the blob server to every TaskManager. On 1-CPU TaskManagers, task deployment took longer than Flink's default `taskmanager.slot.timeout` (10 s), so the slot was released before the task arrived. | Cluster config `taskmanager.slot.timeout: 120 s` (no code change) | Failed runs produced no result files |
| 2026-11-05 | With no job running, TaskManagers still used ~0.1–0.15 cores, and TM logs showed a Beam `KafkaIO` *offset consumer* (`Reader-0_offset_consumer_…`) still polling after its task was CANCELED/FINISHED. **This is a thread leak in Beam's Kafka reader on cancellation.** It accumulates across runs and steals CPU from later runs on 1-core TaskManagers. | The harness now **restarts all TaskManager pods before every run** (fresh JVMs) and waits for 4 TMs / 8 free slots | All runs measured before this fix (10 completed runs of the 1000 el/s grid plus 2 placement runs) were moved to `results/E1_pilot_r1000_leaky_tms/` and are reported only as **pilot data**. The E1 grid was rerun from scratch. |
| 2026-11-05 | The JobManager pod loses the copied jar when it restarts | The harness re-copies the unmodified `FlinkJob.jar` if it is missing | None |
| 2026-11-05 | After the pause and cluster restart, the jar re-copy failed: `kubectl cp` on Windows reads `C:\…` as `pod:path` ("one of src or dest must be a local file specification"). One run (p1 rep 2) aborted before submission. | Copy from a relative path, with the working directory set to `etl/build` | None; the run was repeated |
| 2026-11-05 | Pause and resume: `k3d cluster stop` / `start` restarted every container (Kafka state is not persistent) | The harness recreates the topics before every run | None; each run is self-contained |
| 2026-11-05 (after E1) | **Latency-marker quantiles were stale.** Flink keeps the last `metrics.latency.history-size` = 128 markers per (source, operator) pair. At the 5 s marker interval that is ~640 s, longer than a 360 s run, so "post-warm-up" p50/p99 still contained JIT warm-up markers (the first L1 run showed p50 drifting 210 → 34 ms and p99 stuck at 1001 ms for the whole run) | Cluster config `metrics.latency.history-size: 60` + job option `--latencyTrackingInterval=1000` (harness option `--latency-interval 1000`) = 60 s quantile window; latency summarised only over samples ≥ warm-up + 60 s (`*_clean` fields) | **E1 latency columns are affected** (they were not interpreted; see E1_RESULTS §1 note). E1 throughput/CPU are not. First L1 run kept as `results/L1_pilot_latency_window128/` |
| 2026-11-05 (after E1) | REST metric values are cached by the JobManager for `metrics.fetcher.update-interval` = 10 s (default), so 5 s counter differences alternated between 0 and 2×; a Kafka-backlog estimate built from them showed a spurious 1.5k–10k sawtooth | `metrics.fetcher.update-interval: 1000` | E1 used 60 s-averaged `*PerSecond` meters over a 210 s window: not materially affected |
| 2026-11-05 (after E1) | Reading topic offsets with `kafka-get-offsets.sh` every 5 s started a JVM in the broker pod each time (~0.8 core) | Offsets read every 30 s during measured runs (every 5 s only in the producer-only history run) | Broker pod is not CPU-limited and TaskManagers have their own 1-core limits; no measurable effect expected |
| 2026-11-05 21:30 | **Host-side jobs slow the cluster.** The k3d nodes run inside Docker Desktop's WSL2 VM on the same Windows host, so CPU-heavy host processes compete with them. L1b FD rep 1 (overlapping STREAMLINE's decision runs on the host) deployed in 99 s instead of 31 s, used 1.18 ms CPU per record instead of ~0.94, never caught up with its start-up backlog (mean 22,773 records vs 1,831 in the identical L1 run) and showed a true e2e p50 of 22 s. Overlaps found from the session log (UTC+4): TAXI preparation 19:31–19:42 and TAXI conversion container 19:43–19:50 → profiling runs p1_seg16384, p1_seg32768; STREAMLINE training/decisions 21:12–21:38 and a TAXI parser check 21:33 → L1 p8 rep 1 re-run, L1b p1 reps 1–2 | (1) These 5 runs moved to `results/contaminated_hostload/` and re-measured (`scripts/chain_after_hostload_fix.sh`); (2) from now on nothing CPU-heavy runs on the host during measurements, and STREAMLINE train/decide runs while the cluster is idle; (3) both harnesses now record host load per sample (`s["host"]`: total cores, Docker VM cores, other cores; summary `host_other_cores_mean/max`). Unrelated user applications (e.g. Notion, Edge, Defender: ~1.5–4.5 cores in spot checks on a 20-thread host) cannot be controlled and are now visible in that record | Contaminated runs are kept but not used. Earlier runs could not be checked against a host-load record; the session log shows no heavy host jobs during E1, L1 (17:30–18:30), the other profiling runs or the history run. **Later finding (§13):** the laptop was also on battery 19:33–21:27, which overlaps 4 of these 5 runs and 4 further profiling runs; see §13 |

## 8. Structural properties of the authors' ETL benchmark that cap parallel speed-up (verified in code)

1. `etl/.../transforms/Interpolate.java` keys **every** record with the same constant, `WithKeys.of("")`, before `GroupIntoBatches`. So the whole second vertex (interpolation → annotation → serialisation → Kafka and MongoDB sinks) runs on **one** subtask, whatever the parallelism. Measured: at p = 4–8, one Interpolate subtask is at 727–990 ms/s busy while the vertex average is 93–250 ms/s.
2. The input topic has **1 partition** (as in the authors' chart), so only one source subtask receives data.

Consequence: in this benchmark, extra parallelism can help only by putting the two hot subtasks (the active source and the single keyed subtask) on **different** TaskManagers, i.e. separate CPUs. It cannot spread work. This bears directly on how the paper's ETL gains from tuning parallelism can arise, and is discussed in the report.

## 9. Defect in the authors' Kafka producer (verified in code and in measurements)

`kafkaProducer/.../Main.java` starts `THREAD_COUNT` threads, each `new EventProducer(..., initialOffset = i, stride = THREAD_COUNT, ...)`, and
`EventProducer` sends one line, then calls `reader.skip(STRIDE)` before `readNext()`. Thread *i* therefore sends lines i, i+5, i+10, …:
with 4 threads, **lines with index ≡ 4 (mod 5) are never sent**, i.e. only 80% of the dataset is replayed and the effective rate is
0.8 × the rate encoded in the file (off-by-one: the stride should be `THREAD_COUNT − 1`). Measured: nominal 1000 el/s → 785–816 el/s
ingested (first L1 run); nominal 4000 → 2630 el/s (there the producer is additionally CPU-bound; 3200 would be expected).
The producer is used unmodified; all E3 rates are reported as **effective** (ingested) rates, and workload files are generated
with nominal = effective / 0.8.

## 10. E3 profiling: measurement findings and how the STREAMLINE table is built (`scripts/build_streamline_dataset.py`)

Profiling grid: 12 runs, p ∈ {1, 2, 4, 8} × segment size ∈ {16, 32, 64 KiB}, stepped workload `e3_profile_steps`
(150 s warm-up, then 60 s steps at 300…2000 el/s effective), 4 TaskManagers, results in `results/E3_profile/`. Completed 2026-11-05, 0 failures.

1. **Throughput is set by slot placement, not by p.** Per run, the active source subtask (1 partition) and the single hot keyed
   subtask (§8) are either on the same 1-core TaskManager or not. Best 30 s throughput: co-located 898–1157 el/s
   (all p = 1, p = 2, and 2 of 3 p = 4 runs); separate 1415–1726 el/s (1 of 3 p = 4 runs, all p = 8 runs). Flink's slot
   allocation decides this; STREAMLINE's features (TP_in, parallelism, segment size, operator type) cannot represent it.
2. **Stale latency histograms.** Flink exposes one marker histogram per (operator, subtask). In every run, 6 operators of every
   keyed subtask stop receiving markers and keep their last value forever (6·p frozen histograms). At p ≥ 4 the frozen
   values (up to 93 ms) exceeded the live maximum, so "max over operators" reported a constant. **Fix (dataset only):** a
   histogram is used only if its value changed within the last 60 s (the marker window). The `*_clean` latency fields
   written by the harness still use all histograms; use the dataset builder's rule when interpreting E3 latency.
3. **Markers exclude Kafka waiting time.** Latency markers are emitted by the source, so records queued in Kafka when the job is
   overloaded are not counted. In overloaded steps, marker latency stays bounded (in-flight network buffers, so it grows with
   segment size: p = 4 co-located 126 ms at 32 KiB, 254 ms at 64 KiB) while true latency grows without bound. Overload is
   visible to STREAMLINE only through TP_in < offered rate and Busytime → 1 (its PolynomialRegression capacity model).
4. **CPU attribution.** Per TaskManager: a TM hosting only the active source subtask → v_0; only the hot keyed subtask → v_1;
   both → split by the per-record CPU costs fitted on the separate-placement runs (v_0 0.18 ms/record, v_1 0.42 ms/record,
   so v_0 gets 30%); TMs hosting neither (idle subtasks, ~0.03–0.05 cores each) → split by subtask counts. Busy time cannot
   be used: Beam's legacy source is reported as never idle, and two co-located vertices both report wall-clock busy time
   while waiting for one core (summed busy reached 1.12 per core before capping).
5. **Marker windows are diluted by parallelism.** Flink routes each latency marker to one *random* downstream channel, so each of
   the p keyed subtasks receives ~1/p of the markers, and its 60-marker history spans ~60·p s (480 s at p = 8, longer than a
   run). Marker quantiles at p ≥ 4 therefore still contain warm-up markers; this explains the 0.8–6.7 s marker p95/p99 at
   p = 4/8 in L1 (unchanged by the liveness filter). True end-to-end latency (L1b, E3 evaluation `--e2e`) is the latency
   measure used for conclusions; marker latency is kept only as STREAMLINE's training feature, as the authors' approach uses it.
6. Busytime is capped at 1 (counter differences over slightly misaligned intervals gave up to 1.14).

Resulting table: `results/E3_work/historical_data_etl.csv`, 978 rows (489 intervals × 2 vertices) after the two
host-contaminated profiling runs were re-measured (§7; the first build had 976 rows); per-run report in
`results/E3_work/dataset_report.json`.

STREAMLINE training (`repro_scripts/e3_decide.py train`, Python 3.8 venv `env/`) ran on the host with 2 torch threads at
below-normal priority **while L1 runs (p = 8 re-run / L1b) were measuring on the cluster**; this may add host-level noise to
those L1 runs and is noted with their results. **Resolved:** the affected runs were archived and re-measured, and the final
models (`results/E3_work/models_main/`) were trained by `scripts/chain_after_hostload_fix.sh` with the cluster idle (§7).

## 11. Kafka-backlog estimate is not usable as a latency measure (found with L1b, 2026-11-05)

L1b measured true end-to-end latency (broker LogAppendTime, order-matched, order check 0.98–1.0) at ~780 el/s: p50 33–38 ms,
p95 105–115 ms in every clean run (p = 1, 4, 8). The same runs report `kafka_backlog_mean` 2,000–3,000 records, which by
Little's law would mean ~3 s of waiting (`kafka_wait_ms_est`). The two cannot both be right: the input end offset is read by
`kafka-get-offsets.sh`, a JVM that takes seconds to start, so `input_end` is taken later than the `consumed_total` it is
compared with, and the "backlog" is mostly records produced during that delay. `kafka_backlog_*` and `kafka_wait_ms_est`
are therefore not used for conclusions; true e2e latency (`--e2e`) is.


## 12. Deploy-time RPC timeouts and the E3 evaluation retry (2026-11-06)

**What happened.** Two of the nine E3 evaluation runs failed: `p8_tm4_seg65536` repetitions 2 (23:11) and 3 (00:25).
Both jobs reached FAILED about 40 s after submission. The root exception is `TimeoutException: Invocation of
JobMasterGateway.updateTaskExecutionState … timed out` (Pekko ask timeout 10 s). A TaskManager's report that a keyed
(Interpolate) subtask had switched to RUNNING went unanswered in time, so the subtask failed. With no restart strategy (as in the
authors' setup), the job failed before any measurement. The harness then crashed: the job-cancel call returned HTTP 409 because
the job was already terminal, so no record was written. The Flink exception of repetition 3 is saved in
`results/E3_eval/failed/p8_tm4_seg65536_rep3_flink_exception.txt`. Repetition 2's job had already expired from the JobManager's
1 h job store.

**Not specific to the chosen configuration.** The JobManager log covers 45 jobs (13:26–20:25 UTC). For each one we measured
the time from submission until the last keyed subtask was RUNNING. It grew over the session from ~10 s to 20–50 s (TaskManagers
are restarted before every run and must re-fetch the 206 MB jar). The jobs that failed are the ones where it exceeded ~32 s with
an RPC still pending: 3 of 45. One was a **p = 4** job, L1b p4 rep 2 (17:57 UTC; the deploy-retry logic of §7 resubmitted it and the run succeeded, `deploy_attempts` in its record), and two
were the p = 8 / 64 KiB evaluation runs. Successful jobs needed up to 55 s, so slow deploys alone do not cause a failure; we
did not find what made these three RPCs time out.

**Fix (harness only).** `run_e3_experiment.py` now catches the HTTP 409 on cancel. If the job is not RUNNING at the end, it
writes the full record to `<out>/failed/<label>_<t0>.json` and exits non-zero. The grid then re-runs the missing labels
(`logs/grid_E3_eval_retry.log`). The cluster configuration was **not** changed (no larger `pekko.ask.timeout`), so all
evaluation runs share one configuration.

**Later finding (§13):** the two retry runs overlapped Modern Standby and were re-measured again.

**Data impact.** A failed deployment produces no measurement; it is a start-up failure, not an outcome of the configuration
under load. It is still reported in E3_RESULTS as a deployment failure count per configuration.

## 13. Battery power and Modern Standby during measurements (found 2026-11-06)

**Finding.** Windows' Kernel-Power log (event 105 = power-source change with `AcOnline`; 506/507 = Modern Standby enter/exit)
shows that the laptop was **on battery** during these intervals:
- 11-05 09:28–12:14
- 11-05 12:23–14:16
- 11-05 19:33–21:27
- 11-06 05:52–06:35

It was in **Modern Standby** 11-06 00:42–05:47 and 05:57–06:37. The grid runners' `ES_SYSTEM_REQUIRED` flag did not prevent
standby. Each run's window was estimated from its file time and duration, and checked against these intervals
(scratchpad `power_overlap.py`).

**Battery throttles the cluster measurably.** CPU per processed record was 25–40% higher on battery:
- E1 (saturated runs): 1.08–1.26 ms/record on battery (all repetition 1 runs) vs 0.92–0.98 on AC.
- E3 profiling: 1.41–1.56 vs 1.05–1.15 ms/record.

This **explains the E1 "session effect"**. Repetition 1 (11:45–12:17) ran on battery and was ~20% slower in every
configuration; session 2 (from 14:26) ran on AC.

**Runs affected and what was done:**

| Runs | Power state | Action |
|---|---|---|
| E1 repetition 1 (p = 1, 2, 4, 8) | battery | moved to `results/battery_or_standby/E1_fit_r4000/` and re-measured on AC. The cluster configuration has changed since E1 (latency history 60, metrics fetcher 1 s, slot timeout 120 s, §7). These settings do not affect the throughput meters E1 uses |
| E3 profiling p4_seg16384, p4_seg32768, p4_seg65536, p8_seg32768 | battery. **These were in STREAMLINE's training table**; all p = 4 rows came from battery runs | moved to `results/battery_or_standby/E3_profile/`, re-measured. Table rebuilt and STREAMLINE retrained and re-decided. The previous models are kept as `results/E3_work/models_battery_superseded/` |
| E3 evaluation p8_tm4_seg65536 reps 2 and 3 (retry runs) | Modern Standby for ~170 s and ~470 s of the run windows. The samples show no gaps and the results looked normal, but the power state differs | moved to `results/battery_or_standby/E3_eval/`, re-measured |
| L1b p1 rep 3 | battery for ~230 s (p50 1035 ms, TM CPU 1.04 cores at 780 el/s vs 0.86–0.91 on AC) | moved to `results/battery_or_standby/L1b_e2e_r1000/`, re-measured. L1b p4/p8 rep 3 failed in standby (REST connection reset/timeouts) and produced no files |
| Runs in `results/contaminated_hostload/` (§7) | 4 of the 5 were also on battery | already excluded. Host load and battery power coincided (19:31–21:29), and their contributions cannot be separated. The one contaminated run on AC (L1b p1 rep 2, 21:2x) had normal latency (p50 38 ms) |
| Pilot and design-check runs (smoke, `E1_pilot_r1000_leaky_tms`, `E1_designcheck_r1000_warmup60`, `capacity_probe`) | battery (09:45–11:44) | pilot only, not used for results. The 150 s warm-up was chosen from design-check runs on battery. AC runs reach a steady state well before 150 s, so the warm-up remains conservative |

Every other run used for results (E1 reps 2–3, L1, L1b reps 1–2 and the p1 re-runs, the other 8 profiling runs, E3 evaluation
FD/FMAX and seg65536 rep 1) ran on AC with no standby event in its window.

**Guard (harness only, `scripts/power_guard.py`, used by `run_grid.py` and `run_e3_grid.py`):**
- `ES_DISPLAY_REQUIRED` is added, so the display stays on and the display timeout cannot start Modern Standby. Chains also run `scripts/keep_awake.py` between grids.
- Each run waits for AC power before starting.
- After each run, the Kernel-Power log is read for the run window. A run with any battery or sleep/standby event is moved to `<out>/power_invalid/` with a `.power.json` sidecar, and repeated (max 3 attempts).
- Valid result files get a `power` record (AC at start/end, events = []).

Closing the lid can still suspend the machine; the laptop must stay open and plugged in during measurements.

## 14. E3-T: STREAMLINE on TAXI's real arrival pattern (2026-11-06 evening)

**Workload files** (`scripts/make_taxi_workload.py`; payloads unchanged, in pickup order):

| File | Content |
|---|---|
| `e3t_taxi_hist` | Real trips Mon 2013-01-14 00:00 – Fri 14:00 (real seconds 0–396,000), delays divided by 220, so 1,800 s. This is the authors' own time scaling: converter `SCALING` = 1/220. 2,200,873 events |
| `e3t_taxi_eval` | Real trips Fri 2013-01-18 04:50 – Sat 05:17 (363,000–451,000 s), same scaling, 400 s. The first 150 s are warm-up; the horizon is the last 250 s = Fri 14:00 – Sat 05:17, which follows the history directly. 599,423 events. Effective rate in the horizon (× 0.8): 279–1,752 el/s, mean 1,278; 25 of 50 bins above FD's TAXI capacity (1,337), none above FMAX's (1,956) |
| `e3t_profile_steps` | STREAMLINE profiling load: 150 s at 1,100 nominal, then 60 s steps at 500, 1,000, 1,500, 2,000, 2,500 and 3,200 nominal. Same structure as FIT's `e3_profile_steps`, with higher steps because TAXI capacity is higher (E1-TAXI) |

**Why 220:** it puts the effective peak (1,752 el/s) between FD's and FMAX's measured TAXI capacity, as the FIT sine did for FIT. It also makes the history exactly 1,800 s, as in E3. The paper's 1,000–10,000 el/s implies a scaling of about 1/1000 on its larger cluster, but our producer cannot offer more than about 2,700 el/s without disturbing the measurement (§5).

**Harness changes** (`scripts/run_e3_experiment.py`; the backup of the previous version is in the session scratchpad; FIT defaults unchanged):
- `--dataset TAXI` sets `--experiRunId=TAXI` (the authors' option).
- `--drain S`: after the producer stops, the job keeps running until the output topic has all input records except at most 4, or S s pass. The 4 are records `GroupIntoBatches(5)` can hold in an incomplete batch. Without it, records still queued at run end (FD under overload) are missing from the latency statistics, which censors exactly the worst latencies. E3 on FIT had no drain; its FD latencies are therefore lower bounds.
- `--trace-full`: writes `<label>.e2e_trace.csv.gz` with every input record (broker time in, broker time out, latency). Records with no output are marked by empty fields.
- Every run writes `<label>.placement.log`: the REST subtask → TaskManager map, TaskManager pod names and IPs, and the JobManager's own log lines for the job (DEPLOYING, slot allocation).
- **TAXI order check:** both topics carry SenML JSON, and the input values start with a space, so the sampled-value filter now strips leading spaces. Records are matched on (taxi_identifier, hack_license, pickup_datetime). A smoke run gave match fraction 1.0 on all three slices (`results/E3T_work/harness_smoke/`, smoke only).

**Pipeline** (`scripts/chain_e3_taxi.sh`): history run (power-guarded) → 12 TAXI profiling runs → `build_streamline_dataset.py` → STREAMLINE train + decide (V0, V4; 11 seeds each; cluster idle) → `make_e3_eval_spec.py` (now takes an output dir, workload and extra harness args) → evaluation of FD, FMAX and every chosen configuration, 3 reps each, `--e2e --drain 300 --trace-full`. `analyse_e3.py` reads the evaluation workload from `E3_WORKLOAD` (default `e3_sine_eval`).

**Correction (same day):** the released workload predictor is a Transformer encoder (`TimeSeriesTransformer` in `workload_predictor.py`), not an LSTM. E3_RESULTS and the Week 6 proposal said "LSTM" and were corrected.

## 15. Integrity check of the authors' repositories (2026-11-06, 21:05)

Before packaging the repository, `git status` was run in both authors' clones.

- `Streamline` (d65baec): clean.
- `beam-applications-java` (57524e2): `gradle/wrapper/gradle-wrapper.jar`, `gradle-wrapper.properties`, `gradlew` and `gradlew.bat`
  had been rewritten to Gradle 9.7.1 at 2026-11-06 12:29, together with IDE output folders (`*/bin`, created 12:33). Nothing in this
  project's scripts or transcript ran `gradle wrapper`; the timing matches an IDE (VS Code Java/Gradle extension) project import.
- Effect on results: none. Every artefact used in the experiments was built on 2026-11-05 with the repository's own Gradle 8.6 wrapper:
  `etl/build/FlinkJob.jar` (08:58) and `kafkaProducer/build/KafkaProducer.jar` (09:00). Neither was rebuilt afterwards.
- Action: the four wrapper files were restored with `git checkout --` to commit 57524e2. The clone now differs from the commit only by
  untracked files we added (replay schedules and their `.meta.json` under `data/`, build outputs), which are git-ignored or listed here.

# Reproducing STREAMLINE on a small Kubernetes cluster

Reproduction of the ETL experiments (§5.1.1) of

> S. Pedratscher et al., *STREAMLINE: Dynamic and resource-efficient auto-tuning of stream processing data pipeline ensembles*,
> Internet of Things 34 (2025) 101731. https://doi.org/10.1016/j.iot.2025.101731

AI508 Big Data Technologies and Applications, Assignment 2, The British University in Dubai (2026).
Team: Hamza Alqaruoti (team leader), Mohammad Alqaryouti, Noura Alrahbi, Sulaiman Shawabkeh.

The report is in [`report/`](report/). This repository holds everything behind its numbers: the measurement harness, the
data-preparation and analysis scripts, the cluster manifests, every raw result file (including runs we excluded and why), and the
modification log.

## The original authors' code is included unmodified

| Submodule | Upstream | Pinned commit | Role |
|---|---|---|---|
| [`Streamline/`](Streamline) | https://github.com/UIBK-DPS-DC/Streamline (GPLv3) | `d65baec` | STREAMLINE decision layer (forecaster, surrogate, NSGA-II, scheduler) |
| [`bigdata/beam-applications-java/`](bigdata/beam-applications-java) | https://github.com/DPS-Stream-Processing-Applications/beam-applications-java | `57524e2` | RIoTBench ETL pipeline on Apache Beam, data converter, Kafka producer |

We never edit either repository. Every fix, workaround or alternative behaviour lives in our own scripts and is listed, with its
reason, in [`bigdata/ENVIRONMENT_AND_MODIFICATIONS.md`](bigdata/ENVIRONMENT_AND_MODIFICATIONS.md).

```bash
git clone --recurse-submodules <this repository>
```

## Layout

| Path | Contents |
|---|---|
| `report/` | Final report (Markdown source, Word/PDF, figures) |
| `bigdata/ENVIRONMENT_AND_MODIFICATIONS.md` | Environment record and modification log: every deviation from the authors' setup and why |
| `bigdata/BIGDATA_REPRODUCTION_PLAN.md` | Scope, experiments and their status |
| `bigdata/E1_RESULTS.md`, `E1_TAXI_RESULTS.md`, `L1_RESULTS.md`, `E3_RESULTS.md`, `E3T_RESULTS.md` | Per-experiment write-ups with the measured numbers |
| `bigdata/k8s/` | Kafka, MongoDB and Flink manifests for k3d |
| `bigdata/scripts/` | Harness, workload generators, grid and chain runners, analysis and figure scripts (see below) |
| `bigdata/results/` | Raw per-run JSON (per-sample metrics, placement, host load, power state), summaries, placement logs and per-record latency traces |
| `bigdata/logs/` | Grid, chain, build and training logs |
| `repro_scripts/` | STREAMLINE-side scripts: code audit probes, the 2³ correction study, and training/decisions on our measurements (`e3_decide.py`) |
| `correction_study/` | Correction-study runs, logs and report (`CORRECTION_STUDY_REPORT.md`) |
| `SMOKE_TEST_REPORT.md` | First run of the released STREAMLINE examples (E0) |

### Experiments → files

| ID | Question | Spec / runner | Results | Analysis |
|---|---|---|---|---|
| E0 | Does the released decision layer work, and is it correct? | `repro_scripts/*.py` | `correction_study/` | `repro_scripts/analyse_correction_study.py` |
| E1 | Saturated capacity, FD vs FMAX (FIT) | `scripts/run_grid.py` + `run_fit_experiment.py` | `results/E1_fit_r4000/` | `scripts/analyse_e1.py` |
| E1-TAXI | Same on TAXI | as E1 | `results/E1_taxi_r3500/` | `scripts/analyse_e1.py` |
| L1 | Latency below capacity | as E1 | `results/L1b_e2e_r1000/` (true e2e), `results/L1_fit_r1000/` (markers) | `scripts/analyse_l1.py` |
| E3 | STREAMLINE V0/V4 vs FD/FMAX, FIT sinusoid | `scripts/e3_*_spec.json` + `run_e3_grid.py` | `results/E3_profile/`, `E3_work/`, `E3_eval/` | `scripts/analyse_e3.py` |
| E3-T | Same on TAXI's real arrival pattern | `scripts/chain_e3_taxi.sh` | `results/E3T_*` | `E3_WORKLOAD=e3t_taxi_eval scripts/analyse_e3.py` |

Folders such as `results/battery_or_standby/`, `results/contaminated_hostload/` and `*_pilot*` hold runs that were **excluded** from the
report (power state, host load or a design check). They are kept so the exclusions can be audited; the reason for each is in the
modification log.

## Environment

One Windows 11 laptop (Intel Core Ultra 7 255HX, 31 GB RAM), Docker Desktop (WSL2), k3d (k3s v1.35),
Flink 1.18.1 with 4 TaskManagers × (1 CPU, 2 GiB, 2 slots), Kafka 3.7.0 (1 broker), MongoDB 7, Temurin JDK 11 for the Gradle build,
Python 3.8 + PyTorch 2.4.1 (CPU) + pymoo 0.6.1.2 for STREAMLINE. Full details and versions: modification log §1–§3.

Not in the repository (size or licence; how to recreate them is in the modification log):

- `env/` (Python virtual environment; `pip install -r Streamline/requirements.txt`);
- `bigdata/tools/` (portable JDK, Helm, k3d binaries);
- the datasets: UCI MHEALTH (`bigdata/data/mhealth_raw/`) and the 2013 NYC taxi data (`beam-applications-java/data/`),
  plus the replay files generated from them by `scripts/make_*.py`;
- build outputs (`FlinkJob.jar`, `KafkaProducer.jar`) — build with the submodule's own Gradle 8.6 wrapper;
- correction-study model checkpoints (regenerate with `repro_scripts/train_checkpoints.py`).

The MongoDB credential in `bigdata/k8s/01-mongodb.yaml` is the one hard-coded in the authors' benchmark; it only exists inside the
local cluster.

## Re-running

1. Create the k3d cluster and apply `bigdata/k8s/*.yaml` (modification log §2).
2. Build the benchmark jars inside `bigdata/beam-applications-java` with `./gradlew` (Gradle 8.6, JDK 11).
3. Prepare the datasets (`scripts/prepare_taxi.py`, the authors' `senMlConverter`) and replay files (`scripts/make_*.py`).
4. Run a grid, e.g. `python scripts/run_grid.py ...` or `bash scripts/chain_e3_taxi.sh`, with the laptop on mains power: every
   measured run goes through `scripts/power_guard.py` and records host load and power state.
5. Analyse with `scripts/analyse_*.py`; figures with `scripts/make_report_figures.py OUT_DIR`.

Absolute rates depend on the host. The report compares relative outcomes (ratios between configurations).

## Licence

Our own code is released under the GNU GPL v3 (see `LICENSE`), the licence of the STREAMLINE code it imports. The submodules keep
their own licences. The datasets keep the terms of their sources (UCI MHEALTH; Donovan & Work, NYC taxi trip data 2010–2013,
doi:10.13012/J8PN93H8).

## Use of AI tools

Claude (Anthropic), through Claude Code, was used to set up the environment, write and review scripts, audit the authors' code
and draft documentation. Every number in the report was checked against the raw files in `bigdata/results/`.

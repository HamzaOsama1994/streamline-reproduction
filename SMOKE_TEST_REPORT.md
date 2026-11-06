# STREAMLINE — Smoke test of the public code (Stage 2–3 evidence)

**Paper:** Pedratscher et al., "STREAMLINE: Dynamic and Resource-Efficient Auto-Tuning of Stream Processing Data Pipeline Ensembles", *Internet of Things* 34 (2025) 101731, DOI 10.1016/j.iot.2025.101731.
**Code:** https://github.com/UIBK-DPS-DC/Streamline, commit `d65baecfd19ee7f06e181e36056b8aab8cc427c0` (2026-03-09, "added reference"), GPLv3.
**Date of test:** 2026-11-04. The authors' files were **not modified**. `git status` in `Streamline/` is clean apart from the PNGs the examples write, which were moved to `logs/`.

## 1. Environment

| Item | Value |
|---|---|
| Machine | Intel Core Ultra 7 255HX (20 threads), 31.4 GB RAM, RTX 5070 Ti Laptop 12 GB (not used: CPU-only torch), Windows 11 (build 26200) |
| Python | CPython 3.8.20 in an isolated venv `streamline_repro/env`, created with `uv venv --python 3.8`. System Python 3.14 untouched |
| Pinned deps (requirements.txt) | torch 2.4.1 (+cpu), numpy 1.24.3, pymoo 0.6.1.2, scikit-learn 1.3.2, matplotlib 3.3.2 |
| Added | pandas 2.0.3. It is imported by both examples but missing from requirements.txt |
| Run settings | `MPLBACKEND=Agg`, `PYTHONUNBUFFERED=1`, working dir = repo root (the examples use relative paths) |
| Full freeze | `logs/00_pip_freeze.txt`. Install log: `logs/00_install.log` |

Reproduce:
```bash
uv venv --python 3.8 env
uv pip install --python env/Scripts/python.exe -r Streamline/requirements.txt pandas
cd Streamline && MPLBACKEND=Agg ../env/Scripts/python.exe examples/single_pipeline.py
```

Setup notes:
- The README tells users to run `python3.8`.
- Under Python 3.9 the pin matplotlib==3.3.2 has no Windows wheel and needs a C++ compiler. Python 3.8 installs cleanly.

## 2. Runs

| # | Script | Code | Exit | Wall time | Log |
|---|---|---|---|---|---|
| 1 | `examples/single_pipeline.py` | original | 0 | 197 s (incl. one-off matplotlib font cache build) | `logs/01_single_pipeline_run1.log`, figures in `logs/01_single_pipeline_run1_figs/` |
| 2 | `examples/ensemble.py` | original | 0 | 270 s | `logs/02_ensemble_original.log`, figures in `logs/02_ensemble_original_figs/` |
| 3a | `repro_scripts/ensemble_bugcheck.py` | replica, original key | 0 | 235 s | `logs/03a_…_PERTURBED_by_diag.log`. **Invalid**, see §4.2 |
| 3b | `repro_scripts/ensemble_bugcheck.py` | replica, original key, RNG-safe diagnostics | 0 | 212 s | `logs/03b_ensemble_bugcheck_original.log` |
| 4 | `repro_scripts/ensemble_bugcheck.py --fix` | replica, key corrected to `v_5` | 0 | 208 s | `logs/04_ensemble_bugcheck_fixed.log` |
| 5 | `examples/single_pipeline.py` | original, repeat | 0 | 172 s | `logs/05_single_pipeline_run2.log` |
| 6 | `repro_scripts/predictor_probe.py` | authors' class, alternative inference calls | 0 | ~60 s | `logs/06_predictor_probe.log` |

No errors or exceptions occurred in any run.

## 3. Results

### 3.1 Training metrics

| Model | Pipeline 1 (single and ensemble) | Pipeline 2 (ensemble) | README reference |
|---|---|---|---|
| Transformer workload predictor (MSE, epochs 0 → 100 → 200) | 1.2220 → 0.0130 → 0.0052 | 1.0439 → 0.0131 → 0.0056 | 1.2220 → 0.0130 → 0.0052 |
| Surrogate NN, epoch 200 (train / val loss) | 0.0027 / 0.0075 | 0.0025 / 0.0056 | 0.0027 / 0.0075 |
| Surrogate NN R² (train / val) | 0.96 / 0.90 | 0.96 / 0.90 | 0.96 / 0.90 |
| Polynomial regression | 4 models (operator types 0–3) | 3 models (types 5–7) | — |

The seeded training **reproduces the README exactly** for pipeline 1.

### 3.2 `ensemble.py`: configurations from NSGA-II and the surrogate, against the README

| Step | Operator | Ours: parallelism / segment / est_tp | README |
|---|---|---|---|
| Initial, pipeline 1 | v_0 | 4 / 16384 / 1718.75 | 4 / 16384 / 1716.01 |
| | v_1, v_2 | 4 / 16384 / 859.38 | 4 / 16384 / 858.00 |
| | v_3 | 4 / 16384 / 2315.86 | 4 / 16384 / 2315.86 |
| Initial, pipeline 2 | v_5, v_6 | 5 / 16384 / 917.25 | 5 / 16384 / 916.02 |
| | v_7 | 5 / 16384 / 2013.76 | 5 / 16384 / 2002.68 |
| Periodic, pipeline 1 | v_0 | 2 / 16384 / 277.76 | 2 / 16384 / 279.60 |
| | v_1, v_2 | 2 / 16384 / 140.21 | 2 / 16384 / 141.17 |
| | v_3 | 4 / 16384 / 934.59 | 4 / 16384 / 938.55 |
| Periodic, pipeline 2 | v_5–v_7 | 5 / 16384 (unchanged) | 5 / 16384 (unchanged) |

**Schedules:** identical operator-to-resource placements to the README at every step. The initial pipeline-1 placement uses R1 and R11, pipeline 2 adds R11 and R12, and periodic tuning removes R1.
**Final utilisation:** R11 0.3514 / R12 0.6768 (README: 0.3527 / 0.6774).
**Assessment:** every decision (parallelism, segment size, placement) matches. Continuous estimates differ by ≤0.6%, most likely because of platform and BLAS numerical differences (the README output was presumably produced on Linux).

`single_pipeline.py` produced p = 3/3/2/2, then 4/4/4/4 after periodic tuning, on a single resource (R11, final utilisation 0.7308). The README shows no output for this example, so there is nothing to compare against.

**Determinism:** run 5 is identical to run 1 line for line, apart from timestamps.

### 3.3 Execution-time profile (CPU, from log timestamps)

| Phase | single_pipeline | ensemble |
|---|---|---|
| Model training (Transformer + NN + polynomial regression) | ~44 s | ~97 s (two pipelines) |
| NSGA-II per operator (pop 20, 20 generations, initial population 99) | 10–13 s | 10–15 s |
| One full tuning round | ~44 s (4 operators) | ~79 s (7 operators) |

The NSGA-II search evaluates the surrogate model one configuration at a time (`ElementwiseProblem`), so the cost grows linearly with the number of operators. The paper re-evaluates every **5 minutes**. *Extrapolation, not measured:* on this CPU, a 25-pipeline ensemble with about 150 operators would need roughly 25–30 min per tuning round. Its online overhead should therefore be measured on the paper's ensemble sizes.

## 4. Defects found in the public code

### 4.1 `examples/ensemble.py`: workload update written to the wrong source key (confirmed)

- **Code:** pipeline 2 is created with `observed_source_workloads = {"v_5": …}`, but the periodic update writes `pipeline_2["observed_source_workloads"]["v_0"]`.
- **Original behaviour (run 3b, identical to run 2):** `Controller.predict_workload()` reads only the pipeline's declared sources (`v_5`), so the new data is **silently ignored**. Pipeline 2's input window is unchanged (mean 1026.95, last value 2000.00) and it keeps parallelism 5 on R11 and R12. The README's statement that "no updates are necessary for the second data pipeline" is an artefact of this bug.
- **Corrected behaviour (run 4):** the window becomes the new workload (mean 1024.95, last value 1025.00). The forecast mean drops from 812 to 291 events/s, all pipeline-2 operators go from p = 5 to p = 2, and a full reschedule puts everything on **one** resource (R11, utilisation 0.6173).

| Variant | Resources used | Example cost ($/h, from `utils.get_resources`) | Pipeline 2 parallelism |
|---|---|---|---|
| Original key `v_0` | R11 (0.351) + R12 (0.677) | 0.768 | 5 / 5 / 5 |
| Fixed key `v_5` | R11 (0.617) | 0.384 | 2 / 2 / 2 |

In this toy example, the bug doubles cost and hides the adaptive behaviour the paper claims.

### 4.2 `WorkloadPredictor.predict()` runs with dropout active (confirmed)

- `train_()` leaves the model in `train()` mode, and `predict()` never calls `model.eval()`. The `TransformerEncoderLayer` uses PyTorch's default dropout of 0.1, so **forecasts are random**, and every extra `predict()` call shifts the random-number stream for all later decisions.
- **Evidence:** in run 3a, two diagnostic `predict()` calls on an *identical* window gave forecast means of 812.06 and 917.43. They also changed pipeline 2's configuration (p = 2 instead of 5), so that run is invalid. Saving and restoring the torch RNG state (run 3b) restored exact agreement with run 2.
- **Probe (run 6, one sine series, 20 calls):** under the upstream behaviour, forecasts vary by about ±95 events/s on average, roughly 6% of the mean true load of 1644. In `eval()` mode the variation is 0.

### 4.3 `WorkloadPredictor.predict()` feeds unscaled input (confirmed)

- Training uses a MinMax scaler on [0, 1], but `predict()` passes the raw window (about 50–2000 events/s) to the network and only inverse-scales the output.
- **Probe (run 6):** forecast MAE against the true continuation is 79–96 with raw input and 20–28 with scaled input, about **3–4× worse**. The pipeline-2 window in run 3b produced forecasts down to **−370 events/s**, a physically impossible workload.

### 4.4 Minor issues

- `best_model = model` stores a reference, not a copy, so the "best epoch" tracking in `train_()` has no effect and the last epoch is always used. **Audited later (see `correction_study/CORRECTION_STUDY_REPORT.md`, Part A):** in the authors' examples the best training-loss epoch is the last one, so there is no effect. Under other seeds and the paper's 500-epoch setting, a different checkpoint is returned in 9/10 cases, but held-out forecast error shows no systematic difference. Classified as a **latent defect**, not a result-changing one.
- `pandas` is missing from `requirements.txt`.
- Output plots are written to the working directory (file name `polynomial_regeression_*.png`, misspelt in the code).
- The README's sample output matches `ensemble.py`, not `single_pipeline.py`.

**Implication for the reproduction:** the main experiments should run on the **unmodified** code, as the authors released it. Separate ablations with the fixes (§4.1–4.3) applied one at a time would give the Discussion section direct evidence on reproducibility. All fixes should live in our own repo or a fork, never in the authors' files.

## 5. Scope still not covered by the public code

These are unchanged from `../feasibility/Feasibility_STREAMLINE_vs_COSTREAM.md` §1.4:
- the Flink/Kubernetes adapter;
- the metrics harness (REST API, latency markers, CPU time per operator);
- profiling runs to produce real `historical_data` CSVs;
- Algorithm 5 (seamless stateful update);
- the FD, FMAX and ContTune baselines.

The examples only exercise the decision layer on synthetic sine workloads.

## 6. File index

```
streamline_repro/
  Streamline/                    upstream clone @ d65baec (unmodified)
  env/                           Python 3.8.20 venv (not for the shared repo; recreate from logs/00_pip_freeze.txt)
  repro_scripts/
    ensemble_bugcheck.py         replica of ensemble.py with --fix flag and RNG-safe diagnostics
    predictor_probe.py           dropout / input-scaling probe
  logs/                          00_* environment, 01–06 run logs, *_figs/ generated plots
  SMOKE_TEST_REPORT.md           this file
```

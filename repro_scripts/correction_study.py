"""Controlled correction study for the three confirmed STREAMLINE example/inference defects.

Factors (2^3 full factorial; upstream code at d65baec is never edited, fixes are applied here):
  K  workload key   0 = upstream ensemble.py (pipeline_2 update written to "v_0")   1 = corrected ("v_5")
  D  dropout        0 = upstream predict() (model left in train mode)              1 = model.eval() before predict
  S  input scaling  0 = upstream predict() (raw window fed to the network)          1 = window MinMax-scaled first

All runs load the SAME checkpoints (correction_study/checkpoints/models.pkl, trained once
exactly as ensemble.py does), use the same workload sequences and resource specs
(examples/utils.py), and the same seed set. Seed "orig" restores the RNG state saved right
after training, so V0/orig must reproduce the unmodified ensemble.py run bit-for-bit.

Run from the Streamline repo root:
  python ../repro_scripts/correction_study.py K D S SEED OUT_DIR [TORCH_THREADS]
"""
import json
import logging
import os
import pickle
import random
import sys
import time

import numpy as np
import torch

sys.path.append('src')
sys.path.append('examples')
import utils  # noqa: E402
from streamline import Streamline, WorkloadPredictor  # noqa: E402
from streamline import configuration_selector as cs_mod  # noqa: E402

K, D, S = (int(a) for a in sys.argv[1:4])
SEED = sys.argv[4]
OUT = sys.argv[5]
if len(sys.argv) > 6:
    torch.set_num_threads(int(sys.argv[6]))
NAMES = {(0, 0, 0): "V0_original", (1, 0, 0): "V1_key", (0, 1, 0): "V2_eval", (0, 0, 1): "V3_scaling",
         (1, 1, 1): "V4_all", (1, 1, 0): "V5_key+eval", (1, 0, 1): "V6_key+scaling", (0, 1, 1): "V7_eval+scaling"}
VARIANT = NAMES[(K, D, S)]

# ---------------- ground-truth workload generator (as in examples/ensemble.py) ----------------
N, H = 500, 50
STEP = 10 * np.pi / (N - 1)
gen = lambda f, idx: 50 + (f(STEP * idx) + 1) * (2000 - 50) / 2  # noqa: E731
SIN, COS, NSIN = np.sin, np.cos, (lambda t: -np.sin(t))
TRUTH = {("initial", "pipeline_1"): gen(SIN, np.arange(N, N + H)),
         ("initial", "pipeline_2"): gen(COS, np.arange(N, N + H)),
         # the real workload after the change is -sin for BOTH pipelines (that is what ensemble.py intends)
         ("periodic", "pipeline_1"): gen(NSIN, np.arange(N, N + H)),
         ("periodic", "pipeline_2"): gen(NSIN, np.arange(N, N + H))}
VALID_RANGE = (50.0, 2000.0)  # physical range of the generated workload

# ---------------- instrumentation ----------------
STATE = {"phase": None, "owner": {}, "forecasts": [], "solve_times": [], "reschedule_events": []}
_orig_predict = WorkloadPredictor.predict


def predict_variant(self, inp):
    if D:
        self.model.eval()
    else:
        self.model.train()  # upstream state after train_() (dropout active)
    x = np.asarray(inp, dtype=float)
    window_mean = float(x.mean())
    if S:
        x = self.scaler.transform(x.reshape(-1, 1)).flatten()
    out = _orig_predict(self, x)
    pipe = STATE["owner"].get(id(self), "unknown")
    truth = TRUTH[(STATE["phase"], pipe)]
    err = out - truth
    STATE["forecasts"].append({
        "phase": STATE["phase"], "pipeline": pipe, "window_mean": window_mean,
        "forecast_mean": float(out.mean()), "mae": float(np.abs(err).mean()), "rmse": float(np.sqrt((err ** 2).mean())),
        "n_negative": int((out < 0).sum()), "n_nonfinite": int((~np.isfinite(out)).sum()),
        "n_out_of_range": int(((out < VALID_RANGE[0]) | (out > VALID_RANGE[1])).sum()), "horizon": int(out.size),
        "forecast": [round(float(v), 3) for v in out]})
    return out


WorkloadPredictor.predict = predict_variant
_orig_solve = cs_mod.ConfigurationSelector.solve


def timed_solve(self, max_parallelism):
    t0 = time.perf_counter()
    r = _orig_solve(self, max_parallelism)
    STATE["solve_times"].append({"phase": STATE["phase"], "o_type": int(self.o_type), "seconds": time.perf_counter() - t0})
    return r


cs_mod.ConfigurationSelector.solve = timed_solve


class RescheduleCatcher(logging.Handler):
    def emit(self, record):
        if "Rescheduling" in record.getMessage():
            STATE["reschedule_events"].append({"phase": STATE["phase"], "msg": record.getMessage()})


logging.getLogger("streamline.controller").addHandler(RescheduleCatcher())


def snapshot(streamline, placement):
    used = {r: v for r, v in streamline.resources.items() if v["load"] > 1e-12}
    cfg = {k: {"parallelism": float(v["parallelism"]), "segment_size": int(v["segment-size"]),
               "est_tp": float(v["est_tp"]), "est_l": float(v["est_l"]), "est_cpu": float(v["est_cpu"])}
           for k, v in sorted(streamline.configuration_parameters.items())}
    return {"placement_returned": placement,
            "resources_used": {r: {"load": float(v["load"]), "cores": v["cores"], "utilisation": float(v["load"] / v["cores"]), "cost": v["cost"]}
                               for r, v in sorted(used.items())},
            "n_resources": len(used), "hourly_cost": round(sum(v["cost"] for v in used.values()), 6),
            "total_instances": int(sum(c["parallelism"] for c in cfg.values())), "configuration": cfg}


# ---------------- run ----------------
with open("../correction_study/checkpoints/models.pkl", "rb") as f:
    M = pickle.load(f)

if SEED == "orig":
    torch.set_rng_state(M["rng_after_training"]["torch"])
    np.random.set_state(M["rng_after_training"]["numpy"])
    random.setstate(M["rng_after_training"]["random"])
else:
    s = int(SEED)
    torch.manual_seed(s); np.random.seed(s); random.seed(s)

pipeline_1 = {"structure": utils.get_pipeline_1(), "observed_source_workloads": {"v_0": gen(SIN, np.arange(N))},
              "transformer_model": {"v_0": M["t1"]}, "neural_network": M["nn1"], "polynomial_regression": M["pr1"]}
pipeline_2 = {"structure": utils.get_pipeline_2(), "observed_source_workloads": {"v_5": gen(COS, np.arange(N))},
              "transformer_model": {"v_5": M["t2"]}, "neural_network": M["nn2"], "polynomial_regression": M["pr2"]}
import pandas as pd  # noqa: E402
pipeline_1["historical_data"] = pd.read_csv("examples/data/historical_data_pipeline_1.csv")
pipeline_2["historical_data"] = pd.read_csv("examples/data/historical_data_pipeline_2.csv")
STATE["owner"] = {id(M["t1"]): "pipeline_1", id(M["t2"]): "pipeline_2"}

t_start = time.perf_counter()
streamline = Streamline(deltaT=100, alpha=0.2, resources=utils.get_resources())
STATE["phase"] = "initial"
pl1 = streamline.auto_tune_pipeline(pipeline=pipeline_1, weights=[0.5, 0.25, 0.25], max_p=5)
pl2 = streamline.auto_tune_pipeline(pipeline=pipeline_2, weights=[0.5, 0.25, 0.25], max_p=5)
snap_initial = snapshot(streamline, {"pipeline_1": pl1, "pipeline_2": pl2})

new = gen(NSIN, np.arange(N))
pipeline_1["observed_source_workloads"]["v_0"] = new
pipeline_2["observed_source_workloads"]["v_5" if K else "v_0"] = new.copy()
STATE["phase"] = "periodic"
plp = streamline.auto_tune_periodic()
snap_periodic = snapshot(streamline, plp)
total_s = time.perf_counter() - t_start

result = {"variant": VARIANT, "K": K, "D": D, "S": S, "seed": SEED, "torch_threads": torch.get_num_threads(),
          "forecasts": STATE["forecasts"], "initial": snap_initial, "periodic": snap_periodic,
          "reschedule_events": STATE["reschedule_events"], "solve_times": STATE["solve_times"],
          "optimizer_seconds_total": sum(x["seconds"] for x in STATE["solve_times"]), "tuning_seconds_total": total_s}
os.makedirs(OUT, exist_ok=True)
with open(os.path.join(OUT, f"{VARIANT}__seed-{SEED}.json"), "w") as f:
    json.dump(result, f, indent=1)
u = {r: round(v["utilisation"], 4) for r, v in snap_periodic["resources_used"].items()}
print(f"{VARIANT} seed={SEED} periodic_cost=${snap_periodic['hourly_cost']}/h resources={u} "
      f"p2_periodic_MAE={[round(x['mae'], 1) for x in STATE['forecasts'] if x['phase'] == 'periodic' and x['pipeline'] == 'pipeline_2']} "
      f"opt_s={result['optimizer_seconds_total']:.1f}", flush=True)

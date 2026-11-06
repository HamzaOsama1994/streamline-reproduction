"""E3: STREAMLINE decisions for the authors' ETL/FIT pipeline, trained on OUR cluster measurements.

Upstream STREAMLINE (d65baec) is used unmodified (imported from Streamline/src). Only WorkloadPredictor.predict is
wrapped, exactly as in correction_study.py, to select the variant:
  V0  as released: model left in train mode after train_() (dropout active), raw (unscaled) input window
  V4  corrected:   model.eval() before predicting, input window MinMax-scaled with the predictor's own scaler
(The third defect, the v_0/v_5 key in examples/ensemble.py, only exists in the two-pipeline example; with a single
pipeline V4 == "eval + scaling".)

Models are trained ONCE (same order and settings as examples/single_pipeline.py) and pickled; every variant/seed loads
the same checkpoint, so V0 and V4 differ only in the predict() path and the RNG seed.

Usage (from the Streamline repo root, Python 3.8 env):
  python ../repro_scripts/e3_decide.py train HIST_CSV WORKLOAD_JSON OUT_DIR
  python ../repro_scripts/e3_decide.py decide OUT_DIR [N_SEEDS]
WORKLOAD_JSON: {"interval_s": 5, "series": [...]} observed source workload (el/s), oldest first.
"""
import json
import math
import os
import pickle
import random
import sys
import time

import numpy as np
import pandas as pd
import torch

sys.path.append('src')
from streamline import Streamline, WorkloadPredictor, NeuralNetwork, PolynomialRegression, TrainingManager, Pipeline  # noqa: E402

TM_PRICE = 0.192
N_TM, SLOTS_PER_TM, TM_CORES = 4, 2, 1
WEIGHTS = [0.5, 0.25, 0.25]  # as in the authors' examples (throughput, latency, CPU)
MAX_P = 8                     # all slots of our cluster
ALPHA, DELTA_T = 0.2, 100     # as in the authors' examples


def etl_pipeline():
    # The Beam ETL job becomes two Flink vertices (operator chains); STREAMLINE models one operator per vertex.
    ops = [{"id": "v_0", "label": "source chain: KafkaIO read -> parse -> range filter -> key", "type": 0},
           {"id": "v_1", "label": "Interpolate (GroupIntoBatches) -> annotate -> serialise -> Kafka/MongoDB sinks", "type": 1}]
    return Pipeline("etl_fit", ops, [("v_0", "v_1")], ["v_0"], ["v_1"])


def resources():
    return {f"TM{i + 1}": {"occupied_slots": 0, "cores": TM_CORES, "load": 0.0, "cost": TM_PRICE} for i in range(N_TM)}


def train(hist_csv, workload_json, out):
    os.makedirs(out, exist_ok=True)
    hist = pd.read_csv(hist_csv)
    w = json.load(open(workload_json))
    series = np.asarray(w["series"], dtype=float)
    t0 = time.perf_counter()
    manager = TrainingManager()
    wp = manager.prepare_workload_predictor(series, WorkloadPredictor(epochs=200, output_size=50, input_size=100))
    t_wp = time.perf_counter() - t0
    nn_, pr = manager.prepare_configuration_optimizer(NeuralNetwork(hist, epochs=200), PolynomialRegression(hist, create_plots=False))
    t_all = time.perf_counter() - t0
    # NN validation R^2 (the authors' own metric)
    r2 = {"train": nn_.get_score(nn_.train_loader), "val": nn_.get_score(nn_.test_loader)}
    M = {"wp": wp, "nn": nn_, "pr": pr, "series": series, "interval_s": w.get("interval_s", 5),
         "rng_after_training": {"torch": torch.get_rng_state(), "numpy": np.random.get_state(), "random": random.getstate()}}
    pickle.dump(M, open(os.path.join(out, "models.pkl"), "wb"))
    poly = {str(k): [float(c) for c in v.coeffs] for k, v in pr.items()}
    info = {"hist_rows": int(len(hist)), "rows_per_type": {str(k): int(v) for k, v in hist["Operator Type"].value_counts().items()},
            "series_len": int(len(series)), "series_mean": float(series.mean()), "series_min": float(series.min()), "series_max": float(series.max()),
            "nn_r2": r2, "poly_coeffs": poly,
            "poly_capacity_by_p": {str(k): {p: float(v(p)) for p in range(1, MAX_P + 1)} for k, v in pr.items()},
            "train_seconds": {"workload_predictor": t_wp, "total": t_all}}
    json.dump(info, open(os.path.join(out, "training_info.json"), "w"), indent=1)
    print(json.dumps(info, indent=1))


_orig_predict = WorkloadPredictor.predict
VAR = {"D": 0, "S": 0}
FORECASTS = []


def predict_variant(self, inp):
    if VAR["D"]:
        self.model.eval()
    else:
        self.model.train()  # upstream state after train_() (dropout active)
    x = np.asarray(inp, dtype=float)
    if VAR["S"]:
        x = self.scaler.transform(x.reshape(-1, 1)).flatten()
    out = _orig_predict(self, x)
    FORECASTS.append([float(v) for v in out])
    return out


WorkloadPredictor.predict = predict_variant


def to_flink(cfg, placement):
    """Map STREAMLINE's per-operator decision to what Beam's FlinkRunner can express (plan option B)."""
    p = {k: int(v["parallelism"]) for k, v in cfg.items()}
    p_job = max(p.values())
    used = sorted({r for r, ops in (placement or {}).items() if ops})
    n_tm = max(len(used), math.ceil(p_job / SLOTS_PER_TM))
    seg = int(next(iter(cfg.values()))["segment-size"])
    return {"job_parallelism": p_job, "taskmanagers": n_tm, "segment_size": seg, "streamline_resources_used": used,
            "deviation": ("per-operator parallelism " + str(p) + " collapsed to job-wide max") if len(set(p.values())) > 1 else None,
            "slot_constraint_raised_tms": n_tm > len(used)}


def decide(out, n_seeds):
    M = pickle.load(open(os.path.join(out, "models.pkl"), "rb"))
    hist = None  # historical_data is only used for the TP_in/TP_out ratio -> reload the csv the models were trained on
    info = json.load(open(os.path.join(out, "training_info.json")))
    hist = pd.read_csv(os.path.join(out, "historical_data_etl.csv"))
    results = []
    for name, D, S in (("V0_original", 0, 0), ("V4_corrected", 1, 1)):
        VAR.update(D=D, S=S)
        for seed in ["orig"] + [str(s) for s in range(n_seeds)]:
            if seed == "orig":
                torch.set_rng_state(M["rng_after_training"]["torch"])
                np.random.set_state(M["rng_after_training"]["numpy"])
                random.setstate(M["rng_after_training"]["random"])
            else:
                s = int(seed)
                torch.manual_seed(s); np.random.seed(s); random.seed(s)
            FORECASTS.clear()
            pipe = {"structure": etl_pipeline(), "historical_data": hist,
                    "observed_source_workloads": {"v_0": M["series"]}, "transformer_model": {"v_0": M["wp"]},
                    "neural_network": M["nn"], "polynomial_regression": M["pr"]}
            sl = Streamline(deltaT=DELTA_T, alpha=ALPHA, resources=resources())
            t0 = time.perf_counter()
            rec = {"variant": name, "seed": seed}
            try:
                placement = sl.auto_tune_pipeline(pipeline=pipe, weights=WEIGHTS, max_p=MAX_P)
                cfg = {k.split("_", 2)[-1]: v for k, v in sl.configuration_parameters.items()}
                used = {r: v for r, v in sl.resources.items() if v["load"] > 1e-12}
                rec.update({
                    "ok": True, "seconds": time.perf_counter() - t0,
                    "configuration": {k: {"parallelism": int(v["parallelism"]), "segment_size": int(v["segment-size"]),
                                          "est_tp": float(v["est_tp"]), "est_l": float(v["est_l"]), "est_cpu": float(v["est_cpu"])} for k, v in cfg.items()},
                    "placement": placement, "resources_used": {r: {"load": float(v["load"]), "utilisation": float(v["load"] / v["cores"])} for r, v in used.items()},
                    "hourly_cost_streamline": round(len(used) * TM_PRICE, 3),
                    "flink": to_flink(cfg, placement)})
            except Exception as e:  # e.g. scheduler finds no resource subset for the estimated CPU
                rec.update({"ok": False, "error": f"{type(e).__name__}: {e}", "seconds": time.perf_counter() - t0})
            fc = np.asarray(FORECASTS[0]) if FORECASTS else np.array([])
            rec["forecast"] = [round(float(v), 2) for v in fc]
            rec["forecast_stats"] = {"mean": float(fc.mean()), "min": float(fc.min()), "max": float(fc.max()),
                                     "n_negative": int((fc < 0).sum())} if fc.size else None
            results.append(rec)
            f = rec.get("flink")
            print(name, seed, "ok" if rec.get("ok") else rec.get("error"), rec["forecast_stats"] and round(rec["forecast_stats"]["mean"]),
                  f and (f["job_parallelism"], f["taskmanagers"], f["segment_size"]),
                  rec.get("configuration") and {k: v["parallelism"] for k, v in rec["configuration"].items()}, flush=True)
    json.dump({"training_info": info, "decisions": results}, open(os.path.join(out, "decisions.json"), "w"), indent=1)


if __name__ == "__main__":
    if sys.argv[1] == "train":
        train(sys.argv[2], sys.argv[3], sys.argv[4])
        import shutil
        shutil.copy(sys.argv[2], os.path.join(sys.argv[4], "historical_data_etl.csv"))
    else:
        decide(sys.argv[2], int(sys.argv[3]) if len(sys.argv) > 3 else 10)

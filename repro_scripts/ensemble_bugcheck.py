"""Bug check for STREAMLINE examples/ensemble.py (upstream commit d65baec).

Replicates examples/ensemble.py step by step. The authors' file is not modified.
Upstream, pipeline_2 is created with source key "v_5", but the periodic update writes
observed_source_workloads["v_0"]. Controller.predict_workload() reads only the
pipeline's declared sources (v_5), so the update may be silently ignored.

Usage (run from the Streamline repo root):
    python ../repro_scripts/ensemble_bugcheck.py            # original behaviour + diagnostics
    python ../repro_scripts/ensemble_bugcheck.py --fix      # corrected key "v_5"
"""
import os
import sys
import time

import numpy as np
import pandas as pd

sys.path.append('src')
sys.path.append('examples')
import utils  # noqa: E402
from streamline import Streamline, WorkloadPredictor, NeuralNetwork, PolynomialRegression, TrainingManager  # noqa: E402

FIX = "--fix" in sys.argv


def diag(tag, pipeline, src):
    # WorkloadPredictor.predict() runs with dropout active (model never set to eval()),
    # so each call consumes torch RNG state. Save/restore it so diagnostics do not
    # perturb the experiment (verified: without this, downstream decisions change).
    import torch
    rng_state = torch.get_rng_state()
    w = pipeline["observed_source_workloads"]
    t = pipeline["transformer_model"][src]
    window = w[src][-t.input_size:]
    forecast = np.asarray(t.predict(window)).ravel()
    torch.set_rng_state(rng_state)
    print(f"[DIAG] {tag}: keys={sorted(w.keys())} | {src} window mean={window.mean():.2f} "
          f"last={window[-1]:.2f} | forecast mean={forecast.mean():.2f} min={forecast.min():.2f} max={forecast.max():.2f}",
          flush=True)


def main():
    t0 = time.perf_counter()
    pipeline_1 = {
        "structure": utils.get_pipeline_1(),
        "historical_data": pd.read_csv("examples/data/historical_data_pipeline_1.csv"),
        "observed_source_workloads": {"v_0": 50 + (np.sin(np.linspace(0, 10 * np.pi, 500)) + 1) * (2000 - 50) / 2},
        "transformer_model": {}
    }
    pipeline_2 = {
        "structure": utils.get_pipeline_2(),
        "historical_data": pd.read_csv("examples/data/historical_data_pipeline_2.csv"),
        "observed_source_workloads": {"v_5": 50 + (np.cos(np.linspace(0, 10 * np.pi, 500)) + 1) * (2000 - 50) / 2},
        "transformer_model": {}
    }

    manager = TrainingManager()
    pipeline_1["transformer_model"]["v_0"] = manager.prepare_workload_predictor(pipeline_1["observed_source_workloads"]["v_0"], WorkloadPredictor(epochs=200, output_size=50, input_size=100))
    pipeline_1["neural_network"], pipeline_1["polynomial_regression"] = manager.prepare_configuration_optimizer(NeuralNetwork(pipeline_1["historical_data"], epochs=200),
                                                                                                                PolynomialRegression(pipeline_1["historical_data"], create_plots=True))
    pipeline_2["transformer_model"]["v_5"] = manager.prepare_workload_predictor(pipeline_2["observed_source_workloads"]["v_5"], WorkloadPredictor(epochs=200, output_size=50, input_size=100))
    pipeline_2["neural_network"], pipeline_2["polynomial_regression"] = manager.prepare_configuration_optimizer(NeuralNetwork(pipeline_2["historical_data"], epochs=200),
                                                                                                                PolynomialRegression(pipeline_2["historical_data"], create_plots=True))
    t_train = time.perf_counter()

    streamline = Streamline(deltaT=100, alpha=0.2, resources=utils.get_resources())

    schedule_p1 = streamline.auto_tune_pipeline(pipeline=pipeline_1, weights=[0.5, 0.25, 0.25], max_p=5)
    print(schedule_p1)
    schedule_p2 = streamline.auto_tune_pipeline(pipeline=pipeline_2, weights=[0.5, 0.25, 0.25], max_p=5)
    print(schedule_p2)
    t_initial = time.perf_counter()

    diag("pipeline_2 BEFORE update", pipeline_2, "v_5")

    new_workload = 50 + (-np.sin(np.linspace(0, 10 * np.pi, 500)) + 1) * (2000 - 50) / 2
    pipeline_1["observed_source_workloads"]["v_0"] = new_workload
    p2_key = "v_5" if FIX else "v_0"   # upstream uses "v_0" (suspected bug)
    pipeline_2["observed_source_workloads"][p2_key] = new_workload.copy()
    print(f"[DIAG] mode={'FIXED (v_5)' if FIX else 'ORIGINAL (v_0)'}; pipeline_2 update written to key '{p2_key}'", flush=True)

    diag("pipeline_2 AFTER update (what predict_workload will read)", pipeline_2, "v_5")

    schedule = streamline.auto_tune_periodic()
    print(schedule)
    t_periodic = time.perf_counter()

    print("Resources Utilization: ")
    for resource_id in streamline.resources:
        if streamline.resources[resource_id]["load"] > 0.0:
            print("\t" + resource_id + ": " + str(
                streamline.resources[resource_id]["load"] / streamline.resources[resource_id]["cores"]))

    print("[DIAG] stored configuration parameters after periodic tuning:")
    for k, v in sorted(streamline.configuration_parameters.items()):
        print(f"\t{k}: parallelism={v['parallelism']}, segment-size={v['segment-size']}, est_tp={v['est_tp']:.2f}")

    print(f"[TIMING] training_s={t_train - t0:.1f} initial_tuning_s={t_initial - t_train:.1f} "
          f"periodic_tuning_s={t_periodic - t_initial:.1f} total_s={t_periodic - t0:.1f}", flush=True)


if __name__ == "__main__":
    main()

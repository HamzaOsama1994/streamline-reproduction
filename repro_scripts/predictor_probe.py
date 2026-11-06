"""Probe two suspected inference issues in streamline.WorkloadPredictor (upstream d65baec).

1. predict() never calls model.eval(), so dropout (p=0.1, PyTorch default) stays active.
2. predict() feeds the raw window to the model, but training uses MinMax-scaled data.

The authors' class is used unchanged. Only the inference calls differ between variants.
Run from the Streamline repo root:  python ../repro_scripts/predictor_probe.py
"""
import sys
import time

import numpy as np
import torch

sys.path.append('src')
from streamline import WorkloadPredictor  # noqa: E402

N = 500
step = 10 * np.pi / (N - 1)
series = 50 + (np.sin(np.linspace(0, 10 * np.pi, N)) + 1) * (2000 - 50) / 2       # as in examples/single_pipeline.py
truth = 50 + (np.sin(step * np.arange(N, N + 50)) + 1) * (2000 - 50) / 2            # true continuation

t0 = time.perf_counter()
wp = WorkloadPredictor(epochs=200, output_size=50, input_size=100)
wp.train_(series)
print(f"training_s={time.perf_counter() - t0:.1f}")
window = series[-wp.input_size:]


def run(model_mode, scale_input, reps=20):
    wp.model.train() if model_mode == "train" else wp.model.eval()
    outs = []
    for _ in range(reps):
        x = wp.scaler.transform(window.reshape(-1, 1)).flatten() if scale_input else window
        with torch.no_grad():
            y = wp.model(torch.tensor(x, dtype=torch.float32).unsqueeze(0)).squeeze().numpy()
        outs.append(wp.scaler.inverse_transform(y.reshape(-1, 1)).flatten())
    outs = np.array(outs)
    mean = outs.mean(axis=0)
    return {
        "mae_vs_truth": float(np.abs(mean - truth).mean()),
        "run_to_run_std_mean": float(outs.std(axis=0).mean()),
        "min_forecast": float(outs.min()),
        "share_negative": float((outs < 0).mean()),
    }


# Upstream behaviour = model left in train mode (as after train_), raw input
wp.model.train()
upstream = np.array([wp.predict(window) for _ in range(20)])
print(f"UPSTREAM predict(): mae={np.abs(upstream.mean(0) - truth).mean():.1f} "
      f"std={upstream.std(0).mean():.1f} min={upstream.min():.1f} neg_share={(upstream < 0).mean():.3f}")
for mode in ("train", "eval"):
    for scaled in (False, True):
        r = run(mode, scaled)
        print(f"mode={mode:5s} scaled_input={str(scaled):5s} -> " + ", ".join(f"{k}={v:.3f}" for k, v in r.items()))
print(f"truth range: {truth.min():.1f}..{truth.max():.1f}, mean {truth.mean():.1f}")

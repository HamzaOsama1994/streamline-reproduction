"""Latent-impact probe for `best_model = model` in WorkloadPredictor.train_() (upstream d65baec).

For each (setting, seed): train with an exact copy of upstream train_() and keep a deep copy
of the weights at upstream's own best-training-loss epoch. Then compare the returned model
(= last epoch, because of the alias) with the true best-epoch snapshot on held-out forecasts.
Upstream has no validation split, so "validation" here means held-out forecast error:
  H1 = true continuation of the training series (next output_size steps)
  H2 = continuation of the shifted workload used in the periodic update (-sin)
Both models are scored with corrected inference (eval mode + scaled input), so that
inference defects do not mask the checkpoint effect. Upstream inference is scored too
(mean of 20 stochastic calls).

Run from the Streamline repo root:  python ../repro_scripts/best_model_probe.py OUT_JSON
"""
import copy
import json
import random
import sys
import time

import numpy as np
import torch
import torch.nn as nn

sys.path.append('src')
from streamline import WorkloadPredictor  # noqa: E402
from streamline import workload_predictor as wp_mod  # noqa: E402

N = 500
STEP = 10 * np.pi / (N - 1)
gen = lambda f, idx: 50 + (f(STEP * idx) + 1) * (2000 - 50) / 2  # noqa: E731
train_series = gen(np.sin, np.arange(N))
shift_series = gen(lambda t: -np.sin(t), np.arange(N))


def train_with_snapshot(wp, load):
    data = wp.scaler.fit_transform(load.reshape(-1, 1)).flatten()
    x, y = wp.create_dataset(data)
    model = wp_mod.TimeSeriesTransformer(wp.d_model, wp.nhead, wp.num_layers, wp.input_size, wp.output_size, wp.max_len)
    criterion = nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=wp.lr)
    best_loss, best_epoch, best_state, hist = 10.0, None, None, []
    for epoch in range(wp.epochs + 1):
        model.train()
        epoch_loss = 0
        for i in range(0, len(x), wp.batch_size):
            optimizer.zero_grad()
            loss = criterion(model(x[i:i + wp.batch_size]), y[i:i + wp.batch_size])
            loss.backward()
            optimizer.step()
            epoch_loss += loss.item()
        if epoch_loss < best_loss:
            best_loss, best_epoch, best_state = epoch_loss, epoch, copy.deepcopy(model.state_dict())
        hist.append(epoch_loss)
    wp.model = model  # what upstream returns (alias -> last epoch)
    return best_epoch, best_loss, best_state, hist


def score(wp, window, truth, mode, scaled, reps=1):
    wp.model.train() if mode == "train" else wp.model.eval()
    outs = []
    for _ in range(reps):
        xin = wp.scaler.transform(window.reshape(-1, 1)).flatten() if scaled else window
        with torch.no_grad():
            yhat = wp.model(torch.tensor(xin, dtype=torch.float32).unsqueeze(0)).squeeze().numpy()
        outs.append(wp.scaler.inverse_transform(yhat.reshape(-1, 1)).flatten())
    m = np.mean(outs, axis=0)
    return {"mae": float(np.abs(m - truth).mean()), "rmse": float(np.sqrt(((m - truth) ** 2).mean()))}


def evaluate(wp):
    res = {}
    for hname, series, f in (("H1_continuation", train_series, np.sin), ("H2_shifted", shift_series, lambda t: -np.sin(t))):
        window = series[-wp.input_size:]
        truth = gen(f, np.arange(N, N + wp.output_size))
        res[hname] = {"corrected": score(wp, window, truth, "eval", True),
                      "upstream_inference": score(wp, window, truth, "train", False, reps=20)}
    return res


SETTINGS = {"example": dict(epochs=200, output_size=50, input_size=100),
            "paper_table2": dict(epochs=500, output_size=30, input_size=100)}
SEEDS = [0, 1, 2, 3, 4]
rows = []
for sname, kw in SETTINGS.items():
    for seed in SEEDS:
        random.seed(seed); np.random.seed(seed); torch.manual_seed(seed)
        wp = WorkloadPredictor(**kw)
        t0 = time.perf_counter()
        best_epoch, best_loss, best_state, hist = train_with_snapshot(wp, train_series)
        train_s = time.perf_counter() - t0
        torch.manual_seed(1000 + seed)
        last_eval = evaluate(wp)
        last_state = copy.deepcopy(wp.model.state_dict())
        wp.model.load_state_dict(best_state)
        torch.manual_seed(1000 + seed)
        best_eval = evaluate(wp)
        wp.model.load_state_dict(last_state)
        row = {"setting": sname, "seed": seed, "epochs": kw["epochs"], "best_epoch": best_epoch,
               "best_train_loss": best_loss, "last_train_loss": hist[-1], "same_epoch": best_epoch == kw["epochs"],
               "n_loss_increases": int(sum(hist[i] > hist[i - 1] for i in range(1, len(hist)))),
               "train_s": round(train_s, 1), "last": last_eval, "best": best_eval}
        rows.append(row)
        print(f"{sname} seed={seed} best_epoch={best_epoch}/{kw['epochs']} best_loss={best_loss:.5f} last_loss={hist[-1]:.5f} "
              f"H1 corrected MAE last={last_eval['H1_continuation']['corrected']['mae']:.1f} best={best_eval['H1_continuation']['corrected']['mae']:.1f} | "
              f"H2 corrected MAE last={last_eval['H2_shifted']['corrected']['mae']:.1f} best={best_eval['H2_shifted']['corrected']['mae']:.1f} "
              f"({train_s:.0f}s)", flush=True)

json.dump(rows, open(sys.argv[1], "w"), indent=1)

"""Train STREAMLINE's example models once, in exactly the order of examples/ensemble.py,
and save them as fixed checkpoints for the controlled correction study.

The script also instruments WorkloadPredictor.train_() to answer whether
`best_model = model` (a reference, not a copy) changes which weights are kept.
The training loop below is a line-by-line copy of upstream d65baec train_(). The only
additions are: (a) a deep copy of the state_dict whenever upstream's own best-loss
condition fires, and (b) a per-epoch loss history. Neither consumes RNG, so the training
trajectory is identical to upstream; this is verified by matching the logged losses.

Run from the Streamline repo root:  python ../repro_scripts/train_checkpoints.py OUT_DIR
"""
import copy
import os
import pickle
import random
import sys

import numpy as np
import pandas as pd
import torch
import torch.nn as nn

sys.path.append('src')
sys.path.append('examples')
import utils  # noqa: E402
from streamline import WorkloadPredictor, NeuralNetwork, PolynomialRegression, TrainingManager  # noqa: E402
from streamline import workload_predictor as wp_mod  # noqa: E402

OUT = sys.argv[1]
os.makedirs(OUT, exist_ok=True)
AUDIT = {}


def train_instrumented(self, load):
    # ---- verbatim upstream body (workload_predictor.py, WorkloadPredictor.train_) ----
    data = self.scaler.fit_transform(load.reshape(-1, 1)).flatten()
    x, y = self.create_dataset(data)
    model = wp_mod.TimeSeriesTransformer(self.d_model, self.nhead, self.num_layers, self.input_size, self.output_size, self.max_len)
    criterion = nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=self.lr)

    best_model = model
    best_loss = 10.0
    # ---- instrumentation (no RNG use) ----
    history, best_epoch, best_state = [], None, None
    for epoch in range(self.epochs + 1):
        model.train()
        epoch_loss = 0
        for i in range(0, len(x), self.batch_size):
            x_batch = x[i:i + self.batch_size]
            y_batch = y[i:i + self.batch_size]
            optimizer.zero_grad()
            output = model(x_batch)
            loss = criterion(output, y_batch)
            loss.backward()
            optimizer.step()
            epoch_loss += loss.item()
        if epoch_loss < best_loss:
            best_loss = epoch_loss
            best_model = model
            best_epoch, best_state = epoch, copy.deepcopy(model.state_dict())   # instrumentation
        history.append(epoch_loss)                                                 # instrumentation
        if epoch % 50 == 0:
            wp_mod.logger.info(f"Epoch {epoch}/{self.epochs}, Loss: {epoch_loss:.4f}")
    self.model = best_model
    # ---- audit record ----
    self._audit = {
        "loss_history": history,
        "best_epoch_by_train_loss": best_epoch,
        "best_train_loss": best_loss,
        "last_epoch": self.epochs,
        "last_epoch_train_loss": history[-1],
        "returned_model_is_last_epoch": all(torch.equal(a, b) for a, b in zip(self.model.state_dict().values(), model.state_dict().values())),
        "returned_model_equals_best_snapshot": all(torch.equal(self.model.state_dict()[k], best_state[k]) for k in best_state),
    }
    self._best_state = best_state
    return best_model


WorkloadPredictor.train_ = train_instrumented

# ---- identical to examples/ensemble.py from here on ----
pipeline_1 = {"structure": utils.get_pipeline_1(),
              "historical_data": pd.read_csv("examples/data/historical_data_pipeline_1.csv"),
              "observed_source_workloads": {"v_0": 50 + (np.sin(np.linspace(0, 10 * np.pi, 500)) + 1) * (2000 - 50) / 2},
              "transformer_model": {}}
pipeline_2 = {"structure": utils.get_pipeline_2(),
              "historical_data": pd.read_csv("examples/data/historical_data_pipeline_2.csv"),
              "observed_source_workloads": {"v_5": 50 + (np.cos(np.linspace(0, 10 * np.pi, 500)) + 1) * (2000 - 50) / 2},
              "transformer_model": {}}
manager = TrainingManager()
t1 = manager.prepare_workload_predictor(pipeline_1["observed_source_workloads"]["v_0"], WorkloadPredictor(epochs=200, output_size=50, input_size=100))
nn1, pr1 = manager.prepare_configuration_optimizer(NeuralNetwork(pipeline_1["historical_data"], epochs=200), PolynomialRegression(pipeline_1["historical_data"], create_plots=True))
t2 = manager.prepare_workload_predictor(pipeline_2["observed_source_workloads"]["v_5"], WorkloadPredictor(epochs=200, output_size=50, input_size=100))
nn2, pr2 = manager.prepare_configuration_optimizer(NeuralNetwork(pipeline_2["historical_data"], epochs=200), PolynomialRegression(pipeline_2["historical_data"], create_plots=True))

# RNG state right after training = state at which ensemble.py starts tuning (for exact replay)
rng = {"torch": torch.get_rng_state(), "numpy": np.random.get_state(), "random": random.getstate()}

for name, t in (("p1_v0", t1), ("p2_v5", t2)):
    AUDIT[name] = {k: v for k, v in t._audit.items() if k != "loss_history"}
    AUDIT[name]["loss_history"] = t._audit["loss_history"]
    torch.save(t._best_state, os.path.join(OUT, f"transformer_{name}_best_state.pt"))
    del t._audit, t._best_state

with open(os.path.join(OUT, "models.pkl"), "wb") as f:
    pickle.dump({"t1": t1, "nn1": nn1, "pr1": pr1, "t2": t2, "nn2": nn2, "pr2": pr2, "rng_after_training": rng}, f)
with open(os.path.join(OUT, "best_model_audit.pkl"), "wb") as f:
    pickle.dump(AUDIT, f)
for name, a in AUDIT.items():
    print(f"[AUDIT] {name}: best_epoch_by_train_loss={a['best_epoch_by_train_loss']} best_loss={a['best_train_loss']:.6f} "
          f"last_epoch_loss={a['last_epoch_train_loss']:.6f} returned_is_last={a['returned_model_is_last_epoch']} "
          f"returned_equals_best_snapshot={a['returned_model_equals_best_snapshot']}")
print("saved checkpoints to", OUT)

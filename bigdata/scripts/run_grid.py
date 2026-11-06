"""Run a grid of measured ETL/FIT runs sequentially (one job at a time on the shared cluster).

Keeps Windows awake for the duration via SetThreadExecutionState (process-scoped; released automatically on exit),
so timing is not corrupted by system sleep (see correction_study incident note).

Usage: python run_grid.py OUT_DIR RATE DURATION WARMUP REPS P1 [P2 ...]
(extra harness options via env HARNESS_EXTRA, e.g. "--latency-interval 1000")
Run order is interleaved by repetition (rep1: all p, rep2: all p, ...) to spread slow drifts across configurations.
"""
import subprocess
import sys
import os
import power_guard as pg  # AC/standby guard, ENVIRONMENT section 13

out, rate, dur, warm, reps = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5])
ps = [int(p) for p in sys.argv[6:]]
pg.keep_awake()
here = os.path.dirname(os.path.abspath(__file__))
try:
    for rep in range(1, reps + 1):
        for p in ps:
            label = f"p{p}_r{rate}_rep{rep}"
            if os.path.exists(os.path.join(out, label + ".json")):
                print("skip existing", label, flush=True)
                continue
            r = pg.run_guarded([sys.executable, os.path.join(here, "run_fit_experiment.py"), "--parallelism", str(p), "--rate", str(rate),
                                "--duration", str(dur), "--warmup", str(warm), "--label", label, "--out", out] + os.environ.get("HARNESS_EXTRA", "").split(),
                               os.path.join(out, label + ".json"), label)
            print((r.stdout.strip().splitlines() or ["<no output>"])[-1] if r.returncode == 0 else f"FAILED {label}: {r.stderr[-1500:]}", flush=True)
finally:
    pg.keep_awake(False)
print("GRID_DONE", flush=True)

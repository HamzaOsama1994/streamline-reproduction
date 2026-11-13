"""Run a list of E4 (per-vertex parallelism) measured runs sequentially with run_pv_experiment.py (skips runs whose result file exists).

Usage: python run_e3_grid.py SPEC.json
SPEC.json: {"out": "results/E3_profile", "common": ["--duration", "510", ...],
            "runs": [{"label": "p1_seg16384", "args": ["--parallelism", "1", "--segment-size", "16384"]}, ...]}
Keeps Windows awake (SetThreadExecutionState). Restores the TaskManager deployment to 4 replicas / default args and the JobManager to default args at the end.
"""
import json
import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import power_guard as pg  # AC/standby guard, ENVIRONMENT section 13

here = os.path.dirname(os.path.abspath(__file__))
spec = json.load(open(sys.argv[1]))
out = spec["out"]
pg.keep_awake()
try:
    for r in spec["runs"]:
        if os.path.exists(os.path.join(out, r["label"] + ".json")):
            print("skip existing", r["label"], flush=True)
            continue
        cmd = [sys.executable, os.path.join(here, "run_pv_experiment.py"), "--label", r["label"], "--out", out] + spec["common"] + r["args"]
        p = pg.run_guarded(cmd, os.path.join(out, r["label"] + ".json"), r["label"])
        print((p.stdout.strip().splitlines() or ["<no output>"])[-1][:600] if p.returncode == 0 else f"FAILED {r['label']}: {p.stderr[-1500:]}", flush=True)
finally:
    subprocess.run(["kubectl", "patch", "deploy/flink-taskmanager", "--type=json", "-p",
                    json.dumps([{"op": "replace", "path": "/spec/template/spec/containers/0/args", "value": ["taskmanager"]}])], capture_output=True)
    subprocess.run(["kubectl", "scale", "deploy/flink-taskmanager", "--replicas=4"], capture_output=True)
    subprocess.run(["kubectl", "patch", "deploy/flink-jobmanager", "--type=json", "-p",
                    json.dumps([{"op": "replace", "path": "/spec/template/spec/containers/0/args", "value": ["jobmanager"]}])], capture_output=True)
    pg.keep_awake(False)
print("GRID_DONE", flush=True)

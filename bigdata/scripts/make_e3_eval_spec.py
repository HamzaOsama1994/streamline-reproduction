"""Build the E3 evaluation spec from STREAMLINE's decisions (repro_scripts/e3_decide.py decide -> decisions.json).

Every distinct Flink configuration (job parallelism, TaskManagers, segment size) chosen by any variant/seed is measured,
plus the two baselines of the paper: FD (Flink default: p = 1, 1 TM, 32 KiB) and FMAX (p = max = 8, 4 TMs, 32 KiB).
Each configuration: REPS repetitions, all runs in one seeded random order, on the evaluation workload (warm-up + horizon),
with true end-to-end latency (--e2e). A variant's expected outcome is later computed as the frequency-weighted mean over
its seeds' configurations (analyse_e3.py).

Usage: python make_e3_eval_spec.py DECISIONS_JSON OUT_SPEC_JSON [REPS=3] [OUT_DIR=results/E3_eval] [WORKLOAD=e3_sine_eval] [EXTRA HARNESS ARGS...]
(OUT_DIR, WORKLOAD and extra args added 2026-11-06 for E3-TAXI, e.g. results/E3T_eval e3t_taxi_eval --dataset TAXI --drain 300 --trace-full)
"""
import json
import random
import sys

dec = json.load(open(sys.argv[1]))["decisions"]
reps = int(sys.argv[3]) if len(sys.argv) > 3 else 3
cfgs = {}
for d in dec:
    if not d.get("ok"):
        continue
    f = d["flink"]
    key = (f["job_parallelism"], f["taskmanagers"], f["segment_size"])
    cfgs.setdefault(key, []).append(f"{d['variant']}/{d['seed']}")
base = {(1, 1, 32768): "FD", (8, 4, 32768): "FMAX"}
for k, name in base.items():
    cfgs.setdefault(k, []).append(name)
runs = []
for (p, tm, seg), who in sorted(cfgs.items()):
    for r in range(1, reps + 1):
        runs.append({"label": f"p{p}_tm{tm}_seg{seg}_rep{r}", "chosen_by": who,
                     "args": ["--parallelism", str(p), "--taskmanagers", str(tm), "--segment-size", str(seg)]})
random.Random(508).shuffle(runs)
out_dir = sys.argv[4] if len(sys.argv) > 4 else "results/E3_eval"
workload = sys.argv[5] if len(sys.argv) > 5 else "e3_sine_eval"
spec = {"out": out_dir,
        "common": ["--workload", workload, "--duration", "400", "--warmup", "150", "--latency-interval", "1000", "--e2e"] + sys.argv[6:],
        "configs": {f"p{p}_tm{tm}_seg{seg}": who for (p, tm, seg), who in sorted(cfgs.items())},
        "runs": runs}
json.dump(spec, open(sys.argv[2], "w"), indent=1)
print(json.dumps(spec["configs"], indent=1), len(runs), "runs")

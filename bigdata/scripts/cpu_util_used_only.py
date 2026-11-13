"""CPU utilisation of the TaskManagers that host tasks (report Table 4), recomputed from the per-pod samples of the E1 runs.

Usage: python cpu_util_used_only.py [RESULT_DIR ...]   (default: results/E1_fit_r4000 results/E1_taxi_r3500)

Why: the harness summary field `tm_cpu_utilisation_of_used` divides the summed CPU of ALL TaskManager pods by the number of
TaskManagers hosting tasks. All four TaskManager pods stay deployed, so for p = 1, 2 and 4 the numerator also contains the
background CPU of idle TaskManagers (JVM, heartbeats, metrics: ~0.02-0.05 core each), which pushed FD's value to 1.06-1.12
although its only busy TaskManager never exceeded its 1-CPU limit.

Definition used here: per run, mean pod CPU (Kubernetes metrics API, samples at t >= 150 s) of the k TaskManagers that host
tasks, summed and divided by k (each TaskManager has a 1-CPU limit, so 1.0 = limit fully used). k = `taskmanagers_used`
(from the REST API); the k used pods are the k with the highest mean CPU (idle pods are an order of magnitude lower).
"""
import glob
import json
import os
import statistics as st
import sys
from collections import defaultdict

dirs = sys.argv[1:] or ["results/E1_fit_r4000", "results/E1_taxi_r3500"]
for d in dirs:
    g = defaultdict(list)
    for p in sorted(glob.glob(os.path.join(d, "p*_rep*.json"))):
        r = json.load(open(p))
        k = r["summary"]["taskmanagers_used"]
        S = [s for s in r["samples"] if s.get("pod_cpu_cores") and s.get("t", 0) >= 150]
        pods = sorted({x for s in S for x in s["pod_cpu_cores"] if "taskmanager" in x})
        m = {x: st.mean(s["pod_cpu_cores"].get(x, 0) for s in S) for x in pods}
        used = sorted(pods, key=lambda x: -m[x])[:k]
        idle = [x for x in pods if x not in used]
        g[os.path.basename(p).split("_")[0]].append({
            "harness_value": r["summary"]["tm_cpu_utilisation_of_used"], "used_only": sum(m[x] for x in used) / k,
            "busiest_tm": max(m[x] for x in used), "idle_tms_total": sum(m[x] for x in idle)})
    print(f"\n{d}\n| config | runs | used-only utilisation | busiest TM (cores) | idle TMs total (cores) | harness value |\n|---|---|---|---|---|---|")
    for c, v in sorted(g.items(), key=lambda kv: int(kv[0][1:])):
        f = lambda key: st.mean(x[key] for x in v)
        print(f"| {c} | {len(v)} | {f('used_only'):.2f} | {max(x['busiest_tm'] for x in v):.2f} | {f('idle_tms_total'):.2f} | {f('harness_value'):.2f} |")

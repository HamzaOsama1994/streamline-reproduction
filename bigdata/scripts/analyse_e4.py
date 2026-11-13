"""E4 analysis: STREAMLINE's own per-operator plans deployed with per-vertex parallelism (run_pv_experiment.py), compared
with the E3-T runs on the same TAXI horizon (same workload file, warm-up, measurement).

Usage: python analyse_e4.py [E4_DIR=results/E4_eval] [E3T_DIR=results/E3T_eval] [--only-e4]

Per run, from the result JSON and the complete per-record trace (*.e2e_trace.csv.gz):
  * e2e p50 / p99: true end-to-end latency of records entering in the horizon (t_in >= 150 s), every 10th record, exactly
    as analyse_e3.py (so values equal the E3-T tables);
  * peak window: fixed 60 s window centred on the schedule's peak 5 s bin (210-270 s); input and output rate in it by broker
    time, as in E3T_RESULTS.md (checked: FD rep 1 gives 1,634 / 1,375 el/s as reported there);
  * records unprocessed at the end of the run (+ drain);
  * hot subtasks separated: TaskManager of the busiest source subtask != TaskManager of the busiest Interpolate subtask;
  * host load (other cores) mean, order-match fractions.
Writes <E4_DIR>/summary.md and <E4_DIR>/summary.json.
"""
import glob
import gzip
import json
import os
import statistics as st
import sys

args = [a for a in sys.argv[1:] if not a.startswith("--")]
E4 = args[0] if args else "results/E4_eval"
E3T = args[1] if len(args) > 1 else "results/E3T_eval"
H0 = 150_000
_act = json.load(open(os.path.join(E3T, "horizon_actual_ingestion.json")))["actual_rate_5s_schedule"]
PEAK_LO = 150 + 5 * max(range(len(_act)), key=lambda i: _act[i]) - 30  # = 210 s


def fnum(x):
    return float(x) if x not in (None, "NaN") else 0.0


def separated(r):
    pl = r["placement"]
    names = list(pl)
    src = max(pl[names[0]], key=lambda s: fnum(s.get("numRecordsOutPerSecond")))
    hot = max(pl[names[-1]], key=lambda s: fnum(s.get("busyTimeMsPerSecond")))
    return src["tm"] != hot["tm"], src["subtask"], hot["subtask"]


def q(xs, f):
    return xs[int(f * (len(xs) - 1))]


def peak_window(trace):
    """Fixed 60 s window centred on the replay schedule's peak 5 s bin (t = 240 s -> 210-270 s after the first input record),
    the same window as E3T_RESULTS.md; input and output rates by broker time."""
    tin, tout = [], []
    with gzip.open(trace, "rt") as fh:
        next(fh)
        for line in fh:
            i, trel, a, b, lat = line.rstrip("\n").split(",")
            tin.append(int(a))
            if b:
                tout.append(int(b))
    t0 = tin[0]
    w0, w1 = t0 + PEAK_LO * 1000, t0 + (PEAK_LO + 60) * 1000
    return {"window_s": [PEAK_LO, PEAK_LO + 60], "input_rate": sum(1 for a in tin if w0 <= a < w1) / 60,
            "output_rate": sum(1 for b in tout if w0 <= b < w1) / 60, "unprocessed": len(tin) - len(tout)}


def run_row(path):
    r = json.load(open(path))
    sm = r["summary"]
    L = (r.get("e2e") or {}).get("lat_by_input_time_ms") or []
    xs = sorted(l for t, l in L if t >= H0)
    sep, src_i, hot_i = separated(r)
    tr = path[:-5] + ".e2e_trace.csv.gz"
    pk = peak_window(tr) if os.path.exists(tr) else {}
    pv = {k[:12]: v["parallelism"] for k, v in sm["per_vertex"].items()}
    return {"run": os.path.basename(path)[:-5], "tms": sm["taskmanagers_used"], "cost": sm["emulated_cost_per_h"],
            "vertex_parallelism": list(pv.values()), "separated": sep, "src_subtask": src_i, "hot_subtask": hot_i,
            "p50": q(xs, .5) if xs else None, "p99": q(xs, .99) if xs else None, "max": xs[-1] if xs else None,
            "peak_in": pk.get("input_rate"), "peak_out": pk.get("output_rate"), "peak_window_s": pk.get("window_s"),
            "unprocessed": pk.get("unprocessed"), "order_match": (r.get("e2e") or {}).get("order_match_fraction"),
            "host_other_mean": sm.get("host_other_cores_mean"), "host_other_max": sm.get("host_other_cores_max"),
            "tm_cpu": sm.get("tm_cpu_cores_mean"), "power_events": (r.get("power") or {}).get("events"),
            "ac": [(r.get("power") or {}).get("ac_start"), (r.get("power") or {}).get("ac_end")],
            "drain_s": (r.get("drain") or {}).get("seconds")}


rows = [run_row(p) for p in sorted(glob.glob(os.path.join(E4, "*_rep*.json")))]
if "--only-e4" not in sys.argv:
    rows += [run_row(p) for p in sorted(glob.glob(os.path.join(E3T, "p*_rep*.json")))]

out = ["| run | TMs | $/h | vertex p | separated (src/hot subtask) | e2e p50 ms | e2e p99 ms | peak in / out el/s | unprocessed | host other cores | order match |",
       "|---|---|---|---|---|---|---|---|---|---|---|"]
for x in rows:
    pi = f"{x['peak_in']:.0f} / {x['peak_out']:.0f}" if x["peak_in"] else "n/a"
    om = ",".join(f"{v:.2f}" for v in (x["order_match"] or []))
    out.append(f"| {x['run']} | {x['tms']} | {x['cost']:.3f} | {x['vertex_parallelism']} | {'yes' if x['separated'] else 'no'} "
               f"({x['src_subtask']}/{x['hot_subtask']}) | {x['p50']} | {x['p99']} | {pi} | {x['unprocessed']} | "
               f"{x['host_other_mean']:.1f} (max {x['host_other_max']:.1f}) | {om} |")
txt = "\n".join(out)
print(txt)
os.makedirs(E4, exist_ok=True)
open(os.path.join(E4, "summary.md"), "w", encoding="utf-8").write("# E4 per-run summary (analyse_e4.py)\n\n" + txt + "\n")
json.dump(rows, open(os.path.join(E4, "summary.json"), "w"), indent=1)

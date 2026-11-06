"""Build STREAMLINE's `historical_data` table from E3 profiling runs (our measurements of the authors' ETL/FIT job).

STREAMLINE expects one row per operator observation with columns
  Vertex Id, TP_in, Pred Parallelism, Parallelism, Segment Size, Operator Type, Latency, CPU, Busytime, TP_out
(examples/data/historical_data_pipeline_*.csv). The authors did not release the collector that produced their rows,
so the mapping below is ours (documented in E3_RESULTS.md):

  operators      v_0 = Flink vertex 1 (KafkaIO read -> parse -> range filter -> key), type 0
                 v_1 = Flink vertex 2 (Interpolate/GroupIntoBatches -> annotate -> serialise -> sinks), type 1
  TP_in          v_0: records consumed from Kafka (operator counter after the Kafka read) / s
                 v_1: vertex numRecordsIn / s                     (counter differences between samples)
  TP_out         v_0: vertex numRecordsOut / s; v_1: TP_in * (records written to senml-cleaned / records produced, per run)
  Busytime       (capped at 1) max over the vertex's subtasks of accumulateBusyTimeMs difference / elapsed ms (0..1).
                 (max, not mean: STREAMLINE's PolynomialRegression uses TP_in / Busytime as the vertex capacity, and the
                 keyed v_1 has one hot subtask; a mean would overstate capacity by up to p times. --busy mean for the
                 sensitivity variant.)
  CPU            per-TaskManager attribution of measured pod CPU (metrics.k8s.io): with one partition and one key, each
                 vertex has exactly one active subtask (source: max numRecordsOut; keyed: max numRecordsIn over the run).
                 A TM hosting only the active source subtask -> v_0; only the hot keyed subtask -> v_1; both (co-located)
                 -> split in the ratio of per-record CPU costs fitted on the runs where they are separate (pod CPU vs
                 TP_in, linear; printed as cost_ratio); TMs hosting neither -> split by the vertices' subtask counts.
                 (Busy time is not usable: Beam's legacy Kafka source is reported as never idle, and on a co-located 1-core
                 TM both vertices report wall-clock busy time while waiting for the CPU.)
  Latency        latency-marker p50 (ms), max over operator subtasks whose histogram changed within the last LIVE s
                 (default 60 = the marker history window); 6 operators per keyed subtask stop receiving markers and keep
                 their last value forever, which otherwise dominates the max. Same value for both rows.
  Parallelism    job-wide p; Pred Parallelism: 1 for v_0 (as STREAMLINE's controller uses for sources), p for v_1
  Segment Size   the run's taskmanager.memory.segment-size (bytes; Flink default 32768)

Rows: consecutive sample pairs in the steady part of each profiling step (from step start + SKIP s to the step end),
after the warm-up.

Usage: python build_streamline_dataset.py RESULTS_DIR OUT_CSV [--busy max|mean] [--skip 20]
"""
import argparse
import csv
import glob
import json
import os

ap = argparse.ArgumentParser()
ap.add_argument("results")
ap.add_argument("out")
ap.add_argument("--busy", default="max", choices=["max", "mean"])
ap.add_argument("--skip", type=float, default=20.0)
ap.add_argument("--live", type=float, default=60.0, help="latency histogram must have changed within this many seconds")
ap.add_argument("--src-busy", default="flink", choices=["flink", "cpu"],
                help="v_0 Busytime: Flink REST value (default, = 1 - backpressured for Beam's legacy source) or CPU-derived (sensitivity)")
a = ap.parse_args()
HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(os.path.dirname(HERE), "beam-applications-java", "data")
COLS = ["Vertex Id", "TP_in", "Pred Parallelism", "Parallelism", "Segment Size", "Operator Type", "Latency", "CPU", "Busytime", "TP_out"]


def f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def load(path):
    r = json.load(open(path))
    v0, v1 = r["vertices"][0]["id"], r["vertices"][1]["id"]
    ip2pod = {ip: name for name, ip in r["tm_pods"]}
    tm2pod = {}
    for t in r["taskmanagers_rest"]:
        for ip, name in ip2pod.items():
            if ip and (t["id"].startswith(ip + ":") or (t.get("path") or "").find("@" + ip + ":") >= 0):
                tm2pod[t["id"]] = name
    sub_pod = {vid: {int(k): tm2pod.get(v) for k, v in m.items()} for vid, m in r["subtask_tm"].items()}
    S = [s for s in r["samples"] if "subtasks" in s and s.get("consumed_total") is not None]
    a_, b_ = S[len(S) // 4], S[-1]
    out0 = [(f(y.get("numRecordsOut")) or 0) - (f(x.get("numRecordsOut")) or 0) for x, y in zip(a_["subtasks"][v0], b_["subtasks"][v0])]
    in1 = [(f(y.get("numRecordsIn")) or 0) - (f(x.get("numRecordsIn")) or 0) for x, y in zip(a_["subtasks"][v1], b_["subtasks"][v1])]
    act0 = sub_pod[v0][max(range(len(out0)), key=lambda i: out0[i])]
    act1 = sub_pod[v1][max(range(len(in1)), key=lambda i: in1[i])]
    return r, v0, v1, sub_pod, S, act0, act1, len(tm2pod)


def tp_in0(s0, s1):
    return (s1["consumed_total"] - s0["consumed_total"]) / max(1e-9, s1["t_consumed"] - s0["t_consumed"])


RUNS = {path: load(path) for path in sorted(glob.glob(os.path.join(a.results, "*.json")))}

# per-record CPU cost of each vertex, from runs where the two active subtasks are on different TaskManagers
slopes = {0: [], 1: []}
for path, (r, v0, v1, sub_pod, S, act0, act1, _) in RUNS.items():
    if act0 == act1:
        continue
    X, Y0, Y1 = [], [], []
    for s0, s1 in zip(S, S[1:]):
        c0, c1 = s1["pod_cpu_cores"].get(act0), s1["pod_cpu_cores"].get(act1)
        if c0 is None or c1 is None or c0 > 0.95 or c1 > 0.95:  # skip CPU-capped intervals
            continue
        X.append(tp_in0(s0, s1)); Y0.append(c0); Y1.append(c1)
    n = len(X)
    if n < 10:
        continue
    mx = sum(X) / n
    for k, Y in ((0, Y0), (1, Y1)):
        my = sum(Y) / n
        slopes[k].append(sum((x - mx) * (y - my) for x, y in zip(X, Y)) / sum((x - mx) ** 2 for x in X))
cost0, cost1 = sum(slopes[0]) / len(slopes[0]), sum(slopes[1]) / len(slopes[1])
w0 = cost0 / (cost0 + cost1)  # v_0 share of a co-located TM's CPU

rows, report = [], []
for path, (r, v0, v1, sub_pod, S, act0, act1, n_mapped) in RUNS.items():
    cfg = r["config"]
    meta = json.load(open(os.path.join(DATA, cfg["workload"] + ".meta.json")))
    warm_s, step_s = float(meta["args"][0]), float(meta["args"][2])
    n_steps = len(meta["args"]) - 3
    p, seg = cfg["parallelism"], cfg.get("segment_size") or 32768
    out_ratio = r["topic_counts"]["output_written"] / max(1, r["topic_counts"]["input_produced"])  # whole-run selectivity of the pipeline
    pods = set(sub_pod[v0].values()) | set(sub_pod[v1].values())
    share0 = {}
    for pod in pods:
        if pod == act0 and pod == act1:
            share0[pod] = w0
        elif pod == act0:
            share0[pod] = 1.0
        elif pod == act1:
            share0[pod] = 0.0
        else:
            n0 = sum(1 for x in sub_pod[v0].values() if x == pod)
            n1 = sum(1 for x in sub_pod[v1].values() if x == pod)
            share0[pod] = n0 / max(1, n0 + n1)
    # latency histograms: key = (operator id, occurrence index in the sample); Prometheus line order is stable
    last_val, last_change = {}, {}
    used = 0
    prev = None
    for s1 in r["samples"]:
        c = {}
        for q, v, name in s1.get("latency", []):
            if q != 0.5:
                continue
            key = (name, c.get(name, 0))
            c[name] = c.get(name, 0) + 1
            if last_val.get(key) != v:
                last_val[key], last_change[key] = v, s1["t_abs"]
        live = [last_val[k] for k in last_val if s1["t_abs"] - last_change[k] <= a.live]
        s0, prev = prev, s1
        if s0 is None or "subtasks" not in s1 or "subtasks" not in s0 or s1.get("consumed_total") is None or s0.get("consumed_total") is None:
            continue
        t = s1["t_abs"]
        k = int((t - warm_s) // step_s)
        if t < warm_s or k >= n_steps:
            continue
        if s0["t_abs"] < warm_s + k * step_s + a.skip:
            continue
        dt = s1["t_abs"] - s0["t_abs"]
        if dt <= 0 or not live:
            continue

        def diff(vid, metric):
            return [((f(b.get(metric)) or 0) - (f(c_.get(metric)) or 0)) for b, c_ in zip(s1["subtasks"][vid], s0["subtasks"][vid])]

        busy = {vid: [min(1.0, x / (dt * 1000)) for x in diff(vid, "accumulateBusyTimeMs")] for vid in (v0, v1)}
        cpu0 = sum(s1["pod_cpu_cores"].get(pod, 0.0) * share0[pod] for pod in pods)
        cpu1 = sum(s1["pod_cpu_cores"].get(pod, 0.0) * (1 - share0[pod]) for pod in pods)
        latency = max(live)
        tp0 = tp_in0(s0, s1)
        tp_out0 = sum(diff(v0, "numRecordsOut")) / dt
        tp_in1 = sum(diff(v1, "numRecordsIn")) / dt
        if tp0 <= 0 or tp_in1 <= 0:
            continue
        agg = max if a.busy == "max" else (lambda xs: sum(xs) / len(xs))
        b0 = agg(busy[v0]) if a.src_busy == "flink" else min(1.0, cpu0)
        rows.append(["v_0", tp0, 1, p, seg, 0, latency, cpu0, max(1e-6, b0), tp_out0])
        rows.append(["v_1", tp_in1, p, p, seg, 1, latency, cpu1, max(1e-6, agg(busy[v1])), tp_in1 * out_ratio])
        used += 1
    report.append({"run": os.path.basename(path), "p": p, "segment": seg, "intervals_used": used, "tm_mapped": n_mapped,
                   "colocated": act0 == act1})

with open(a.out, "w", newline="") as fo:
    w = csv.writer(fo)
    w.writerow(COLS)
    w.writerows(rows)
print(json.dumps({"rows": len(rows), "cost_ms_per_record": {"v_0": cost0 * 1000, "v_1": cost1 * 1000}, "cost_ratio_v0_share": w0, "runs": report}, indent=1))

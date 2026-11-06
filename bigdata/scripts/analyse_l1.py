"""L1: latency at a sub-capacity rate (FD vs p=4 vs FMAX). Usage: python analyse_l1.py RESULTS_DIR"""
import glob
import json
import os
import statistics as st
import sys
from collections import defaultdict

d = sys.argv[1]
runs = [json.load(open(p)) for p in sorted(glob.glob(os.path.join(d, "p*_r*_rep*.json")))]
by_p = defaultdict(list)


def f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return 0.0


def live_quantiles(samples, window=60.0):
    """[(t, {q: [values of live histograms]})]; histogram key = (operator id, occurrence index in the sample)."""
    last, changed, res = {}, {}, []
    for s in samples:
        cur, c = {}, {}
        for q, v, name in s.get("latency", []):
            if q == 0.5:
                c[name] = c.get(name, 0) + 1
            key = (name, c.get(name, 1) - 1)
            cur.setdefault(key, {})[q] = v
        for key, qs in cur.items():
            tup = tuple(sorted(qs.items()))
            if last.get(key) != tup:
                last[key], changed[key] = tup, s["t"]
        lq = {}
        for key, tup in last.items():
            if s["t"] - changed[key] <= window:
                for q, v in tup:
                    lq.setdefault(q, []).append(v)
        res.append((s["t"], lq))
    return res


for r in runs:
    sm = dict(r["summary"])
    # per-sample latency p50/p95/p99, max over LIVE histograms (changed within the 60 s marker window; some operator subtasks
    # stop receiving markers and keep their last value forever), then mean over samples >= warm-up + 60 s
    live_q = live_quantiles(r["samples"])
    for q, k in ((0.5, "p50"), (0.95, "p95"), (0.99, "p99")):
        xs = [max(lq[q]) for t, lq in live_q if t >= r["config"]["warmup"] + 60 and lq.get(q)]
        sm[f"lat_{k}"] = st.mean(xs) if xs else None
        xs_all = [max(v for qq, v, _ in s["latency"] if qq == q) for s in r["samples"] if s["t"] >= r["config"]["warmup"] + 60 and any(qq == q for qq, _, _ in s["latency"])]
        sm[f"lat_{k}_allhist"] = st.mean(xs_all) if xs_all else None
    # backlog at offset-read samples (offsets every 30 s; consumed counter <= ~1 s stale)
    bl = [s["input_end"] - s["consumed_total"] for s in r["samples"] if s["t"] >= r["config"]["warmup"] and s.get("input_end") is not None and s.get("consumed_total") is not None]
    sm["backlog_max"] = max(bl) if bl else None
    pl = r.get("placement") or {}
    names = list(pl)
    if names:
        src, kv = pl[names[0]], pl[names[-1]]
        a_src = max(src, key=lambda s: f(s.get("numRecordsOutPerSecond")))
        a_kv = max(kv, key=lambda s: f(s.get("busyTimeMsPerSecond")))
        sm["colocated"] = a_src["tm"] == a_kv["tm"]
        sm["hot_busy"] = f(a_kv.get("busyTimeMsPerSecond"))
    sm["label"] = r["label"]
    by_p[r["config"]["parallelism"]].append(sm)


def ms(xs, nd=0):
    xs = [x for x in xs if x is not None]
    if not xs:
        return "n/a"
    return f"{st.mean(xs):.{nd}f} ± {st.stdev(xs):.{nd}f}" if len(xs) > 1 else f"{xs[0]:.{nd}f}"


print("| Config | runs | TMs | ingested el/s | throughput el/s | latency p50 ms | p95 ms | p99 ms | Kafka backlog (mean / max records) "
      "| busiest Interpolate subtask ms/s | TM CPU cores | util. of used TMs | cost $/h |")
print("|---|---|---|---|---|---|---|---|---|---|---|---|---|")
name = {1: "FD (p=1)", 4: "p=4", 8: "FMAX (p=8)"}
for p in sorted(by_p):
    g = by_p[p]
    print(f"| {name.get(p, p)} | {len(g)} | {g[0]['taskmanagers_used']} | {ms([x['input_rate_achieved'] for x in g])} | {ms([x['source_out_rate_mean'] for x in g])} "
          f"| {ms([x['lat_p50'] for x in g], 1)} | {ms([x['lat_p95'] for x in g], 1)} | {ms([x['lat_p99'] for x in g], 1)} "
          f"| {ms([x['kafka_backlog_mean'] for x in g])} / {max([x['backlog_max'] or 0 for x in g]):.0f} | {ms([x['hot_busy'] for x in g])} "
          f"| {ms([x['tm_cpu_cores_mean'] for x in g], 2)} | {ms([x['tm_cpu_utilisation_of_used'] for x in g], 2)} | {g[0]['emulated_cost_per_h']:.3f} |")
print("\nPer run:\n\n| run | p50 | p95 | p99 | throughput | hot subtasks co-located | busiest Interpolate ms/s | TMs |\n|---|---|---|---|---|---|---|---|")
for p in sorted(by_p):
    for x in by_p[p]:
        print(f"| {x['label']} | {x['lat_p50']:.1f} ({x['lat_p50_allhist']:.0f}) | {x['lat_p95']:.1f} ({x['lat_p95_allhist']:.0f}) | {x['lat_p99']:.1f} ({x['lat_p99_allhist']:.0f}) | {x['source_out_rate_mean']:.0f} | {'yes' if x.get('colocated') else 'no'} "
              f"| {x.get('hot_busy', 0):.0f} | {x['taskmanagers_used']} |")

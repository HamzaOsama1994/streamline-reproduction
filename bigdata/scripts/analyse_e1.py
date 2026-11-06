"""Aggregate E1 (ETL/FIT) runs: mean ± SD over repetitions per parallelism; relative comparison FD vs FMAX
against the paper's FIT statements (§5.1.1).

Usage: python analyse_e1.py RESULTS_DIR > summary.md
"""
import glob
import json
import os
import statistics as st
import sys
from collections import defaultdict

d = sys.argv[1]
runs = [json.load(open(p)) for p in sorted(glob.glob(os.path.join(d, "p*_r*_rep*.json")))]
by_p = defaultdict(list)
for r in runs:
    W = [s for s in r["samples"] if s["t"] >= r["config"]["warmup"]]
    # latency per sample: (a) max over operator/subtask entries (harness summary), (b) median over entries
    p50_med = [st.median([v for q, v, _ in s["latency"] if q == 0.5]) for s in W if any(q == 0.5 for q, _, _ in s["latency"])]
    p99_med = [st.median([v for q, v, _ in s["latency"] if q == 0.99]) for s in W if any(q == 0.99 for q, _, _ in s["latency"])]
    sm = r["summary"]
    sm["lat_p50_median_ops"] = st.mean(p50_med) if p50_med else None
    sm["lat_p99_median_ops"] = st.mean(p99_med) if p99_med else None
    sm["backlog_growth"] = sm["input_rate_achieved"] - sm["source_out_rate_mean"]
    by_p[r["config"]["parallelism"]].append(sm)


def ms(xs, f=0):
    xs = [x for x in xs if x is not None]
    if not xs:
        return "n/a"
    return f"{st.mean(xs):.{f}f} ± {st.stdev(xs):.{f}f}" if len(xs) > 1 else f"{xs[0]:.{f}f}"


def m(p, k):
    xs = [x[k] for x in by_p[p] if x[k] is not None]
    return st.mean(xs) if xs else None


print("| p | reps | TMs | input achieved el/s | throughput (source out) el/s | sink in el/s | latency p50 ms (max / median over ops) | latency p99 ms (max / median) "
      "| TM CPU cores | CPU util of used TMs | Interpolate busy max ms/s | cost $/h |")
print("|---|---|---|---|---|---|---|---|---|---|---|---|")
for p in sorted(by_p):
    g = by_p[p]
    busy2 = [list(x["per_vertex"].values())[-1]["busy_ms_per_s_max"] for x in g]
    print(f"| {p} | {len(g)} | {g[0]['taskmanagers_used']} | {ms([x['input_rate_achieved'] for x in g])} | {ms([x['source_out_rate_mean'] for x in g])} "
          f"| {ms([x['sink_in_rate_mean'] for x in g])} | {ms([x['latency_marker_p50_ms_mean_of_max'] for x in g])} / {ms([x['lat_p50_median_ops'] for x in g])} "
          f"| {ms([x['latency_marker_p99_ms_mean_of_max'] for x in g])} / {ms([x['lat_p99_median_ops'] for x in g])} | {ms([x['tm_cpu_cores_mean'] for x in g], 2)} "
          f"| {ms([x['tm_cpu_utilisation_of_used'] for x in g], 2)} | {ms(busy2)} | {g[0]['emulated_cost_per_h']:.3f} |")

if 1 in by_p and max(by_p) in by_p:
    fmax = max(by_p)
    tp_fd, tp_fx = m(1, "source_out_rate_mean"), m(fmax, "source_out_rate_mean")
    print()
    print(f"FD (p=1) vs FMAX (p={fmax}): throughput {100 * (tp_fd / tp_fx - 1):+.1f}% "
          f"(paper FIT: FD 34% lower than STREAMLINE, FMAX within 1% of STREAMLINE => FD ≈ -34% vs FMAX); "
          f"cost FMAX/FD = {m(fmax, 'emulated_cost_per_h') / m(1, 'emulated_cost_per_h'):.1f}x; "
          f"CPU utilisation of used TMs FD {m(1, 'tm_cpu_utilisation_of_used'):.2f} vs FMAX {m(fmax, 'tm_cpu_utilisation_of_used'):.2f}.")

# ---------------- placement analysis (runs that recorded per-subtask placement) ----------------
rows = []
for r in runs:
    pl = r.get("placement")
    if not pl:
        continue
    names = list(pl)
    src, kv = pl[names[0]], pl[names[-1]]
    f = lambda x: float(x) if x not in (None, "NaN") else 0.0  # noqa: E731
    a_src = max(src, key=lambda s: f(s.get("numRecordsOutPerSecond")))
    a_kv = max(kv, key=lambda s: f(s.get("busyTimeMsPerSecond")))
    rows.append((r["config"]["parallelism"], r["label"], a_src["tm"] == a_kv["tm"], r["summary"]["source_out_rate_mean"],
                 f(a_kv.get("busyTimeMsPerSecond")), len({s["tm"] for s in src + kv})))
if rows:
    print("\n### Placement of the two hot subtasks (active source subtask, busiest Interpolate subtask)\n")
    print("| run | p | hot subtasks on same TM | throughput el/s | busiest Interpolate subtask ms/s | TMs used |")
    print("|---|---|---|---|---|---|")
    for p, lab, same, tp, busy, ntm in sorted(rows):
        print(f"| {lab} | {p} | {'yes' if same else 'no'} | {tp:.0f} | {busy:.0f} | {ntm} |")
    for same in (True, False):
        xs = [tp for p, _, s, tp, _, _ in rows if s == same and p > 2]
        if xs:
            print(f"\np>2, hot subtasks {'co-located' if same else 'separated'}: throughput {st.mean(xs):.0f} el/s (n={len(xs)})", end="")
    print()

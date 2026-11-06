"""E3 analysis: measured outcome of STREAMLINE's decisions (V0 as released vs V4 corrected) and of the FD/FMAX baselines.

Usage: python analyse_e3.py EVAL_DIR DECISIONS_JSON [HORIZON_FROM_S=150]

Latency = true end-to-end latency (broker LogAppendTime of the i-th output record - that of the i-th input record) of the
records that ENTERED during the horizon (t >= warm-up); every 10th record is stored by the harness.
Forecast error: STREAMLINE's 50-step forecast (5 s steps) vs the ingestion measured in the same run, per 5 s bin of input
record broker time (horizon = 150..400 s of the evaluation replay = 1800..2050 s of the sine the predictor was trained on).
"""
import glob
import json
import os
import statistics as st
import sys
from collections import defaultdict

ev, decf = sys.argv[1], sys.argv[2]
H0 = float(sys.argv[3]) if len(sys.argv) > 3 else 150.0
dec = json.load(open(decf))["decisions"]


def q(xs, f):
    return xs[int(f * (len(xs) - 1))]


by_cfg = defaultdict(list)
ingest = []
for path in sorted(glob.glob(os.path.join(ev, "p*_tm*_seg*_rep*.json"))):
    r = json.load(open(path))
    cfg = os.path.basename(path).rsplit("_rep", 1)[0]
    sm = r["summary"]
    e = r.get("e2e") or {}
    L = e.get("lat_by_input_time_ms") or []
    xs = sorted(l for t, l in L if t >= H0 * 1000)
    # ingestion per 5 s bin from input broker timestamps (every 10th record stored -> x10)
    bins = defaultdict(int)
    for t, _ in L:
        b = int((t / 1000 - H0) // 5)
        if 0 <= b < 50:
            bins[b] += 10
    rate = [bins[b] / 5 for b in range(50)]
    ingest.append(rate)
    # latency over time: per 25 s bin p50 (for the plot / sanity)
    lt = defaultdict(list)
    for t, l in L:
        lt[int(t / 1000 // 25)].append(l)
    by_cfg[cfg].append({
        "run": os.path.basename(path)[:-5], "n": len(xs),
        "p50": q(xs, .5) if xs else None, "p95": q(xs, .95) if xs else None, "p99": q(xs, .99) if xs else None,
        "mean": st.mean(xs) if xs else None, "max": xs[-1] if xs else None,
        "order_match": e.get("order_match_fraction"),
        "ingested": sm["input_rate_achieved"], "processed": sm["output_written"] / max(1, sm["input_produced"]),
        "tms": sm["taskmanagers_used"], "cost": sm["emulated_cost_per_h"], "cpu": sm["tm_cpu_cores_mean"],
        "backlog_mean": sm.get("kafka_backlog_mean"),
        "lat_p50_by_25s": {k * 25: sorted(v)[len(v) // 2] for k, v in sorted(lt.items())},
        "colocated": None,
    })


def ms(xs, nd=0):
    xs = [x for x in xs if x is not None]
    if not xs:
        return "n/a"
    return f"{st.mean(xs):.{nd}f} ± {st.stdev(xs):.{nd}f}" if len(xs) > 1 else f"{xs[0]:.{nd}f}"


print("## Per configuration (true end-to-end latency of records entering in the horizon)\n")
print("| config | runs | TMs | cost $/h | e2e p50 ms | p95 ms | p99 ms | mean ms | max ms | TM CPU cores | records processed / produced |")
print("|---|---|---|---|---|---|---|---|---|---|---|")
for cfg, g in sorted(by_cfg.items()):
    print(f"| {cfg} | {len(g)} | {g[0]['tms']} | {g[0]['cost']:.3f} | {ms([x['p50'] for x in g])} | {ms([x['p95'] for x in g])} | {ms([x['p99'] for x in g])} "
          f"| {ms([x['mean'] for x in g])} | {ms([x['max'] for x in g])} | {ms([x['cpu'] for x in g], 2)} | {ms([x['processed'] for x in g], 3)} |")
print("\nPer run:\n\n| run | e2e p50 | p95 | p99 | max | order match (3 slices) |\n|---|---|---|---|---|---|")
for cfg, g in sorted(by_cfg.items()):
    for x in g:
        print(f"| {x['run']} | {x['p50']} | {x['p95']} | {x['p99']} | {x['max']} | {x['order_match']} |")

# variants: frequency-weighted over seeds
print("\n## Variants (expected outcome over seeds = frequency-weighted mean of the measured configurations)\n")
print("| variant | seeds ok | configurations chosen (count) | E[cost $/h] | E[e2e p50 ms] | E[p95 ms] | E[p99 ms] | E[mean ms] |")
print("|---|---|---|---|---|---|---|---|")
cfg_mean = {c: {k: st.mean([x[k] for x in g if x[k] is not None]) for k in ("p50", "p95", "p99", "mean", "cost")} for c, g in by_cfg.items() if g}
for var in ("V0_original", "V4_corrected"):
    D = [d for d in dec if d["variant"] == var and d.get("ok")]
    cnt = defaultdict(int)
    for d in D:
        f = d["flink"]
        cnt[f"p{f['job_parallelism']}_tm{f['taskmanagers']}_seg{f['segment_size']}"] += 1
    if not D or any(c not in cfg_mean for c in cnt):
        print(f"| {var} | {len(D)} | {dict(cnt)} | (not all configurations measured) |")
        continue
    E = {k: sum(cfg_mean[c][k] * n for c, n in cnt.items()) / len(D) for k in ("cost", "p50", "p95", "p99", "mean")}
    print(f"| {var} | {len(D)}/{sum(1 for d in dec if d['variant'] == var)} | {dict(cnt)} | {E['cost']:.3f} | {E['p50']:.0f} | {E['p95']:.0f} | {E['p99']:.0f} | {E['mean']:.0f} |")

# forecast accuracy
HERE = os.path.dirname(os.path.abspath(__file__))
WL = os.path.join(os.path.dirname(HERE), "beam-applications-java", "data", os.environ.get("E3_WORKLOAD", "e3_sine_eval") + ".csv")  # E3-TAXI: E3_WORKLOAD=e3t_taxi_eval
if ingest and os.path.exists(WL):
    # actual workload in the horizon: replay schedule x 0.8 (the authors' producer sends 4 of every 5 lines, section 9);
    # the per-run broker-timestamp ingestion is printed as a cross-check (it misses records still unprocessed at run end)
    sched = [0] * 50
    with open(WL) as fh:
        next(fh)
        for line in fh:
            b = int((int(line.split("|", 1)[0]) / 1000 - H0) // 5)
            if 0 <= b < 50:
                sched[b] += 1
    actual = [0.8 * c / 5 for c in sched]
    measured = [st.mean(col) for col in zip(*ingest)]
    print(f"\nmeasured ingestion (broker timestamps, mean of runs): mean {st.mean(measured):.0f} el/s vs schedule x 0.8: {st.mean(actual):.0f} el/s")
    print(f"\n## Workload forecast vs actual workload (horizon {H0:.0f}-{H0 + 250:.0f} s, 50 x 5 s; actual = replay schedule x 0.8)\n")
    print(f"actual: mean {st.mean(actual):.0f}, min {min(actual):.0f}, max {max(actual):.0f} el/s\n")
    print("| variant | seed | forecast mean | MAE el/s | MAPE % | bias el/s | negative values |\n|---|---|---|---|---|---|---|")
    agg = defaultdict(list)
    for d in dec:
        fc = d.get("forecast") or []
        if len(fc) != 50:
            continue
        err = [f - a for f, a in zip(fc, actual)]
        mae = st.mean(abs(x) for x in err)
        mape = 100 * st.mean(abs(x) / a for x, a in zip(err, actual) if a > 0)
        agg[d["variant"]].append((mae, mape, st.mean(err)))
        print(f"| {d['variant']} | {d['seed']} | {st.mean(fc):.0f} | {mae:.0f} | {mape:.1f} | {st.mean(err):+.0f} | {sum(1 for f in fc if f < 0)} |")
    print("\n| variant | MAE mean ± sd over seeds | MAPE % | bias |\n|---|---|---|---|")
    for v, xs in agg.items():
        print(f"| {v} | {ms([x[0] for x in xs])} | {ms([x[1] for x in xs], 1)} | {ms([x[2] for x in xs])} |")
    json.dump({"actual_rate_5s_schedule": actual, "measured_rate_5s_mean_of_runs": measured, "runs": len(ingest)}, open(os.path.join(ev, "horizon_actual_ingestion.json"), "w"))

"""Aggregate correction-study runs (correction_study/runs/*.json) into tables and factorial effects.

Usage (from streamline_repro/):  env/Scripts/python.exe repro_scripts/analyse_correction_study.py correction_study
Writes  <dir>/results/summary.md, per_run.csv, effects.csv
"""
import collections
import glob
import itertools
import json
import os
import sys

import numpy as np
import pandas as pd

ROOT = sys.argv[1]
RES = os.path.join(ROOT, "results")
os.makedirs(RES, exist_ok=True)
ORDER = ["V0_original", "V1_key", "V2_eval", "V3_scaling", "V4_all", "V5_key+eval", "V6_key+scaling", "V7_eval+scaling"]
SLOTS = [("initial", "pipeline_1"), ("initial", "pipeline_2"), ("periodic", "pipeline_1"), ("periodic", "pipeline_2")]
OPS = ["pipeline_1_v_0", "pipeline_1_v_1", "pipeline_1_v_2", "pipeline_1_v_3", "pipeline_2_v_5", "pipeline_2_v_6", "pipeline_2_v_7"]

runs = [json.load(open(p)) for p in sorted(glob.glob(os.path.join(ROOT, "runs", "*.json")))]
timing = {}
for p in glob.glob(os.path.join(ROOT, "runs_timing", "*.json")):
    r = json.load(open(p))
    timing[r["variant"]] = r

rows = []
for r in runs:
    row = {"variant": r["variant"], "K": r["K"], "D": r["D"], "S": r["S"], "seed": r["seed"]}
    fc = {(f["phase"], f["pipeline"]): f for f in r["forecasts"]}
    pts = neg = oor = nonfinite = 0
    for ph, pl in SLOTS:
        f = fc[(ph, pl)]
        tag = f"{ph[:4]}_{pl[-1]}"
        row[f"mae_{tag}"] = f["mae"]
        row[f"rmse_{tag}"] = f["rmse"]
        row[f"winmean_{tag}"] = f["window_mean"]
        pts += f["horizon"]; neg += f["n_negative"]; oor += f["n_out_of_range"]; nonfinite += f["n_nonfinite"]
    row["mae_mean"] = np.mean([row[f"mae_{p[:4]}_{q[-1]}"] for p, q in SLOTS])
    row["rmse_mean"] = np.mean([row[f"rmse_{p[:4]}_{q[-1]}"] for p, q in SLOTS])
    row["neg_share"] = neg / pts
    row["neg_count"] = neg
    row["out_of_range_share"] = oor / pts
    row["nonfinite"] = nonfinite
    for ph in ("initial", "periodic"):
        snap = r[ph]
        row[f"cost_{ph}"] = snap["hourly_cost"]
        row[f"nres_{ph}"] = snap["n_resources"]
        row[f"instances_{ph}"] = snap["total_instances"]
        row[f"util_{ph}"] = ";".join(f"{k}:{v['utilisation']:.3f}" for k, v in snap["resources_used"].items())
        row[f"placement_{ph}"] = "|".join(f"{k}:{v['utilisation']:.4f}" for k, v in snap["resources_used"].items())
    row["par_periodic"] = "-".join(str(int(r["periodic"]["configuration"][o]["parallelism"])) for o in OPS)
    row["par_initial"] = "-".join(str(int(r["initial"]["configuration"][o]["parallelism"])) for o in OPS)
    row["seg_sizes"] = ",".join(sorted({str(v["segment_size"]) for v in r["periodic"]["configuration"].values()}))
    row["rescheduled"] = len(r["reschedule_events"])
    row["opt_s_contended"] = r["optimizer_seconds_total"]
    # The laptop slept 2026-11-04 ~23:59 -> 2026-11-05 ~07:03; QueryPerformanceCounter keeps counting through
    # sleep, so runs straddling it have invalid timings (decisions/forecasts are unaffected).
    row["timing_invalid_sleep"] = r["tuning_seconds_total"] > 1000
    rows.append(row)

df = pd.DataFrame(rows)
df["variant"] = pd.Categorical(df["variant"], ORDER, ordered=True)
df = df.sort_values(["variant", "seed"])
df.to_csv(os.path.join(RES, "per_run.csv"), index=False)


def ms(x, d=1):
    return f"{np.mean(x):.{d}f} ± {np.std(x, ddof=1):.{d}f}" if len(x) > 1 else f"{np.mean(x):.{d}f}"


def mode_share(s):
    c = collections.Counter(s)
    v, n = c.most_common(1)[0]
    return f"{v} ({n}/{len(s)})" + (f"; {len(c)} distinct" if len(c) > 1 else "")


out = ["# Correction study — results", "",
       f"Runs: {len(df)} ({df.groupby('variant', observed=True).size().to_dict()}). Values are mean ± SD across seeds "
       "(seed 'orig' + seeds 0–9). Same checkpoints, workloads, resources and seed set for every variant.", ""]

out += ["## 1. Forecast accuracy (source operators, horizon 50 steps, events/s)", "",
        "| Variant | K D S | MAE init p1 | MAE init p2 | MAE periodic p1 | MAE periodic p2 | mean MAE | mean RMSE | negative share | out-of-range share |",
        "|---|---|---|---|---|---|---|---|---|---|"]
for v, g in df.groupby("variant", observed=True):
    out.append(f"| {v} | {g.K.iat[0]} {g.D.iat[0]} {g.S.iat[0]} | {ms(g.mae_init_1)} | {ms(g.mae_init_2)} | {ms(g.mae_peri_1)} | {ms(g.mae_peri_2)} "
               f"| {ms(g.mae_mean)} | {ms(g.rmse_mean)} | {ms(g.neg_share, 3)} | {ms(g.out_of_range_share, 3)} |")
out += ["", "Pipeline-2 periodic input window mean (shows whether the new workload reached the predictor): " +
        ", ".join(f"{v}: {g.winmean_peri_2.mean():.1f}" for v, g in df.groupby('variant', observed=True)), ""]

out += ["## 2. Decisions: parallelism, placement, cost", "",
        "Parallelism vector order: p1 v_0,v_1,v_2,v_3 | p2 v_5,v_6,v_7. Cost = sum of $/h of resources with load > 0 (examples/utils.py prices).", "",
        "| Variant | initial parallelism (mode) | periodic parallelism (mode) | segment sizes | resources after periodic | distinct periodic placements | cost initial $/h | cost periodic $/h | reschedules |",
        "|---|---|---|---|---|---|---|---|---|"]
for v, g in df.groupby("variant", observed=True):
    out.append(f"| {v} | {mode_share(g.par_initial)} | {mode_share(g.par_periodic)} | {mode_share(g.seg_sizes)} | {ms(g.nres_periodic, 2)} "
               f"| {g.placement_periodic.nunique()} | {ms(g.cost_initial, 3)} | {ms(g.cost_periodic, 3)} | {ms(g.rescheduled, 2)} |")
out.append("")

out += ["## 3. Optimizer execution time", ""]
if timing:
    out += ["Uncontended sequential pass (seed 'orig', 1 process, all threads), NSGA-II `solve()` wall time summed over the 14 operator optimisations:", "",
            "| Variant | optimizer s (total) | per-operator s (mean ± SD) | tuning total s |", "|---|---|---|---|"]
    for v in ORDER:
        if v in timing:
            t = timing[v]
            per = [x["seconds"] for x in t["solve_times"]]
            out.append(f"| {v} | {t['optimizer_seconds_total']:.1f} | {ms(per, 2)} | {t['tuning_seconds_total']:.1f} |")
    out.append("")
out += ["Contended batch (8 parallel processes × 2 threads; comparable between variants only), median optimizer seconds per run "
        "(runs whose timing spans the laptop's sleep period are excluded: " +
        ", ".join(f"{r.variant}/{r.seed}" for r in df[df.timing_invalid_sleep].itertuples()) + "): " +
        ", ".join(f"{v}: {g[~g.timing_invalid_sleep].opt_s_contended.median():.0f}s (n={int((~g.timing_invalid_sleep).sum())})"
                  for v, g in df.groupby('variant', observed=True)), ""]

# ---------------- factorial effects (paired by seed) ----------------
code = lambda x: 2 * x - 1  # noqa: E731
terms = {"K": ("K",), "D": ("D",), "S": ("S",), "KxD": ("K", "D"), "KxS": ("K", "S"), "DxS": ("D", "S"), "KxDxS": ("K", "D", "S")}
outcomes = {"mae_peri_2": "MAE periodic p2", "mae_mean": "mean MAE (4 forecasts)", "rmse_mean": "mean RMSE",
            "neg_share": "negative-forecast share", "cost_periodic": "periodic cost $/h", "instances_periodic": "operator instances (periodic)"}
eff_rows = []
seeds = sorted(set(df.seed))
complete = [s for s in seeds if (df.seed == s).sum() == 8]
for y, label in outcomes.items():
    for tname, factors in terms.items():
        per_seed = []
        for s in complete:
            g = df[df.seed == s]
            sign = np.prod([g[f].map(code).values for f in factors], axis=0)
            per_seed.append(float((g[y].values * sign).sum() / 4))  # effect = mean(+) - mean(-)
        per_seed = np.array(per_seed)
        se = per_seed.std(ddof=1) / np.sqrt(len(per_seed)) if len(per_seed) > 1 else float("nan")
        eff_rows.append({"outcome": label, "term": tname, "effect": per_seed.mean(), "se": se,
                         "ci95_lo": per_seed.mean() - 1.96 * se, "ci95_hi": per_seed.mean() + 1.96 * se, "n_seeds": len(per_seed)})
eff = pd.DataFrame(eff_rows)
eff.to_csv(os.path.join(RES, "effects.csv"), index=False)

out += ["## 4. Factorial effects (2³, paired by seed)", "",
        f"Effect = mean(outcome | factor on) − mean(outcome | factor off), averaged over the other factors; interactions use the standard ±1 contrast. "
        f"Computed per seed, then mean ± 95% CI over {len(complete)} seeds. An interaction whose CI excludes 0 means the fixes are **not additive**.", ""]
for label in outcomes.values():
    e = eff[eff.outcome == label]
    out.append(f"**{label}:** " + "; ".join(
        f"{r.term} {r.effect:+.3f} [{r.ci95_lo:+.3f}, {r.ci95_hi:+.3f}]{' *' if (r.ci95_lo > 0 or r.ci95_hi < 0) else ''}" for r in e.itertuples()))
    out.append("")
out.append("`*` = 95% CI excludes zero.")

# additivity check for the five pre-registered variants
out += ["", "## 5. Additivity check (pre-registered V0–V4)", "",
        "Predicted V4 = V0 + (V1−V0) + (V2−V0) + (V3−V0), from variant means. A large gap between predicted and observed V4 shows the single-fix contrasts cannot be summed.", "",
        "| Outcome | V0 | V1 | V2 | V3 | V4 observed | V4 additive prediction | gap |", "|---|---|---|---|---|---|---|---|"]
means = df.groupby("variant", observed=True)[list(outcomes)].mean()
needed = ["V0_original", "V1_key", "V2_eval", "V3_scaling", "V4_all"]
for y, label in (outcomes.items() if all(v in means.index for v in needed) else []):
    m = means[y]
    pred = m["V0_original"] + (m["V1_key"] - m["V0_original"]) + (m["V2_eval"] - m["V0_original"]) + (m["V3_scaling"] - m["V0_original"])
    out.append(f"| {label} | {m['V0_original']:.3f} | {m['V1_key']:.3f} | {m['V2_eval']:.3f} | {m['V3_scaling']:.3f} | {m['V4_all']:.3f} | {pred:.3f} | {m['V4_all'] - pred:+.3f} |")

open(os.path.join(RES, "summary.md"), "w", encoding="utf-8").write("\n".join(out) + "\n")
print("\n".join(out))

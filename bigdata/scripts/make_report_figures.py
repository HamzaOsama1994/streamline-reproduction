"""Figures for the final report (Assignment 2/report/). Reads only result files; writes PNGs (300 dpi).

  fig1_throughput_placement.png  E1 FIT + E1-TAXI: saturated throughput per run, by configuration and hot-subtask placement
  fig2_e3_forecast.png           E3 (FIT): STREAMLINE forecasts (V0 x 11 seeds, V4) vs the actual workload over the horizon
  fig3_e3t.png                   E3-T (TAXI real pattern): workload + forecasts, and end-to-end latency over time per configuration

Usage: python make_report_figures.py OUT_DIR [fig1|fig2|fig3 ...]
Palette: validated reference slots blue #2a78d6, orange #eb6834, aqua #1baf7a (dataviz skill, all-pairs PASS on light);
identity is never colour-only (marker shape / legend / direct labels).
"""
import glob
import gzip
import json
import os
import statistics as st
import sys
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
BIG = os.path.dirname(HERE)
OUT = sys.argv[1]
WHICH = set(sys.argv[2:]) or {"fig1", "fig2", "fig3"}
BLUE, ORANGE, AQUA = "#2a78d6", "#eb6834", "#1baf7a"
INK, INK2, GRID = "#0b0b0b", "#52514e", "#e4e3df"
plt.rcParams.update({"font.family": "serif", "font.serif": ["Times New Roman", "DejaVu Serif"], "font.size": 8.5,
                     "axes.edgecolor": INK2, "axes.labelcolor": INK, "xtick.color": INK2, "ytick.color": INK2,
                     "axes.spines.top": False, "axes.spines.right": False, "axes.grid": True, "grid.color": GRID,
                     "grid.linewidth": 0.6, "axes.axisbelow": True, "legend.frameon": False, "lines.linewidth": 1.4})


def colocated(r):
    pl = r.get("placement")
    names = list(pl)
    f = lambda x: float(x) if x not in (None, "NaN") else 0.0  # noqa: E731
    a_src = max(pl[names[0]], key=lambda s: f(s.get("numRecordsOutPerSecond")))
    a_kv = max(pl[names[-1]], key=lambda s: f(s.get("busyTimeMsPerSecond")))
    return a_src["tm"] == a_kv["tm"]


def fig1():
    fig, axes = plt.subplots(1, 2, figsize=(7.2, 2.7), sharey=True)
    cfg = [(1, "FD\n(p = 1)"), (2, "p = 2"), (4, "p = 4"), (8, "FMAX\n(p = 8)")]
    for ax, (title, d) in zip(axes, [("FIT (E1)", "E1_fit_r4000"), ("TAXI (E1-TAXI)", "E1_taxi_r3500")]):
        for k, (p, _) in enumerate(cfg):
            runs = [json.load(open(f)) for f in sorted(glob.glob(os.path.join(BIG, "results", d, f"p{p}_r*_rep*.json")))]
            tps = [r["summary"]["source_out_rate_mean"] for r in runs]
            for j, r in enumerate(runs):
                co = colocated(r)
                ax.scatter(k + (j - 1) * 0.12, tps[j], s=34, marker="o" if co else "^", color=ORANGE if co else BLUE,
                           edgecolor="white", linewidth=0.8, zorder=3)
            ax.hlines(st.mean(tps), k - 0.28, k + 0.28, color=INK2, linewidth=1.0, zorder=2)
        ax.set_xticks(range(len(cfg)), [c for _, c in cfg])
        ax.set_title(title, fontsize=9, color=INK)
        ax.set_xlim(-0.6, 3.6)
        ax.grid(axis="x", visible=False)
    axes[0].set_ylabel("Throughput (el/s)")
    axes[0].set_ylim(900, 2100)
    h = [plt.Line2D([], [], marker="o", ls="", color=ORANGE, markeredgecolor="white", markersize=6, label="hot subtasks on the same TaskManager"),
         plt.Line2D([], [], marker="^", ls="", color=BLUE, markeredgecolor="white", markersize=6, label="hot subtasks on different TaskManagers"),
         plt.Line2D([], [], color=INK2, lw=1.0, label="mean of 3 runs")]
    fig.legend(handles=h, loc="lower center", ncol=3, fontsize=8, bbox_to_anchor=(0.5, -0.02))
    fig.tight_layout(rect=(0, 0.08, 1, 1))
    fig.savefig(os.path.join(OUT, "fig1_throughput_placement.png"), dpi=300)


def forecast_panel(ax, decisions, actual, t0=0.0):
    dec = json.load(open(decisions))["decisions"]
    x = [t0 + 5 * i for i in range(50)]
    first = True
    for d in dec:
        if d["variant"] == "V0_original" and len(d.get("forecast") or []) == 50:
            ax.plot(x, d["forecast"], color=ORANGE, lw=0.8, alpha=0.55, label="V0 as released (11 seeds)" if first else None)
            first = False
    v4 = next(d for d in dec if d["variant"] == "V4_corrected" and len(d.get("forecast") or []) == 50)
    ax.plot(x, v4["forecast"], color=BLUE, lw=1.6, label="V4 corrected")
    ax.plot(x, actual, color=INK, lw=1.6, ls="--", label="actual (replay schedule × 0.8)")
    ax.set_ylabel("Workload (el/s)")


def fig2():
    act = json.load(open(os.path.join(BIG, "results", "E3_eval", "horizon_actual_ingestion.json")))["actual_rate_5s_schedule"]
    fig, ax = plt.subplots(figsize=(3.45, 2.5))
    forecast_panel(ax, os.path.join(BIG, "results", "E3_work", "models_main", "decisions.json"), act)
    ax.axhline(1100, color=INK2, lw=0.8, ls=":")
    ax.text(247, 1110, "FD capacity ≈ 1,100", ha="right", va="bottom", fontsize=7, color=INK2)
    ax.set_xlabel("Time into the 250 s horizon (s)")
    ax.legend(fontsize=7, loc="lower left")
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "fig2_e3_forecast.png"), dpi=300)


def fig3():
    ev = os.path.join(BIG, "results", "E3T_eval")
    act = json.load(open(os.path.join(ev, "horizon_actual_ingestion.json")))["actual_rate_5s_schedule"]
    spec = json.load(open(os.path.join(BIG, "scripts", "e3t_eval_spec.json")))
    # workload over the whole run: replay schedule x 0.8 in 5 s bins
    sched = defaultdict(int)
    with open(os.path.join(BIG, "beam-applications-java", "data", "e3t_taxi_eval.csv"), encoding="utf-8") as f:
        next(f)
        for line in f:
            sched[int(int(line.split("|", 1)[0]) / 1000 // 5)] += 1
    xs = sorted(k for k in sched if k < 80)
    fig, (a1, a2) = plt.subplots(2, 1, figsize=(7.2, 4.6), sharex=True, gridspec_kw={"height_ratios": [1, 1.25]})
    a1.plot([5 * k for k in xs], [0.8 * sched[k] / 5 for k in xs], color=INK, lw=1.4, ls="--", label="actual (replay schedule × 0.8)")
    forecast_panel(a1, os.path.join(BIG, "results", "E3T_work", "models_main", "decisions.json"), act, t0=150.0)
    a1.lines[0].set_label(None)
    a1.axvspan(0, 150, color=GRID, alpha=0.5, lw=0)
    a1.text(75, 120, "warm-up (discarded)", ha="center", va="bottom", fontsize=7, color=INK2)
    a1.legend(fontsize=7, loc="lower center", bbox_to_anchor=(0.5, 1.0), ncol=3)
    # latency: runs grouped by configuration AND hot-subtask placement (placement, not configuration, decides the outcome);
    # per group, median over its runs of the per-5 s-bin median e2e latency (complete traces)
    groups = defaultdict(list)
    for c, who in spec["configs"].items():
        for tr in sorted(glob.glob(os.path.join(ev, f"{c}_rep*.e2e_trace.csv.gz"))):
            co = colocated(json.load(open(tr.replace(".e2e_trace.csv.gz", ".json"))))
            if who == ["FD"]:
                g = "FD (p = 1, one TaskManager)"
            elif who == ["FMAX"]:
                g = "FMAX, hot subtasks " + ("on the same TaskManager" if co else "on different TaskManagers")
            else:
                g = "STREAMLINE p = 6 and p = 7" + (", co-located" if co else ", hot subtasks separated")
            groups[g].append(tr)
    style = {"FD": (ORANGE, "-"), "FMAX, hot subtasks on the same": (BLUE, "-"), "FMAX, hot subtasks on different": (BLUE, "--"),
             "STREAMLINE": (AQUA, "-")}
    for g in sorted(groups, key=lambda g: ("FD" not in g, "FMAX" not in g, g)):
        per_run = []
        for tr in groups[g]:
            b = defaultdict(list)
            with gzip.open(tr, "rt") as f:
                next(f)
                for line in f:
                    i, trel, _, _, lat = line.rstrip("\n").split(",")
                    if lat:
                        b[int(int(trel) / 1000 // 5)].append(int(lat))
            per_run.append({k2: st.median(v) for k2, v in b.items()})
        bins = sorted(set().union(*per_run))
        ys = [st.median([r[k2] for r in per_run if k2 in r]) for k2 in bins]
        col, ls = next(v for k, v in style.items() if g.startswith(k))
        n = len(per_run)
        a2.plot([5 * k2 for k2 in bins], ys, color=col, ls=ls, lw=1.4, label=f"{g} ({n} run{'s' if n > 1 else ''})")
    a2.set_yscale("log")
    a2.set_ylabel("e2e latency, bin median (ms)")
    a2.set_xlabel("Time since start of run (s); horizon = 150–400 s (Fri 14:00 – Sat 05:17)")
    a2.axvspan(0, 150, color=GRID, alpha=0.5, lw=0)
    a2.legend(fontsize=7, loc="upper left")
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "fig3_e3t.png"), dpi=300)


for name in ("fig1", "fig2", "fig3"):
    if name in WHICH:
        globals()[name]()
        print("written", name)

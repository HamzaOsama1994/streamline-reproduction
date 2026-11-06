"""Build time-varying FIT replay files for the authors' KafkaProducer (E3).

The SenML payloads are taken unchanged, in order, from the authors' senMlConverter output
(riot_events_FIT_scale1.csv); only the delay column (cumulative ms since producer start, as read by
EventProducer: it sends a line once `delay` ms have elapsed) is generated from a nominal rate schedule r(t).
If a schedule needs more events than the file holds, payloads wrap around to the start (recorded in the .meta.json).

Modes
  steps  WARMUP_S WARMUP_RATE STEP_S RATE1 RATE2 ...   -> warm-up phase, then one constant-rate step per RATE
  sine   T0_S T1_S MEAN AMP PERIOD_S                     -> r(t) = MEAN + AMP*sin(2*pi*t/PERIOD) for t in [T0, T1),
                                                            delays re-based so the file starts at 0 (t = T0)
Usage: python make_workload.py OUT_NAME MODE ARGS...
       (files are written to beam-applications-java/data/OUT_NAME.csv + OUT_NAME.meta.json)
"""
import json
import math
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(os.path.dirname(HERE), "beam-applications-java", "data")


def schedule(mode, a):
    """Return (list of (t_start, t_end, rate_fn)) and total duration."""
    if mode == "steps":
        warm_s, warm_r, step_s = float(a[0]), float(a[1]), float(a[2])
        rates = [float(x) for x in a[3:]]
        segs = [(0.0, warm_s, lambda t, r=warm_r: r)]
        t = warm_s
        for r in rates:
            segs.append((t, t + step_s, lambda t_, r=r: r))
            t += step_s
        return segs, t
    if mode == "sine":
        t0, t1, mean, amp, period = (float(x) for x in a[:5])
        return [(0.0, t1 - t0, lambda t, t0=t0: mean + amp * math.sin(2 * math.pi * (t + t0) / period))], t1 - t0
    raise SystemExit("unknown mode " + mode)


def main():
    name, mode, a = sys.argv[1], sys.argv[2], sys.argv[3:]
    segs, total = schedule(mode, a)
    payloads = []
    with open(os.path.join(DATA, "riot_events_FIT_scale1.csv"), encoding="utf-8") as f:
        for line in f:
            payloads.append(line.split("|", 1)[1])
    # integrate the rate in 1 ms steps: emit an event whenever the cumulative count crosses an integer
    n, acc, wraps = 0, 0.0, 0
    with open(os.path.join(DATA, name + ".csv"), "w", newline="\n", encoding="utf-8") as out:
        out.write("delay|string\n")
        for (ts, te, fn) in segs:
            ms = int(round(ts * 1000))
            while ms < int(round(te * 1000)):
                acc += max(fn(ms / 1000.0), 0.0) / 1000.0
                while acc >= 1.0:
                    out.write(f"{ms}|{payloads[n % len(payloads)]}")
                    n += 1
                    acc -= 1.0
                ms += 1
    wraps = n // len(payloads)
    meta = {"name": name, "mode": mode, "args": a, "duration_s": total, "events": n, "payload_wraps": wraps,
            "source": "riot_events_FIT_scale1.csv (authors' senMlConverter, DATASET=FIT, SCALING=1)",
            "note": "nominal schedule; the authors' KafkaProducer may achieve less than nominal (measured per run)"}
    json.dump(meta, open(os.path.join(DATA, name + ".meta.json"), "w"), indent=1)
    print(json.dumps(meta))


if __name__ == "__main__":
    main()

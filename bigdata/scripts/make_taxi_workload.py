"""TAXI replay files for E3-TAXI (variable workload) from the authors' converter output.

Input: beam-applications-java/data/taxi_week/riot_events_TAXI_scale1.csv
  (authors' senMlConverter, DATASET=TAXI, SCALING=1: delay = real ms since 2013-01-14 00:00, one week, pickup order, no header)
SenML payloads are always copied unchanged and in pickup order. A "delay|string" header is prepended because the authors'
KafkaProducer skips the first line.

Modes
  trace NAME A_S B_S S   Real arrival pattern. The trips whose real time is in [A_S, B_S) seconds since 2013-01-14 00:00, with
                         delay = (real_ms - A_S*1000) / S. This is the authors' own time scaling (converter SCALING factor
                         = 1/S, README: "(timestamp - startTime) * scalingFactor ... impacting only the frequency of events"),
                         applied to a window of the week.
  steps NAME WARM_S WARM_RATE STEP_S R1 R2 ...
                         Stepped profiling load (as make_workload.py "steps", used for FIT in E3) filled with TAXI payloads
                         from the start of the week.
Files: beam-applications-java/data/NAME.csv + NAME.meta.json
Usage: python make_taxi_workload.py MODE NAME ARGS...
"""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
DATA = os.path.join(os.path.dirname(HERE), "beam-applications-java", "data")
SRC = os.path.join(DATA, "taxi_week", "riot_events_TAXI_scale1.csv")


def main():
    mode, name, a = sys.argv[1], sys.argv[2], sys.argv[3:]
    dst = os.path.join(DATA, name + ".csv")
    n = 0
    if mode == "trace":
        A, B, S = float(a[0]), float(a[1]), float(a[2])
        with open(SRC, encoding="utf-8") as fin, open(dst, "w", newline="\n", encoding="utf-8") as fout:
            fout.write("delay|string\n")
            for line in fin:
                d, payload = line.split("|", 1)
                d = int(d)
                if d < A * 1000:
                    continue
                if d >= B * 1000:
                    break
                fout.write(f"{round((d - A * 1000) / S)}|{payload}")
                n += 1
        meta = {"name": name, "mode": "trace", "args": a, "real_window_s": [A, B], "time_compression": S,
                "converter_scaling_equivalent": 1 / S, "duration_s": (B - A) / S, "events": n}
    elif mode == "steps":
        from make_workload import schedule
        segs, total = schedule("steps", a)
        fin = open(SRC, encoding="utf-8")
        acc = 0.0
        with open(dst, "w", newline="\n", encoding="utf-8") as fout:
            fout.write("delay|string\n")
            for (ts, te, fn) in segs:
                ms = int(round(ts * 1000))
                while ms < int(round(te * 1000)):
                    acc += max(fn(ms / 1000.0), 0.0) / 1000.0
                    while acc >= 1.0:
                        fout.write(f"{ms}|{fin.readline().split('|', 1)[1]}")
                        n += 1
                        acc -= 1.0
                    ms += 1
        fin.close()
        meta = {"name": name, "mode": "steps", "args": a, "duration_s": total, "events": n, "payload_wraps": 0}
    else:
        raise SystemExit("unknown mode " + mode)
    meta.update({"source": "taxi_week/riot_events_TAXI_scale1.csv (authors' senMlConverter, DATASET=TAXI, SCALING=1), pickup order",
                 "note": "nominal schedule; the authors' KafkaProducer sends ~0.8 x nominal (ENVIRONMENT section 9)"})
    json.dump(meta, open(os.path.join(DATA, name + ".meta.json"), "w"), indent=1)
    print(json.dumps(meta))


if __name__ == "__main__":
    main()

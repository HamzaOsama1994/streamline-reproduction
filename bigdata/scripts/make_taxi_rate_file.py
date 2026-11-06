"""Constant-rate TAXI replay file for saturated capacity runs (E1-TAXI).

Input : beam-applications-java/data/taxi_week/riot_events_TAXI_scale1.csv (authors' senMlConverter, DATASET=TAXI, SCALING=1:
        delay = real elapsed time since 2013-01-14 00:00, one week = 604,799 s; no header line).
Output: beam-applications-java/data/riot_events_TAXI_r<RATE>.csv with the first N_LINES events.
The SenML payload is copied unchanged and in pickup order; only the delay column is replaced by a constant-rate schedule,
delay_i = round(i * 1000 / RATE) ms. This is the same treatment as the FIT rate files (make_rate_files.py), whose source delays
were already uniform. For TAXI it discards the real arrival pattern; that is acceptable for saturated runs, where every
configuration is fed faster than it can process and throughput measures capacity. A "delay|string" header is prepended
because the authors' KafkaProducer skips the first line.

Usage: python make_taxi_rate_file.py RATE N_LINES
"""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(os.path.dirname(HERE), "beam-applications-java", "data")
rate, n_lines = int(sys.argv[1]), int(sys.argv[2])
src = os.path.join(DATA, "taxi_week", "riot_events_TAXI_scale1.csv")
dst = os.path.join(DATA, f"riot_events_TAXI_r{rate}.csv")
n = 0
with open(src, encoding="utf-8") as fin, open(dst, "w", newline="\n", encoding="utf-8") as fout:
    fout.write("delay|string\n")
    for line in fin:
        if n >= n_lines:
            break
        _, payload = line.split("|", 1)
        fout.write(f"{round(n * 1000 / rate)}|{payload}")
        n += 1
meta = {"name": os.path.basename(dst), "rate_nominal": rate, "events": n, "span_s": n / rate,
        "source": "taxi_week/riot_events_TAXI_scale1.csv (authors' senMlConverter, DATASET=TAXI, SCALING=1), first events in pickup order",
        "note": "constant-rate schedule replaces the real arrival times; the authors' KafkaProducer sends ~0.8 x nominal (ENVIRONMENT section 9)"}
json.dump(meta, open(dst[:-4] + ".meta.json", "w"), indent=1)
print(meta)

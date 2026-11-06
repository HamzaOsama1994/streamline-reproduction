"""Derive constant-rate FIT replay files from the authors' senMlConverter output.

Input : beam-applications-java/data/riot_events_FIT_scale1.csv  (senMlConverter, DATASET=FIT, SCALING=1:
        delay column advances 2 ms per MHEALTH row -> 500 events/s; no header line)
Output: beam-applications-java/data/riot_events_FIT_r<RATE>.csv for each requested rate.
Only the delay column is rescaled: delay' = round(delay * 500 / RATE). The SenML payload is copied unchanged.
A "delay|string" header is prepended because KafkaProducer skips the first line (withSkipLines(1)); the
repo's own kafkaProducer/test_FIT.csv has this header, the converter output does not.
This is equivalent to running senMlConverter with SCALING = 500 / RATE, without the pandas re-conversion.

Usage: python make_rate_files.py DATA_DIR RATE [RATE ...]
"""
import os
import sys

data_dir = sys.argv[1]
rates = [int(r) for r in sys.argv[2:]]
src = os.path.join(data_dir, "riot_events_FIT_scale1.csv")
outs = {r: open(os.path.join(data_dir, f"riot_events_FIT_r{r}.csv"), "w", newline="\n", encoding="utf-8") for r in rates}
for f in outs.values():
    f.write("delay|string\n")
n = 0
with open(src, encoding="utf-8") as fin:
    for line in fin:
        delay, payload = line.split("|", 1)
        d = int(delay)
        for r, f in outs.items():
            f.write(f"{round(d * 500 / r)}|{payload}")
        n += 1
for r, f in outs.items():
    f.close()
    print(f"rate {r} el/s: {n} events, replay span {n / r / 60:.1f} min -> {f.name}")

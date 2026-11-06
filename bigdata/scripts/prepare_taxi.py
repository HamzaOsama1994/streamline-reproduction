"""Prepare TAXI input for the authors' senMlConverter (unmodified) from the archive.org copy of the 2013 FOIL data.

Source: https://archive.org/details/nycTaxiTripData2013  trip_data.7z (MD5 f03d0a7749f44db2a8999cc592e2c828),
        trip_fare.7z (MD5 52cf3fdfc2af2705db40fc1cd5d6b079); only trip_data_1.csv + trip_fare_1.csv (January 2013) are used,
        as the converter README prescribes for "the same dataset used in the original RIoTBench paper".
Deviations from the authors' source (Illinois Databank FOIL2013.zip, which refuses scripted download and failed for us):
  1. medallion / hack_license are MD5 hashes in this copy; the converter reads them as int64. Each distinct hash is
     replaced by an integer (order of first appearance, separate maps). The converter writes them out as strings, so
     only the identifier text changes.
  2. Rows are sorted by pickup_datetime (stable). This copy is not chronological (47% backward steps in the first 2M
     rows); the converter computes each event's replay delay relative to the FIRST row, so unsorted input would give
     negative delays and a replay order that does not follow time.
Data and fare rows are paired by position (as the converter does); the pairing is verified on medallion, hack_license,
vendor_id and pickup_datetime before anything is written.

Usage: python prepare_taxi.py DATA_DIR      (writes DATA_DIR/trip_data_1.csv, trip_fare_1.csv, taxi_prep.meta.json)
"""
import json
import os
import subprocess
import sys

d = sys.argv[1]
TAR = r"C:\Windows\System32\tar.exe"
GNU_SORT = r"C:\Program Files\Git\usr\bin\sort.exe"  # not Windows' own sort.exe, which CreateProcess would find first
tmp = os.path.join(d, "taxi_tmp")
os.makedirs(tmp, exist_ok=True)


def stream(archive, member):
    p = subprocess.Popen([TAR, "-xOf", os.path.join(d, archive), member], stdout=subprocess.PIPE)
    return p, (line.decode("utf-8").rstrip("\r\n") for line in p.stdout)


pd_, data = stream("trip_data.7z", "trip_data_1.csv")
pf_, fare = stream("trip_fare.7z", "trip_fare_1.csv")
hd, hf = next(data), next(fare)
med, hack = {}, {}
n = mism = 0
comb = os.path.join(tmp, "combined.tsv")
with open(comb, "w", encoding="utf-8", newline="\n") as out:
    for dl, fl in zip(data, fare):
        a, b = dl.split(","), fl.split(",")
        # data: medallion,hack_license,vendor_id,...,pickup_datetime(5); fare: medallion,hack_license,vendor_id,pickup_datetime(3)
        if (a[0], a[1], a[2], a[5]) != (b[0], b[1], b[2], b[3]):
            mism += 1
            if mism <= 5:
                print("MISMATCH", n, dl[:120], "|", fl[:120], flush=True)
            continue
        m = med.setdefault(a[0], len(med) + 1)
        h = hack.setdefault(a[1], len(hack) + 1)
        a[0], a[1] = str(m), str(h)
        b[0], b[1] = str(m), str(h)
        out.write(a[5] + "\t" + ",".join(a) + "\t" + ",".join(b) + "\n")
        n += 1
        if n % 2000000 == 0:
            print("paired", n, flush=True)
leftover = sum(1 for _ in data) + sum(1 for _ in fare)  # rows present in only one file
pd_.wait()
pf_.wait()
print("paired", n, "mismatched", mism, "unpaired", leftover, flush=True)

# stable external sort on the pickup_datetime key (ISO format: lexical order == chronological order)
env = dict(os.environ, LC_ALL="C")
srt = os.path.join(tmp, "sorted.tsv")
subprocess.run([GNU_SORT, "-s", "-t", "\t", "-k1,1", "-S", "3G", "-T", tmp, "-o", srt, comb], check=True, env=env)
with open(srt, encoding="utf-8") as fi, \
        open(os.path.join(d, "trip_data_1.csv"), "w", encoding="utf-8", newline="\n") as fd, \
        open(os.path.join(d, "trip_fare_1.csv"), "w", encoding="utf-8", newline="\n") as ff:
    fd.write(hd + "\n")
    ff.write(hf + "\n")
    first = last = None
    for line in fi:
        k, dl, fl = line.rstrip("\n").split("\t")
        first = first or k
        last = k
        fd.write(dl + "\n")
        ff.write(fl + "\n")
meta = {"source": "archive.org nycTaxiTripData2013 trip_data.7z/trip_fare.7z (MD5 verified)", "rows": n, "mismatched_pairs_dropped": mism,
        "unpaired_rows": leftover, "distinct_medallions": len(med), "distinct_hack_licenses": len(hack),
        "first_pickup": first, "last_pickup": last, "sorted_by": "pickup_datetime (stable)",
        "id_mapping": "MD5 hash -> integer by first appearance in file order (separate maps for medallion and hack_license)"}
json.dump(meta, open(os.path.join(d, "taxi_prep.meta.json"), "w"), indent=1)
os.remove(comb)
os.remove(srt)
print(json.dumps(meta, indent=1))

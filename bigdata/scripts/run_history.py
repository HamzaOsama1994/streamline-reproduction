"""E3 history run: observe the source workload STREAMLINE's workload predictor is trained on.

Replays a workload file with the authors' KafkaProducer (unmodified) into a fresh senml-source topic, with no Flink job
running, and records the topic's end offset every INTERVAL s. The ingestion rate per interval is the observed source
workload (what STREAMLINE's README says to obtain "e.g. using the rest api of Apache Flink"; at the source it is the
same quantity, measured here at the broker so it is independent of any Flink configuration).

Usage: python run_history.py WORKLOAD_NAME DURATION_S OUT_JSON [INTERVAL_S=5]
"""
import ctypes
import json
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
BIG = os.path.dirname(HERE)
REPO = os.path.join(BIG, "beam-applications-java")
JAVA = [d for d in os.listdir(os.path.join(BIG, "tools")) if d.startswith("jdk-11")][0]
JAVA_BIN = os.path.join(BIG, "tools", JAVA, "bin", "java.exe")

name, duration, out = sys.argv[1], float(sys.argv[2]), sys.argv[3]
interval = float(sys.argv[4]) if len(sys.argv) > 4 else 5.0


def kexec(*cmd):
    return subprocess.run(["kubectl", "exec", "deploy/kafka", "--"] + list(cmd), capture_output=True, text=True, timeout=120).stdout


def topic_end(topic):
    return sum(int(x.split(":")[-1]) for x in kexec("/opt/kafka/bin/kafka-get-offsets.sh", "--bootstrap-server", "localhost:9092",
                                                     "--topic", topic).split() if x.count(":") == 2)


ctypes.windll.kernel32.SetThreadExecutionState(0x80000000 | 0x00000001)
pf = subprocess.Popen(["kubectl", "port-forward", "svc/kafka-cluster-kafka-bootstrap", "9093:9093"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
producer = None
try:
    time.sleep(4)
    kexec("/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:9092", "--delete", "--if-exists", "--topic", "senml-source")
    time.sleep(3)
    kexec("/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:9092", "--create", "--if-not-exists", "--topic", "senml-source",
          "--partitions", "1", "--replication-factor", "1", "--config", "retention.ms=7200000")
    env = dict(os.environ)
    env.pop("JAVA_TOOL_OPTIONS", None)
    producer = subprocess.Popen([JAVA_BIN, "-jar", os.path.join(REPO, "kafkaProducer", "build", "KafkaProducer.jar"),
                                 os.path.join(REPO, "data", name + ".csv"), "4", "senml-source"],
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env)
    t0 = time.time()
    ts, ends = [], []
    k = 0
    while time.time() - t0 < duration:
        target = t0 + k * interval
        time.sleep(max(0.0, target - time.time()))
        e = topic_end("senml-source")
        ts.append(round(time.time() - t0, 3))
        ends.append(e)
        k += 1
    series = [(ends[i] - ends[i - 1]) / (ts[i] - ts[i - 1]) for i in range(1, len(ends))]
    json.dump({"workload": name, "interval_s": interval, "t": ts, "input_end": ends, "series": series,
               "note": "series[i] = ingestion rate (el/s) between offset reads i and i+1; the authors' producer sends 4 of every 5 lines"},
              open(out, "w"), indent=1)
    print(json.dumps({"samples": len(series), "mean": sum(series) / len(series), "min": min(series), "max": max(series)}))
finally:
    if producer:
        producer.kill()
    pf.terminate()
    ctypes.windll.kernel32.SetThreadExecutionState(0x80000000)

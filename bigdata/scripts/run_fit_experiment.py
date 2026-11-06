"""One measured run of the authors' ETL pipeline (FIT) on the local k3d Flink cluster.

The benchmark jar (beam-applications-java/etl/build/FlinkJob.jar) is used unmodified; only job-wide
--parallelism varies (Beam's FlinkRunner exposes no per-operator parallelism; see BIGDATA_REPRODUCTION_PLAN.md).

Per run:
  1. cancel running jobs, recreate topics senml-source / senml-cleaned (1 partition, as the authors' chart)
  2. `flink run -d` inside the JobManager pod with --experiRunId=FIT --parallelism=P
  3. replay riot_events_FIT_r<RATE>.csv with the authors' KafkaProducer (host, via port-forward localhost:9093)
  4. every 5 s sample: Flink REST per-vertex metrics, Prometheus latency markers + JVM CPU (via API-server pod proxy),
     pod CPU from metrics.k8s.io
  5. stop producer, cancel job, read topic end offsets
Writes <out>/<label>.json (raw time series + summary over the post-warm-up window).

Usage: python run_fit_experiment.py --parallelism 1 --rate 2000 --duration 240 --warmup 60 --label FD_r2000_rep1 --out results
"""
import argparse
import json
import os
import re
import subprocess
import sys
import time
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
BIG = os.path.dirname(HERE)
REPO = os.path.join(BIG, "beam-applications-java")
JAVA = [d for d in os.listdir(os.path.join(BIG, "tools")) if d.startswith("jdk-11")][0]
JAVA_BIN = os.path.join(BIG, "tools", JAVA, "bin", "java.exe")
JAR_IN_POD = "/opt/flink/usrlib/FlinkJob.jar"
REST = "http://localhost:18081"
TM_PRICE = 0.192  # $/h, paper Table 1 type-1 price (our TaskManager = type 1 scaled /4)
# operator-level counter of records consumed from Kafka (first operator after the KafkaIO read, metadata removed)
CONSUMED_METRIC_Q = urllib.parse.quote("ReadSenMLSource/Values/Values/Map/ParMultiDo(Anonymous).numRecordsOut", safe="")

ap = argparse.ArgumentParser()
ap.add_argument("--parallelism", type=int, required=True)
ap.add_argument("--rate", type=int, required=True)
ap.add_argument("--dataset", choices=["FIT", "TAXI"], default="FIT", help="ETL variant (--experiRunId) and replay file riot_events_<DATASET>_r<RATE>.csv")
ap.add_argument("--duration", type=int, default=240)
ap.add_argument("--warmup", type=int, default=60)
ap.add_argument("--producer-threads", type=int, default=4)
ap.add_argument("--latency-interval", type=int, default=5000, help="ms; E1 used 5000. From L1 on: 1000 with metrics.latency.history-size 60 (= 60 s quantile window)")
ap.add_argument("--e2e", action="store_true", help="topics with message.timestamp.type=LogAppendTime; after the run, dump per-record broker timestamps of both topics and compute true end-to-end latency (order-matched)")
ap.add_argument("--label", required=True)
ap.add_argument("--out", default=os.path.join(BIG, "results"))
args = ap.parse_args()


def sh(cmd, check=True, timeout=180, cwd=None):
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, cwd=cwd)
    if check and r.returncode != 0:
        raise RuntimeError(f"{cmd}\n{r.stdout}\n{r.stderr}")
    return r.stdout


def kexec(*cmd, **kw):
    return sh(["kubectl", "exec", "deploy/kafka", "--"] + list(cmd), **kw)


def rest(path, method="GET"):
    req = urllib.request.Request(REST + path, method=method)
    with urllib.request.urlopen(req, timeout=30) as r:
        body = r.read()
        return json.loads(body) if body else {}


def raw(path):
    return sh(["kubectl", "get", "--raw", path], timeout=30)


def topic_end(topic):
    out = kexec("/opt/kafka/bin/kafka-get-offsets.sh", "--bootstrap-server", "localhost:9092", "--topic", topic, check=False)
    return sum(int(x.split(":")[-1]) for x in out.split() if x.count(":") == 2)


def reset_topics():
    for t in ("senml-source", "senml-cleaned"):
        kexec("/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:9092", "--delete", "--if-exists", "--topic", t, check=False)
    time.sleep(3)
    for t in ("senml-source", "senml-cleaned"):
        kexec("/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:9092", "--create", "--if-not-exists", "--topic", t,
              "--partitions", "1", "--replication-factor", "1", "--config", "retention.ms=7200000",
              *(["--config", "message.timestamp.type=LogAppendTime"] if args.e2e else []))


def cancel_all():
    for j in rest("/jobs").get("jobs", []):
        if j["status"] in ("RUNNING", "CREATED", "RESTARTING", "INITIALIZING"):
            rest(f"/jobs/{j['id']}?mode=cancel", method="PATCH")
    time.sleep(5)


def pods(component):
    out = sh(["kubectl", "get", "pods", "-l", f"app=flink,component={component}", "-o", "jsonpath={.items[*].metadata.name}"])
    return out.split()


def prom_sample(tm_pods):
    lat, cpu = [], {}
    for p in tm_pods:
        try:
            txt = raw(f"/api/v1/namespaces/default/pods/{p}:9249/proxy/metrics")
        except Exception:
            continue
        for line in txt.splitlines():
            if line.startswith("flink_taskmanager_job_latency_source_id_operator_id_operator_subtask_index_latency{"):
                m = re.search(r'quantile="([0-9.]+)".*\}\s+([0-9.eE+-]+|NaN)$', line)
                if m and m.group(2) != "NaN":
                    lat.append((float(m.group(1)), float(m.group(2)), re.search(r'operator_id="([^"]+)"', line).group(1)))
            elif line.startswith("flink_taskmanager_Status_JVM_CPU_Load"):
                cpu[p] = float(line.split()[-1])
    return lat, cpu



# --- host-load monitor (added 2026-11-05 after host-side jobs were found to slow the cluster) ---
try:
    import psutil
except ImportError:  # monitor is optional
    psutil = None
_HOST_PREV = {}
_VM_NAMES = ("vmmem",)  # Docker Desktop's WSL2 VM (all k3d nodes run inside it)


def host_load():
    """Cores used on the Windows host since the previous call: total, by the Docker/WSL VM, and by everything else
    (other = total - VM; includes the producer JVM and kubectl calls of this harness, and any unrelated host activity)."""
    if psutil is None:
        return None
    now = __import__("time").time()
    ct = psutil.cpu_times()
    busy = sum(ct) - ct.idle
    vm = 0.0
    for p in psutil.process_iter(["name"]):
        if (p.info["name"] or "").lower().startswith(_VM_NAMES):
            try:
                vm += sum(p.cpu_times()[:2])
            except Exception:
                pass
    prev = _HOST_PREV.copy()
    _HOST_PREV.update(t=now, busy=busy, vm=vm)
    if not prev or now - prev["t"] <= 0:
        return None
    dt = now - prev["t"]
    tot, v = (busy - prev["busy"]) / dt, (vm - prev["vm"]) / dt
    return {"total_cores": round(tot, 2), "vm_cores": round(v, 2), "other_cores": round(tot - v, 2)}


def pod_cpu():
    try:
        d = json.loads(raw("/apis/metrics.k8s.io/v1beta1/namespaces/default/pods"))
    except Exception:
        return {}
    res = {}
    for it in d.get("items", []):
        for c in it["containers"]:
            v = c["usage"]["cpu"]
            # Kubernetes quantity suffixes: n (1e-9), u (1e-6, seen from metrics-server), m (1e-3), none
            unit = {"n": 1e-9, "u": 1e-6, "m": 1e-3}.get(v[-1])
            res[it["metadata"]["name"]] = float(v[:-1]) * unit if unit else float(v)
    return res


def main():
    os.makedirs(args.out, exist_ok=True)
    pf = [subprocess.Popen(["kubectl", "port-forward", "svc/flink-jobmanager", "18081:8081"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL),
          subprocess.Popen(["kubectl", "port-forward", "svc/kafka-cluster-kafka-bootstrap", "9093:9093"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)]
    # 2026-11-06 (ENVIRONMENT section 13): a stale port-forward left by an earlier session kept the local port, so ours exited
    # silently and the run talked to a broken tunnel. Fail loudly instead.
    time.sleep(3)
    if any(x.poll() is not None for x in pf):
        for x in pf:
            x.terminate()
        raise SystemExit("kubectl port-forward exited at start (local port 18081/9093 in use by a stale port-forward?)")
    producer = None
    rec = {"label": args.label, "config": vars(args), "samples": []}
    try:
        time.sleep(4)
        cancel_all()
        reset_topics()
        # make sure the unmodified benchmark jar is present in the JobManager pod (lost whenever the JM pod restarts)
        jm = pods("jobmanager")[0]
        if subprocess.run(["kubectl", "exec", jm, "--", "test", "-s", JAR_IN_POD]).returncode != 0:
            sh(["kubectl", "exec", jm, "--", "mkdir", "-p", "/opt/flink/usrlib"])
            # relative source path: kubectl cp on Windows mistakes the "C:" drive prefix for a pod name
            sh(["kubectl", "cp", "FlinkJob.jar", f"default/{jm}:{JAR_IN_POD}"], timeout=600, cwd=os.path.join(REPO, "etl", "build"))
        # fresh TaskManager JVMs for every run: cancelled Beam KafkaIO readers leak a polling offset-consumer thread
        # (~0.1 core per TM, accumulating across runs); restarting TMs removes this interference (ENVIRONMENT doc section 7)
        sh(["kubectl", "rollout", "restart", "deploy/flink-taskmanager"])
        sh(["kubectl", "rollout", "status", "deploy/flink-taskmanager", "--timeout=300s"], timeout=320)
        for _ in range(180):
            try:
                ov = rest("/overview")
                if ov["taskmanagers"] == 4 and ov["slots-total"] == 8 and ov["slots-available"] == 8:
                    break
            except Exception:
                pass
            time.sleep(1)
        time.sleep(10)
        jid, attempts = None, []
        for attempt in (1, 2):
            # wait until every slot is free (previous job's slots are released asynchronously; see ENVIRONMENT doc)
            for _ in range(120):
                ov = rest("/overview")
                if ov["slots-available"] == ov["slots-total"] and ov["jobs-running"] == 0:
                    break
                time.sleep(1)
            t_submit = time.time()
            out = sh(["kubectl", "exec", "deploy/flink-jobmanager", "--", "flink", "run", "-d", JAR_IN_POD,
                      f"--experiRunId={args.dataset}", f"--parallelism={args.parallelism}", "--attachedMode=false",
                      f"--latencyTrackingInterval={args.latency_interval}"], timeout=300)
            m = re.search(r"JobID ([0-9a-f]{32})", out)
            if not m:
                raise RuntimeError("no JobID in flink run output: " + out[-2000:])
            jid = m.group(1)
            ok = False
            for _ in range(120):
                j = rest(f"/jobs/{jid}")
                if j["state"] == "RUNNING" and all(v["status"] == "RUNNING" for v in j["vertices"]):
                    ok = True
                    break
                if j["state"] in ("FAILED", "CANCELED"):
                    break
                time.sleep(1)
            attempts.append({"job_id": jid, "deployed": ok, "state": j["state"]})
            if ok:
                break
            cancel_all()
        rec["deploy_attempts"] = attempts
        if not attempts[-1]["deployed"]:
            raise RuntimeError(f"deployment failed twice: {attempts}")
        rec["job_id"] = jid
        rec["deploy_seconds"] = time.time() - t_submit
        plan = rest(f"/jobs/{jid}")
        vertices = [{"id": v["id"], "name": v["name"], "parallelism": v["parallelism"]} for v in plan["vertices"]]
        rec["vertices"] = vertices
        tms_used = set()
        for v in vertices:
            for tm in rest(f"/jobs/{jid}/vertices/{v['id']}/taskmanagers").get("taskmanagers", []):
                tms_used.add(tm.get("taskmanager-id") or tm.get("host"))
        rec["taskmanagers_used"] = sorted(tms_used)
        tm_pods = pods("taskmanager")

        in0, out0 = topic_end("senml-source"), topic_end("senml-cleaned")
        env = dict(os.environ)
        env.pop("JAVA_TOOL_OPTIONS", None)
        producer = subprocess.Popen([JAVA_BIN, "-jar", os.path.join(REPO, "kafkaProducer", "build", "KafkaProducer.jar"),
                                     os.path.join(REPO, "data", f"riot_events_{args.dataset}_r{args.rate}.csv"), str(args.producer_threads), "senml-source"],
                                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env)
        t0 = time.time()
        last_off = -1e9
        while time.time() - t0 < args.duration:
            ts = time.time() - t0
            s = {"t": round(ts, 1), "vertices": {}}
            for v in vertices:
                ms = rest(f"/jobs/{jid}/vertices/{v['id']}/subtasks/metrics?get=numRecordsInPerSecond,numRecordsOutPerSecond,"
                          f"busyTimeMsPerSecond,backPressuredTimeMsPerSecond&agg=sum,max,avg")
                s["vertices"][v["name"][:60]] = {x["id"]: {k: x.get(k) for k in ("sum", "max", "avg")} for x in ms}
            # Kafka backlog = produced - consumed. Latency markers start at the source operator, so time spent waiting in
            # the input topic is invisible to them; backlog / input rate (Little's law) estimates that waiting time.
            try:
                c = rest(f"/jobs/{jid}/vertices/{vertices[0]['id']}/subtasks/metrics?get={CONSUMED_METRIC_Q}&agg=sum")
                s["consumed_total"] = c[0]["sum"] if c else None
            except Exception:
                s["consumed_total"] = None
            if ts - last_off >= 30:
                s["input_end"], s["input_end_t"] = topic_end("senml-source") - in0, round(time.time() - t0, 2)
                last_off = ts
            lat, jcpu = prom_sample(tm_pods)
            s["latency"] = lat
            s["jvm_cpu_load"] = jcpu
            s["pod_cpu_cores"] = pod_cpu()
            s["host"] = host_load()
            s["state"] = rest(f"/jobs/{jid}")["state"]
            rec["samples"].append(s)
            time.sleep(max(0.0, 5 - (time.time() - t0 - ts)))
        rec["placement"] = {}
        for v in vertices:
            info = rest(f"/jobs/{jid}/vertices/{v['id']}")
            subs = []
            for stt in info.get("subtasks", []):
                i = stt["subtask"]
                mm = {x["id"]: x.get("value") for x in rest(f"/jobs/{jid}/vertices/{v['id']}/subtasks/{i}/metrics?get=busyTimeMsPerSecond,numRecordsOutPerSecond,numRecordsInPerSecond")}
                subs.append({"subtask": i, "tm": stt.get("taskmanager-id"), "host": stt.get("host"), **mm})
            rec["placement"][v["name"][:60]] = subs
        producer.terminate()
        producer.wait(timeout=30)
        producer = None
        time.sleep(3)
        in1, out1 = topic_end("senml-source"), topic_end("senml-cleaned")
        rec["topic_counts"] = {"input_produced": in1 - in0, "output_written": out1 - out0, "window_s": time.time() - t0}
        rec["final_state"] = rest(f"/jobs/{jid}")["state"]
        exc = rest(f"/jobs/{jid}/exceptions")
        rec["exceptions"] = (exc.get("root-exception") or "")[:2000]
        rest(f"/jobs/{jid}?mode=cancel", method="PATCH")
        if args.e2e:
            rec["e2e"] = e2e_latency(in0, out0, t0)
    finally:
        if producer:
            producer.kill()
        for p in pf:
            p.terminate()
    rec["summary"] = summarise(rec)
    with open(os.path.join(args.out, f"{args.label}.json"), "w") as f:
        json.dump(rec, f, indent=1)
    print(json.dumps({"label": args.label, **rec["summary"]}), flush=True)


def dump_ts(topic, n):
    """Broker (LogAppendTime) timestamps of the first n records of a 1-partition topic, in offset order."""
    out = kexec("/opt/kafka/bin/kafka-console-consumer.sh", "--bootstrap-server", "localhost:9092", "--topic", topic,
                "--from-beginning", "--max-messages", str(n), "--timeout-ms", "30000", "--property", "print.timestamp=true",
                "--property", "print.value=false", check=False, timeout=600)
    return [int(x.split(":")[1]) for x in out.split() if x.startswith(("LogAppendTime:", "CreateTime:"))]


def dump_vals(topic, offset, n):
    out = kexec("/opt/kafka/bin/kafka-console-consumer.sh", "--bootstrap-server", "localhost:9092", "--topic", topic,
                "--partition", "0", "--offset", str(offset), "--max-messages", str(n), "--timeout-ms", "30000", check=False, timeout=300)
    return [ln for ln in out.splitlines() if ln.startswith("[")]


def key_fields(v):
    # fields the ETL leaves unchanged unless out of range: subject + chest accelerometer + ECG

    try:
        d = {e["n"]: e.get("v", e.get("vs")) for e in json.loads(v)}
    except Exception:
        return None
    return tuple(d.get(k) for k in ("subjectId", "acc_chest_x", "acc_chest_y", "acc_chest_z", "ecg_lead_1", "ecg_lead_2"))


def e2e_latency(in0, out0, t0):
    n_out = topic_end("senml-cleaned") - out0
    tin, tout = dump_ts("senml-source", n_out), dump_ts("senml-cleaned", n_out)
    n = min(len(tin), len(tout))
    # verify order-matching on 3 slices: i-th output record must carry the i-th input record's payload fields
    checks = []
    for off in (n // 10, n // 2, (9 * n) // 10):
        a, b = dump_vals("senml-source", off, 50), dump_vals("senml-cleaned", off, 50)
        checks.append(sum(1 for x, y in zip(a, b) if key_fields(x) == key_fields(y)) / max(1, min(len(a), len(b))))
    t_start = tin[0] if tin else 0
    lat = [(tin[i] - t_start, tout[i] - tin[i]) for i in range(n)]
    return {"n_matched": n, "order_match_fraction": checks, "lat_by_input_time_ms": lat[::10],
            "note": "broker LogAppendTime(output) - LogAppendTime(input) for the i-th record; every 10th record stored"}


def summarise(rec):
    W = [s for s in rec["samples"] if s["t"] >= rec["config"]["warmup"]]
    src = rec["vertices"][0]["name"][:60]
    snk = rec["vertices"][-1]["name"][:60]
    mean = lambda xs: sum(xs) / len(xs) if xs else None  # noqa: E731

    def vmetric(name, metric, agg="sum"):
        return [s["vertices"][name][metric][agg] for s in W if s["vertices"].get(name, {}).get(metric, {}).get(agg) is not None]

    Wl = [s for s in rec["samples"] if s["t"] >= rec["config"]["warmup"] + 60]
    l50c = [max(v for q, v, _ in s["latency"] if q == 0.5) for s in Wl if any(q == 0.5 for q, _, _ in s["latency"])]
    l99c = [max(v for q, v, _ in s["latency"] if q == 0.99) for s in Wl if any(q == 0.99 for q, _, _ in s["latency"])]
    l50m = [sorted(v for q, v, _ in s["latency"] if q == 0.5)[len([1 for q, _, _ in s["latency"] if q == 0.5]) // 2] for s in Wl if any(q == 0.5 for q, _, _ in s["latency"])]
    lat50 = [max(v for q, v, _ in s["latency"] if q == 0.5) for s in W if any(q == 0.5 for q, _, _ in s["latency"])]
    lat99 = [max(v for q, v, _ in s["latency"] if q == 0.99) for s in W if any(q == 0.99 for q, _, _ in s["latency"])]
    tm_cpu = [sum(c for p, c in s["pod_cpu_cores"].items() if "taskmanager" in p) for s in W if s["pod_cpu_cores"]]
    n_tm = len(rec["taskmanagers_used"])
    e2e = {}
    if rec.get("e2e") and rec["e2e"]["lat_by_input_time_ms"]:
        xs = sorted(l for t, l in rec["e2e"]["lat_by_input_time_ms"] if t >= (rec["config"]["warmup"] + 60) * 1000)
        if xs:
            e2e = {"e2e_p50_ms": xs[len(xs) // 2], "e2e_p95_ms": xs[int(0.95 * (len(xs) - 1))], "e2e_p99_ms": xs[int(0.99 * (len(xs) - 1))],
                   "e2e_max_ms": xs[-1], "e2e_mean_ms": sum(xs) / len(xs), "e2e_order_match": rec["e2e"]["order_match_fraction"]}
    bl = [(s["t"], s["input_end"] - s["consumed_total"]) for s in W if s.get("consumed_total") is not None and s.get("input_end") is not None]
    in_rate = rec["topic_counts"]["input_produced"] / rec["topic_counts"]["window_s"]
    if len(bl) >= 2:
        tb, bb = zip(*bl)
        tm_, bm_ = sum(tb) / len(tb), sum(bb) / len(bb)
        slope = sum((t - tm_) * (b - bm_) for t, b in bl) / sum((t - tm_) ** 2 for t in tb)
    else:
        bm_, slope = None, None
    return {
        "parallelism": rec["config"]["parallelism"], "rate_target": rec["config"]["rate"],
        "n_vertices": len(rec["vertices"]), "vertex_names": [v["name"][:60] for v in rec["vertices"]],
        "source_out_rate_mean": mean(vmetric(src, "numRecordsOutPerSecond")),
        "sink_in_rate_mean": mean(vmetric(snk, "numRecordsInPerSecond")),
        "max_backpressured_ms_per_s": max([max(vmetric(v["name"][:60], "backPressuredTimeMsPerSecond", "max") or [0]) for v in rec["vertices"]]),
        "max_busy_ms_per_s": max([max(vmetric(v["name"][:60], "busyTimeMsPerSecond", "max") or [0]) for v in rec["vertices"]]),
        "latency_marker_p50_ms_mean_of_max": mean(lat50), "latency_marker_p99_ms_mean_of_max": mean(lat99),
        "tm_cpu_cores_mean": mean(tm_cpu), "taskmanagers_used": n_tm,
        "host_other_cores_mean": mean([s["host"]["other_cores"] for s in W if s.get("host")]),
        "host_other_cores_max": max([s["host"]["other_cores"] for s in W if s.get("host")] or [None], key=lambda x: -1 if x is None else x),
        "tm_cpu_utilisation_of_used": (mean(tm_cpu) / n_tm) if tm_cpu and n_tm else None,
        "emulated_cost_per_h": round(n_tm * TM_PRICE, 3),
        "input_produced": rec["topic_counts"]["input_produced"], "output_written": rec["topic_counts"]["output_written"],
        "input_rate_achieved": rec["topic_counts"]["input_produced"] / rec["topic_counts"]["window_s"],
        "per_vertex": {v["name"][:60]: {"parallelism": v["parallelism"],
                                         "out_rate_mean": mean(vmetric(v["name"][:60], "numRecordsOutPerSecond")),
                                         "busy_ms_per_s_avg": mean(vmetric(v["name"][:60], "busyTimeMsPerSecond", "avg")),
                                         "busy_ms_per_s_max": mean(vmetric(v["name"][:60], "busyTimeMsPerSecond", "max")),
                                         "backpressured_ms_per_s_max": mean(vmetric(v["name"][:60], "backPressuredTimeMsPerSecond", "max"))}
                       for v in rec["vertices"]},
        "final_state": rec["final_state"], "deploy_seconds": round(rec["deploy_seconds"], 1),
        "warmup_s": rec["config"]["warmup"], "n_samples_after_warmup": len(W),
        # backlog in the input topic (records produced but not yet consumed), post-warm-up window
        "latency_clean_from_s": rec["config"]["warmup"] + 60, "latency_interval_ms": rec["config"].get("latency_interval"),
        "lat_p50_max_ops_clean": mean(l50c), "lat_p99_max_ops_clean": mean(l99c), "lat_p50_median_ops_clean": mean(l50m),
        **e2e,
        "kafka_backlog_mean": bm_, "kafka_backlog_slope_per_s": slope,
        "kafka_wait_ms_est": (1000 * bm_ / in_rate) if bm_ is not None and in_rate else None,
    }


if __name__ == "__main__":
    main()

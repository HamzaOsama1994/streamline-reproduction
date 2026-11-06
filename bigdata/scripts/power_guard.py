"""Power guard for measured runs (added 2026-11-06, ENVIRONMENT section 13).

The laptop ran on battery during some measurements, and Windows throttles the CPU on battery (+25-40% CPU per record), and it
entered Modern Standby during others. ES_SYSTEM_REQUIRED alone did not prevent Modern Standby. This module:
  * keep_awake(): ES_CONTINUOUS | ES_SYSTEM_REQUIRED | ES_DISPLAY_REQUIRED for the calling process (display stays on, so the
    display timeout cannot start Modern Standby; closing the lid still can);
  * wait_for_ac(): blocks until the charger is connected;
  * power_events(t0, t1): Kernel-Power events in the run window (42 sleep, 105 power-source change, 506/507 Modern Standby
    enter/exit) read from the Windows System log;
  * run_guarded(): runs one measurement; if the run overlapped battery power or a sleep/standby event, its result file is
    moved to <out>/power_invalid/ (kept, not used) and the run is repeated (max attempts). A valid result file gets a
    "power" record (AC state at start/end, events = []).
"""
import ctypes
import datetime as dt
import json
import os
import shutil
import subprocess
import time

try:
    import psutil
except ImportError:
    psutil = None

ES_CONTINUOUS, ES_SYSTEM_REQUIRED, ES_DISPLAY_REQUIRED = 0x80000000, 0x00000001, 0x00000002


def keep_awake(on=True):
    ctypes.windll.kernel32.SetThreadExecutionState(ES_CONTINUOUS | ((ES_SYSTEM_REQUIRED | ES_DISPLAY_REQUIRED) if on else 0))


def on_ac():
    b = psutil.sensors_battery() if psutil else None
    return True if b is None else bool(b.power_plugged)


def wait_for_ac(log=print):
    if on_ac():
        return 0
    t = time.time()
    log(f"{time.strftime('%H:%M:%S')} on battery - waiting for AC power before the next run", flush=True)
    while not on_ac():
        time.sleep(30)
    time.sleep(60)  # let the power plan settle
    log(f"{time.strftime('%H:%M:%S')} AC power back after {time.time() - t:.0f} s", flush=True)
    return time.time() - t


def power_events(t0, t1):
    """Kernel-Power events between two datetimes (local time), as [(iso time, id), ...]."""
    ps = ("$s=[datetime]'%s'; $e=[datetime]'%s'; "
          "Get-WinEvent -FilterHashtable @{LogName='System'; ProviderName='Microsoft-Windows-Kernel-Power'; Id=42,105,506,507; "
          "StartTime=$s; EndTime=$e} -ErrorAction SilentlyContinue | ForEach-Object { $_.TimeCreated.ToString('s') + ' ' + $_.Id }"
          % (t0.strftime("%Y-%m-%dT%H:%M:%S"), t1.strftime("%Y-%m-%dT%H:%M:%S")))
    r = subprocess.run(["powershell", "-NoProfile", "-Command", ps], capture_output=True, text=True)
    return [tuple(x.split()) for x in r.stdout.split("\n") if x.strip()]


def run_guarded(cmd, result_path, label, log=print, max_attempts=3):
    """Run cmd (one measurement writing result_path). Returns the last subprocess.CompletedProcess."""
    out_dir = os.path.dirname(result_path)
    for attempt in range(1, max_attempts + 1):
        wait_for_ac(log)
        t0, ac0 = dt.datetime.now(), on_ac()
        r = subprocess.run(cmd, capture_output=True, text=True)
        t1, ac1 = dt.datetime.now(), on_ac()
        ev = power_events(t0 - dt.timedelta(seconds=5), t1 + dt.timedelta(seconds=5))
        bad = bool(ev) or not ac0 or not ac1
        if os.path.exists(result_path):
            if bad:
                d = os.path.join(out_dir, "power_invalid")
                os.makedirs(d, exist_ok=True)
                dst = os.path.join(d, f"{label}_{t0.strftime('%Y%m%dT%H%M%S')}.json")
                shutil.move(result_path, dst)
                json.dump({"events": ev, "ac_start": ac0, "ac_end": ac1, "start": t0.isoformat(), "end": t1.isoformat()},
                          open(dst[:-5] + ".power.json", "w"))
                log(f"POWER-INVALID {label} attempt {attempt}: events {ev} ac {ac0}/{ac1} -> {dst}; repeating", flush=True)
                continue
            rec = json.load(open(result_path))
            rec["power"] = {"ac_start": ac0, "ac_end": ac1, "events": ev, "start": t0.isoformat(), "end": t1.isoformat(),
                            "attempt": attempt}
            json.dump(rec, open(result_path, "w"), indent=1)
            return r
        # no result file: failed run (deploy failure etc.)
        log(f"FAILED {label} attempt {attempt}{' (power event: ' + str(ev) + ')' if bad else ''}: {r.stderr[-1200:]}", flush=True)
        if attempt >= 2 and not bad:
            return r
    return r

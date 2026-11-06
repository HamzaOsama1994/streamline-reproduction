#!/usr/bin/env bash
# 2026-11-06: E3-TAXI, the variable-workload comparison on the real TAXI arrival pattern (paper §5.1.1 regime).
# history (measured) -> TAXI profiling (12 runs) -> dataset -> STREAMLINE train + decide (V0, V4) -> evaluation
# (FD, FMAX, every STREAMLINE configuration; 3 reps; true e2e latency with drain; complete per-record traces; placement logs).
# Every measured step runs under the power guard (scripts/power_guard.py, ENVIRONMENT section 13).
set -u
cd "$(dirname "$0")/.."
log() { echo "$(date +%H:%M:%S) $*"; }
python scripts/keep_awake.py & KA=$!
trap 'kill $KA 2>/dev/null' EXIT
W=results/E3T_work
mkdir -p $W results/E3T_profile results/E3T_eval

# 1. workload history: replay real Mon 00:00 - Fri 14:00 at 220x (1800 s), no Flink job, broker ingestion every 5 s
if [ ! -f results/E3T_history_taxi.json ]; then
  python - <<'EOF'
import sys
sys.path.insert(0, "scripts")
import power_guard as pg
p = pg.run_guarded([sys.executable, "scripts/run_history.py", "e3t_taxi_hist", "1805", "results/E3T_history_taxi.json"],
                   "results/E3T_history_taxi.json", "E3T_history")
print(p.stdout[-500:], p.stderr[-1500:])
EOF
fi
[ -f results/E3T_history_taxi.json ] || { log "ABORT: history run failed"; echo ALL_DONE; exit 1; }
python - <<'EOF'
import json
h = json.load(open("results/E3T_history_taxi.json"))
t, s = h["t"], h["series"]
keep = [x for i, x in enumerate(s) if t[i + 1] <= 1800.0]  # drop intervals that extend past the end of the replay file
json.dump({"interval_s": 5, "series": keep, "source": "results/E3T_history_taxi.json",
           "note": f"{len(s) - len(keep)} trailing interval(s) ending after 1800 s (end of e3t_taxi_hist) dropped as end-of-file artefacts, as in E3"},
          open("results/E3T_work/workload_history.json", "w"), indent=1)
print("history bins", len(keep), "mean", sum(keep) / len(keep), "max", max(keep))
EOF
log "history done"

# 2. profiling on TAXI (same 12 configurations as E3/FIT)
python scripts/run_e3_grid.py scripts/e3t_profile_spec.json > logs/grid_E3T_profile.log 2>&1
n=$(ls results/E3T_profile/p*_seg*.json 2>/dev/null | wc -l)
log "profiling done: $n of 12"
if [ "$n" -ne 12 ]; then log "ABORT: only $n of 12 profiling runs present - not training"; echo ALL_DONE; exit 1; fi

# 3. dataset + STREAMLINE training and decisions (cluster idle)
python scripts/build_streamline_dataset.py results/E3T_profile $W/historical_data_etl.csv > $W/dataset_report.json
log "dataset: $(head -2 $W/dataset_report.json | tail -1)"
[ -d $W/models_main ] && mv $W/models_main $W/models_prev_$(date +%H%M%S)
mkdir -p $W/models_main
( cd ../Streamline && OMP_NUM_THREADS=4 MKL_NUM_THREADS=4 ../env/Scripts/python.exe ../repro_scripts/e3_decide.py train \
    ../bigdata/$W/historical_data_etl.csv ../bigdata/$W/workload_history.json ../bigdata/$W/models_main \
    > ../bigdata/logs/E3T_train.log 2> ../bigdata/logs/E3T_train.err
  OMP_NUM_THREADS=4 MKL_NUM_THREADS=4 ../env/Scripts/python.exe ../repro_scripts/e3_decide.py decide ../bigdata/$W/models_main 10 \
    > ../bigdata/logs/E3T_decide.log 2> ../bigdata/logs/E3T_decide.err )
log "train + decide done"
cat logs/E3T_decide.log
[ -s $W/models_main/decisions.json ] || { log "ABORT: no decisions"; echo ALL_DONE; exit 1; }

# 4. evaluation: FD, FMAX and every chosen configuration, 3 reps, real TAXI pattern Fri 05:50 - Sat 05:17 (horizon from 14:00)
python scripts/make_e3_eval_spec.py $W/models_main/decisions.json scripts/e3t_eval_spec.json 3 results/E3T_eval e3t_taxi_eval \
    --dataset TAXI --drain 300 --trace-full > logs/E3T_eval_spec.log 2>&1
cat logs/E3T_eval_spec.log
python scripts/run_e3_grid.py scripts/e3t_eval_spec.json > logs/grid_E3T_eval.log 2>&1
log "evaluation done: $(ls results/E3T_eval/p*_rep*.json | wc -l) result files"
echo ALL_DONE

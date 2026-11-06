#!/usr/bin/env bash
# 2026-11-06: re-measure every run still in use that overlapped battery power or Modern Standby (ENVIRONMENT section 13),
# then retrain/re-decide STREAMLINE on the corrected profiling table. All grids use the power guard (scripts/power_guard.py).
set -u
cd "$(dirname "$0")/.."
log() { echo "$(date +%H:%M:%S) $*"; }
python scripts/keep_awake.py & KA=$!
trap 'kill $KA 2>/dev/null' EXIT
A=results/battery_or_standby
mkdir -p $A/E3_profile $A/E3_eval $A/L1b_e2e_r1000 $A/E1_fit_r4000
for f in p4_seg16384 p4_seg32768 p4_seg65536 p8_seg32768; do [ -f results/E3_profile/$f.json ] && mv results/E3_profile/$f.json $A/E3_profile/; done
for f in p8_tm4_seg65536_rep2 p8_tm4_seg65536_rep3; do [ -f results/E3_eval/$f.json ] && mv results/E3_eval/$f.json $A/E3_eval/; done
[ -f results/L1b_e2e_r1000/p1_r1000_rep3.json ] && mv results/L1b_e2e_r1000/p1_r1000_rep3.json $A/L1b_e2e_r1000/
for p in 1 2 4 8; do [ -f results/E1_fit_r4000/p${p}_r4000_rep1.json ] && mv results/E1_fit_r4000/p${p}_r4000_rep1.json $A/E1_fit_r4000/; done
log "archived battery/standby runs"

python scripts/run_e3_grid.py scripts/e3_profile_spec.json > logs/grid_E3_profile_power.log 2>&1
log "profiling re-runs done"
n=$(ls results/E3_profile/p*_seg*.json 2>/dev/null | wc -l)
if [ "$n" -ne 12 ]; then log "ABORT: only $n of 12 profiling runs present - not retraining"; echo ALL_DONE; exit 1; fi
python scripts/build_streamline_dataset.py results/E3_profile results/E3_work/historical_data_etl.csv > results/E3_work/dataset_report.json
python scripts/build_streamline_dataset.py results/E3_profile results/E3_work/historical_data_etl_srccpu.csv --src-busy cpu > results/E3_work/dataset_report_srccpu.json
log "dataset rebuilt: $(head -2 results/E3_work/dataset_report.json | tail -1)"

# previous models are already in results/E3_work/models_battery_superseded (never delete them)
[ -d results/E3_work/models_main ] && mv results/E3_work/models_main results/E3_work/models_prev_$(date +%H%M%S)
mkdir -p results/E3_work/models_main
( cd ../Streamline && OMP_NUM_THREADS=4 MKL_NUM_THREADS=4 ../env/Scripts/python.exe ../repro_scripts/e3_decide.py train \
    ../bigdata/results/E3_work/historical_data_etl.csv ../bigdata/results/E3_work/workload_history.json ../bigdata/results/E3_work/models_main \
    > ../bigdata/logs/E3_train_power.log 2> ../bigdata/logs/E3_train_power.err
  OMP_NUM_THREADS=4 MKL_NUM_THREADS=4 ../env/Scripts/python.exe ../repro_scripts/e3_decide.py decide ../bigdata/results/E3_work/models_main 10 \
    > ../bigdata/logs/E3_decide_power.log 2> ../bigdata/logs/E3_decide_power.err )
log "train + decide done (cluster idle)"
cat logs/E3_decide_power.log

python scripts/make_e3_eval_spec.py results/E3_work/models_main/decisions.json scripts/e3_eval_spec.json 3 > logs/E3_eval_spec_power.log 2>&1
cat logs/E3_eval_spec_power.log
python scripts/run_e3_grid.py scripts/e3_eval_spec.json > logs/grid_E3_eval_power.log 2>&1
log "E3 evaluation completed: $(ls results/E3_eval/p*_rep*.json | wc -l) result files"

HARNESS_EXTRA="--latency-interval 1000 --e2e" python scripts/run_grid.py results/L1b_e2e_r1000 1000 360 150 3 1 4 8 > logs/grid_L1b_e2e_power.log 2>&1
log "L1b rep3 done"
python scripts/run_grid.py results/E1_fit_r4000 4000 360 150 3 1 2 4 8 > logs/grid_E1_rep1_power.log 2>&1
log "E1 rep1 re-run done"
echo ALL_DONE

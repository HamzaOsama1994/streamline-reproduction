#!/usr/bin/env bash
# Serial chain (2026-11-05, ~21:45): no host-side computation runs while the cluster is measuring.
# 1. wait for the running L1b grid; 2. archive runs that overlapped host-side jobs (see ENVIRONMENT section 7);
# 3. re-measure them; 4. rebuild the STREAMLINE table; 5. train + decide while the cluster is idle; 6. E3 evaluation.
set -u
cd "$(dirname "$0")/.."
log() { echo "$(date +%H:%M:%S) $*"; }

until grep -q GRID_DONE logs/grid_L1b_e2e.log 2>/dev/null; do sleep 30; done
log "L1b grid done"

mkdir -p results/contaminated_hostload/L1b_e2e_r1000 results/contaminated_hostload/L1_fit_r1000 results/contaminated_hostload/E3_profile
for f in p1_r1000_rep1 p1_r1000_rep2; do [ -f results/L1b_e2e_r1000/$f.json ] && mv results/L1b_e2e_r1000/$f.json results/contaminated_hostload/L1b_e2e_r1000/; done
[ -f results/L1_fit_r1000/p8_r1000_rep1.json ] && mv results/L1_fit_r1000/p8_r1000_rep1.json results/contaminated_hostload/L1_fit_r1000/
for f in p1_seg16384 p1_seg32768; do [ -f results/E3_profile/$f.json ] && mv results/E3_profile/$f.json results/contaminated_hostload/E3_profile/; done
log "archived contaminated runs"

HARNESS_EXTRA="--latency-interval 1000 --e2e" python scripts/run_grid.py results/L1b_e2e_r1000 1000 360 150 2 1 4 8 > logs/grid_L1b_e2e_rerun.log 2>&1
log "L1b re-runs done"
HARNESS_EXTRA="--latency-interval 1000" python scripts/run_grid.py results/L1_fit_r1000 1000 360 150 3 8 > logs/grid_L1_r1000_p8rerun2.log 2>&1
log "L1 p8 re-run done"
python scripts/run_e3_grid.py scripts/e3_profile_spec.json > logs/grid_E3_profile_rerun.log 2>&1
log "profiling re-runs done"

python scripts/build_streamline_dataset.py results/E3_profile results/E3_work/historical_data_etl.csv > results/E3_work/dataset_report.json
python scripts/build_streamline_dataset.py results/E3_profile results/E3_work/historical_data_etl_srccpu.csv --src-busy cpu > results/E3_work/dataset_report_srccpu.json
log "dataset rebuilt: $(head -2 results/E3_work/dataset_report.json | tail -1)"

rm -rf results/E3_work/models_main
mkdir -p results/E3_work/models_main
( cd ../Streamline && OMP_NUM_THREADS=4 MKL_NUM_THREADS=4 ../env/Scripts/python.exe ../repro_scripts/e3_decide.py train \
    ../bigdata/results/E3_work/historical_data_etl.csv ../bigdata/results/E3_work/workload_history.json ../bigdata/results/E3_work/models_main \
    > ../bigdata/logs/E3_train.log 2> ../bigdata/logs/E3_train.err
  OMP_NUM_THREADS=4 MKL_NUM_THREADS=4 ../env/Scripts/python.exe ../repro_scripts/e3_decide.py decide ../bigdata/results/E3_work/models_main 10 \
    > ../bigdata/logs/E3_decide.log 2> ../bigdata/logs/E3_decide.err )
log "train + decide done (cluster idle)"
cat logs/E3_decide.log

python scripts/make_e3_eval_spec.py results/E3_work/models_main/decisions.json scripts/e3_eval_spec.json 3 > logs/E3_eval_spec.log 2>&1
cat logs/E3_eval_spec.log
python scripts/run_e3_grid.py scripts/e3_eval_spec.json > logs/grid_E3_eval.log 2>&1
log "E3 evaluation done"
echo ALL_DONE

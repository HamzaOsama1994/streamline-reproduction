#!/usr/bin/env bash
# Parallel replicate batch: 8 workers x 2 torch threads. Run from streamline_repro/.
cd Streamline
export MPLBACKEND=Agg PYTHONUNBUFFERED=1 OMP_NUM_THREADS=2
xargs -P 8 -L 1 sh -c '../env/Scripts/python.exe ../repro_scripts/correction_study.py $0 $1 $2 $3 ../correction_study/runs 2 2>/dev/null | grep -E "^V[0-9]" >> ../correction_study/logs/batch_summary.log' < ../correction_study/tasks.txt
echo BATCH_DONE >> ../correction_study/logs/batch_summary.log

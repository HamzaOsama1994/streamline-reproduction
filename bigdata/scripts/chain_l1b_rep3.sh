#!/usr/bin/env bash
# 2026-11-06: after the E3 evaluation retry, add repetition 3 of L1b (all configs) so every L1b config has 3 runs like L1,
# and to see whether the transient stall of FD rep 1 (re-run) recurs. Serial: nothing else measures meanwhile.
set -u
cd "$(dirname "$0")/.."
until grep -q GRID_DONE logs/grid_E3_eval_retry.log 2>/dev/null; do sleep 30; done
echo "$(date +%H:%M:%S) E3 retry done"
HARNESS_EXTRA="--latency-interval 1000 --e2e" python scripts/run_grid.py results/L1b_e2e_r1000 1000 360 150 3 1 4 8 > logs/grid_L1b_e2e_rep3.log 2>&1
echo "$(date +%H:%M:%S) L1b rep3 done"
echo ALL_DONE

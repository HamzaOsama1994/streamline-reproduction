# Excluded: p1_tm1_seg32768_rep3 (FD), first measurement, 2026-11-06 23:09-23:21

The job itself ran normally (power valid, host load 1.4 cores, broker counts input 479,539 / output 479,535 after a 1.9 s drain).
After the run, reading the output topic back with kafka-console-consumer returned only 469,538 timestamps, so:

- the per-record trace marks the last 10,001 records as "unprocessed", although the broker counts show they were processed;
- the order check had no output records to compare and reported 0.0 on all three slices (a failed read, not a mismatch).

Topics are recreated at the start of every run, so the data could not be read again. The run was re-measured under the same
spec (scripts/e3t_eval_spec.json) on 2026-11-07; the replacement is results/E3T_eval/p1_tm1_seg32768_rep3.json.

# ORDER BY Parallel Sort Performance Report

This report evaluates the new `ParallelSortBuffer` against the legacy serial
`SortBuffer` for ORDER BY workloads. The benchmark emits all raw measurements to
JSON, and the companion notebook renders charts and tables that highlight where
parallel sort is faster or slower than serial.

## Benchmark matrix

The target report matrix is:

- Threads: `1, 4, 8, 16`
- Sort key type: `all_int, mixed, all_string`
- Sort key count: `1, 3, 5`
- Payload fields: default `2`; payload fields alternate `VARCHAR, BIGINT, ...`
- ORDER BY memory limit: `4GB, 8GB`
- Logical data size: `512MB, 2GB, 4GB, 16GB`
- Implementations: `parallel_on, parallel_off`

## Data collection

Run the full matrix with:

```bash
/workspaces/bolt-github/_build/Release/bolt/exec/bolt_parallel_order_by_benchmark_pb \
  --data_gb_values=1g,2g,16g \
  --memory_gb_values=8g \
  --batch_rows=8192 \
  --output_rows=65536 \
  --thread_values=1,4,16 \
  --parallel_values=both \
  --key_shape_values=all_int,mixed,all_string \
  --num_key_values=1,3 \
  --num_payload_fields=2 \
  --split_size=256MB \
  --json_output=ORDER_BY_PARALLEL_SORT_RESULTS.json
```

The full matrix is large: 4 thread counts × 3 key shapes × 3 key counts × 2
memory limits × 4 data sizes × 2 implementations = 576 benchmark runs. Use the
filter flags to iterate on smaller subsets before launching the full run.

## Raw JSON fields

Each JSON result includes:

- Workload parameters: `parallelThreads`, `keyShape`, `numKeys`,
  `numPayloadFields`, `dataBytes`, `memoryBytes`, `parallel`,
  `splitSizeBytes`, `numSplits`, `fileBytes`
- Top-line timings: `elapsedUs`, `cpuUs`, `cpuPct`
- Spill metrics: `spilledBytes`, `spillRuns`, `spillReadIOTimeUs`,
  `spillWriteTimeUs`
- Sort phase metrics: `sortColToRowTimeUs`, `sortInSortTimeUs`,
  `sortOutputTimeUs`
- Parallel-only diagnostics: `inputRunsCreated`, `maxRunningInputRuns`,
  `maxRunningSpillRuns`, `mergeTargetRows`, `mergeTasks`,
  `mergePlanningTimeUs`, `mergeExecutionTimeUs`, `maxRunningMergeBatches`

## Reading the results

The companion notebook `ORDER_BY_PARALLEL_SORT_PERFORMANCE_REPORT.ipynb` reads
`ORDER_BY_PARALLEL_SORT_RESULTS.json` and computes matched parallel-vs-serial
pairs for each workload. It reports:

- Speedup: `serial_elapsed_ms / parallel_elapsed_ms`
- Parallel delta: `parallel_elapsed_ms - serial_elapsed_ms`
- Win/loss status for every matrix point
- Heatmaps by data size, memory limit, key shape, key count, and thread count
- Scatter plots showing how spill run count and merge planning time correlate
  with regressions

Interpretation:

- `speedup > 1.0`: parallel is faster
- `speedup = 1.0`: tie
- `speedup < 1.0`: parallel is slower

## Current known behavior

The optimization now adaptively grows input run size and merge target rows. In
the focused 16GB / 1GB / all-int / 1-key / 4-thread workload, the adaptive merge
target reduced merge tasks from 512 to 128 and reduced merge planning time from
roughly 9 seconds to roughly 3 seconds. The parallel path can now beat serial in
that workload, but it still creates more spill runs than serial and still uses a
row-oriented spilled merge, so string-heavy or high-fan-in cases must be checked
carefully in the full matrix.

Expected areas where parallel should do well:

- Larger in-memory workloads where input sorting can run concurrently
- Spilled workloads where run fan-in remains moderate
- Workloads with enough rows to amortize executor scheduling overhead

Expected areas where serial may still win:

- Very small data sizes, especially with many threads
- Spilled workloads where parallel creates many more runs than serial
- String-heavy keys where row-at-a-time parallel spilled merge has high compare
  and copy overhead
- Cases where merge planning is a large fraction of elapsed time

## Next analysis questions

After collecting the full JSON matrix, focus on:

1. Which combinations have `speedup < 1.0`?
2. Are regressions correlated with `spillRuns`, `mergeTasks`, or
   `mergePlanningTimeUs`?
3. Does increasing thread count improve elapsed time or only CPU time?
4. Does string-heavy data shift bottlenecks from input sort to merge compare?
5. Do 4GB and 8GB memory limits change the run fan-in enough to alter wins and
   losses?

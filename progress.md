# Parallel SortBuffer Implementation Progress

## References

- Design document: `ORDER_BY_PARALLEL_SORT_DESIGN.md`
- Current target: Stage 6, "One-Pass Parallel Slice Merge" complete; next
  target is Stage 7, "Async Prefetch and I/O Scheduling".

## Current Status

- Stage 0 implementation is complete and accepted.
- Stage 1 implementation is complete and accepted for the in-memory sorted-run
  path.
- Stage 2 implementation is complete and accepted for indexed spill metadata
  plumbing.
- Stage 3 implementation is complete and accepted for the spilled sorted-run
  sequential reader path.
- Stage 4 implementation is complete and accepted for indexed random row lookup
  and lower-bound search over spilled sorted runs.
- Stage 5 implementation is complete and accepted for DuckDB-style k-run merge
  boundary planning.
- Stage 6 implementation is complete and accepted for one-pass merge task
  execution over bounded in-memory and spilled run slices.
- Stage 0 scope:
  - [x] Add query config flags for the staged parallel sort rollout.
  - [x] Add placeholder `SortedRun` interfaces.
  - [x] Add `ISortBuffer` interface shape based on `mergepath.patch`.
  - [x] Keep legacy `SortBuffer` behavior as the default path.
  - [x] Add Stage 0 config/interface tests.
  - [x] Run release-mode acceptance commands.

## Implementation Notes

- `SortBuffer` now implements `ISortBuffer`.
- `OrderBy` stores `std::unique_ptr<ISortBuffer>` and now constructs
  `ParallelSortBuffer` when `order_by_parallel_sort_enabled=true` and spilling
  is not enabled for the operator.
- `order_by_parallel_sort_enabled=true` still falls back to legacy `SortBuffer`
  when spilling is enabled because Stage 1 only supports in-memory sorted runs.
- `SortedRun.h` contains the shared `SortedRun` interface and first concrete
  in-memory run implementation.
- `InMemorySortedRun` now owns a `RowContainer` and sorted row-pointer index,
  reusing the row-pointer sorting approach from `mergepath.patch` and legacy
  `SortBuffer`.
- `ParallelSortBuffer` materializes each input batch as an independent
  `InMemorySortedRun` and produces output through a serial k-way merge. Parallel
  run generation and parallel merge remain deferred to later stages.
- `SpillConfig` now carries `indexedSpillEnabled` through `SpillIOConfig`,
  stringification, and serialization/deserialization.
- `SpillWriteFile::writeBlock()` reports the logical byte offset and size of
  each serialized write, including the io_uring path where physical file size
  can lag pending writes.
- `SpillWriter` records `SpillBlockInfo` entries when indexed spill is enabled,
  tracking block offset, byte size, row offset, and row count per finished spill
  file.
- `OrderBy` enables indexed spill metadata for its spill config when
  `order_by_indexed_spill_enabled=true`.
- `SpilledSortedRun` now spills already-sorted row pointers as ordered row-vector
  blocks without re-sorting, preserves spill sorting metadata, validates indexed
  block row counts, and reads spill files sequentially through `SpillReadFile`.
- `IndexedSpillReadFile` uses `SpillBlockInfo` metadata to find a row ordinal,
  reads only the containing serialized block, and keeps a bounded per-file block
  cache.
- `SpilledSortedRun` now supports `rowAt()`, `compare()`, and `lowerBound()` for
  indexed spilled runs.
- `MergePathBoundaryPlanner` computes output-cardinality-balanced merge task
  boundaries using the generalized k-run merge path approach described in the
  DuckDB sorting redesign: iteratively choose lookahead steps across active runs,
  advance the smallest lookahead row by internal total order, and produce
  disjoint per-run slices for each output range.
- `RunSliceReader` reads a bounded half-open slice from either an in-memory run
  or an indexed spilled run and copies only the current row directly into the
  output vector.
- `MergeTaskExecutor` performs a local heap-based k-way merge over a single
  `MergeTask` and emits output batches directly, preserving the internal total
  order `(ORDER BY key, run id, ordinal-in-run)` without writing intermediate
  merge output back to spill.
- `OrderedMergeTaskOutputQueue` buffers completed merge-task batches and exposes
  them through `getOutput()` strictly in task order. Later tasks may finish
  first, but their output remains blocked until all earlier task output has been
  consumed. The queue also supports splitting task batches by caller-provided
  `maxOutputRows` and a bounded buffered-row limit for backpressure.

## Acceptance Commands for Stage 0

```bash
cmake --build --preset conan-release --target bolt_exec_test --parallel 15
_build/Release/bolt/exec/tests/bolt_exec_test --gtest_filter='*QueryConfig*:*OrderBy*:*SortBuffer*'
```

## Notes

- Release builds are required for acceptance (`release_with_test`,
  `conan-release`).
- Build acceptance should use `cmake --build ... --parallel 15`.
- Reuse from `mergepath.patch` is required where applicable, especially the
  `ISortBuffer` interface shape.

## Build Attempts

- `cmake --build --preset conan-release --target bolt_exec_test` failed because
  a compiler process was killed by the environment.
- `cmake --build --preset conan-release --target bolt_exec_test --parallel 2`
  also failed before the clarification that acceptance builds should use
  `--parallel 15`.
- `cmake --build --preset conan-release --target bolt_exec_test --parallel 15`
  passed.

## Acceptance Results for Stage 0

- `cmake --build --preset conan-release --target bolt_exec_test --parallel 15`
  passed.
- `_build/Release/bolt/exec/tests/bolt_exec_test --gtest_filter='*QueryConfig*:*OrderBy*:*SortBuffer*'`
  passed: 28 tests from 3 test suites.

## Acceptance Commands for Stage 1

```bash
cmake --build --preset conan-release --target bolt_exec_test --parallel 15
_build/Release/bolt/exec/tests/bolt_exec_test --gtest_filter='*SortBuffer*:*OrderBy*:*SortAlgo*'
```

## Acceptance Results for Stage 1

- `cmake --build --preset conan-release --target bolt_exec_test --parallel 15`
  passed.
- `_build/Release/bolt/exec/tests/bolt_exec_test --gtest_filter='*SortBuffer*:*OrderBy*:*SortAlgo*'`
  passed: 34 tests from 4 test suites.
- `_build/Release/bolt/exec/tests/bolt_exec_test --gtest_filter='SortBufferTest.inMemory*Sort*'`
  passed: 3 tests from 1 test suite.
- `_build/Release/bolt/exec/tests/bolt_exec_test --gtest_filter='OrderByTestOnCPUOrGPU/OrderByTest.parallelSortEnabledInMemory/CPU'`
  passed: 1 test from 1 test suite.
- `SKIP=clang-tidy pre-commit run --all-files` passed. A plain
  `pre-commit run --all-files` was attempted first and failed in clang-tidy due
  to missing optional external headers for GCS, ABFS, Folly, fmt, and Celeborn;
  the design document now records the accepted `SKIP=clang-tidy` command.

## Acceptance Commands for Stage 2

```bash
cmake --build --preset conan-release --target bolt_exec_test --parallel 15
_build/Release/bolt/exec/tests/bolt_exec_test --gtest_filter='*SpillTest*:*SpillerTest*:*SortBuffer*'
_build/Release/bolt/exec/tests/bolt_exec_test --gtest_filter='*Spill*'
cmake --build --preset conan-release --target bolt_base_test --parallel 15
_build/Release/bolt/common/base/tests/bolt_base_test --gtest_filter='SpillConfig*:*SpillStats*'
cmake --build --preset conan-release --target row_based_spill_test --parallel 15
_build/Release/bolt/exec/tests/row_based_spill_test
cmake --build --preset conan-release --target iouring_spill_test --parallel 15
_build/Release/bolt/exec/tests/iouring_spill_test
SKIP=clang-tidy pre-commit run --all-files
```

## Acceptance Results for Stage 2

- `cmake --build --preset conan-release --target bolt_exec_test --parallel 15`
  passed. An earlier interrupted/failed build was resumed; a compile error in
  row-based indexed spill metadata capture was fixed before the final passing
  build.
- `_build/Release/bolt/exec/tests/bolt_exec_test --gtest_filter='*SpillTest*:*SpillerTest*:*SortBuffer*'`
  passed: 114 tests from 9 test suites.
- `_build/Release/bolt/exec/tests/bolt_exec_test --gtest_filter='*Spill*'`
  passed: 132 tests from 16 test suites.
- `cmake --build --preset conan-release --target bolt_base_test --parallel 15`
  passed.
- `_build/Release/bolt/common/base/tests/bolt_base_test --gtest_filter='SpillConfig*:*SpillStats*'`
  passed: 11 tests from 1 test suite.
- `cmake --build --preset conan-release --target row_based_spill_test --parallel 15`
  passed.
- `_build/Release/bolt/exec/tests/row_based_spill_test` passed: 10 tests from 2
  test suites.

## Acceptance Commands for Stage 5

```bash
cmake --build --preset conan-release --target bolt_exec_test --parallel 15
_build/Release/bolt/exec/tests/bolt_exec_test --gtest_filter='*MergePath*:*SortBuffer*:*OrderBy*:*TreeOfLosers*:*MergerTest*'
SKIP=clang-tidy pre-commit run --all-files
```

## Acceptance Results for Stage 5

- `cmake --build --preset conan-release --target bolt_exec_test --parallel 15`
  passed. An initial build caught a constness issue in memory-row comparison,
  which was fixed before the final passing build.
- `_build/Release/bolt/exec/tests/bolt_exec_test --gtest_filter='*MergePath*'`
  passed: 5 tests from 1 test suite.
- `_build/Release/bolt/exec/tests/bolt_exec_test --gtest_filter='*MergePath*:*SortBuffer*:*OrderBy*:*TreeOfLosers*:*MergerTest*'`
  passed: 40 tests from 4 test suites.
- Merge-path test coverage includes two-run planning, duplicate-heavy inputs,
  all-equal runs, highly skewed and empty runs, mixed in-memory and spilled runs,
  and 100 randomized duplicate-heavy corner cases that validate slice coverage
  against a full in-memory merged reference.

## Acceptance Commands for Stage 6

```bash
cmake --build --preset conan-release --target bolt_exec_test --parallel 15
cmake --build --preset conan-release --target row_based_spill_test --parallel 15
_build/Release/bolt/exec/tests/bolt_exec_test --gtest_filter='*MergePath*:*SortBuffer*:*OrderBy*:*SpillTest*:*SpillerTest*:*MergerTest*:*ConcatFilesSpillMergeStreamTest*'
_build/Release/bolt/exec/tests/row_based_spill_test
SKIP=clang-tidy pre-commit run --all-files
```

## Acceptance Results for Stage 6

- `cmake --build --preset conan-release --target bolt_exec_test --parallel 15`
  passed.
- `_build/Release/bolt/exec/tests/bolt_exec_test --gtest_filter='*MergePath*:*SortBuffer*:*OrderBy*:*SpillTest*:*SpillerTest*:*MergerTest*:*ConcatFilesSpillMergeStreamTest*'`
  passed: 157 tests from 12 test suites.
- `cmake --build --preset conan-release --target row_based_spill_test --parallel 15`
  passed.
- `_build/Release/bolt/exec/tests/row_based_spill_test` passed: 10 tests from 2
  test suites.
- Stage 6 test coverage includes bounded slice reading for in-memory and spilled
  runs, memory-only merge tasks with varying output batch sizes, spilled-only
  duplicate-heavy merge, mixed in-memory/spilled all-equal tie-breaking,
  out-of-order task completion consumed through `OrderedMergeTaskOutputQueue`,
  repeated `getOutput()` calls with varying `maxOutputRows`, and bounded
  buffered-row backpressure.
- `SKIP=clang-tidy pre-commit run --all-files` passed after clang-format applied
  formatting on the first attempt.

## Acceptance Commands for Stage 4

```bash
cmake --build --preset conan-release --target bolt_exec_test --parallel 15
cmake --build --preset conan-release --target row_based_spill_test --parallel 15
_build/Release/bolt/exec/tests/bolt_exec_test --gtest_filter='*IndexedSpill*:*SpillTest*:*SpillerTest*:*SortBuffer*:*OrderBy*'
_build/Release/bolt/exec/tests/row_based_spill_test
SKIP=clang-tidy pre-commit run --all-files
```

## Acceptance Results for Stage 4

- `cmake --build --preset conan-release --target bolt_exec_test --parallel 15`
  passed.
- `_build/Release/bolt/exec/tests/bolt_exec_test --gtest_filter='*spilledSortedRunIndexedRandomAccessAndLowerBound*:*spilledSortedRunReadsSequentially*:*indexedSpillMetadataRecordsVectorBlocks*'`
  passed: 18 tests from 1 test suite. An earlier run exposed an off-by-one bug
  in block lookup for the first block; `IndexedSpillReadFile::blockIndexForRow()`
  was fixed before the final passing run.
- `cmake --build --preset conan-release --target row_based_spill_test --parallel 15`
  passed.
- `_build/Release/bolt/exec/tests/bolt_exec_test --gtest_filter='*IndexedSpill*:*SpillTest*:*SpillerTest*:*SortBuffer*:*OrderBy*'`
  passed: 142 tests from 10 test suites.
- `_build/Release/bolt/exec/tests/row_based_spill_test` passed: 10 tests from 2
  test suites.
- `cmake --build --preset conan-release --target iouring_spill_test --parallel 15`
  passed.
- `_build/Release/bolt/exec/tests/iouring_spill_test` passed: 54 tests from 6
  test suites.
- `SKIP=clang-tidy pre-commit run --all-files` passed after clang-format applied
  formatting on the first attempt.

## Acceptance Commands for Stage 3

```bash
cmake --build --preset conan-release --target bolt_exec_test --parallel 15
cmake --build --preset conan-release --target row_based_spill_test --parallel 15
_build/Release/bolt/exec/tests/bolt_exec_test --gtest_filter='*OrderBy*:*SortBuffer*:*SpillTest*:*SpillerTest*:*MergerTest*:*ConcatFilesSpillMergeStreamTest*'
_build/Release/bolt/exec/tests/row_based_spill_test
SKIP=clang-tidy pre-commit run --all-files
```

## Acceptance Results for Stage 3

- `cmake --build --preset conan-release --target bolt_exec_test --parallel 15`
  passed. An earlier build caught a constness issue in `SpilledSortedRun` row
  extraction, which was fixed before the final passing build.
- `_build/Release/bolt/exec/tests/bolt_exec_test --gtest_filter='*spilledSortedRunReadsSequentially*:*indexedSpillMetadataRecordsVectorBlocks*'`
  passed: 12 tests from 1 test suite.
- `cmake --build --preset conan-release --target row_based_spill_test --parallel 15`
  passed.
- `_build/Release/bolt/exec/tests/bolt_exec_test --gtest_filter='*OrderBy*:*SortBuffer*:*SpillTest*:*SpillerTest*:*MergerTest*:*ConcatFilesSpillMergeStreamTest*'`
  passed: 140 tests from 12 test suites.
- `_build/Release/bolt/exec/tests/row_based_spill_test` passed: 10 tests from 2
  test suites.

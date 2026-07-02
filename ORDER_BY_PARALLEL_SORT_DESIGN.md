# Parallel `SortBuffer` Design: Indexed Sorted Runs and One-Pass Parallel Merge

## Summary

This document proposes a staged redesign of `SortBuffer` / `OrderBy` to support
multi-threaded sorting, spilling, spill reading, and final merge without adding
extra full-data merge passes.

The core design is:

```text
input RowVectors
  -> parallel sorted-run generation
  -> memory or indexed spilled SortedRuns
  -> positional merge-path boundary planning
  -> one-pass parallel slice merge
  -> ordered RowVector output
```

The most important constraint is to avoid staged external merge passes. Pairwise
external merge would parallelize CPU, but it would also read and write the full
data set one or more additional times. For large spilled `ORDER BY`, I/O is the
dominant cost, so the final merge must read each spilled byte approximately once
and produce output directly.

## Current State and Constraints

The existing `SortBuffer` has two major modes:

1. Accumulate all rows in memory, then sort row pointers in one container.
2. If spilling occurs, spill sorted runs through the existing spilling stack and
   read them back through sequential merge streams.

Important current code paths:

- `SortBuffer::addInput()` materializes input into a single `RowContainer`.
- `SortBuffer::noMoreInput()` sorts a single in-memory row-pointer vector when
  no spilling occurs.
- `SortBuffer::spillInput()` uses `Spiller` to spill the current row container.
- `SpillPartition::createOrderedReader()` builds sequential spill merge streams.
- `FileSpillMergeStream::nextBatch()` advances through spilled files
  sequentially.
- `TreeOfLosers` is an efficient serial k-way merge primitive, but it is not a
  parallel final-merge strategy by itself.

The current spill file abstraction is sequential:

- `SpillFileInfo` contains file-level metadata such as path, size, row count,
  sorting keys, compression kind, serde kind, and row format info.
- `SpillInputStream` is explicitly stream-oriented. Its comment says the
  `ByteInputStream` random-access APIs do not work properly.

Therefore, a true one-pass parallel final merge requires a minimal indexed spill
format for sorted runs.

## Goals

1. Preserve existing `ORDER BY` semantics.
2. Reuse the existing `Spiller`, `SpillWriter`, `SpillReadFile`, serde, and
   compression infrastructure where practical.
3. Support mixed in-memory and spilled sorted runs.
4. Parallelize sorted-run generation.
5. Spill already-sorted runs independently and concurrently.
6. Add block-level random access for sorted spill files.
7. Use positional merge-path planning to divide the final merge into independent
   output ranges.
8. Execute final merge tasks in parallel while reading each spilled run slice
   once.
9. Emit output in global sorted order from `SortBuffer::getOutput()`.
10. Provide feature flags and fallbacks for incremental rollout.

## Non-Goals

1. Do not implement pairwise external merge as the primary spilled path. It can
   remain a fallback, but it adds too much I/O for the target workload.
2. Do not require normalized-key pages in the first implementation.
3. Do not rewrite all spill infrastructure into a new page store in the first
   implementation.
4. Do not require value-based range partitioning. It is vulnerable to skew and
   large duplicate-key regions.

## Key Design Principles

### 1. `SortedRun` is the unit of sorting and spilling

A sorted run is an immutable, internally sorted sequence of rows. A run may be
backed by memory or by one or more indexed spill files.

```cpp
class SortedRun {
 public:
  uint64_t numRows() const;
  bool spilled() const;

  RowRef rowAt(uint64_t ordinal);
  uint64_t lowerBound(const SortKeyRef& key);
  std::unique_ptr<RunSliceReader> createSliceReader(
      uint64_t begin,
      uint64_t end);
};
```

Suggested variants:

```cpp
class InMemorySortedRun;
class SpilledSortedRun;
```

### 2. Use a total internal order

SQL-visible ordering is defined by the `ORDER BY` keys. Internally, parallel
merge planning needs a total order to split duplicate-key regions safely.

Use:

```text
(ORDER BY key, run_id, ordinal_in_run)
```

The `run_id` and `ordinal_in_run` tie-breakers are internal only. They do not
change SQL semantics, but they make merge-path boundaries deterministic.

### 3. Random access is needed for planning, not for the full merge

The final merge should be mostly sequential I/O.

Random access is used to compute task boundaries:

```text
output row K -> vector of per-run offsets
```

Once a task receives its slices, each worker reads those slices sequentially.

### 4. Partition final merge by output cardinality

Avoid value-based splitters. Instead, create tasks by output row ranges:

```text
task 0: output rows [0, 64K)
task 1: output rows [64K, 128K)
task 2: output rows [128K, 192K)
...
```

This remains balanced even if many rows have identical sort keys.

## Architecture

### High-Level Data Flow

```text
SortBufferMergePath::addInput()
  -> collect input morsels
  -> submit SortRunTask

SortRunTask
  -> materialize rows into RowContainer
  -> sort row pointers
  -> keep in memory or spill as indexed sorted run
  -> publish SortedRun

SortBufferMergePath::noMoreInput()
  -> wait for SortRunTasks
  -> compute merge-path boundaries
  -> create ordered MergeTasks
  -> schedule bounded number of MergeTasks

SortBufferMergePath::getOutput()
  -> consume completed MergeTasks in output ordinal order
  -> return RowVector batches
```

### Main Components

#### `SortedRun`

Owns or references an immutable sorted sequence.

Responsibilities:

- Report row count.
- Expose row lookup by ordinal for boundary planning.
- Expose lower-bound search for rank planning.
- Create sequential slice readers for merge execution.

#### `InMemorySortedRun`

Backed by:

```cpp
std::shared_ptr<RowContainer> container;
std::vector<char*> sortedRows;
```

#### `SpilledSortedRun`

Backed by:

```cpp
std::vector<SpillFileInfo> files;
std::vector<IndexedSpillBlockInfo> blocks;
RowTypePtr rowType;
std::vector<SpillSortKey> sortingKeys;
```

The run treats multiple files as one logical sorted sequence.

#### `IndexedSpillReadFile`

Provides random block reads for indexed sorted spill files.

```cpp
class IndexedSpillReadFile {
 public:
  RowVectorPtr readBlock(uint32_t blockIndex);
  RowVectorPtr readRows(uint64_t beginRow, uint64_t endRow);
};
```

It should be separate from the existing sequential `SpillReadFile` to avoid
destabilizing other spill users.

#### `RunSliceReader`

Sequentially reads `[begin, end)` from a run.

For spilled runs, it maps row ordinals to indexed blocks, then performs
sequential block reads for the task slice.

#### `MergeBoundaryPlanner`

Computes boundary vectors:

```cpp
struct MergeBoundary {
  uint64_t outputOrdinal;
  std::vector<uint64_t> runOffsets;
};
```

Adjacent boundaries define independent merge tasks.

#### `MergeTask`

```cpp
struct RunSlice {
  SortedRun* run;
  uint64_t begin;
  uint64_t end;
};

struct MergeTask {
  uint64_t outputBegin;
  uint64_t outputEnd;
  std::vector<RunSlice> slices;
};
```

Each task merges its slices into output batches. Tasks are independent and can
run in parallel.

#### `OrderedOutputQueue`

Maintains global output order.

Workers may finish tasks out of order, but `getOutput()` emits only from the
next expected task ordinal.

## Indexed Sorted Spill Format

### Minimal metadata

Add block metadata for sorted spill files:

```cpp
struct SpillBlockInfo {
  uint64_t fileOffset;
  uint64_t compressedSize;
  uint64_t uncompressedSize;
  uint64_t firstRow;
  uint32_t rowCount;
};
```

Extend `SpillFileInfo`:

```cpp
struct SpillFileInfo {
  ...
  std::vector<SpillBlockInfo> blocks;
};
```

The first implementation does not require min/max keys in block metadata.

### Optional future metadata

Later optimizations may add:

```cpp
struct SpillBlockInfo {
  ...
  SerializedSortKey firstKey;
  SerializedSortKey lastKey;
};
```

This can reduce random reads during boundary planning.

### Writer behavior

For indexed sorted spill mode, each `SpillWriter::flush()` is one logical block.

For each block, record:

- logical file offset before write,
- written byte size,
- uncompressed byte size if available,
- starting logical row ordinal,
- row count.

The existing `SpillWriteFile::write()` returns bytes written, but indexed mode
also needs the logical file offset. Add a block-oriented API:

```cpp
struct SpillWriteResult {
  uint64_t offset;
  uint64_t bytes;
};

SpillWriteResult SpillWriteFile::writeBlock(std::unique_ptr<folly::IOBuf> data);
SpillWriteResult SpillWriteFile::writeBlock(std::string_view data);
```

For async I/O, the offset must be assigned from a logical write cursor before
submission. It must not depend on completion order.

### Reader behavior

`IndexedSpillReadFile` should:

1. Locate the block containing a target ordinal.
2. `pread` the block by `fileOffset` and `compressedSize`.
3. Decompress if needed.
4. Deserialize into a `RowVector` or row-buffer representation.
5. Cache recent blocks for boundary planning.

## Boundary Planning

### Boundary definition

For output ordinal `K`, find offsets:

```text
p[0], p[1], ..., p[n-1]
```

such that:

```text
sum(p) = K
all rows before p are <= all rows at or after p
```

using the internal total order:

```text
(ORDER BY key, run_id, ordinal_in_run)
```

### Practical first algorithm

Implement a rank-search planner using lower bounds.

For a candidate key `x`:

```text
rank(x) = sum(lower_bound(run_i, x))
```

To find the boundary for output ordinal `K`:

1. Select candidate rows from one or more runs.
2. Compute their global rank using lower bounds in all runs.
3. Binary search candidate space until rank brackets `K`.
4. Resolve exact offsets using the total tie-breaker.

This is simpler than a fully generalized k-way merge-path implementation and is
sufficient for the first production version.

### Boundary size

Make task size configurable:

```text
order_by_parallel_merge_target_rows = 64K or 256K initially
```

Large task sizes reduce boundary-planning overhead and keep merge execution
mostly sequential.

## Parallel Merge Execution

Each adjacent boundary pair creates one task.

```text
boundary[i] and boundary[i + 1]
  -> slices from each run
  -> task output range [boundary[i].outputOrdinal,
                        boundary[i + 1].outputOrdinal)
```

Each task:

1. Creates one `RunSliceReader` per non-empty slice.
2. Uses a local loser tree or heap to merge slice readers.
3. Produces output `RowVector` batches.
4. Publishes batches to an ordered output queue.

No task writes intermediate sorted data.

## Memory Management and Backpressure

The design must bound memory in all stages.

### Run generation

- Limit active input morsel bytes.
- Limit number of pending sort tasks.
- Spill sorted runs when memory pressure is detected.

### Boundary planning

- Use a small block cache per planning thread.
- Bound random-access block reads.

### Merge execution

- Limit scheduled merge tasks to `parallelism + lookahead`.
- Limit input read-ahead blocks per task.
- Limit completed-but-not-emitted output batches.

### Output

- Emit tasks strictly in output ordinal order.
- Do not allow fast later tasks to accumulate unbounded memory while waiting for
  earlier tasks.

## Configuration

Suggested flags:

```text
order_by_parallel_sort_enabled
order_by_parallel_sort_spill_enabled
order_by_indexed_spill_enabled
order_by_parallel_merge_enabled
order_by_parallel_merge_threads
order_by_parallel_merge_target_rows
order_by_parallel_merge_lookahead_tasks
order_by_indexed_spill_block_rows
order_by_indexed_spill_block_bytes
```

Rollout should allow enabling each stage independently.

## Fallback Strategy

Fallbacks are required for incremental rollout and correctness safety.

1. If parallel sorted-run generation is disabled, use legacy `SortBuffer`.
2. If indexed spill is disabled, spilled runs may use legacy sequential merge.
3. If boundary planning fails, fall back to sequential k-way stream merge over
   sorted runs.
4. If unsupported type combinations appear, fall back to the legacy comparator
   and sequential merge path.

Avoid falling back to full legacy `SortBuffer` after data has already been
partitioned into sorted runs unless absolutely necessary.

## Implementation Plan

### Reuse from Existing `mergepath.patch`

The initial `SortedRun` implementation should reuse the working pieces from the
existing merge-path prototype instead of starting from scratch. In particular:

- Reuse the `ISortBuffer` interface shape and shared helper methods from the
  patch where applicable.
- Reuse the parallel input collection / sort-task scheduling pattern from
  `SortBufferMergePath::addInput()` and `SortBufferMergePath::addSortTask()` in
  `mergepath.patch`.
- Reuse the existing row materialization logic from `OrderByRowContainer` where
  it is correct, but do not carry forward the disabled custom spill/load path as
  the primary spilled-run design.
- Reuse comparator helpers from the prototype, including the JIT/non-JIT split,
  but move them behind `SortedRun` / merge-planning abstractions instead of
  keeping them tied to `OrderedContainer`.
- Reuse the merge-path unit-test ideas and data cases from
  `SortBufferMergePathTest` in the patch, then extend them for spilled and
  indexed-run behavior.

The prototype types that should evolve or be replaced are:

- `OrderedContainer` should become or be wrapped by `InMemorySortedRun`.
- `SpillableRowContainer` / `RowContainerRef` should not be the main long-term
  disk abstraction because the desired spilled path uses indexed `SpillWriter`
  files and one-pass slice readers.
- The fallback path in `SortBufferMergePath::fallbackToSingleThread()` should be
  removed only after spilled `SortedRun` sequential merge is working.

Acceptance for code reuse:

- Stage 1 implementation notes or PR description must explicitly identify which
  `mergepath.patch` components were reused, moved, or intentionally discarded.
- New `SortedRun` tests should cover the same in-memory cases present in the
  prototype merge-path tests before adding spill-specific cases.

### Test Command Conventions

Every stage below lists the exact commands that must pass before the stage is
accepted. Commands assume the repository root as the working directory.

Use the existing configured build when possible. If the build has not been
configured with tests, configure once with:

```bash
make release_with_test BOLT_CONAN_CONFIGURE_ONLY=1
```

Use narrow target builds while developing:

```bash
cmake --build --preset conan-release --target bolt_exec_test --parallel 15
cmake --build --preset conan-release --target row_based_spill_test --parallel 15
```

When a stage touches `io_uring` spill code and the target exists in the current
configuration, also run:

```bash
cmake --build --preset conan-release --target iouring_spill_test --parallel 15
```

Direct test binaries are expected at:

```text
_build/Release/bolt/exec/tests/bolt_exec_test
_build/Release/bolt/exec/tests/row_based_spill_test
_build/Release/bolt/exec/tests/iouring_spill_test
```

All stage acceptance commands should use release builds. If a local build uses a
different build type for investigation, the final acceptance run must still use
`Release` and `conan-release`.

Before a stage is considered ready to merge, run formatting and the relevant
direct test binaries listed in that stage:

```bash
SKIP=clang-tidy pre-commit run --all-files
```

If `pre-commit` or `iouring_spill_test` is unavailable in the environment, the
stage report must explicitly say so and include the successful narrower command
set that was run instead.

### Stage 0: Land Interfaces and Feature Flags

#### Scope

- Add query config flags.
- Add placeholder `SortedRun` interfaces.
- Add no-op disabled path.
- Keep legacy behavior by default.

#### Tests

- Unit test config parsing defaults.
- Unit test explicit config values.
- Build test to ensure new interfaces compile.
- Existing `OrderBy` tests must pass unchanged with flags disabled.

#### Acceptance Criteria

- No behavior change when all new flags are disabled.
- Existing spill and non-spill `OrderBy` tests pass.
- New config flags are visible through `QueryConfig`.
- The following commands pass:

```bash
cmake --build --preset conan-release --target bolt_exec_test --parallel 15
_build/Release/bolt/exec/tests/bolt_exec_test --gtest_filter='*QueryConfig*:*OrderBy*:*SortBuffer*'
```

### Stage 1: In-Memory `SortedRun` Generation

#### Scope

- Implement `InMemorySortedRun`.
- Convert parallel sort tasks to produce sorted runs instead of ordered
  containers.
- Keep final output merge serial initially.
- Support JIT and non-JIT comparators.

#### Tests

- Single-run in-memory sort.
- Multi-run in-memory sort.
- Multiple sort keys.
- ASC/DESC combinations.
- NULLS FIRST / NULLS LAST combinations.
- Duplicate-key heavy inputs.
- Empty input.
- Single-row input.
- Variable-width payload columns.
- Compare output against legacy `SortBuffer` and SQL reference runner.

#### Acceptance Criteria

- Output is byte-for-byte equivalent to legacy `SortBuffer` for deterministic
  test cases.
- For duplicate-key cases, output satisfies SQL sort order even if tie order
  differs from legacy.
- Parallel run generation produces the same row count as input.
- No spills occur in this stage.
- The following commands pass:

```bash
cmake --build --preset conan-release --target bolt_exec_test --parallel 15
_build/Release/bolt/exec/tests/bolt_exec_test --gtest_filter='*SortBuffer*:*OrderBy*:*SortAlgo*'
```

### Stage 2: Indexed Spill Metadata Plumbing

#### Scope

- Add `SpillBlockInfo`.
- Extend `SpillFileInfo` with block metadata.
- Add `SpillWriteFile::writeBlock()` API returning logical offset and bytes.
- Add indexed mode to `SpillWriter` where every flush records a block.
- Do not enable parallel merge yet.

#### Tests

- Unit test `writeBlock()` offset accounting with multiple writes.
- Unit test offsets under async spill if `io_uring` is available.
- Unit test block row-count accounting.
- Unit test `SpillFileInfo.blocks` survives `SpillWriter::finish()`.
- Existing spill tests must pass when indexed mode is disabled.

#### Acceptance Criteria

- Indexed metadata is correct for sync writes.
- Indexed metadata is correct for async writes or async mode is explicitly
  disabled for indexed spill until supported.
- Existing non-indexed spill behavior is unchanged.
- The following commands pass:

```bash
cmake --build --preset conan-release --target bolt_exec_test --parallel 15
cmake --build --preset conan-release --target row_based_spill_test --parallel 15
_build/Release/bolt/exec/tests/bolt_exec_test --gtest_filter='*SpillTest*:*SpillerTest*:*ConcatFilesSpillMergeStreamTest*:*MergerTest*:*SortBuffer*'
_build/Release/bolt/exec/tests/row_based_spill_test
```

- If `iouring_spill_test` is configured, the following additional commands pass:

```bash
cmake --build --preset conan-release --target iouring_spill_test --parallel 15
_build/Release/bolt/exec/tests/iouring_spill_test --gtest_filter='*Spill*:*Spiller*'
```

### Stage 3: Spilled `SortedRun` Writer

#### Scope

- Implement `SpilledSortedRun`.
- Add an API to spill already-sorted row pointers as a sorted run.
- Preserve sorting keys in `SpillFileInfo`.
- Do not sort again inside the spill writer.
- Allow sort tasks to decide memory vs spill after sorting.

#### Tests

- Spill a single sorted run and read it sequentially.
- Spill multiple sorted runs and read each sequentially.
- Verify `SpillFileInfo.sortingKeys` is populated.
- Verify block row counts sum to file/run row count.
- Verify compressed and uncompressed modes.
- Verify row-vector spill mode.
- Verify row-based spill mode if supported; otherwise explicitly reject with a
  clear fallback.

#### Acceptance Criteria

- `SortBufferMergePath` no longer falls back to legacy `SortBuffer` solely
  because a sorted run spills.
- Spilled sorted runs can be sequentially merged into correct output.
- Spill stats report spilled rows, bytes, files, serialization time, and write
  time.
- The following commands pass:

```bash
cmake --build --preset conan-release --target bolt_exec_test --parallel 15
cmake --build --preset conan-release --target row_based_spill_test --parallel 15
_build/Release/bolt/exec/tests/bolt_exec_test --gtest_filter='*OrderBy*:*SortBuffer*:*SpillTest*:*SpillerTest*:*MergerTest*:*ConcatFilesSpillMergeStreamTest*'
_build/Release/bolt/exec/tests/row_based_spill_test
```

### Stage 4: Indexed Random-Access Reader

#### Scope

- Implement `IndexedSpillReadFile`.
- Implement block lookup by row ordinal.
- Implement `SpilledSortedRun::rowAt(ordinal)`.
- Implement a small block cache.
- Implement `SpilledSortedRun::lowerBound(key)` using binary search.

#### Tests

- Read first, middle, and last block.
- Read first, middle, and last row by ordinal.
- Read rows crossing block boundaries.
- `lowerBound()` on unique keys.
- `lowerBound()` on duplicate-heavy keys using internal tie-breaker.
- `lowerBound()` for keys below minimum and above maximum.
- Mixed fixed-width and variable-width keys.
- Corrupt or missing block metadata fails clearly.

#### Acceptance Criteria

- Random row lookup returns the same row as sequential scan for all tested
  ordinals.
- `lowerBound()` matches a reference in-memory vector implementation.
- Block cache memory is bounded.
- Reader does not affect existing `SpillReadFile` behavior.
- The following commands pass:

```bash
cmake --build --preset conan-release --target bolt_exec_test --parallel 15
cmake --build --preset conan-release --target row_based_spill_test --parallel 15
_build/Release/bolt/exec/tests/bolt_exec_test --gtest_filter='*IndexedSpill*:*SpillTest*:*SpillerTest*:*SortBuffer*:*OrderBy*'
_build/Release/bolt/exec/tests/row_based_spill_test
```

### Stage 5: Boundary Planner

#### Scope

- Implement internal total-order row references.
- Implement boundary computation for output ordinals.
- Support mixed in-memory and spilled runs.
- Produce `MergeTask` slice definitions.

#### Tests

- Two-run boundary planning compared to classic two-array merge path.
- K-run boundary planning compared to full in-memory merged reference.
- Duplicate-key heavy inputs.
- All rows equal.
- Highly skewed run sizes.
- Empty runs among non-empty runs.
- Mixed memory/spilled runs.
- Boundary row-count conservation:
  - every input row belongs to exactly one task,
  - no gaps,
  - no overlaps.

#### Acceptance Criteria

- For every boundary, sum of run offsets equals the target output ordinal.
- Adjacent task slices exactly cover all input rows.
- Task output ranges are balanced by target row count except the last task.
- Boundary planner produces deterministic results for duplicate keys.
- The following commands pass:

```bash
cmake --build --preset conan-release --target bolt_exec_test --parallel 15
_build/Release/bolt/exec/tests/bolt_exec_test --gtest_filter='*MergePath*:*SortBuffer*:*OrderBy*:*TreeOfLosers*:*MergerTest*'
```

### Stage 6: One-Pass Parallel Slice Merge

#### Scope

- Implement `RunSliceReader` for in-memory and spilled runs.
- Implement `MergeTask` execution.
- Merge task slices using local loser tree or heap.
- Produce output batches directly.
- Add ordered output queue.

#### Tests

- Parallel merge with only memory runs.
- Parallel merge with only spilled runs.
- Parallel merge with mixed memory and spilled runs.
- Output batch sizes smaller than task size.
- `getOutput()` called repeatedly with varying `maxOutputRows`.
- Slow earlier task and fast later task still emit in correct order.
- Memory backpressure prevents unbounded completed outputs.
- Compare against legacy sequential merge and SQL reference runner.

#### Acceptance Criteria

- Final merge performs no intermediate spill writes.
- Each merge task reads only its assigned slices.
- Output is globally sorted.
- Output row count equals input row count.
- `getOutput()` preserves the existing operator contract.
- The following commands pass:

```bash
cmake --build --preset conan-release --target bolt_exec_test --parallel 15
cmake --build --preset conan-release --target row_based_spill_test --parallel 15
_build/Release/bolt/exec/tests/bolt_exec_test --gtest_filter='*MergePath*:*SortBuffer*:*OrderBy*:*SpillTest*:*SpillerTest*:*MergerTest*:*ConcatFilesSpillMergeStreamTest*'
_build/Release/bolt/exec/tests/row_based_spill_test
```

### Stage 7: Async Prefetch and I/O Scheduling

#### Scope

- Add bounded prefetch for `RunSliceReader`.
- Use executor or existing async file support for block reads.
- Overlap disk read, decompression, deserialization, and merge.

#### Tests

- Prefetch disabled/enabled correctness equivalence.
- Bounded memory under many runs and many tasks.
- `io_uring` enabled/disabled correctness if supported.
- Fault injection for read errors.
- Spill-read stats include read, I/O, and decompression time.

#### Acceptance Criteria

- Prefetch improves or does not materially regress benchmark performance.
- Memory remains bounded by configured limits.
- Read errors are propagated to `getOutput()` and fail the query clearly.
- The following commands pass:

```bash
cmake --build --preset conan-release --target bolt_exec_test --parallel 15
cmake --build --preset conan-release --target row_based_spill_test --parallel 15
_build/Release/bolt/exec/tests/bolt_exec_test --gtest_filter='*MergePath*:*SortBuffer*:*OrderBy*:*SpillTest*:*SpillerTest*:*Async*'
_build/Release/bolt/exec/tests/row_based_spill_test
```

- If `iouring_spill_test` is configured, the following additional commands pass:

```bash
cmake --build --preset conan-release --target iouring_spill_test --parallel 15
_build/Release/bolt/exec/tests/iouring_spill_test
```

### Stage 8: Metrics, Observability, and Rollout

#### Scope

- Add runtime metrics:
  - number of sorted runs,
  - number of spilled sorted runs,
  - indexed spill blocks,
  - boundary planning time,
  - random block reads during planning,
  - merge task count,
  - merge task wall time,
  - merge input read bytes,
  - output queue wait time.
- Add debug validation mode.
- Add benchmark coverage.

#### Tests

- Metrics are present when feature is enabled.
- Metrics are absent or zero when feature is disabled.
- Debug validation catches intentionally corrupted boundaries in unit tests.
- Benchmark tests for in-memory, spilling, duplicate-heavy, and wide-row cases.

#### Acceptance Criteria

- Metrics are sufficient to diagnose CPU, I/O, and planning bottlenecks.
- Feature can be rolled out by config.
- Legacy path remains available.
- The following commands pass:

```bash
cmake --build --preset conan-release --target bolt_exec_test --parallel 15
cmake --build --preset conan-release --target row_based_spill_test --parallel 15
_build/Release/bolt/exec/tests/bolt_exec_test --gtest_filter='*OrderBy*:*SortBuffer*:*MergePath*:*Spill*:*RuntimeMetric*'
_build/Release/bolt/exec/tests/row_based_spill_test
SKIP=clang-tidy pre-commit run --all-files
```

- If `iouring_spill_test` is configured, the following additional commands pass:

```bash
cmake --build --preset conan-release --target iouring_spill_test --parallel 15
_build/Release/bolt/exec/tests/iouring_spill_test
```

## Correctness Invariants

These invariants should be asserted in debug builds and tested directly.

1. Every `SortedRun` is internally sorted.
2. Every `SortedRun` reports an accurate row count.
3. For spilled runs, block row counts sum to file row counts.
4. For spilled runs, file row counts sum to run row count.
5. Boundary offsets are monotonic per run.
6. Boundary offset sums equal output ordinals.
7. Adjacent task slices do not overlap.
8. Adjacent task slices do not leave gaps.
9. All task slice row counts sum to total input rows.
10. Output row count equals input row count.
11. Output is globally sorted by SQL-visible sort keys.
12. Internal tie-breakers are used only for deterministic planning and do not
    appear in output.

## Performance Acceptance Criteria

Initial performance targets should be conservative and measurable.

### In-memory path

- New parallel path should not regress small in-memory sorts by more than a
  configured threshold when disabled by default.
- For large in-memory sorts, parallel run generation should improve wall time
  versus legacy single-container sort.

### Spilled path

- Spilled parallel path should avoid intermediate merge writes entirely.
- Total spilled data read during final merge should be close to the spilled data
  size plus small boundary-planning overhead.
- Boundary-planning random reads should be a small fraction of total spilled
  bytes.
- Large spilled `ORDER BY` should improve wall time versus legacy serial spill
  merge when enough CPU and I/O parallelism are available.

## Risks and Mitigations

### Risk: random block reads are too expensive during boundary planning

Mitigations:

- Use larger merge task sizes.
- Cache recently read blocks.
- Add optional first/last key metadata to blocks.
- Fall back to sequential merge if planning overhead exceeds threshold.

### Risk: duplicate keys make boundaries ambiguous

Mitigation:

- Always compare using `(ORDER BY key, run_id, ordinal_in_run)` internally.

### Risk: async write offsets are hard to track

Mitigation:

- Introduce a logical write cursor for indexed writes.
- Initially disable indexed sorted spill with async writes if necessary.

### Risk: memory blow-up from parallel output tasks

Mitigation:

- Use bounded task scheduling.
- Use bounded output queues.
- Emit strictly in order.

### Risk: integration destabilizes other spill users

Mitigation:

- Keep indexed sorted spill mode opt-in.
- Add `IndexedSpillReadFile` separately from `SpillReadFile`.
- Preserve existing `SpillWriter` behavior when indexed mode is disabled.

## Open Questions

1. Should indexed sorted spill initially support row-vector spill only, or also
   row-based spill?
2. What is the best default block size: rows, bytes, or both?
3. Should boundary planning use a full k-way merge-path algorithm immediately,
   or start with rank-search using per-run lower bounds?
4. Should final merge tasks output fully materialized `RowVector`s or expose a
   streaming producer interface consumed by `getOutput()`?
5. How should memory reservations be split between run generation, planning,
   merge input buffers, and output queues?

## Recommended First Milestone Sequence

The most practical sequence is:

1. Stage 0: interfaces and flags.
2. Stage 1: in-memory `SortedRun` generation.
3. Stage 2: indexed spill metadata plumbing.
4. Stage 3: spilled `SortedRun` writer with sequential merge fallback.
5. Stage 4: indexed random-access reader.
6. Stage 5: boundary planner.
7. Stage 6: one-pass parallel slice merge.

This sequence removes the current spill fallback before requiring the final
parallel merge implementation, but still keeps the final target aligned with
one-pass I/O.

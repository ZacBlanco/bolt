/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <deque>
#include <limits>
#include <optional>
#include <queue>
#include <vector>

#include "bolt/exec/SortedRun.h"
#include "bolt/exec/SpillFile.h"

namespace bytedance::bolt::exec {

struct MergeRunSlice {
  size_t runIndex{0};
  uint32_t runId{0};
  uint64_t begin{0};
  uint64_t end{0};

  uint64_t size() const {
    BOLT_CHECK_GE(end, begin);
    return end - begin;
  }
};

struct MergeTask {
  uint64_t outputBegin{0};
  uint64_t outputEnd{0};
  std::vector<MergeRunSlice> slices;

  uint64_t outputSize() const {
    BOLT_CHECK_GE(outputEnd, outputBegin);
    return outputEnd - outputBegin;
  }
};

/// Computes output-cardinality-balanced merge task slices over immutable sorted
/// runs. Ordering is the internal total order: (ORDER BY key, run id,
/// ordinal-in-run). This deterministic tie-breaker is important for duplicate
/// heavy inputs and all-equal runs.
class MergePathBoundaryPlanner {
 public:
  struct Stats {
    uint64_t computeBoundariesTimeUs{0};
    uint64_t buildTasksTimeUs{0};
    uint64_t totalRowsTimeUs{0};
    uint64_t boundaryIterations{0};
    uint64_t cursorComparisons{0};
    uint64_t rowReferenceLoads{0};
    uint64_t boundaries{0};
  };

  MergePathBoundaryPlanner(
      std::vector<SortedRun*> runs,
      memory::MemoryPool* pool);

  std::vector<MergeTask> plan(uint64_t targetRowsPerTask);

  const Stats& stats() const {
    return stats_;
  }

 private:
  struct Cursor {
    size_t runIndex{0};
    uint64_t ordinal{0};
  };

  struct RowReference {
    const InMemorySortedRun* memoryRun{nullptr};
    const char* memoryRow{nullptr};
    RowVectorPtr vectorBatch;
    vector_size_t vectorIndex{0};
  };

  std::vector<std::vector<uint64_t>> computeBoundaries(
      uint64_t targetRowsPerTask);
  bool cursorLess(const Cursor& left, const Cursor& right);
  int32_t compareRows(const Cursor& left, const Cursor& right);
  RowReference rowReference(const Cursor& cursor);
  int32_t compareRowReferences(
      const RowReference& left,
      const RowReference& right) const;
  VectorPtr extractMemoryKey(
      const InMemorySortedRun& run,
      const char* row,
      column_index_t channel) const;
  std::vector<SpillSortKey> sortingKeysForRun(SortedRun* run) const;
  const std::vector<SpillSortKey>& sortingKeys() const;
  uint64_t totalRows() const;

  std::vector<SortedRun*> runs_;
  memory::MemoryPool* const pool_;
  std::vector<SpillSortKey> sortingKeys_;
  Stats stats_;
};

/// Reads a bounded half-open slice [begin, end) from a SortedRun. For spilled
/// runs, rows are fetched through indexed spill-block metadata, so merge tasks
/// only touch blocks that contain their assigned slice.
class RunSliceReader {
 public:
  struct RowReference {
    const InMemorySortedRun* memoryRun{nullptr};
    const char* memoryRow{nullptr};
    RowVectorPtr vectorBatch;
    vector_size_t vectorIndex{0};
    uint32_t runId{0};
    uint64_t ordinal{0};
  };

  RunSliceReader(SortedRun* run, MergeRunSlice slice);

  bool hasNext() const;
  uint64_t currentOrdinal() const;
  uint64_t rowsRead() const;
  const MergeRunSlice& slice() const;
  const RowReference& current() const;
  void copyCurrentRowTo(const RowVectorPtr& output, vector_size_t outputRow)
      const;
  void advance();

 private:
  SortedRun* const run_;
  const MergeRunSlice slice_;
  uint64_t ordinal_;
  mutable std::optional<RowReference> current_;
  mutable std::vector<std::unique_ptr<IndexedSpillReadFile>> indexedReaders_;
};

/// Executes a single merge-path task and emits output batches directly. The
/// executor performs a local k-way merge over RunSliceReaders and never writes
/// intermediate merge output back to spill.
class MergeTaskExecutor {
 public:
  MergeTaskExecutor(
      std::vector<SortedRun*> runs,
      MergeTask task,
      RowTypePtr outputType,
      memory::MemoryPool* pool,
      RowRowCompare rowCompare = nullptr);

  RowVectorPtr getOutput(vector_size_t maxOutputRows);
  uint64_t numOutputRows() const;
  bool finished() const;

 private:
  int32_t compareRows(
      const RunSliceReader::RowReference& left,
      const RunSliceReader::RowReference& right) const;
  bool rowLess(
      const RunSliceReader::RowReference& left,
      const RunSliceReader::RowReference& right) const;
  VectorPtr extractMemoryKey(
      const InMemorySortedRun& run,
      const char* row,
      column_index_t channel) const;
  std::vector<SpillSortKey> sortingKeysForRun(SortedRun* run) const;
  void initializeHeap();
  RowVectorPtr makeOutput(vector_size_t outputBatchSize) const;

  struct ReaderGreater {
    explicit ReaderGreater(const MergeTaskExecutor* executor)
        : executor(executor) {}

    bool operator()(size_t left, size_t right) const;

    const MergeTaskExecutor* executor;
  };

  const std::vector<SortedRun*> runs_;
  const MergeTask task_;
  const RowTypePtr outputType_;
  memory::MemoryPool* const pool_;
  const std::vector<SpillSortKey> sortingKeys_;
#ifdef ENABLE_BOLT_JIT
  RowRowCompare rowCompare_{nullptr};
#endif
  std::vector<RunSliceReader> readers_;
  std::priority_queue<size_t, std::vector<size_t>, ReaderGreater> heap_;
  uint64_t numOutputRows_{0};
  bool heapInitialized_{false};
};

/// Buffers completed merge-task output and exposes it in task order. Later
/// tasks may finish before earlier tasks, but getOutput() only returns rows
/// from the lowest not-yet-consumed task, preserving the global ORDER BY
/// sequence.
class OrderedMergeTaskOutputQueue {
 public:
  OrderedMergeTaskOutputQueue(
      size_t numTasks,
      RowTypePtr outputType,
      memory::MemoryPool* pool,
      uint64_t maxBufferedRows = std::numeric_limits<uint64_t>::max());

  bool canEnqueue(vector_size_t rows) const;
  bool canEnqueue(size_t taskIndex, vector_size_t rows) const;
  void enqueue(size_t taskIndex, RowVectorPtr output);
  void finishTask(size_t taskIndex);
  RowVectorPtr getOutput(vector_size_t maxOutputRows);
  uint64_t numOutputRows() const;
  uint64_t bufferedRows() const;
  bool finished() const;

 private:
  struct TaskOutput {
    std::deque<RowVectorPtr> batches;
    bool finished{false};
  };

  RowVectorPtr makeOutput(vector_size_t outputBatchSize) const;
  void advanceCompletedTasks();

  const RowTypePtr outputType_;
  memory::MemoryPool* const pool_;
  const uint64_t maxBufferedRows_;
  std::vector<TaskOutput> tasks_;
  size_t nextTaskToEmit_{0};
  vector_size_t currentBatchOffset_{0};
  uint64_t numOutputRows_{0};
  uint64_t bufferedRows_{0};
};

} // namespace bytedance::bolt::exec

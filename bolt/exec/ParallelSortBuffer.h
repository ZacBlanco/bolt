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

#include <atomic>
#include <queue>

#include <folly/Executor.h>
#include <folly/Synchronized.h>
#include <folly/futures/Future.h>

#include "bolt/common/base/SortStat.h"
#include "bolt/common/base/SpillConfig.h"
#include "bolt/exec/ISortBuffer.h"
#include "bolt/exec/MergePath.h"
#include "bolt/exec/Operator.h"
#include "bolt/exec/SortedRun.h"
#include "bolt/exec/SpilledSortedRun.h"
#include "bolt/vector/BaseVector.h"

namespace bytedance::bolt::exec {

/// SortBuffer implementation for the new sorted-run path. Inputs are
/// materialized as independent SortedRuns and output can be produced using
/// merge-path planned parallel merge tasks.
class ParallelSortBuffer : public ISortBuffer {
 public:
  ParallelSortBuffer(
      const RowTypePtr& input,
      const std::vector<column_index_t>& sortColumnIndices,
      const std::vector<CompareFlags>& sortCompareFlags,
      memory::MemoryPool* pool,
      uint64_t parallelMergeTargetRows = 0,
      uint64_t parallelMergeLookaheadTasks = 0,
      folly::Executor* mergeExecutor = nullptr,
      tsan_atomic<bool>* nonReclaimableSection = nullptr,
      const common::SpillConfig* spillConfig = nullptr,
      uint64_t spillMemoryThreshold = 0,
      uint64_t parallelMergeConcurrency = 0,
      bool enableJit = false);

  ~ParallelSortBuffer() override;

  void addInput(const VectorPtr& input) override;
  void noMoreInput() override;
  RowVectorPtr getOutput(vector_size_t maxOutputRows) override;

  void reclaim(uint64_t /*targetBytes*/) override;

  std::optional<uint64_t> estimateOutputRowSize() const override {
    return estimatedOutputRowSize_;
  }

  size_t numInputRows() const override {
    return numInputRows_;
  }

  size_t numOutputRows() const override {
    return numOutputRows_;
  }

  std::optional<common::SpillStats> spilledStats() const override;

  std::optional<common::SpillReadStats> spillReadStats() const override;

  std::optional<common::SortStats> sortStats() const override {
    common::SortStats sortStats;
    sortStats.sortColToRowTimeUs = sortColToRowTimeUs_;
    sortStats.sortInSortTimeUs = sortInSortTimeUs_;
    sortStats.sortOutputTimeUs = sortOutputTimeUs_;
    return sortStats;
  }

  size_t numRuns() const {
    return runs_.size();
  }

  struct DebugStats {
    uint64_t inputRunTargetBytes{0};
    uint64_t inputRunsCreated{0};
    uint64_t inputRunsScheduledSync{0};
    uint64_t inputRunsScheduledAsync{0};
    uint64_t maxPendingInputRuns{0};
    uint64_t maxRunningInputRuns{0};
    uint64_t inputRunCollects{0};
    uint64_t inputRunWaitTimeUs{0};
    uint64_t spillRunsScheduled{0};
    uint64_t maxRunningSpillRuns{0};
    uint64_t mergeTargetRows{0};
    uint64_t mergeTasks{0};
    uint64_t mergeTaskLookahead{0};
    uint64_t mergeBatchesScheduled{0};
    uint64_t mergeBatchesCompleted{0};
    uint64_t maxRunningMergeBatches{0};
    uint64_t maxActiveMergeTasks{0};
    uint64_t maxBufferedOutputRows{0};
    uint64_t estimatedInputRowBytes{0};
    uint64_t mergePlanningTimeUs{0};
    uint64_t mergeBoundaryPlanningTimeUs{0};
    uint64_t mergeTaskBuildTimeUs{0};
    uint64_t mergePlanningTotalRowsTimeUs{0};
    uint64_t mergePlanningBoundaryIterations{0};
    uint64_t mergePlanningCursorComparisons{0};
    uint64_t mergePlanningRowReferenceLoads{0};
    uint64_t mergePlanningBoundaries{0};
    uint64_t mergeExecutionTimeUs{0};
    uint64_t mergeWaitTimeUs{0};
    uint64_t mergeOutputQueueTimeUs{0};
    uint64_t outputProjectionTimeUs{0};
  };

  DebugStats debugStats() const;

 private:
  struct RunCursor {
    size_t runIndex;
    uint64_t ordinal;
  };

  struct PendingInMemoryRun {
    std::unique_ptr<InMemorySortedRun> run;
    size_t numInputRows{0};
    std::optional<uint64_t> estimatedOutputRowSize;
    uint64_t sortColToRowTimeUs{0};
    uint64_t sortInSortTimeUs{0};
  };

  struct SpilledRunResult {
    size_t runIndex{0};
    std::unique_ptr<SpilledSortedRun> run;
  };

  struct StreamingMergeTask {
    size_t taskIndex{0};
    std::unique_ptr<MergeTaskExecutor> executor;
    std::optional<folly::Future<RowVectorPtr>> future;
    RowVectorPtr pendingOutput;
    bool finished{false};
  };

  std::unique_ptr<RowContainer> makeContainer() const;
  void updateEstimatedOutputRowSize(const RowContainer& container);
  void updateEstimatedOutputRowSize(std::optional<uint64_t> rowSize);
  void updateEstimatedInputRowSize(const VectorPtr& input);
  uint64_t estimatedInputRowSize() const;
  bool asyncInputRunCreationAllowed() const;
  bool cursorLess(const RunCursor& left, const RunCursor& right) const;
  void initializeMerge();
  void initializeParallelMerge();
  void ensureInputFits(const VectorPtr& input);
  PendingInMemoryRun createRun(uint32_t runId, std::vector<VectorPtr> inputs)
      const;
  uint64_t executorConcurrency() const;
  uint64_t baseInputRunTargetBytes() const;
  uint64_t inputRunTargetBytes() const;
  uint64_t parallelMergeTargetRows() const;
  uint64_t maxPendingRuns() const;
  void scheduleInputRun(std::vector<VectorPtr> inputs);
  void spillAllInputToDisk();
  void flushBufferedInputs(
      bool finalFlush = false,
      bool splitFinalFlushForParallelism = false);
  std::optional<SpilledRunResult> spillRun(size_t runIndex);
  void collectPendingRuns();
  void spillAllRuns();
  bool hasSpilledRuns() const;
  void initializeMergeJit();
  uint64_t estimatedMergeTaskBytes() const;
  uint64_t mergeTaskLookahead() const;
  vector_size_t mergeOutputBatchRows() const;
  std::unique_ptr<MergeTaskExecutor> createMergeTaskExecutor(
      const MergeTask& task) const;
  void scheduleMergeTaskBatch(StreamingMergeTask& task);
  void finishMergeTask(StreamingMergeTask& task);
  bool tryEnqueueMergeTaskOutput(StreamingMergeTask& task);
  void scheduleMergeTasks();
  void drainReadyMergeTasks();
  void drainNextMergeTask();
  void prepareOutput(vector_size_t outputBatchSize);
  RowVectorPtr getSerialOutput(vector_size_t maxOutputRows);
  RowVectorPtr getParallelOutput(vector_size_t maxOutputRows);
  RowVectorPtr projectInternalOutput(const RowVectorPtr& internalOutput);
  static void updateMax(std::atomic<uint64_t>& target, uint64_t value);

  const RowTypePtr input_;
  const std::vector<column_index_t> sortColumnIndices_;
  const std::vector<CompareFlags> sortCompareFlags_;
  memory::MemoryPool* const pool_;
  const uint64_t parallelMergeTargetRows_;
  const uint64_t parallelMergeLookaheadTasks_;
  folly::Executor* const mergeExecutor_;
  tsan_atomic<bool>* const nonReclaimableSection_;
  const common::SpillConfig* const spillConfig_;
  const uint64_t spillMemoryThreshold_;
  const uint64_t parallelMergeConcurrency_;
  const bool enableJit_;

  std::vector<TypePtr> sortedColumnTypes_;
  std::vector<TypePtr> nonSortedColumnTypes_;
  RowTypePtr internalType_;
  std::vector<column_index_t> internalChannels_;
  std::vector<IdentityProjection> columnMap_;
  bool outputProjectionIdentity_{true};

  std::vector<VectorPtr> bufferedInputs_;
  uint64_t bufferedInputBytes_{0};
  uint64_t bufferedInputRows_{0};
  std::vector<std::unique_ptr<SortedRun>> runs_;
  std::vector<folly::Future<PendingInMemoryRun>> pendingRuns_;
  folly::Synchronized<common::SpillStats> spillStats_;
  bool noMoreInput_{false};
  bool mergeInitialized_{false};
  size_t numInputRows_{0};
  uint32_t nextRunId_{0};
  size_t numOutputRows_{0};
  std::optional<uint64_t> estimatedOutputRowSize_;
  uint64_t estimatedInputBytes_{0};
  uint64_t estimatedInputRows_{0};
  RowVectorPtr output_;
  bool parallelMergeInitialized_{false};
  std::vector<MergeTask> mergeTasks_;
  std::vector<StreamingMergeTask> streamingMergeTasks_;
  std::unique_ptr<OrderedMergeTaskOutputQueue> mergeOutputQueue_;
  size_t nextMergeTaskToSchedule_{0};
#ifdef ENABLE_BOLT_JIT
  RowRowCompare mergeRowCompare_{nullptr};
  bolt::jit::CompiledModuleSP mergeJitModule_;
#endif

  struct CursorGreater {
    explicit CursorGreater(const ParallelSortBuffer* buffer) : buffer(buffer) {}

    bool operator()(const RunCursor& left, const RunCursor& right) const {
      return buffer->cursorLess(right, left);
    }

    const ParallelSortBuffer* buffer;
  };

  std::priority_queue<RunCursor, std::vector<RunCursor>, CursorGreater> heap_{
      CursorGreater{this}};

  uint64_t sortColToRowTimeUs_{0};
  uint64_t sortInSortTimeUs_{0};
  uint64_t sortOutputTimeUs_{0};

  mutable std::atomic<uint64_t> inputRunsCreated_{0};
  mutable std::atomic<uint64_t> inputRunsScheduledSync_{0};
  mutable std::atomic<uint64_t> inputRunsScheduledAsync_{0};
  mutable std::atomic<uint64_t> runningInputRuns_{0};
  mutable std::atomic<uint64_t> maxRunningInputRuns_{0};
  mutable std::atomic<uint64_t> maxPendingInputRuns_{0};
  mutable std::atomic<uint64_t> inputRunCollects_{0};
  mutable std::atomic<uint64_t> inputRunWaitTimeUs_{0};
  mutable std::atomic<uint64_t> spillRunsScheduled_{0};
  mutable std::atomic<uint64_t> runningSpillRuns_{0};
  mutable std::atomic<uint64_t> maxRunningSpillRuns_{0};
  mutable std::atomic<uint64_t> mergeBatchesScheduled_{0};
  mutable std::atomic<uint64_t> mergeBatchesCompleted_{0};
  mutable std::atomic<uint64_t> runningMergeBatches_{0};
  mutable std::atomic<uint64_t> maxRunningMergeBatches_{0};
  mutable std::atomic<uint64_t> maxActiveMergeTasks_{0};
  mutable std::atomic<uint64_t> maxBufferedOutputRows_{0};
  mutable std::atomic<uint64_t> mergePlanningTimeUs_{0};
  mutable std::atomic<uint64_t> mergeBoundaryPlanningTimeUs_{0};
  mutable std::atomic<uint64_t> mergeTaskBuildTimeUs_{0};
  mutable std::atomic<uint64_t> mergePlanningTotalRowsTimeUs_{0};
  mutable std::atomic<uint64_t> mergePlanningBoundaryIterations_{0};
  mutable std::atomic<uint64_t> mergePlanningCursorComparisons_{0};
  mutable std::atomic<uint64_t> mergePlanningRowReferenceLoads_{0};
  mutable std::atomic<uint64_t> mergePlanningBoundaries_{0};
  mutable std::atomic<uint64_t> mergeExecutionTimeUs_{0};
  mutable std::atomic<uint64_t> mergeWaitTimeUs_{0};
  mutable std::atomic<uint64_t> mergeOutputQueueTimeUs_{0};
  mutable std::atomic<uint64_t> outputProjectionTimeUs_{0};
};

} // namespace bytedance::bolt::exec

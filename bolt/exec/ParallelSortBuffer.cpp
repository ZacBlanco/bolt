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

#include "bolt/exec/ParallelSortBuffer.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <numeric>
#include <thread>
#include <unordered_set>

#include <folly/ScopeGuard.h>
#include <folly/futures/Future.h>

#include "bolt/common/time/Timer.h"
#include "bolt/exec/OperatorUtils.h"
#include "bolt/exec/Spill.h"

namespace bytedance::bolt::exec {
namespace {

constexpr uint64_t kDefaultEstimatedRowBytes = 1UL << 10;
constexpr uint64_t kMinMergeTaskMemoryBytes = 8UL << 20;
constexpr uint64_t kMaxAutoMergeLookaheadTasks = 16;
constexpr uint64_t kMinInputRunBytes = 32UL << 20;
constexpr uint64_t kIndexedSpillBlockRows = 2'048;
constexpr vector_size_t kMergeOutputBatchRows = 64UL << 10;
constexpr uint64_t kInMemoryMergeOutputBatchBytes = 32UL << 20;
constexpr uint64_t kInputRunGrowthRunsPerThread = 4;
constexpr uint64_t kTargetMergeTasksPerThread = 8;
constexpr uint64_t kMaxAutoMergeTargetRows = 4 * kMergeOutputBatchRows;

} // namespace

ParallelSortBuffer::ParallelSortBuffer(
    const RowTypePtr& input,
    const std::vector<column_index_t>& sortColumnIndices,
    const std::vector<CompareFlags>& sortCompareFlags,
    memory::MemoryPool* pool,
    uint64_t parallelMergeTargetRows,
    uint64_t parallelMergeLookaheadTasks,
    folly::Executor* mergeExecutor,
    tsan_atomic<bool>* nonReclaimableSection,
    const common::SpillConfig* spillConfig,
    uint64_t spillMemoryThreshold,
    uint64_t parallelMergeConcurrency,
    bool enableJit)
    : input_(input),
      sortColumnIndices_(sortColumnIndices),
      sortCompareFlags_(sortCompareFlags),
      pool_(pool),
      parallelMergeTargetRows_(parallelMergeTargetRows),
      parallelMergeLookaheadTasks_(parallelMergeLookaheadTasks),
      mergeExecutor_(mergeExecutor),
      nonReclaimableSection_(nonReclaimableSection),
      spillConfig_(spillConfig),
      spillMemoryThreshold_(spillMemoryThreshold),
      parallelMergeConcurrency_(parallelMergeConcurrency),
      enableJit_(enableJit) {
  BOLT_CHECK_GE(input_->size(), sortCompareFlags_.size());
  BOLT_CHECK_GT(sortCompareFlags_.size(), 0);
  BOLT_CHECK_EQ(sortColumnIndices_.size(), sortCompareFlags_.size());

  sortedColumnTypes_.reserve(sortColumnIndices_.size());
  nonSortedColumnTypes_.reserve(input_->size() - sortColumnIndices_.size());
  std::vector<std::string> internalNames;
  std::vector<TypePtr> internalTypes;
  internalNames.reserve(input_->size());
  internalTypes.reserve(input_->size());
  internalChannels_.reserve(input_->size());

  std::unordered_set<column_index_t> sortedChannelSet;
  for (column_index_t i = 0; i < sortColumnIndices_.size(); ++i) {
    const auto inputChannel = sortColumnIndices_.at(i);
    sortedChannelSet.emplace(inputChannel);
    sortedColumnTypes_.emplace_back(input_->childAt(inputChannel));
    internalChannels_.push_back(inputChannel);
    internalNames.emplace_back(input_->nameOf(inputChannel));
    internalTypes.emplace_back(input_->childAt(inputChannel));
    outputProjectionIdentity_ &= i == inputChannel;
    columnMap_.emplace_back(i, inputChannel);
  }

  for (column_index_t i = 0, internalChannel = sortCompareFlags_.size();
       i < input_->size();
       ++i) {
    if (sortedChannelSet.count(i) != 0) {
      continue;
    }
    nonSortedColumnTypes_.emplace_back(input_->childAt(i));
    internalChannels_.push_back(i);
    internalNames.emplace_back(input_->nameOf(i));
    internalTypes.emplace_back(input_->childAt(i));
    outputProjectionIdentity_ &= internalChannel == i;
    columnMap_.emplace_back(internalChannel++, i);
  }

  internalType_ = ROW(std::move(internalNames), std::move(internalTypes));
}

ParallelSortBuffer::~ParallelSortBuffer() = default;

void ParallelSortBuffer::updateMax(
    std::atomic<uint64_t>& target,
    uint64_t value) {
  auto current = target.load(std::memory_order_relaxed);
  while (current < value &&
         !target.compare_exchange_weak(
             current,
             value,
             std::memory_order_relaxed,
             std::memory_order_relaxed)) {
  }
}

ParallelSortBuffer::DebugStats ParallelSortBuffer::debugStats() const {
  DebugStats stats;
  stats.inputRunTargetBytes = inputRunTargetBytes();
  stats.inputRunsCreated = inputRunsCreated_.load(std::memory_order_relaxed);
  stats.inputRunsScheduledSync =
      inputRunsScheduledSync_.load(std::memory_order_relaxed);
  stats.inputRunsScheduledAsync =
      inputRunsScheduledAsync_.load(std::memory_order_relaxed);
  stats.maxPendingInputRuns =
      maxPendingInputRuns_.load(std::memory_order_relaxed);
  stats.maxRunningInputRuns =
      maxRunningInputRuns_.load(std::memory_order_relaxed);
  stats.inputRunCollects = inputRunCollects_.load(std::memory_order_relaxed);
  stats.inputRunWaitTimeUs =
      inputRunWaitTimeUs_.load(std::memory_order_relaxed);
  stats.spillRunsScheduled =
      spillRunsScheduled_.load(std::memory_order_relaxed);
  stats.maxRunningSpillRuns =
      maxRunningSpillRuns_.load(std::memory_order_relaxed);
  stats.mergeTargetRows =
      parallelMergeInitialized_ ? parallelMergeTargetRows() : 0;
  stats.mergeTasks = mergeTasks_.size();
  stats.mergeTaskLookahead =
      parallelMergeInitialized_ ? mergeTaskLookahead() : 0;
  stats.mergeBatchesScheduled =
      mergeBatchesScheduled_.load(std::memory_order_relaxed);
  stats.mergeBatchesCompleted =
      mergeBatchesCompleted_.load(std::memory_order_relaxed);
  stats.maxRunningMergeBatches =
      maxRunningMergeBatches_.load(std::memory_order_relaxed);
  stats.maxActiveMergeTasks =
      maxActiveMergeTasks_.load(std::memory_order_relaxed);
  stats.maxBufferedOutputRows =
      maxBufferedOutputRows_.load(std::memory_order_relaxed);
  stats.estimatedInputRowBytes = estimatedInputRowSize();
  stats.mergePlanningTimeUs =
      mergePlanningTimeUs_.load(std::memory_order_relaxed);
  stats.mergeBoundaryPlanningTimeUs =
      mergeBoundaryPlanningTimeUs_.load(std::memory_order_relaxed);
  stats.mergeTaskBuildTimeUs =
      mergeTaskBuildTimeUs_.load(std::memory_order_relaxed);
  stats.mergePlanningTotalRowsTimeUs =
      mergePlanningTotalRowsTimeUs_.load(std::memory_order_relaxed);
  stats.mergePlanningBoundaryIterations =
      mergePlanningBoundaryIterations_.load(std::memory_order_relaxed);
  stats.mergePlanningCursorComparisons =
      mergePlanningCursorComparisons_.load(std::memory_order_relaxed);
  stats.mergePlanningRowReferenceLoads =
      mergePlanningRowReferenceLoads_.load(std::memory_order_relaxed);
  stats.mergePlanningBoundaries =
      mergePlanningBoundaries_.load(std::memory_order_relaxed);
  stats.mergeExecutionTimeUs =
      mergeExecutionTimeUs_.load(std::memory_order_relaxed);
  stats.mergeWaitTimeUs = mergeWaitTimeUs_.load(std::memory_order_relaxed);
  stats.mergeOutputQueueTimeUs =
      mergeOutputQueueTimeUs_.load(std::memory_order_relaxed);
  stats.outputProjectionTimeUs =
      outputProjectionTimeUs_.load(std::memory_order_relaxed);
  return stats;
}

std::optional<common::SpillStats> ParallelSortBuffer::spilledStats() const {
  auto stats = spillStats_.rlock();
  if (stats->empty()) {
    return std::nullopt;
  }
  return *stats;
}

std::optional<common::SpillReadStats> ParallelSortBuffer::spillReadStats()
    const {
  if (!hasSpilledRuns()) {
    return std::nullopt;
  }

  common::SpillReadStats stats;
  for (const auto& run : runs_) {
    if (auto* spilledRun = dynamic_cast<SpilledSortedRun*>(run.get())) {
      const auto runStats = spilledRun->spillReadStats();
      stats.spillReadTimeUs += runStats.spillReadTimeUs;
      stats.spillDecompressTimeUs += runStats.spillDecompressTimeUs;
      stats.spillReadIOTimeUs += runStats.spillReadIOTimeUs;
    }
  }
  return stats;
}

std::unique_ptr<RowContainer> ParallelSortBuffer::makeContainer() const {
  return std::make_unique<RowContainer>(
      sortedColumnTypes_,
      nonSortedColumnTypes_,
      true /*useListRowIndex*/,
      pool_);
}

void ParallelSortBuffer::updateEstimatedOutputRowSize(
    const RowContainer& container) {
  updateEstimatedOutputRowSize(container.estimateRowSize());
}

void ParallelSortBuffer::updateEstimatedOutputRowSize(
    std::optional<uint64_t> rowSize) {
  if (!rowSize.has_value() || rowSize.value() == 0) {
    return;
  }

  const auto value = rowSize.value();
  if (!estimatedOutputRowSize_.has_value()) {
    estimatedOutputRowSize_ = value;
  } else if (value > estimatedOutputRowSize_.value()) {
    estimatedOutputRowSize_ = value;
  }
}

void ParallelSortBuffer::updateEstimatedInputRowSize(const VectorPtr& input) {
  BOLT_CHECK_NOT_NULL(input);
  if (input->size() == 0) {
    return;
  }
  estimatedInputBytes_ += input->estimateFlatSize();
  estimatedInputRows_ += input->size();
}

uint64_t ParallelSortBuffer::estimatedInputRowSize() const {
  if (estimatedInputRows_ != 0) {
    const auto rowBytes =
        (estimatedInputBytes_ + estimatedInputRows_ - 1) / estimatedInputRows_;
    if (rowBytes != 0) {
      return rowBytes;
    }
  }
  return kDefaultEstimatedRowBytes;
}

bool ParallelSortBuffer::asyncInputRunCreationAllowed() const {
  if (mergeExecutor_ == nullptr) {
    return false;
  }
  // Async RowContainer materialization allocates from the ORDER BY query memory
  // pool on executor threads which are not task drivers. If spilling is enabled,
  // these allocations can trigger arbitration and task-level reclaim while the
  // driver is waiting for pending run futures, making the task unable to pause.
  // Keep spill-capable buffers synchronous so allocations happen under the
  // driver/non-reclaimable section and reclaim can force-spill deterministically.
  return spillConfig_ == nullptr;
}

ParallelSortBuffer::PendingInMemoryRun ParallelSortBuffer::createRun(
    uint32_t runId,
    std::vector<VectorPtr> inputs) const {
  BOLT_CHECK(!inputs.empty());
  inputRunsCreated_.fetch_add(1, std::memory_order_relaxed);
  const auto runningInputRuns =
      runningInputRuns_.fetch_add(1, std::memory_order_relaxed) + 1;
  updateMax(maxRunningInputRuns_, runningInputRuns);
  auto runningGuard = folly::makeGuard(
      [this]() { runningInputRuns_.fetch_sub(1, std::memory_order_relaxed); });
  PendingInMemoryRun result;
  result.numInputRows = std::accumulate(
      inputs.begin(),
      inputs.end(),
      size_t{0},
      [](auto total, const auto& input) { return total + input->size(); });

  auto container = makeContainer();
  {
    MicrosecondTimer materializeTimer(&result.sortColToRowTimeUs);
    for (const auto& input : inputs) {
      const auto* inputRow = input->as<RowVector>();
      auto internalInput =
          wrapColumns(inputRow, internalChannels_, internalType_, pool_);
      container->store(internalInput);
    }
    result.estimatedOutputRowSize = container->estimateRowSize();
  }

  {
    MicrosecondTimer sortTimer(&result.sortInSortTimeUs);
    result.run = InMemorySortedRun::createSorted(
        runId,
        std::move(container),
        sortCompareFlags_,
        HybridSorter{},
        enableJit_);
  }

  return result;
}

uint64_t ParallelSortBuffer::executorConcurrency() const {
  if (parallelMergeConcurrency_ != 0) {
    return parallelMergeConcurrency_;
  }
  if (parallelMergeLookaheadTasks_ != 0) {
    return parallelMergeLookaheadTasks_;
  }
  return std::max<uint64_t>(1, std::thread::hardware_concurrency());
}

uint64_t ParallelSortBuffer::baseInputRunTargetBytes() const {
  if (spillMemoryThreshold_ == 0) {
    return std::max<uint64_t>(kMinInputRunBytes, 128UL << 20);
  }
  return std::max<uint64_t>(
      kMinInputRunBytes, spillMemoryThreshold_ / executorConcurrency());
}

uint64_t ParallelSortBuffer::inputRunTargetBytes() const {
  const auto baseTarget = baseInputRunTargetBytes();
  if (spillMemoryThreshold_ == 0) {
    return baseTarget;
  }

  // Start with one run per executor thread to maximize early input sort
  // parallelism, then grow run size as fan-in accumulates. This preserves the
  // small/medium input wins while avoiding an unbounded number of final spill
  // runs for very large inputs. Cap at 50% of the ORDER BY spill memory so we
  // can still keep multiple run materializations active under memory pressure.
  const auto maxTarget = std::max<uint64_t>(baseTarget, spillMemoryThreshold_ / 2);
  const auto growthInterval = std::max<uint64_t>(
      1, executorConcurrency() * kInputRunGrowthRunsPerThread);
  const auto growthSteps =
      inputRunsCreated_.load(std::memory_order_relaxed) / growthInterval;
  const auto growthNumerator = std::min<uint64_t>(8, 4 + growthSteps);
  const auto grownTarget = baseTarget > std::numeric_limits<uint64_t>::max() /
          growthNumerator
      ? std::numeric_limits<uint64_t>::max()
      : baseTarget * growthNumerator;
  return std::min(maxTarget, grownTarget / 4);
}

uint64_t ParallelSortBuffer::parallelMergeTargetRows() const {
  if (parallelMergeTargetRows_ != 0) {
    return parallelMergeTargetRows_;
  }
  if (spillMemoryThreshold_ == 0) {
    return 64UL << 10;
  }
  const auto rowBytes = estimatedInputRowSize();
  const auto taskBudgetBytes = static_cast<uint64_t>(std::floor(
      (static_cast<long double>(spillMemoryThreshold_) * 0.9L) /
      static_cast<long double>(executorConcurrency())));
  uint64_t spillCacheBytes = 0;
  if (hasSpilledRuns()) {
    const auto blockBytes =
        rowBytes > std::numeric_limits<uint64_t>::max() / kIndexedSpillBlockRows
        ? std::numeric_limits<uint64_t>::max()
        : rowBytes * kIndexedSpillBlockRows;
    spillCacheBytes = blockBytes > std::numeric_limits<uint64_t>::max() /
                std::max<size_t>(1, runs_.size())
        ? std::numeric_limits<uint64_t>::max()
        : blockBytes * runs_.size();
  }
  if (taskBudgetBytes <= spillCacheBytes) {
    return 1;
  }
  const auto outputBudgetBytes = taskBudgetBytes - spillCacheBytes;
  const auto bytesPerBufferedOutputRow =
      rowBytes > std::numeric_limits<uint64_t>::max() / 2
      ? std::numeric_limits<uint64_t>::max()
      : rowBytes * 2;
  BOLT_CHECK_GT(bytesPerBufferedOutputRow, 0);
  const auto memoryBudgetRows =
      std::max<uint64_t>(1, outputBudgetBytes / bytesPerBufferedOutputRow);

  // Keep roughly a fixed amount of merge work per executor thread. Large
  // spilled inputs otherwise create hundreds of tiny merge-path tasks, making
  // boundary planning dominate. The cap keeps enough tasks for load balance.
  const auto targetTasks = std::max<uint64_t>(
      1, executorConcurrency() * kTargetMergeTasksPerThread);
  const auto rowsByOutputSize = numInputRows_ == 0
      ? uint64_t{kMergeOutputBatchRows}
      : (numInputRows_ + targetTasks - 1) / targetTasks;
  const auto targetRows = std::min<uint64_t>(
      kMaxAutoMergeTargetRows,
      std::max<uint64_t>(kMergeOutputBatchRows, rowsByOutputSize));
  return std::min(memoryBudgetRows, targetRows);
}

uint64_t ParallelSortBuffer::maxPendingRuns() const {
  return executorConcurrency();
}

void ParallelSortBuffer::scheduleInputRun(std::vector<VectorPtr> inputs) {
  BOLT_CHECK(!inputs.empty());
  if (pendingRuns_.size() >= maxPendingRuns()) {
    collectPendingRuns();
  }

  const auto runId = nextRunId_++;
  auto createRun = [this, runId, inputs = std::move(inputs)]() mutable {
    return this->createRun(runId, std::move(inputs));
  };
  if (asyncInputRunCreationAllowed()) {
    inputRunsScheduledAsync_.fetch_add(1, std::memory_order_relaxed);
    pendingRuns_.push_back(folly::via(mergeExecutor_, std::move(createRun)));
  } else {
    inputRunsScheduledSync_.fetch_add(1, std::memory_order_relaxed);
    std::optional<memory::ReclaimableSectionGuard> guard;
    if (nonReclaimableSection_ != nullptr) {
      guard.emplace(nonReclaimableSection_);
    }
    pendingRuns_.push_back(folly::makeFuture(createRun()));
  }
  updateMax(maxPendingInputRuns_, pendingRuns_.size());
}

void ParallelSortBuffer::flushBufferedInputs(
    bool finalFlush,
    bool splitFinalFlushForParallelism) {
  if (bufferedInputs_.empty()) {
    return;
  }

  auto inputs = std::move(bufferedInputs_);
  const auto totalBytes = bufferedInputBytes_;
  bufferedInputs_.clear();
  bufferedInputBytes_ = 0;
  bufferedInputRows_ = 0;

  auto targetBytes = inputRunTargetBytes();
  if (finalFlush && splitFinalFlushForParallelism &&
      totalBytes > kMinInputRunBytes) {
    const auto concurrency = executorConcurrency();
    const auto baseTargetBytes = baseInputRunTargetBytes();
    const auto targetConcurrency = std::min<uint64_t>(
        concurrency,
        std::max<uint64_t>(
            1, (concurrency * baseTargetBytes + targetBytes - 1) / targetBytes));
    const auto availableConcurrency = pendingRuns_.size() >= targetConcurrency
        ? uint64_t{1}
        : targetConcurrency - pendingRuns_.size();
    const auto targetForAvailableParallelism =
        (totalBytes + availableConcurrency - 1) / availableConcurrency;
    targetBytes = std::min(
        targetBytes,
        std::max<uint64_t>(kMinInputRunBytes, targetForAvailableParallelism));
  }

  std::vector<VectorPtr> runInputs;
  uint64_t runBytes = 0;
  for (auto& input : inputs) {
    const auto inputBytes = input->estimateFlatSize();
    if (!runInputs.empty() && runBytes + inputBytes > targetBytes) {
      scheduleInputRun(std::move(runInputs));
      runInputs.clear();
      runBytes = 0;
    }
    runBytes += inputBytes;
    runInputs.push_back(std::move(input));
  }
  if (!runInputs.empty()) {
    scheduleInputRun(std::move(runInputs));
  }
}

void ParallelSortBuffer::addInput(const VectorPtr& input) {
  BOLT_CHECK(!noMoreInput_);
  if (input->size() == 0) {
    return;
  }

  ensureInputFits(input);
  updateEstimatedInputRowSize(input);

  const auto inputBytes = input->estimateFlatSize();
  const auto targetBytes = inputRunTargetBytes();
  if (!bufferedInputs_.empty() &&
      bufferedInputBytes_ + inputBytes > targetBytes) {
    flushBufferedInputs();
  }
  bufferedInputs_.push_back(input);
  bufferedInputBytes_ += inputBytes;
  bufferedInputRows_ += input->size();

  if (bufferedInputBytes_ >= inputRunTargetBytes()) {
    flushBufferedInputs();
  }

  if (spillConfig_ != nullptr && testingTriggerSpill()) {
    spillAllInputToDisk();
  }
}

void ParallelSortBuffer::noMoreInput() {
  BOLT_CHECK(!noMoreInput_);
  noMoreInput_ = true;
  flushBufferedInputs(true, true);
  collectPendingRuns();
  if (spillConfig_ != nullptr && hasSpilledRuns()) {
    spillAllInputToDisk();
  }
  pool_->release();
}

void ParallelSortBuffer::reclaim(uint64_t /*targetBytes*/) {
  // Reclaim is a forced spill boundary: all buffered, pending, and in-memory
  // runs must be dumped to disk by spillAllInputToDisk(), which releases memory
  // as it replaces each in-memory run with a spilled run.
  spillAllInputToDisk();
}

bool ParallelSortBuffer::cursorLess(
    const RunCursor& left,
    const RunCursor& right) const {
  auto& leftRun = *runs_[left.runIndex];
  auto& rightRun = *runs_[right.runIndex];
  const auto* leftMemoryRun = dynamic_cast<const InMemorySortedRun*>(&leftRun);
  const auto* rightMemoryRun =
      dynamic_cast<const InMemorySortedRun*>(&rightRun);
  BOLT_CHECK_NOT_NULL(leftMemoryRun);
  BOLT_CHECK_NOT_NULL(rightMemoryRun);
  const auto compare = const_cast<RowContainer&>(leftMemoryRun->container())
                           .compareRows(
                               leftMemoryRun->rowAt(left.ordinal),
                               rightMemoryRun->rowAt(right.ordinal),
                               sortCompareFlags_);
  if (compare != 0) {
    return compare < 0;
  }
  if (leftRun.id() != rightRun.id()) {
    return leftRun.id() < rightRun.id();
  }
  return left.ordinal < right.ordinal;
}

void ParallelSortBuffer::collectPendingRuns() {
  if (pendingRuns_.empty()) {
    return;
  }

  const auto startUs = getCurrentTimeMicro();
  auto results = folly::collectAll(pendingRuns_).get();
  inputRunWaitTimeUs_.fetch_add(
      getCurrentTimeMicro() - startUs, std::memory_order_relaxed);
  inputRunCollects_.fetch_add(1, std::memory_order_relaxed);
  pendingRuns_.clear();
  runs_.reserve(runs_.size() + results.size());
  for (auto& result : results) {
    if (result.hasException()) {
      result.exception().throw_exception();
    }
    auto pendingRun = std::move(result.value());
    numInputRows_ += pendingRun.numInputRows;
    updateEstimatedOutputRowSize(pendingRun.estimatedOutputRowSize);
    sortColToRowTimeUs_ += pendingRun.sortColToRowTimeUs;
    sortInSortTimeUs_ += pendingRun.sortInSortTimeUs;
    runs_.push_back(std::move(pendingRun.run));
  }
}

void ParallelSortBuffer::ensureInputFits(const VectorPtr& input) {
  if (spillConfig_ == nullptr ||
      (runs_.empty() && pendingRuns_.empty() && bufferedInputs_.empty())) {
    return;
  }

  if (!pendingRuns_.empty() && !asyncInputRunCreationAllowed()) {
    collectPendingRuns();
  }

  if (testingTriggerSpill()) {
    spillAllInputToDisk();
    return;
  }

  if (spillMemoryThreshold_ != 0 &&
      pool_->currentBytes() > spillMemoryThreshold_) {
    spillAllInputToDisk();
    return;
  }

  const auto availableReservationBytes = pool_->availableReservation();
  const auto estimatedIncrementalBytes = input->estimateFlatSize();
  if (availableReservationBytes > 2 * estimatedIncrementalBytes) {
    return;
  }

  const auto currentMemoryUsage = pool_->currentBytes();
  const auto minReservationBytes =
      currentMemoryUsage * spillConfig_->minSpillableReservationPct / 100;
  if (availableReservationBytes > minReservationBytes) {
    return;
  }

  const auto targetIncrementBytes = std::max<uint64_t>(
      estimatedIncrementalBytes * 2,
      currentMemoryUsage * spillConfig_->spillableReservationGrowthPct / 100);
  std::optional<memory::ReclaimableSectionGuard> guard;
  if (nonReclaimableSection_ != nullptr) {
    guard.emplace(nonReclaimableSection_);
  }
  if (!pool_->maybeReserve(targetIncrementBytes)) {
    spillAllInputToDisk();
  }
}

std::optional<ParallelSortBuffer::SpilledRunResult>
ParallelSortBuffer::spillRun(size_t runIndex) {
  BOLT_CHECK_NOT_NULL(spillConfig_);
  BOLT_CHECK_LT(runIndex, runs_.size());
  if (runs_[runIndex]->spilled()) {
    return std::nullopt;
  }

  auto* memoryRun = dynamic_cast<InMemorySortedRun*>(runs_[runIndex].get());
  BOLT_CHECK_NOT_NULL(memoryRun);
  if (memoryRun->numRows() == 0) {
    return std::nullopt;
  }
  spillRunsScheduled_.fetch_add(1, std::memory_order_relaxed);
  const auto runningSpillRuns =
      runningSpillRuns_.fetch_add(1, std::memory_order_relaxed) + 1;
  updateMax(maxRunningSpillRuns_, runningSpillRuns);
  auto runningGuard = folly::makeGuard(
      [this]() { runningSpillRuns_.fetch_sub(1, std::memory_order_relaxed); });

  auto ioConfig = spillConfig_->spillIOConfig(1);
  ioConfig.indexedSpillEnabled = true;
  const auto spilledInputBytes = memoryRun->container().usedBytes();
  auto spilledRun = SpilledSortedRun::create(
      memoryRun->id(),
      internalType_,
      memoryRun->container(),
      memoryRun->sortedRows(),
      memoryRun->sortCompareFlags(),
      ioConfig,
      ioConfig.maxFileSize,
      pool_,
      &spillStats_,
      2'048);
  {
    auto stats = spillStats_.wlock();
    stats->spilledInputBytes += spilledInputBytes;
    stats->spilledPartitions = 1;
    ++stats->spillRuns;
  }
  return SpilledRunResult{.runIndex = runIndex, .run = std::move(spilledRun)};
}

void ParallelSortBuffer::spillAllRuns() {
  if (spillConfig_ == nullptr) {
    return;
  }

  flushBufferedInputs(true, true);
  collectPendingRuns();

  std::vector<size_t> runIndices;
  runIndices.reserve(runs_.size());
  for (size_t i = 0; i < runs_.size(); ++i) {
    if (!runs_[i]->spilled() && runs_[i]->numRows() != 0) {
      runIndices.push_back(i);
    }
  }
  if (runIndices.empty()) {
    return;
  }

  for (auto runIndex : runIndices) {
    auto spilledRun = spillRun(runIndex);
    if (spilledRun.has_value()) {
      runs_[spilledRun->runIndex] = std::move(spilledRun->run);
      pool_->release();
    }
  }
  pool_->release();
}

void ParallelSortBuffer::spillAllInputToDisk() {
  spillAllRuns();
}

bool ParallelSortBuffer::hasSpilledRuns() const {
  return std::any_of(runs_.begin(), runs_.end(), [](const auto& run) {
    return run->spilled();
  });
}

uint64_t ParallelSortBuffer::estimatedMergeTaskBytes() const {
  const auto rowBytes = estimatedInputRowSize();
  const auto outputRows =
      std::min<uint64_t>(parallelMergeTargetRows(), mergeOutputBatchRows());
  const auto outputBytes =
      outputRows > std::numeric_limits<uint64_t>::max() / rowBytes
      ? std::numeric_limits<uint64_t>::max()
      : outputRows * rowBytes;
  // A task needs memory for its output batch and transient copy buffers. For
  // spilled runs it also owns one independent indexed reader cache per input
  // run. Estimate spill cache memory by one decoded indexed block per run;
  // RunSliceReader reuses that block for sequential rows in the slice.
  const auto outputAndCopyBytes =
      outputBytes > std::numeric_limits<uint64_t>::max() / 2
      ? std::numeric_limits<uint64_t>::max()
      : outputBytes * 2;
  uint64_t spillCacheBytes = 0;
  if (hasSpilledRuns()) {
    const auto blockBytes =
        rowBytes > std::numeric_limits<uint64_t>::max() / kIndexedSpillBlockRows
        ? std::numeric_limits<uint64_t>::max()
        : rowBytes * kIndexedSpillBlockRows;
    spillCacheBytes = blockBytes > std::numeric_limits<uint64_t>::max() /
                std::max<size_t>(1, runs_.size())
        ? std::numeric_limits<uint64_t>::max()
        : blockBytes * runs_.size();
  }
  const auto taskBytes = outputAndCopyBytes >
          std::numeric_limits<uint64_t>::max() - spillCacheBytes
      ? std::numeric_limits<uint64_t>::max()
      : outputAndCopyBytes + spillCacheBytes;
  return std::max<uint64_t>(taskBytes, kMinMergeTaskMemoryBytes);
}

uint64_t ParallelSortBuffer::mergeTaskLookahead() const {
  const auto configuredLimit = parallelMergeLookaheadTasks_ == 0
      ? kMaxAutoMergeLookaheadTasks
      : parallelMergeLookaheadTasks_;
  const auto coreLimit = executorConcurrency();

  uint64_t memoryLimit = configuredLimit;
  if (spillMemoryThreshold_ != 0) {
    // Reserve at least half of the ORDER BY memory limit for spill readers,
    // output projection, and allocator fragmentation. The lookahead budget only
    // accounts for completed/active merge-task output and transient task state.
    const auto lookaheadMemoryBudget =
        std::max<uint64_t>(kMinMergeTaskMemoryBytes, spillMemoryThreshold_ / 2);
    memoryLimit = std::max<uint64_t>(
        1, lookaheadMemoryBudget / estimatedMergeTaskBytes());
  }

  return std::max<uint64_t>(
      1, std::min({configuredLimit, coreLimit, memoryLimit}));
}

vector_size_t ParallelSortBuffer::mergeOutputBatchRows() const {
  if (hasSpilledRuns()) {
    return kMergeOutputBatchRows;
  }

  const auto rowBytes = estimatedInputRowSize();
  BOLT_CHECK_GT(rowBytes, 0);
  const auto memoryBasedRows = std::max<uint64_t>(
      kMergeOutputBatchRows, kInMemoryMergeOutputBatchBytes / rowBytes);
  return static_cast<vector_size_t>(std::min<uint64_t>(
      memoryBasedRows, std::numeric_limits<vector_size_t>::max()));
}

void ParallelSortBuffer::initializeMerge() {
  if (mergeInitialized_) {
    return;
  }
  collectPendingRuns();
  mergeInitialized_ = true;

  for (size_t runIndex = 0; runIndex < runs_.size(); ++runIndex) {
    if (runs_[runIndex]->numRows() != 0) {
      heap_.push(RunCursor{runIndex, 0});
    }
  }
}

void ParallelSortBuffer::initializeParallelMerge() {
  if (parallelMergeInitialized_) {
    return;
  }
  collectPendingRuns();
  parallelMergeInitialized_ = true;

  std::vector<SortedRun*> sortedRuns;
  sortedRuns.reserve(runs_.size());
  for (const auto& run : runs_) {
    sortedRuns.push_back(run.get());
  }

  uint64_t mergePlanningTimeUs = 0;
  MergePathBoundaryPlanner::Stats plannerStats;
  {
    MicrosecondTimer timer(&mergePlanningTimeUs);
    MergePathBoundaryPlanner planner(sortedRuns, pool_);
    mergeTasks_ = planner.plan(parallelMergeTargetRows());
    plannerStats = planner.stats();
  }
  mergePlanningTimeUs_.fetch_add(
      mergePlanningTimeUs, std::memory_order_relaxed);
  mergeBoundaryPlanningTimeUs_.fetch_add(
      plannerStats.computeBoundariesTimeUs, std::memory_order_relaxed);
  mergeTaskBuildTimeUs_.fetch_add(
      plannerStats.buildTasksTimeUs, std::memory_order_relaxed);
  mergePlanningTotalRowsTimeUs_.fetch_add(
      plannerStats.totalRowsTimeUs, std::memory_order_relaxed);
  mergePlanningBoundaryIterations_.fetch_add(
      plannerStats.boundaryIterations, std::memory_order_relaxed);
  mergePlanningCursorComparisons_.fetch_add(
      plannerStats.cursorComparisons, std::memory_order_relaxed);
  mergePlanningRowReferenceLoads_.fetch_add(
      plannerStats.rowReferenceLoads, std::memory_order_relaxed);
  mergePlanningBoundaries_.fetch_add(
      plannerStats.boundaries, std::memory_order_relaxed);
  for (const auto& run : runs_) {
    if (auto* spilledRun = dynamic_cast<SpilledSortedRun*>(run.get())) {
      spilledRun->clearIndexedBlockCache();
    }
  }
  initializeMergeJit();
  pool_->release();
  streamingMergeTasks_.reserve(mergeTasks_.size());
  mergeOutputQueue_ = std::make_unique<OrderedMergeTaskOutputQueue>(
      mergeTasks_.size(),
      internalType_,
      pool_,
      mergeOutputBatchRows() * mergeTaskLookahead());
}

void ParallelSortBuffer::initializeMergeJit() {
#ifdef ENABLE_BOLT_JIT
  if (!enableJit_ || mergeRowCompare_ != nullptr || runs_.empty()) {
    return;
  }

  auto* memoryRun = dynamic_cast<InMemorySortedRun*>(runs_.front().get());
  if (memoryRun == nullptr ||
      !memoryRun->container().JITable(memoryRun->container().keyTypes())) {
    return;
  }

  auto [jitModule, rowRowCmpFn] = memoryRun->container().codegenCompare(
      memoryRun->container().keyTypes(),
      memoryRun->sortCompareFlags(),
      bytedance::bolt::jit::CmpType::CMP,
      true);
  mergeJitModule_ = std::move(jitModule);
  mergeRowCompare_ = (RowRowCompare)mergeJitModule_->getFuncPtr(rowRowCmpFn);
#endif
}

std::unique_ptr<MergeTaskExecutor> ParallelSortBuffer::createMergeTaskExecutor(
    const MergeTask& task) const {
  std::vector<SortedRun*> taskRuns;
  taskRuns.reserve(runs_.size());
  for (const auto& run : runs_) {
    taskRuns.push_back(run.get());
  }
  uint64_t buildTimeUs = 0;
  MicrosecondTimer timer(&buildTimeUs);
  auto executor = std::make_unique<MergeTaskExecutor>(
      std::move(taskRuns),
      task,
      internalType_,
      pool_
#ifdef ENABLE_BOLT_JIT
      ,
      mergeRowCompare_
#endif
  );
  mergeTaskBuildTimeUs_.fetch_add(buildTimeUs, std::memory_order_relaxed);
  return executor;
}

void ParallelSortBuffer::scheduleMergeTaskBatch(StreamingMergeTask& task) {
  BOLT_CHECK_NOT_NULL(task.executor);
  BOLT_CHECK(!task.finished);
  BOLT_CHECK(!task.future.has_value());
  BOLT_CHECK_NULL(task.pendingOutput);

  auto* executor = task.executor.get();
  mergeBatchesScheduled_.fetch_add(1, std::memory_order_relaxed);
  auto executeBatch = [this, executor]() {
    const auto runningMergeBatches =
        runningMergeBatches_.fetch_add(1, std::memory_order_relaxed) + 1;
    updateMax(maxRunningMergeBatches_, runningMergeBatches);
    auto runningGuard = folly::makeGuard([this]() {
      runningMergeBatches_.fetch_sub(1, std::memory_order_relaxed);
    });
    const auto startUs = getCurrentTimeMicro();
    auto output = executor->getOutput(mergeOutputBatchRows());
    mergeExecutionTimeUs_.fetch_add(
        getCurrentTimeMicro() - startUs, std::memory_order_relaxed);
    mergeBatchesCompleted_.fetch_add(1, std::memory_order_relaxed);
    return output;
  };
  if (mergeExecutor_ == nullptr) {
    task.future = folly::makeFuture(executeBatch());
  } else {
    task.future = folly::via(mergeExecutor_, std::move(executeBatch));
  }
}

void ParallelSortBuffer::finishMergeTask(StreamingMergeTask& task) {
  if (task.finished) {
    return;
  }
  task.finished = true;
  task.future.reset();
  task.pendingOutput = nullptr;
  task.executor.reset();
  mergeOutputQueue_->finishTask(task.taskIndex);
  pool_->release();
}

bool ParallelSortBuffer::tryEnqueueMergeTaskOutput(StreamingMergeTask& task) {
  if (task.pendingOutput == nullptr) {
    if (!task.future.has_value() && task.executor != nullptr &&
        task.executor->finished()) {
      finishMergeTask(task);
    }
    return true;
  }
  if (!mergeOutputQueue_->canEnqueue(
          task.taskIndex, task.pendingOutput->size())) {
    return false;
  }
  mergeOutputQueue_->enqueue(task.taskIndex, std::move(task.pendingOutput));
  updateMax(maxBufferedOutputRows_, mergeOutputQueue_->bufferedRows());
  if (task.executor->finished()) {
    finishMergeTask(task);
  } else {
    scheduleMergeTaskBatch(task);
  }
  return true;
}

void ParallelSortBuffer::scheduleMergeTasks() {
  const auto lookaheadTasks = mergeTaskLookahead();
  const auto activeTasks = std::count_if(
      streamingMergeTasks_.begin(),
      streamingMergeTasks_.end(),
      [](const auto& task) { return !task.finished; });
  if (activeTasks >= lookaheadTasks) {
    return;
  }
  updateMax(maxActiveMergeTasks_, activeTasks);
  const auto maxScheduledTasks = lookaheadTasks - activeTasks;
  uint64_t scheduledTasks = 0;
  while (nextMergeTaskToSchedule_ < mergeTasks_.size() &&
         scheduledTasks < maxScheduledTasks) {
    const auto taskIndex = nextMergeTaskToSchedule_++;
    streamingMergeTasks_.push_back(StreamingMergeTask{
        .taskIndex = taskIndex,
        .executor = createMergeTaskExecutor(mergeTasks_[taskIndex]),
        .future = std::nullopt,
        .pendingOutput = nullptr,
        .finished = false});
    scheduleMergeTaskBatch(streamingMergeTasks_.back());
    ++scheduledTasks;
  }
  updateMax(maxActiveMergeTasks_, activeTasks + scheduledTasks);
}

void ParallelSortBuffer::drainReadyMergeTasks() {
  bool madeProgress = true;
  while (madeProgress) {
    madeProgress = false;
    for (auto& task : streamingMergeTasks_) {
      if (task.finished) {
        continue;
      }
      if (!tryEnqueueMergeTaskOutput(task)) {
        continue;
      }
      if (task.future.has_value() && task.future->isReady()) {
        task.pendingOutput = std::move(task.future.value()).get();
        task.future.reset();
        madeProgress = true;
        tryEnqueueMergeTaskOutput(task);
      }
    }
    scheduleMergeTasks();
  }
}

void ParallelSortBuffer::drainNextMergeTask() {
  for (auto& task : streamingMergeTasks_) {
    if (task.finished) {
      continue;
    }
    if (!tryEnqueueMergeTaskOutput(task)) {
      return;
    }
    if (task.future.has_value()) {
      const auto startUs = getCurrentTimeMicro();
      task.pendingOutput = std::move(task.future.value()).get();
      mergeWaitTimeUs_.fetch_add(
          getCurrentTimeMicro() - startUs, std::memory_order_relaxed);
      task.future.reset();
      tryEnqueueMergeTaskOutput(task);
      scheduleMergeTasks();
      return;
    }
  }
}

void ParallelSortBuffer::prepareOutput(vector_size_t outputBatchSize) {
  if (output_ != nullptr) {
    VectorPtr output = std::move(output_);
    BaseVector::prepareForReuse(output, outputBatchSize);
    output_ = std::static_pointer_cast<RowVector>(output);
  } else {
    output_ = std::static_pointer_cast<RowVector>(
        BaseVector::create(input_, outputBatchSize, pool_));
  }

  for (auto& child : output_->children()) {
    child->resize(outputBatchSize);
  }
}

RowVectorPtr ParallelSortBuffer::getOutput(vector_size_t maxOutputRows) {
  BOLT_CHECK(noMoreInput_);
  if (numOutputRows_ == numInputRows_) {
    return nullptr;
  }

  BOLT_CHECK_GT(maxOutputRows, 0);
  return getParallelOutput(maxOutputRows);
}

RowVectorPtr ParallelSortBuffer::getSerialOutput(vector_size_t maxOutputRows) {
  initializeMerge();

  const vector_size_t batchSize =
      std::min<uint64_t>(numInputRows_ - numOutputRows_, maxOutputRows);
  prepareOutput(batchSize);

  MicrosecondTimer timer(&sortOutputTimeUs_);
  for (vector_size_t outputRow = 0; outputRow < batchSize; ++outputRow) {
    BOLT_CHECK(!heap_.empty());
    const auto cursor = heap_.top();
    heap_.pop();

    auto* run = dynamic_cast<InMemorySortedRun*>(runs_[cursor.runIndex].get());
    BOLT_CHECK_NOT_NULL(run);
    const char* row = run->rowAt(cursor.ordinal);
    for (const auto& columnProjection : columnMap_) {
      run->container().extractColumn(
          &row,
          1,
          columnProjection.inputChannel,
          outputRow,
          output_->childAt(columnProjection.outputChannel));
    }

    const auto nextOrdinal = cursor.ordinal + 1;
    if (nextOrdinal < run->numRows()) {
      heap_.push(RunCursor{cursor.runIndex, nextOrdinal});
    }
  }
  numOutputRows_ += batchSize;
  return output_;
}

RowVectorPtr ParallelSortBuffer::getParallelOutput(
    vector_size_t maxOutputRows) {
  MicrosecondTimer timer(&sortOutputTimeUs_);
  initializeParallelMerge();

  for (;;) {
    scheduleMergeTasks();
    drainReadyMergeTasks();

    const auto queueStartUs = getCurrentTimeMicro();
    auto internalOutput = mergeOutputQueue_->getOutput(maxOutputRows);
    mergeOutputQueueTimeUs_.fetch_add(
        getCurrentTimeMicro() - queueStartUs, std::memory_order_relaxed);
    if (internalOutput) {
      auto output = projectInternalOutput(internalOutput);
      numOutputRows_ += output->size();
      return output;
    }

    if (mergeOutputQueue_->finished()) {
      return nullptr;
    }

    drainNextMergeTask();
  }
}

RowVectorPtr ParallelSortBuffer::projectInternalOutput(
    const RowVectorPtr& internalOutput) {
  const auto startUs = getCurrentTimeMicro();
  auto guard = folly::makeGuard([&]() {
    outputProjectionTimeUs_.fetch_add(
        getCurrentTimeMicro() - startUs, std::memory_order_relaxed);
  });
  if (outputProjectionIdentity_) {
    return internalOutput;
  }

  const auto outputBatchSize = internalOutput->size();
  auto output = std::static_pointer_cast<RowVector>(
      BaseVector::create(input_, outputBatchSize, pool_));
  for (auto& child : output->children()) {
    child->resize(outputBatchSize);
  }

  for (const auto& columnProjection : columnMap_) {
    output->childAt(columnProjection.outputChannel)
        ->copy(
            internalOutput->childAt(columnProjection.inputChannel).get(),
            0,
            0,
            outputBatchSize);
  }
  return output;
}

} // namespace bytedance::bolt::exec

/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include "bolt/exec/OrderBy.h"

#include "bolt/exec/OperatorUtils.h"
#include "bolt/exec/ParallelSortBuffer.h"
#include "bolt/exec/SortBuffer.h"
#include "bolt/exec/Task.h"
#include "bolt/vector/FlatVector.h"
#include "exec/OperatorMetric.h"
namespace bytedance::bolt::exec {

namespace {
CompareFlags fromSortOrderToCompareFlags(const core::SortOrder& sortOrder) {
  return {
      sortOrder.isNullsFirst(),
      sortOrder.isAscending(),
      false,
      CompareFlags::NullHandlingMode::kNullAsValue};
}
} // namespace

OrderBy::OrderBy(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::OrderByNode>& orderByNode)
    : Operator(
          driverCtx,
          orderByNode->outputType(),
          operatorId,
          orderByNode->id(),
          "OrderBy",
          orderByNode->canSpill(driverCtx->queryConfig())
              ? driverCtx->makeSpillConfig(
                    operatorId,
                    driverCtx->queryConfig().rowBasedSpillMode())
              : std::nullopt) {
  maxOutputRows_ = outputBatchRows(std::nullopt);
  BOLT_CHECK(pool()->trackUsage());
  // isOutputStageSpillEnabled_ =
  //     driverCtx->queryConfig().orderBySpillInOutputStageEnabled();
  std::vector<column_index_t> sortColumnIndices;
  std::vector<CompareFlags> sortCompareFlags;
  sortColumnIndices.reserve(orderByNode->sortingKeys().size());
  sortCompareFlags.reserve(orderByNode->sortingKeys().size());
  for (int i = 0; i < orderByNode->sortingKeys().size(); ++i) {
    const auto channel =
        exprToChannel(orderByNode->sortingKeys()[i].get(), outputType_);
    BOLT_CHECK(
        channel != kConstantChannel,
        "OrderBy doesn't allow constant sorting keys");
    sortColumnIndices.push_back(channel);
    sortCompareFlags.push_back(
        fromSortOrderToCompareFlags(orderByNode->sortingOrders()[i]));
  }
  auto hybridSortEnabled = driverCtx->queryConfig().hybridSortEnabled();
  auto scatteredModeEnabled =
      driverCtx->queryConfig().hybridSortScatteredModeEnabled();
  if (driverCtx->queryConfig().orderByParallelSortEnabled()) {
    sortBuffer_ = std::make_unique<ParallelSortBuffer>(
        outputType_,
        sortColumnIndices,
        sortCompareFlags,
        pool(),
        driverCtx->queryConfig().orderByParallelMergeTargetRows(),
        driverCtx->queryConfig().orderByParallelMergeLookaheadTasks(),
        operatorCtx_->task()->queryCtx()->executor(),
        &nonReclaimableSection_,
        spillConfig_.has_value() ? &(spillConfig_.value()) : nullptr,
        operatorCtx_->driverCtx()->queryConfig().orderBySpillMemoryThreshold(),
        driverCtx->queryConfig().orderByParallelMergeThreads(),
        driverCtx->queryConfig().enableJitRowCmpRow());
    addRuntimeStat("parallelSortBufferUsed", RuntimeCounter(1));
  } else {
    sortBuffer_ = std::make_unique<SortBuffer>(
        outputType_,
        sortColumnIndices,
        sortCompareFlags,
        pool(),
        &nonReclaimableSection_,
        spillConfig_.has_value() ? &(spillConfig_.value()) : nullptr,
        operatorCtx_->driverCtx()->queryConfig().orderBySpillMemoryThreshold(),
        operatorCtx_.get(),
        hybridSortEnabled,
        scatteredModeEnabled);
  }

  this->setRuntimeMetric(
      OperatorMetricKey::kCanUsedToEstimateHashBuildPartitionNum, "true");
  this->setRuntimeMetric(
      OperatorMetricKey::kTotalRowCount, folly::to<std::string>(0));
  this->setRuntimeMetric(
      OperatorMetricKey::kHasBeenProcessedRowCount, folly::to<std::string>(0));
  LOG(INFO) << name() << " construct, output type: " << outputType_->toString();
}

void OrderBy::addInput(RowVectorPtr input) {
  sortBuffer_->addInput(input);
}

void OrderBy::reclaim(
    uint64_t targetBytes,
    memory::MemoryReclaimer::Stats& stats) {
  BOLT_CHECK(canReclaim());
  BOLT_CHECK(!nonReclaimableSection_);

  // TODO: support fine-grain disk spilling based on 'targetBytes' after
  // having row container memory compaction support later.
  sortBuffer_->reclaim(targetBytes);

  // Release the minimum reserved memory.
  pool()->release();
}

void OrderBy::noMoreInput() {
  Operator::noMoreInput();
  sortBuffer_->noMoreInput();
  maxOutputRows_ = outputBatchRows(sortBuffer_->estimateOutputRowSize());
  recordSpillStats();
}

RowVectorPtr OrderBy::getOutput() {
  if (finished_ || !noMoreInput_) {
    return nullptr;
  }

  // When the OrderBy operator has no more output, the sortbuffer memory is
  // cleaned up immediately, without waiting until operator::close() is called.
  // This reduces the peak memory usage of the Pipeline.
  // Immediate cleanup is safe because the data is deep-copied into the output.
  auto guard = folly::makeGuard([&]() {
    if (finished_) {
      sortBuffer_.reset();
    }
  });

  RowVectorPtr output = sortBuffer_->getOutput(maxOutputRows_);
  finished_ = (output == nullptr);
  if (finished_) {
    recordSpillReadStats();
    recordSortStats();
    recordParallelSortStats();
  }

  this->setRuntimeMetric(
      OperatorMetricKey::kTotalRowCount,
      folly::to<std::string>(sortBuffer_->numInputRows()));
  this->setRuntimeMetric(
      OperatorMetricKey::kHasBeenProcessedRowCount,
      folly::to<std::string>(sortBuffer_->numOutputRows()));

  return output;
}

void OrderBy::close() {
  Operator::close();
  sortBuffer_.reset();
}

void OrderBy::recordSpillStats() {
  BOLT_CHECK_NOT_NULL(sortBuffer_);
  auto spillStats = sortBuffer_->spilledStats();
  if (spillStats.has_value()) {
    Operator::recordSpillStats(spillStats.value());
  }
}

void OrderBy::recordSpillReadStats() {
  auto spillReadStatsOr = sortBuffer_->spillReadStats();
  if (spillReadStatsOr.has_value()) {
    Operator::recordSpillReadStats(spillReadStatsOr.value());
  }
}

void OrderBy::recordSortStats() {
  auto sortStatsOr = sortBuffer_->sortStats();
  if (sortStatsOr.has_value()) {
    Operator::recordSortStats(sortStatsOr.value());
  }
}

void OrderBy::recordParallelSortStats() {
  auto* parallelBuffer = dynamic_cast<ParallelSortBuffer*>(sortBuffer_.get());
  if (parallelBuffer == nullptr) {
    return;
  }

  const auto stats = parallelBuffer->debugStats();
  auto lockedStats = stats_.wlock();
  lockedStats->addRuntimeStat(
      "parallelInputRunTargetBytes", RuntimeCounter(stats.inputRunTargetBytes));
  lockedStats->addRuntimeStat(
      "parallelInputRunsCreated", RuntimeCounter(stats.inputRunsCreated));
  lockedStats->addRuntimeStat(
      "parallelInputRunsScheduledSync",
      RuntimeCounter(stats.inputRunsScheduledSync));
  lockedStats->addRuntimeStat(
      "parallelInputRunsScheduledAsync",
      RuntimeCounter(stats.inputRunsScheduledAsync));
  lockedStats->addRuntimeStat(
      "parallelMaxPendingInputRuns", RuntimeCounter(stats.maxPendingInputRuns));
  lockedStats->addRuntimeStat(
      "parallelMaxRunningInputRuns", RuntimeCounter(stats.maxRunningInputRuns));
  lockedStats->addRuntimeStat(
      "parallelInputRunCollects", RuntimeCounter(stats.inputRunCollects));
  lockedStats->addRuntimeStat(
      "parallelInputRunWaitTime",
      RuntimeCounter(
          static_cast<int64_t>(stats.inputRunWaitTimeUs * 1'000),
          RuntimeCounter::Unit::kNanos));
  lockedStats->addRuntimeStat(
      "parallelSpillRunsScheduled", RuntimeCounter(stats.spillRunsScheduled));
  lockedStats->addRuntimeStat(
      "parallelMaxRunningSpillRuns", RuntimeCounter(stats.maxRunningSpillRuns));
  lockedStats->addRuntimeStat(
      "parallelMergeTargetRows", RuntimeCounter(stats.mergeTargetRows));
  lockedStats->addRuntimeStat(
      "parallelMergeTasks", RuntimeCounter(stats.mergeTasks));
  lockedStats->addRuntimeStat(
      "parallelMergeTaskLookahead", RuntimeCounter(stats.mergeTaskLookahead));
  lockedStats->addRuntimeStat(
      "parallelMergeBatchesScheduled",
      RuntimeCounter(stats.mergeBatchesScheduled));
  lockedStats->addRuntimeStat(
      "parallelMergeBatchesCompleted",
      RuntimeCounter(stats.mergeBatchesCompleted));
  lockedStats->addRuntimeStat(
      "parallelMaxRunningMergeBatches",
      RuntimeCounter(stats.maxRunningMergeBatches));
  lockedStats->addRuntimeStat(
      "parallelMaxActiveMergeTasks", RuntimeCounter(stats.maxActiveMergeTasks));
  lockedStats->addRuntimeStat(
      "parallelMaxBufferedOutputRows",
      RuntimeCounter(stats.maxBufferedOutputRows));
  lockedStats->addRuntimeStat(
      "parallelEstimatedInputRowBytes",
      RuntimeCounter(stats.estimatedInputRowBytes));
  lockedStats->addRuntimeStat(
      "parallelMergePlanningTime",
      RuntimeCounter(
          static_cast<int64_t>(stats.mergePlanningTimeUs * 1'000),
          RuntimeCounter::Unit::kNanos));
  lockedStats->addRuntimeStat(
      "parallelMergeBoundaryPlanningTime",
      RuntimeCounter(
          static_cast<int64_t>(stats.mergeBoundaryPlanningTimeUs * 1'000),
          RuntimeCounter::Unit::kNanos));
  lockedStats->addRuntimeStat(
      "parallelMergeTaskBuildTime",
      RuntimeCounter(
          static_cast<int64_t>(stats.mergeTaskBuildTimeUs * 1'000),
          RuntimeCounter::Unit::kNanos));
  lockedStats->addRuntimeStat(
      "parallelMergePlanningTotalRowsTime",
      RuntimeCounter(
          static_cast<int64_t>(stats.mergePlanningTotalRowsTimeUs * 1'000),
          RuntimeCounter::Unit::kNanos));
  lockedStats->addRuntimeStat(
      "parallelMergePlanningBoundaryIterations",
      RuntimeCounter(stats.mergePlanningBoundaryIterations));
  lockedStats->addRuntimeStat(
      "parallelMergePlanningCursorComparisons",
      RuntimeCounter(stats.mergePlanningCursorComparisons));
  lockedStats->addRuntimeStat(
      "parallelMergePlanningRowReferenceLoads",
      RuntimeCounter(stats.mergePlanningRowReferenceLoads));
  lockedStats->addRuntimeStat(
      "parallelMergePlanningBoundaries",
      RuntimeCounter(stats.mergePlanningBoundaries));
  lockedStats->addRuntimeStat(
      "parallelMergeExecutionTime",
      RuntimeCounter(
          static_cast<int64_t>(stats.mergeExecutionTimeUs * 1'000),
          RuntimeCounter::Unit::kNanos));
  lockedStats->addRuntimeStat(
      "parallelMergeWaitTime",
      RuntimeCounter(
          static_cast<int64_t>(stats.mergeWaitTimeUs * 1'000),
          RuntimeCounter::Unit::kNanos));
  lockedStats->addRuntimeStat(
      "parallelMergeOutputQueueTime",
      RuntimeCounter(
          static_cast<int64_t>(stats.mergeOutputQueueTimeUs * 1'000),
          RuntimeCounter::Unit::kNanos));
  lockedStats->addRuntimeStat(
      "parallelOutputProjectionTime",
      RuntimeCounter(
          static_cast<int64_t>(stats.outputProjectionTimeUs * 1'000),
          RuntimeCounter::Unit::kNanos));
}

} // namespace bytedance::bolt::exec

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

#include "bolt/exec/MergePath.h"

#include <algorithm>
#include <numeric>
#include <optional>
#include <queue>

#include "bolt/common/time/Timer.h"
#include "bolt/exec/SpilledSortedRun.h"
#include "bolt/vector/BaseVector.h"

namespace bytedance::bolt::exec {

MergePathBoundaryPlanner::MergePathBoundaryPlanner(
    std::vector<SortedRun*> runs,
    memory::MemoryPool* pool)
    : runs_(std::move(runs)), pool_(pool) {
  BOLT_CHECK_NOT_NULL(pool_);
  BOLT_CHECK(
      !runs_.empty(), "Merge boundary planning requires at least one run");
  for (auto* run : runs_) {
    BOLT_CHECK_NOT_NULL(run);
    if (sortingKeys_.empty()) {
      sortingKeys_ = sortingKeysForRun(run);
    }
  }
  BOLT_CHECK(
      !sortingKeys_.empty(), "Merge boundary planning requires sort keys");
}

std::vector<MergeTask> MergePathBoundaryPlanner::plan(
    uint64_t targetRowsPerTask) {
  BOLT_CHECK_GT(targetRowsPerTask, 0);
  std::vector<std::vector<uint64_t>> boundaries;
  {
    MicrosecondTimer timer(&stats_.computeBoundariesTimeUs);
    boundaries = computeBoundaries(targetRowsPerTask);
  }
  stats_.boundaries = boundaries.size();
  BOLT_CHECK_GE(boundaries.size(), 1);

  std::vector<MergeTask> tasks;
  tasks.reserve(boundaries.size() - 1);
  {
    MicrosecondTimer timer(&stats_.buildTasksTimeUs);
    for (size_t boundary = 1; boundary < boundaries.size(); ++boundary) {
      MergeTask task;
      task.outputBegin = std::accumulate(
          boundaries[boundary - 1].begin(),
          boundaries[boundary - 1].end(),
          uint64_t{0});
      task.outputEnd =
          std::accumulate(
              boundaries[boundary].begin(),
              boundaries[boundary].end(),
              uint64_t{0});
      task.slices.reserve(runs_.size());
      for (size_t runIndex = 0; runIndex < runs_.size(); ++runIndex) {
        task.slices.push_back(MergeRunSlice{
            .runIndex = runIndex,
            .runId = runs_[runIndex]->id(),
            .begin = boundaries[boundary - 1][runIndex],
            .end = boundaries[boundary][runIndex]});
      }
      tasks.push_back(std::move(task));
    }
  }
  return tasks;
}

std::vector<std::vector<uint64_t>> MergePathBoundaryPlanner::computeBoundaries(
    uint64_t targetRowsPerTask) {
  std::vector<uint64_t> offsets(runs_.size(), 0);
  std::vector<std::vector<uint64_t>> boundaries;
  boundaries.push_back(offsets);

  uint64_t plannedRows = 0;
  while (plannedRows < totalRows()) {
    auto remainingForBoundary =
        std::min(targetRowsPerTask, totalRows() - plannedRows);
    while (remainingForBoundary > 0) {
      ++stats_.boundaryIterations;
      size_t activeRuns = 0;
      for (size_t runIndex = 0; runIndex < runs_.size(); ++runIndex) {
        activeRuns += offsets[runIndex] < runs_[runIndex]->numRows() ? 1 : 0;
      }
      BOLT_CHECK_GT(activeRuns, 0);

      const auto step =
          std::max<uint64_t>(1, remainingForBoundary / activeRuns);
      std::optional<Cursor> bestCandidate;
      uint64_t bestAdvance = 0;
      for (size_t runIndex = 0; runIndex < runs_.size(); ++runIndex) {
        const auto available = runs_[runIndex]->numRows() - offsets[runIndex];
        if (available == 0) {
          continue;
        }
        const auto advance = std::min<uint64_t>(step, available);
        Cursor candidate{
            .runIndex = runIndex, .ordinal = offsets[runIndex] + advance - 1};
        if (!bestCandidate.has_value() ||
            cursorLess(candidate, bestCandidate.value())) {
          bestCandidate = candidate;
          bestAdvance = advance;
        }
      }
      BOLT_CHECK(bestCandidate.has_value());
      offsets[bestCandidate->runIndex] += bestAdvance;
      remainingForBoundary -= bestAdvance;
    }
    plannedRows += std::min(targetRowsPerTask, totalRows() - plannedRows);
    boundaries.push_back(offsets);
  }
  return boundaries;
}

bool MergePathBoundaryPlanner::cursorLess(
    const Cursor& left,
    const Cursor& right) {
  ++stats_.cursorComparisons;
  const auto result = compareRows(left, right);
  if (result != 0) {
    return result < 0;
  }
  const auto leftRunId = runs_[left.runIndex]->id();
  const auto rightRunId = runs_[right.runIndex]->id();
  if (leftRunId != rightRunId) {
    return leftRunId < rightRunId;
  }
  return left.ordinal < right.ordinal;
}

int32_t MergePathBoundaryPlanner::compareRows(
    const Cursor& left,
    const Cursor& right) {
  return compareRowReferences(rowReference(left), rowReference(right));
}

MergePathBoundaryPlanner::RowReference MergePathBoundaryPlanner::rowReference(
    const Cursor& cursor) {
  ++stats_.rowReferenceLoads;
  auto* run = runs_[cursor.runIndex];
  if (auto* memoryRun = dynamic_cast<InMemorySortedRun*>(run)) {
    return RowReference{
        .memoryRun = memoryRun,
        .memoryRow = memoryRun->rowAt(cursor.ordinal),
        .vectorBatch = nullptr,
        .vectorIndex = 0};
  }
  auto* spilledRun = dynamic_cast<SpilledSortedRun*>(run);
  BOLT_CHECK_NOT_NULL(spilledRun);
  auto row = spilledRun->rowAt(cursor.ordinal);
  return RowReference{
      .vectorBatch = std::move(row.batch), .vectorIndex = row.index};
}

int32_t MergePathBoundaryPlanner::compareRowReferences(
    const RowReference& left,
    const RowReference& right) const {
  if (left.memoryRun != nullptr && right.memoryRun != nullptr) {
    return const_cast<RowContainer&>(left.memoryRun->container())
        .compareRows(
            left.memoryRow,
            right.memoryRow,
            left.memoryRun->sortCompareFlags());
  }

  for (const auto& [channel, flags] : sortingKeys()) {
    VectorPtr leftVector;
    vector_size_t leftIndex = left.vectorIndex;
    if (left.memoryRun != nullptr) {
      leftVector = extractMemoryKey(*left.memoryRun, left.memoryRow, channel);
      leftIndex = 0;
    } else {
      leftVector = left.vectorBatch->childAt(channel);
    }

    VectorPtr rightVector;
    vector_size_t rightIndex = right.vectorIndex;
    if (right.memoryRun != nullptr) {
      rightVector =
          extractMemoryKey(*right.memoryRun, right.memoryRow, channel);
      rightIndex = 0;
    } else {
      rightVector = right.vectorBatch->childAt(channel);
    }

    auto result =
        leftVector->compare(rightVector.get(), leftIndex, rightIndex, flags);
    BOLT_CHECK(result.has_value());
    if (result.value() != 0) {
      return result.value();
    }
  }
  return 0;
}

VectorPtr MergePathBoundaryPlanner::extractMemoryKey(
    const InMemorySortedRun& run,
    const char* row,
    column_index_t channel) const {
  auto result =
      BaseVector::create(run.container().columnTypes().at(channel), 1, pool_);
  const_cast<RowContainer&>(run.container())
      .extractColumn(&row, 1, channel, result);
  return result;
}

std::vector<SpillSortKey> MergePathBoundaryPlanner::sortingKeysForRun(
    SortedRun* run) const {
  if (auto* memoryRun = dynamic_cast<InMemorySortedRun*>(run)) {
    std::vector<SpillSortKey> keys;
    const auto& flags = memoryRun->sortCompareFlags();
    keys.reserve(flags.size());
    for (column_index_t channel = 0; channel < flags.size(); ++channel) {
      keys.emplace_back(channel, flags[channel]);
    }
    return keys;
  }
  auto* spilledRun = dynamic_cast<SpilledSortedRun*>(run);
  BOLT_CHECK_NOT_NULL(spilledRun);
  BOLT_CHECK(!spilledRun->files().empty());
  return spilledRun->files()[0].sortingKeys;
}

const std::vector<SpillSortKey>& MergePathBoundaryPlanner::sortingKeys() const {
  return sortingKeys_;
}

uint64_t MergePathBoundaryPlanner::totalRows() const {
  MicrosecondTimer timer(&const_cast<MergePathBoundaryPlanner*>(this)->stats_.totalRowsTimeUs);
  return std::accumulate(
      runs_.begin(), runs_.end(), uint64_t{0}, [](auto total, auto* run) {
        return total + run->numRows();
      });
}

RunSliceReader::RunSliceReader(SortedRun* run, MergeRunSlice slice)
    : run_(run), slice_(std::move(slice)), ordinal_(slice_.begin) {
  BOLT_CHECK_NOT_NULL(run_);
  BOLT_CHECK_EQ(run_->id(), slice_.runId);
  BOLT_CHECK_LE(slice_.begin, slice_.end);
  BOLT_CHECK_LE(slice_.end, run_->numRows());
}

bool RunSliceReader::hasNext() const {
  return ordinal_ < slice_.end;
}

uint64_t RunSliceReader::currentOrdinal() const {
  BOLT_CHECK(hasNext());
  return ordinal_;
}

uint64_t RunSliceReader::rowsRead() const {
  BOLT_CHECK_GE(ordinal_, slice_.begin);
  return ordinal_ - slice_.begin;
}

const MergeRunSlice& RunSliceReader::slice() const {
  return slice_;
}

const RunSliceReader::RowReference& RunSliceReader::current() const {
  BOLT_CHECK(hasNext());
  if (current_.has_value()) {
    return current_.value();
  }

  if (auto* memoryRun = dynamic_cast<InMemorySortedRun*>(run_)) {
    current_ = RowReference{
        .memoryRun = memoryRun,
        .memoryRow = memoryRun->rowAt(ordinal_),
        .vectorBatch = nullptr,
        .vectorIndex = 0,
        .runId = memoryRun->id(),
        .ordinal = ordinal_};
    return current_.value();
  }

  auto* spilledRun = dynamic_cast<SpilledSortedRun*>(run_);
  BOLT_CHECK_NOT_NULL(spilledRun);
  auto row = spilledRun->rowAt(
      ordinal_, indexedReaders_, 1 /*maxCachedBlocksPerFile*/);
  current_ = RowReference{
      .memoryRun = nullptr,
      .memoryRow = nullptr,
      .vectorBatch = std::move(row.batch),
      .vectorIndex = row.index,
      .runId = spilledRun->id(),
      .ordinal = ordinal_};
  return current_.value();
}

void RunSliceReader::copyCurrentRowTo(
    const RowVectorPtr& output,
    vector_size_t outputRow) const {
  BOLT_CHECK_NOT_NULL(output);
  const auto& row = current();
  for (column_index_t channel = 0; channel < output->childrenSize();
       ++channel) {
    if (row.memoryRun != nullptr) {
      const_cast<RowContainer&>(row.memoryRun->container())
          .extractColumn(
              &row.memoryRow, 1, channel, outputRow, output->childAt(channel));
    } else {
      output->childAt(channel)->copy(
          row.vectorBatch->childAt(channel).get(),
          outputRow,
          row.vectorIndex,
          1);
    }
  }
}

void RunSliceReader::advance() {
  BOLT_CHECK(hasNext());
  ++ordinal_;
  if (!hasNext()) {
    current_.reset();
    return;
  }

  if (current_.has_value() && current_->memoryRun == nullptr &&
      current_->vectorBatch != nullptr &&
      current_->vectorIndex + 1 < current_->vectorBatch->size()) {
    ++current_->vectorIndex;
    current_->ordinal = ordinal_;
    return;
  }

  current_.reset();
}

MergeTaskExecutor::MergeTaskExecutor(
    std::vector<SortedRun*> runs,
    MergeTask task,
    RowTypePtr outputType,
    memory::MemoryPool* pool,
    RowRowCompare rowCompare)
    : runs_(std::move(runs)),
      task_(std::move(task)),
      outputType_(std::move(outputType)),
      pool_(pool),
      sortingKeys_(
          runs_.empty() ? std::vector<SpillSortKey>{}
                        : sortingKeysForRun(runs_.front())),
      heap_(ReaderGreater{this}) {
  BOLT_CHECK(!runs_.empty(), "Merge task execution requires at least one run");
  BOLT_CHECK_NOT_NULL(outputType_);
  BOLT_CHECK_NOT_NULL(pool_);
  BOLT_CHECK(!sortingKeys_.empty(), "Merge task execution requires sort keys");

#ifdef ENABLE_BOLT_JIT
  rowCompare_ = rowCompare;
#endif

  readers_.reserve(task_.slices.size());
  uint64_t sliceRows = 0;
  for (const auto& slice : task_.slices) {
    BOLT_CHECK_LT(slice.runIndex, runs_.size());
    BOLT_CHECK_EQ(slice.runId, runs_[slice.runIndex]->id());
    sliceRows += slice.size();
    readers_.emplace_back(runs_[slice.runIndex], slice);
  }
  BOLT_CHECK_EQ(sliceRows, task_.outputSize());
}

RowVectorPtr MergeTaskExecutor::getOutput(vector_size_t maxOutputRows) {
  BOLT_CHECK_GT(maxOutputRows, 0);
  initializeHeap();
  if (finished()) {
    return nullptr;
  }

  const auto batchSize =
      std::min<uint64_t>(task_.outputSize() - numOutputRows_, maxOutputRows);
  auto output = makeOutput(batchSize);
  for (vector_size_t outputRow = 0; outputRow < batchSize;) {
    BOLT_CHECK(!heap_.empty());
    const auto readerIndex = heap_.top();
    heap_.pop();

    auto& reader = readers_[readerIndex];
    if (reader.current().memoryRun != nullptr) {
      const auto outputStart = outputRow;
      auto* memoryRun = reader.current().memoryRun;
      std::vector<const char*> rows;
      rows.reserve(batchSize - outputRow);
      for (;;) {
        rows.push_back(reader.current().memoryRow);
        ++outputRow;
        reader.advance();
        if (outputRow == batchSize || !reader.hasNext()) {
          break;
        }

        if (!heap_.empty() &&
            !rowLess(reader.current(), readers_[heap_.top()].current())) {
          break;
        }
      }
      for (column_index_t channel = 0; channel < output->childrenSize();
           ++channel) {
        const_cast<RowContainer&>(memoryRun->container())
            .extractColumn(
                rows.data(),
                rows.size(),
                channel,
                outputStart,
                output->childAt(channel));
      }
    } else {
      for (;;) {
        reader.copyCurrentRowTo(output, outputRow++);
        reader.advance();
        if (outputRow == batchSize || !reader.hasNext()) {
          break;
        }

        if (!heap_.empty() &&
            !rowLess(reader.current(), readers_[heap_.top()].current())) {
          break;
        }
      }
    }

    if (reader.hasNext()) {
      heap_.push(readerIndex);
    }
  }
  numOutputRows_ += batchSize;
  return output;
}

uint64_t MergeTaskExecutor::numOutputRows() const {
  return numOutputRows_;
}

bool MergeTaskExecutor::finished() const {
  return numOutputRows_ == task_.outputSize();
}

int32_t MergeTaskExecutor::compareRows(
    const RunSliceReader::RowReference& left,
    const RunSliceReader::RowReference& right) const {
  if (left.memoryRun != nullptr && right.memoryRun != nullptr) {
#ifdef ENABLE_BOLT_JIT
    if (rowCompare_ != nullptr) {
      return rowCompare_(left.memoryRow, right.memoryRow);
    }
#endif
    return const_cast<RowContainer&>(left.memoryRun->container())
        .compareRows(
            left.memoryRow,
            right.memoryRow,
            left.memoryRun->sortCompareFlags());
  }

  for (const auto& [channel, flags] : sortingKeys_) {
    VectorPtr leftVector;
    vector_size_t leftIndex = left.vectorIndex;
    if (left.memoryRun != nullptr) {
      leftVector = extractMemoryKey(*left.memoryRun, left.memoryRow, channel);
      leftIndex = 0;
    } else {
      leftVector = left.vectorBatch->childAt(channel);
    }

    VectorPtr rightVector;
    vector_size_t rightIndex = right.vectorIndex;
    if (right.memoryRun != nullptr) {
      rightVector =
          extractMemoryKey(*right.memoryRun, right.memoryRow, channel);
      rightIndex = 0;
    } else {
      rightVector = right.vectorBatch->childAt(channel);
    }

    auto result =
        leftVector->compare(rightVector.get(), leftIndex, rightIndex, flags);
    BOLT_CHECK(result.has_value());
    if (result.value() != 0) {
      return result.value();
    }
  }
  return 0;
}

bool MergeTaskExecutor::rowLess(
    const RunSliceReader::RowReference& left,
    const RunSliceReader::RowReference& right) const {
  const auto result = compareRows(left, right);
  if (result != 0) {
    return result < 0;
  }
  if (left.runId != right.runId) {
    return left.runId < right.runId;
  }
  return left.ordinal < right.ordinal;
}

VectorPtr MergeTaskExecutor::extractMemoryKey(
    const InMemorySortedRun& run,
    const char* row,
    column_index_t channel) const {
  auto result =
      BaseVector::create(run.container().columnTypes().at(channel), 1, pool_);
  const_cast<RowContainer&>(run.container())
      .extractColumn(&row, 1, channel, result);
  return result;
}

std::vector<SpillSortKey> MergeTaskExecutor::sortingKeysForRun(
    SortedRun* run) const {
  BOLT_CHECK_NOT_NULL(run);
  if (auto* memoryRun = dynamic_cast<InMemorySortedRun*>(run)) {
    std::vector<SpillSortKey> keys;
    const auto& flags = memoryRun->sortCompareFlags();
    keys.reserve(flags.size());
    for (column_index_t channel = 0; channel < flags.size(); ++channel) {
      keys.emplace_back(channel, flags[channel]);
    }
    return keys;
  }
  auto* spilledRun = dynamic_cast<SpilledSortedRun*>(run);
  BOLT_CHECK_NOT_NULL(spilledRun);
  BOLT_CHECK(!spilledRun->files().empty());
  return spilledRun->files()[0].sortingKeys;
}

void MergeTaskExecutor::initializeHeap() {
  if (heapInitialized_) {
    return;
  }
  heapInitialized_ = true;
  for (size_t readerIndex = 0; readerIndex < readers_.size(); ++readerIndex) {
    if (readers_[readerIndex].hasNext()) {
      heap_.push(readerIndex);
    }
  }
}

RowVectorPtr MergeTaskExecutor::makeOutput(
    vector_size_t outputBatchSize) const {
  auto output = std::static_pointer_cast<RowVector>(
      BaseVector::create(outputType_, outputBatchSize, pool_));
  for (auto& child : output->children()) {
    child->resize(outputBatchSize);
  }
  return output;
}

bool MergeTaskExecutor::ReaderGreater::operator()(size_t left, size_t right)
    const {
  return executor->rowLess(
      executor->readers_[right].current(), executor->readers_[left].current());
}

OrderedMergeTaskOutputQueue::OrderedMergeTaskOutputQueue(
    size_t numTasks,
    RowTypePtr outputType,
    memory::MemoryPool* pool,
    uint64_t maxBufferedRows)
    : outputType_(std::move(outputType)),
      pool_(pool),
      maxBufferedRows_(maxBufferedRows),
      tasks_(numTasks) {
  BOLT_CHECK_NOT_NULL(outputType_);
  BOLT_CHECK_NOT_NULL(pool_);
}

bool OrderedMergeTaskOutputQueue::canEnqueue(vector_size_t rows) const {
  BOLT_CHECK_GE(rows, 0);
  return bufferedRows_ + rows <= maxBufferedRows_;
}

bool OrderedMergeTaskOutputQueue::canEnqueue(
    size_t taskIndex,
    vector_size_t rows) const {
  BOLT_CHECK_LT(taskIndex, tasks_.size());
  return canEnqueue(rows) || taskIndex == nextTaskToEmit_;
}

void OrderedMergeTaskOutputQueue::enqueue(
    size_t taskIndex,
    RowVectorPtr output) {
  BOLT_CHECK_LT(taskIndex, tasks_.size());
  BOLT_CHECK_NOT_NULL(output);
  BOLT_CHECK(!tasks_[taskIndex].finished);
  BOLT_CHECK(
      canEnqueue(taskIndex, output->size()),
      "Ordered merge output queue exceeded buffered row limit");

  bufferedRows_ += output->size();
  tasks_[taskIndex].batches.push_back(std::move(output));
}

void OrderedMergeTaskOutputQueue::finishTask(size_t taskIndex) {
  BOLT_CHECK_LT(taskIndex, tasks_.size());
  tasks_[taskIndex].finished = true;
  advanceCompletedTasks();
}

RowVectorPtr OrderedMergeTaskOutputQueue::getOutput(
    vector_size_t maxOutputRows) {
  BOLT_CHECK_GT(maxOutputRows, 0);
  advanceCompletedTasks();
  if (nextTaskToEmit_ == tasks_.size() ||
      tasks_[nextTaskToEmit_].batches.empty()) {
    return nullptr;
  }

  auto& task = tasks_[nextTaskToEmit_];
  auto& batch = task.batches.front();
  const auto remainingRows = batch->size() - currentBatchOffset_;
  const auto outputRows = std::min<vector_size_t>(remainingRows, maxOutputRows);
  if (currentBatchOffset_ == 0 && outputRows == batch->size()) {
    auto output = std::move(batch);
    task.batches.pop_front();
    numOutputRows_ += outputRows;
    bufferedRows_ -= outputRows;
    advanceCompletedTasks();
    return output;
  }

  auto output = makeOutput(outputRows);
  for (column_index_t channel = 0; channel < output->childrenSize();
       ++channel) {
    output->childAt(channel)->copy(
        batch->childAt(channel).get(), 0, currentBatchOffset_, outputRows);
  }

  currentBatchOffset_ += outputRows;
  numOutputRows_ += outputRows;
  bufferedRows_ -= outputRows;
  if (currentBatchOffset_ == batch->size()) {
    task.batches.pop_front();
    currentBatchOffset_ = 0;
  }
  advanceCompletedTasks();
  return output;
}

uint64_t OrderedMergeTaskOutputQueue::numOutputRows() const {
  return numOutputRows_;
}

uint64_t OrderedMergeTaskOutputQueue::bufferedRows() const {
  return bufferedRows_;
}

bool OrderedMergeTaskOutputQueue::finished() const {
  return nextTaskToEmit_ == tasks_.size();
}

RowVectorPtr OrderedMergeTaskOutputQueue::makeOutput(
    vector_size_t outputBatchSize) const {
  auto output = std::static_pointer_cast<RowVector>(
      BaseVector::create(outputType_, outputBatchSize, pool_));
  for (auto& child : output->children()) {
    child->resize(outputBatchSize);
  }
  return output;
}

void OrderedMergeTaskOutputQueue::advanceCompletedTasks() {
  while (nextTaskToEmit_ < tasks_.size()) {
    auto& task = tasks_[nextTaskToEmit_];
    if (!task.finished || !task.batches.empty()) {
      break;
    }
    currentBatchOffset_ = 0;
    ++nextTaskToEmit_;
  }
}

} // namespace bytedance::bolt::exec

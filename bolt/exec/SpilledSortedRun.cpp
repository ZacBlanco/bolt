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

#include "bolt/exec/SpilledSortedRun.h"

#include <algorithm>
#include <numeric>

#include "bolt/exec/Spill.h"
#include "bolt/vector/BaseVector.h"

namespace bytedance::bolt::exec {
namespace {

uint64_t totalRows(const SpillFiles& files) {
  return std::accumulate(
      files.begin(), files.end(), uint64_t{0}, [](uint64_t total, auto& file) {
        return total + file.rowCount;
      });
}

std::vector<uint64_t> fileRowOffsets(const SpillFiles& files) {
  std::vector<uint64_t> offsets;
  offsets.reserve(files.size() + 1);
  offsets.push_back(0);
  for (const auto& file : files) {
    offsets.push_back(offsets.back() + file.rowCount);
  }
  return offsets;
}

RowVectorPtr extractRows(
    const RowTypePtr& type,
    RowContainer& container,
    const char* const* rows,
    vector_size_t numRows,
    memory::MemoryPool* pool) {
  auto output = std::static_pointer_cast<RowVector>(
      BaseVector::create(type, numRows, pool));
  for (auto& child : output->children()) {
    child->resize(numRows);
  }
  for (column_index_t channel = 0; channel < type->size(); ++channel) {
    container.extractColumn(rows, numRows, channel, output->childAt(channel));
  }
  return output;
}

} // namespace

SpilledSortedRun::SpilledSortedRun(
    uint32_t id,
    SpillFiles files,
    memory::MemoryPool* pool,
    bool spillUringEnabled,
    size_t maxCachedBlocksPerFile)
    : id_(id),
      files_(std::move(files)),
      pool_(pool),
      spillUringEnabled_(spillUringEnabled),
      maxCachedBlocksPerFile_(std::max<size_t>(1, maxCachedBlocksPerFile)),
      numRows_(totalRows(files_)),
      fileRowOffsets_(fileRowOffsets(files_)),
      indexedReaders_(files_.size()) {
  BOLT_CHECK_NOT_NULL(pool_);
  validateFiles();
}

std::unique_ptr<SpilledSortedRun> SpilledSortedRun::create(
    uint32_t id,
    const RowTypePtr& type,
    RowContainer& container,
    const std::vector<char*>& sortedRows,
    const std::vector<CompareFlags>& sortCompareFlags,
    const common::SpillConfig::SpillIOConfig& ioConfig,
    uint64_t targetFileSize,
    memory::MemoryPool* pool,
    folly::Synchronized<common::SpillStats>* stats,
    uint32_t maxRowsPerBlock) {
  BOLT_CHECK_NOT_NULL(type);
  BOLT_CHECK_NOT_NULL(pool);
  BOLT_CHECK_NOT_NULL(stats);
  BOLT_CHECK_EQ(type->size(), container.columnTypes().size());
  BOLT_CHECK(
      sortCompareFlags.empty() ||
          sortCompareFlags.size() == container.keyTypes().size(),
      "Sort compare flags must be empty or match the number of key columns");

  SpillWriter writer(
      type,
      SpillState::makeSortingKeys(sortCompareFlags),
      fmt::format("{}/spilled-sorted-run-{}", ioConfig.getSpillDirPathCb(), id),
      targetFileSize,
      ioConfig,
      pool,
      stats);

  const auto rowsPerBlock =
      maxRowsPerBlock == 0 ? sortedRows.size() : maxRowsPerBlock;
  for (size_t offset = 0; offset < sortedRows.size(); offset += rowsPerBlock) {
    const auto batchSize =
        std::min<size_t>(rowsPerBlock, sortedRows.size() - offset);
    auto batch = extractRows(
        type,
        container,
        reinterpret_cast<const char* const*>(sortedRows.data() + offset),
        batchSize,
        pool);
    IndexRange range{0, batch->size()};
    writer.writeAndFlush(batch, folly::Range<IndexRange*>(&range, 1));
  }

  return std::make_unique<SpilledSortedRun>(
      id, writer.finish(), pool, ioConfig.spillUringEnabled);
}

bool SpilledSortedRun::hasIndexedBlocks() const {
  return std::all_of(files_.begin(), files_.end(), [](const auto& file) {
    return file.rowCount == 0 || !file.blocks.empty();
  });
}

bool SpilledSortedRun::nextBatch(RowVectorPtr& batch) {
  while (auto* reader = currentReader()) {
    if (reader->nextBatch(batch)) {
      return true;
    }
    reader_.reset();
    ++fileIndex_;
  }
  batch = nullptr;
  return false;
}

void SpilledSortedRun::resetSequentialReader() {
  reader_.reset();
  fileIndex_ = 0;
}

SpilledSortedRun::RowReference SpilledSortedRun::rowAt(uint64_t ordinal) {
  std::lock_guard<std::mutex> l(mutex_);
  const auto [fileIndex, fileOrdinal] = locateOrdinal(ordinal);
  auto row = indexedReader(fileIndex)->rowAt(fileOrdinal);
  return RowReference{std::move(row.batch), row.index, ordinal};
}

SpilledSortedRun::RowReference SpilledSortedRun::rowAt(
    uint64_t ordinal,
    std::vector<std::unique_ptr<IndexedSpillReadFile>>& indexedReaders,
    size_t maxCachedBlocksPerFile) const {
  const auto [fileIndex, fileOrdinal] = locateOrdinal(ordinal);
  auto row = indexedReader(fileIndex, indexedReaders, maxCachedBlocksPerFile)
                 ->rowAt(fileOrdinal);
  return RowReference{std::move(row.batch), row.index, ordinal};
}

int32_t SpilledSortedRun::compare(uint64_t leftOrdinal, uint64_t rightOrdinal) {
  const auto left = rowAt(leftOrdinal);
  const auto right = rowAt(rightOrdinal);
  return compareRowToExternal(left, right.batch, right.index);
}

void SpilledSortedRun::clearIndexedBlockCache() {
  std::lock_guard<std::mutex> l(mutex_);
  for (const auto& reader : indexedReaders_) {
    if (reader != nullptr) {
      reader->clearCache();
    }
  }
}

common::SpillReadStats SpilledSortedRun::spillReadStats() const {
  std::lock_guard<std::mutex> l(mutex_);
  common::SpillReadStats stats;
  if (reader_ != nullptr) {
    stats.spillReadIOTimeUs += reader_->getSpillReadIOTime();
  }
  for (const auto& reader : indexedReaders_) {
    if (reader != nullptr) {
      stats.spillReadIOTimeUs += reader->getSpillReadIOTime();
    }
  }
  stats.spillReadTimeUs = stats.spillReadIOTimeUs;
  return stats;
}

uint64_t SpilledSortedRun::lowerBound(
    const RowVectorPtr& external,
    vector_size_t externalRow) {
  uint64_t lower = 0;
  uint64_t upper = numRows_;
  while (lower < upper) {
    const auto mid = lower + (upper - lower) / 2;
    const auto row = rowAt(mid);
    if (compareRowToExternal(row, external, externalRow) < 0) {
      lower = mid + 1;
    } else {
      upper = mid;
    }
  }
  return lower;
}

size_t SpilledSortedRun::testingCachedBlocks() const {
  size_t cachedBlocks = 0;
  for (const auto& reader : indexedReaders_) {
    if (reader != nullptr) {
      cachedBlocks += reader->cachedBlocks();
    }
  }
  return cachedBlocks;
}

void SpilledSortedRun::validateFiles() const {
  for (const auto& file : files_) {
    uint64_t blockRows = 0;
    for (const auto& block : file.blocks) {
      BOLT_CHECK_LE(block.offset + block.size, file.size);
      BOLT_CHECK_EQ(block.rowOffset, blockRows);
      blockRows += block.rowCount;
    }
    BOLT_CHECK(
        file.blocks.empty() || blockRows == file.rowCount,
        "Indexed spill block row counts must sum to file row count");
  }
}

SpillReadFile* SpilledSortedRun::currentReader() {
  if (fileIndex_ >= files_.size()) {
    return nullptr;
  }
  if (reader_ == nullptr) {
    reader_ =
        SpillReadFile::create(files_[fileIndex_], pool_, spillUringEnabled_);
  }
  return reader_.get();
}

IndexedSpillReadFile* SpilledSortedRun::indexedReader(size_t fileIndex) {
  BOLT_CHECK_LT(fileIndex, files_.size());
  if (indexedReaders_[fileIndex] == nullptr) {
    indexedReaders_[fileIndex] = IndexedSpillReadFile::create(
        files_[fileIndex], pool_, maxCachedBlocksPerFile_);
  }
  return indexedReaders_[fileIndex].get();
}

IndexedSpillReadFile* SpilledSortedRun::indexedReader(
    size_t fileIndex,
    std::vector<std::unique_ptr<IndexedSpillReadFile>>& indexedReaders,
    size_t maxCachedBlocksPerFile) const {
  BOLT_CHECK_LT(fileIndex, files_.size());
  if (indexedReaders.empty()) {
    indexedReaders.resize(files_.size());
  }
  BOLT_CHECK_EQ(indexedReaders.size(), files_.size());
  if (indexedReaders[fileIndex] == nullptr) {
    indexedReaders[fileIndex] = IndexedSpillReadFile::create(
        files_[fileIndex], pool_, std::max<size_t>(1, maxCachedBlocksPerFile));
  }
  return indexedReaders[fileIndex].get();
}

std::pair<size_t, uint64_t> SpilledSortedRun::locateOrdinal(
    uint64_t ordinal) const {
  BOLT_CHECK_LT(ordinal, numRows_);
  auto it =
      std::upper_bound(fileRowOffsets_.begin(), fileRowOffsets_.end(), ordinal);
  BOLT_CHECK(it != fileRowOffsets_.begin());
  const auto fileIndex = (it - fileRowOffsets_.begin()) - 1;
  BOLT_CHECK_LT(fileIndex, files_.size());
  return {fileIndex, ordinal - fileRowOffsets_[fileIndex]};
}

int32_t SpilledSortedRun::compareRowToExternal(
    const RowReference& left,
    const RowVectorPtr& external,
    vector_size_t externalRow) const {
  BOLT_CHECK_NOT_NULL(left.batch);
  BOLT_CHECK_NOT_NULL(external);
  for (const auto& [channel, flags] : sortingKeys()) {
    auto result = left.batch->childAt(channel)->compare(
        external->childAt(channel).get(), left.index, externalRow, flags);
    BOLT_CHECK(result.has_value());
    if (result.value() != 0) {
      return result.value();
    }
  }
  return 0;
}

const std::vector<SpillSortKey>& SpilledSortedRun::sortingKeys() const {
  BOLT_CHECK(!files_.empty(), "SpilledSortedRun has no spill files");
  return files_[0].sortingKeys;
}

} // namespace bytedance::bolt::exec

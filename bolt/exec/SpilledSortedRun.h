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

#include <memory>
#include <mutex>
#include <vector>

#include "bolt/common/base/SpillConfig.h"
#include "bolt/common/base/SpillStats.h"
#include "bolt/exec/SortedRun.h"
#include "bolt/exec/SpillFile.h"

namespace bytedance::bolt::exec {

/// SortedRun backed by one or more sorted spill files.
///
/// Supports sequential readback plus indexed random row lookup over spill files
/// that include block metadata.
class SpilledSortedRun : public SortedRun {
 public:
  struct RowReference {
    RowVectorPtr batch;
    vector_size_t index{0};
    uint64_t ordinal{0};
  };

  SpilledSortedRun(
      uint32_t id,
      SpillFiles files,
      memory::MemoryPool* pool,
      bool spillUringEnabled = false,
      size_t maxCachedBlocksPerFile = 2);

  /// Spills already-sorted row pointers without re-sorting them. The row order
  /// in 'sortedRows' becomes the order of rows in the resulting spill files.
  static std::unique_ptr<SpilledSortedRun> create(
      uint32_t id,
      const RowTypePtr& type,
      RowContainer& container,
      const std::vector<char*>& sortedRows,
      const std::vector<CompareFlags>& sortCompareFlags,
      const common::SpillConfig::SpillIOConfig& ioConfig,
      uint64_t targetFileSize,
      memory::MemoryPool* pool,
      folly::Synchronized<common::SpillStats>* stats,
      uint32_t maxRowsPerBlock = 4'096);

  uint32_t id() const override {
    return id_;
  }

  uint64_t numRows() const override {
    return numRows_;
  }

  bool spilled() const override {
    return true;
  }

  const SpillFiles& files() const {
    return files_;
  }

  bool hasIndexedBlocks() const;

  /// Reads the next sequential row-vector batch from this run.
  bool nextBatch(RowVectorPtr& batch);

  /// Rewinds the sequential reader to the first spill file.
  void resetSequentialReader();

  /// Returns the row at the specified run ordinal using indexed spill block
  /// metadata. The returned RowReference keeps the block vector alive.
  RowReference rowAt(uint64_t ordinal);

  /// Returns the row at the specified run ordinal using caller-owned indexed
  /// spill readers. This lets parallel merge tasks read the same spilled run
  /// concurrently without contending on this run's shared reader mutex/cache.
  RowReference rowAt(
      uint64_t ordinal,
      std::vector<std::unique_ptr<IndexedSpillReadFile>>& indexedReaders,
      size_t maxCachedBlocksPerFile) const;

  /// Compares two rows in this run using the spill sorting keys.
  int32_t compare(uint64_t leftOrdinal, uint64_t rightOrdinal);

  /// Drops cached indexed spill blocks for this run. Use this after random
  /// boundary planning so execution starts with a small live spill-block set.
  void clearIndexedBlockCache();

  common::SpillReadStats spillReadStats() const;

  /// Finds the first row whose sort key is not less than 'externalRow' in
  /// 'external' using the spill sorting keys.
  uint64_t lowerBound(const RowVectorPtr& external, vector_size_t externalRow);

  size_t testingCachedBlocks() const;

 private:
  void validateFiles() const;
  SpillReadFile* currentReader();
  IndexedSpillReadFile* indexedReader(size_t fileIndex);
  IndexedSpillReadFile* indexedReader(
      size_t fileIndex,
      std::vector<std::unique_ptr<IndexedSpillReadFile>>& indexedReaders,
      size_t maxCachedBlocksPerFile) const;
  std::pair<size_t, uint64_t> locateOrdinal(uint64_t ordinal) const;
  int32_t compareRowToExternal(
      const RowReference& left,
      const RowVectorPtr& external,
      vector_size_t externalRow) const;
  const std::vector<SpillSortKey>& sortingKeys() const;

  const uint32_t id_;
  SpillFiles files_;
  memory::MemoryPool* const pool_;
  const bool spillUringEnabled_;
  const size_t maxCachedBlocksPerFile_;
  const uint64_t numRows_;
  std::vector<uint64_t> fileRowOffsets_;
  size_t fileIndex_{0};
  std::unique_ptr<SpillReadFile> reader_;
  std::vector<std::unique_ptr<IndexedSpillReadFile>> indexedReaders_;
  mutable std::mutex mutex_;
};

} // namespace bytedance::bolt::exec

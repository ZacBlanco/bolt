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

#include <cstdint>
#include <memory>
#include <vector>
#include "bolt/exec/HybridSorter.h"
#include "bolt/exec/RowContainer.h"
#include "bolt/vector/ComplexVector.h"

namespace bytedance::bolt::exec {

/// Internal total-order row position inside a SortedRun. The run id and ordinal
/// are used as tie-breakers for deterministic merge-path boundary planning.
struct SortedRunPosition {
  uint32_t runId{0};
  uint64_t ordinal{0};
};

/// Interface for an immutable internally sorted sequence of rows.
class SortedRun {
 public:
  virtual ~SortedRun() = default;

  virtual uint32_t id() const = 0;
  virtual uint64_t numRows() const = 0;
  virtual bool spilled() const = 0;
};

/// In-memory SortedRun backed by a RowContainer and a sorted row-pointer index.
/// The row pointers are non-owning aliases into 'container_', so the container
/// must outlive the pointer index and must not be cleared while the run exists.
class InMemorySortedRun : public SortedRun {
 public:
  InMemorySortedRun(
      uint32_t id,
      std::unique_ptr<RowContainer> container,
      std::vector<char*> sortedRows,
      std::vector<CompareFlags> sortCompareFlags)
      : id_(id),
        container_(std::move(container)),
        sortedRows_(std::move(sortedRows)),
        sortCompareFlags_(std::move(sortCompareFlags)) {
    BOLT_CHECK_NOT_NULL(container_);
    BOLT_CHECK_EQ(container_->numRows(), sortedRows_.size());
    BOLT_CHECK(
        sortCompareFlags_.empty() ||
            sortCompareFlags_.size() == container_->keyTypes().size(),
        "Sort compare flags must be empty or match the number of key columns");
  }

  /// Lists and sorts all rows in 'container' into an immutable in-memory run.
  /// This reuses the existing RowContainer row-pointer sorting shape from
  /// SortBuffer and the HybridSorter dispatch used by the legacy path.
  static std::unique_ptr<InMemorySortedRun> createSorted(
      uint32_t id,
      std::unique_ptr<RowContainer> container,
      std::vector<CompareFlags> sortCompareFlags,
      HybridSorter sorter = HybridSorter{},
      bool enableJit = false) {
    BOLT_CHECK_NOT_NULL(container);
    BOLT_CHECK(
        sortCompareFlags.empty() ||
            sortCompareFlags.size() == container->keyTypes().size(),
        "Sort compare flags must be empty or match the number of key columns");

    std::vector<char*> sortedRows(container->numRows());
    if (!sortedRows.empty()) {
      RowContainerIterator iter;
      container->listRows(&iter, sortedRows.size(), sortedRows.data());
#ifdef ENABLE_BOLT_JIT
      RowRowCompare cmp{nullptr};
      bolt::jit::CompiledModuleSP jitModule;
      if (enableJit && container->JITable(container->keyTypes())) {
        auto [module, rowRowCmpFn] = container->codegenCompare(
            container->keyTypes(),
            sortCompareFlags,
            bytedance::bolt::jit::CmpType::SORT_LESS,
            true);
        jitModule = std::move(module);
        cmp = (RowRowCompare)jitModule->getFuncPtr(rowRowCmpFn);
      }
      if (cmp != nullptr) {
        sorter.sort(
            sortedRows.begin(),
            sortedRows.end(),
            [cmp](const char* leftRow, const char* rightRow) {
              return cmp(leftRow, rightRow);
            });
      } else {
#endif
      sorter.sort(
          sortedRows.begin(),
          sortedRows.end(),
          [container = container.get(), &sortCompareFlags](
              const char* leftRow, const char* rightRow) {
            return container->compareRows(leftRow, rightRow, sortCompareFlags) <
                0;
          });
#ifdef ENABLE_BOLT_JIT
      }
#endif
    }

    return std::make_unique<InMemorySortedRun>(
        id,
        std::move(container),
        std::move(sortedRows),
        std::move(sortCompareFlags));
  }

  uint32_t id() const override {
    return id_;
  }

  uint64_t numRows() const override {
    return sortedRows_.size();
  }

  bool spilled() const override {
    return false;
  }

  const RowContainer& container() const {
    return *container_;
  }

  RowContainer& container() {
    return *container_;
  }

  const std::vector<char*>& sortedRows() const {
    return sortedRows_;
  }

  const char* rowAt(uint64_t ordinal) const {
    BOLT_CHECK_LT(ordinal, sortedRows_.size());
    return sortedRows_[ordinal];
  }

  int32_t compare(uint64_t leftOrdinal, uint64_t rightOrdinal) const {
    return container_->compareRows(
        rowAt(leftOrdinal), rowAt(rightOrdinal), sortCompareFlags_);
  }

  const std::vector<CompareFlags>& sortCompareFlags() const {
    return sortCompareFlags_;
  }

 private:
  const uint32_t id_;
  std::unique_ptr<RowContainer> container_;
  std::vector<char*> sortedRows_;
  std::vector<CompareFlags> sortCompareFlags_;
};

} // namespace bytedance::bolt::exec

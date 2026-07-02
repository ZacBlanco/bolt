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

#include "bolt/exec/SortBuffer.h"
#include <gtest/gtest.h>

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/common/file/FileSystems.h"
#include "bolt/core/QueryConfig.h"
#include "bolt/exec/ISortBuffer.h"
#include "bolt/exec/MergePath.h"
#include "bolt/exec/ParallelSortBuffer.h"
#include "bolt/exec/SortedRun.h"
#include "bolt/exec/SpilledSortedRun.h"
#include "bolt/exec/tests/utils/OperatorTestBase.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/type/Type.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::exec::test;
using namespace bytedance::bolt;
using namespace bytedance::bolt::memory;
namespace bytedance::bolt::functions::test {

class SortBufferTest : public OperatorTestBase {
 protected:
  void SetUp() override {
    filesystems::registerLocalFileSystem();
    if (!isRegisteredVectorSerde()) {
      this->registerVectorSerde();
    }
    rng_.seed(123);
  }

  void TearDown() override {
    pool_.reset();
    rootPool_.reset();
    OperatorTestBase::TearDown();
  }

  common::SpillConfig getSpillConfig(const std::string& spillDir) const {
    return common::SpillConfig(
        [&]() -> const std::string& { return spillDir; },
        [&](uint64_t) {},
        "0.0.0",
        0,
        false,
        0,
        executor_.get(),
        5,
        10,
        0,
        0,
        0,
        0,
        0,
        0,
        "none");
  }

  std::unique_ptr<InMemorySortedRun> makeBigintMemoryRun(
      uint32_t id,
      const std::vector<int64_t>& values) {
    auto container = std::make_unique<RowContainer>(
        std::vector<TypePtr>{BIGINT()},
        std::vector<TypePtr>{},
        true,
        pool_.get());
    container->store(makeRowVector({makeFlatVector<int64_t>(values)}));
    return InMemorySortedRun::createSorted(
        id,
        std::move(container),
        {{true, true, false, CompareFlags::NullHandlingMode::kNullAsValue}});
  }

  std::unique_ptr<SpilledSortedRun> makeBigintSpilledRun(
      uint32_t id,
      const std::vector<int64_t>& values,
      const std::string& spillDir,
      uint32_t maxRowsPerBlock = 2) {
    RowContainer container(
        std::vector<TypePtr>{BIGINT()},
        std::vector<TypePtr>{},
        true,
        pool_.get());
    container.store(makeRowVector({makeFlatVector<int64_t>(values)}));

    std::vector<char*> rows(container.numRows());
    RowContainerIterator iter;
    container.listRows(&iter, rows.size(), rows.data());

    common::SpillConfig::SpillIOConfig ioConfig{
        .getSpillDirPathCb = [&]() -> const std::string& { return spillDir; },
        .updateAndCheckSpillLimitCb = [&](uint64_t) {},
        .fileNamePrefix = "merge-path-test",
        .maxFileSize = 0,
        .spillUringEnabled = false,
        .writeBufferSize = 0,
        .compressionKind = common::CompressionKind::CompressionKind_NONE,
        .fileCreateConfig = "",
        .spillSerdeKind = std::optional<VectorSerde::Kind>{},
        .indexedSpillEnabled = true};
    return SpilledSortedRun::create(
        id,
        ROW({"c0"}, {BIGINT()}),
        container,
        rows,
        {{true, true, false, CompareFlags::NullHandlingMode::kNullAsValue}},
        ioConfig,
        1'000'000,
        pool_.get(),
        &spillStats_,
        maxRowsPerBlock);
  }

  std::unique_ptr<InMemorySortedRun> makeBigintPayloadMemoryRun(
      uint32_t id,
      const std::vector<int64_t>& keys) {
    auto container = std::make_unique<RowContainer>(
        std::vector<TypePtr>{BIGINT()},
        std::vector<TypePtr>{BIGINT()},
        true,
        pool_.get());
    std::vector<int64_t> payloads;
    payloads.reserve(keys.size());
    for (size_t ordinal = 0; ordinal < keys.size(); ++ordinal) {
      payloads.push_back(id * 1'000 + ordinal);
    }
    container->store(makeRowVector(
        {makeFlatVector<int64_t>(keys), makeFlatVector<int64_t>(payloads)}));

    std::vector<char*> rows(container->numRows());
    if (!rows.empty()) {
      RowContainerIterator iter;
      container->listRows(&iter, rows.size(), rows.data());
    }
    return std::make_unique<InMemorySortedRun>(
        id,
        std::move(container),
        std::move(rows),
        std::vector<CompareFlags>{
            {true, true, false, CompareFlags::NullHandlingMode::kNullAsValue}});
  }

  std::unique_ptr<SpilledSortedRun> makeBigintPayloadSpilledRun(
      uint32_t id,
      const std::vector<int64_t>& keys,
      const std::string& spillDir,
      uint32_t maxRowsPerBlock = 2) {
    RowContainer container(
        std::vector<TypePtr>{BIGINT()},
        std::vector<TypePtr>{BIGINT()},
        true,
        pool_.get());
    std::vector<int64_t> payloads;
    payloads.reserve(keys.size());
    for (size_t ordinal = 0; ordinal < keys.size(); ++ordinal) {
      payloads.push_back(id * 1'000 + ordinal);
    }
    container.store(makeRowVector(
        {makeFlatVector<int64_t>(keys), makeFlatVector<int64_t>(payloads)}));

    std::vector<char*> rows(container.numRows());
    if (!rows.empty()) {
      RowContainerIterator iter;
      container.listRows(&iter, rows.size(), rows.data());
    }

    common::SpillConfig::SpillIOConfig ioConfig{
        .getSpillDirPathCb = [&]() -> const std::string& { return spillDir; },
        .updateAndCheckSpillLimitCb = [&](uint64_t) {},
        .fileNamePrefix = "merge-path-payload-test",
        .maxFileSize = 0,
        .spillUringEnabled = false,
        .writeBufferSize = 0,
        .compressionKind = common::CompressionKind::CompressionKind_NONE,
        .fileCreateConfig = "",
        .spillSerdeKind = std::optional<VectorSerde::Kind>{},
        .indexedSpillEnabled = true};
    return SpilledSortedRun::create(
        id,
        ROW({"c0", "payload"}, {BIGINT(), BIGINT()}),
        container,
        rows,
        {{true, true, false, CompareFlags::NullHandlingMode::kNullAsValue}},
        ioConfig,
        1'000'000,
        pool_.get(),
        &spillStats_,
        maxRowsPerBlock);
  }

  const RowTypePtr inputType_ = ROW(
      {{"c0", BIGINT()},
       {"c1", INTEGER()},
       {"c2", SMALLINT()},
       {"c3", REAL()},
       {"c4", DOUBLE()},
       {"c5", VARCHAR()}});
  // Specifies the sort columns ["c4", "c1"].
  std::vector<column_index_t> sortColumnIndices_{4, 1};
  std::vector<CompareFlags> sortCompareFlags_{
      {true, true, false, CompareFlags::NullHandlingMode::kNullAsValue},
      {true, true, false, CompareFlags::NullHandlingMode::kNullAsValue}};

  const std::shared_ptr<folly::Executor> executor_{
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency())};

  folly::Synchronized<common::SpillStats> spillStats_;
  tsan_atomic<bool> nonReclaimableSection_{false};
  folly::Random::DefaultGenerator rng_;
};

namespace {

struct MergePathEntry {
  int64_t key;
  uint32_t runId;
  uint64_t ordinal;

  bool operator==(const MergePathEntry& other) const {
    return key == other.key && runId == other.runId && ordinal == other.ordinal;
  }
};

bool mergePathEntryLess(
    const MergePathEntry& left,
    const MergePathEntry& right) {
  return std::tie(left.key, left.runId, left.ordinal) <
      std::tie(right.key, right.runId, right.ordinal);
}

std::vector<MergePathEntry> mergedReference(
    const std::vector<std::vector<int64_t>>& runValues,
    const std::vector<uint32_t>& runIds) {
  std::vector<MergePathEntry> reference;
  for (size_t runIndex = 0; runIndex < runValues.size(); ++runIndex) {
    for (size_t ordinal = 0; ordinal < runValues[runIndex].size(); ++ordinal) {
      reference.push_back(MergePathEntry{
          .key = runValues[runIndex][ordinal],
          .runId = runIds[runIndex],
          .ordinal = ordinal});
    }
  }
  std::sort(reference.begin(), reference.end(), mergePathEntryLess);
  return reference;
}

void assertMergeTasksMatchReference(
    const std::vector<MergeTask>& tasks,
    const std::vector<std::vector<int64_t>>& runValues,
    const std::vector<uint32_t>& runIds,
    uint64_t targetRowsPerTask) {
  const auto reference = mergedReference(runValues, runIds);
  std::vector<uint64_t> previousOffsets(runValues.size(), 0);
  uint64_t expectedOutputBegin = 0;

  for (const auto& task : tasks) {
    ASSERT_EQ(task.outputBegin, expectedOutputBegin);
    ASSERT_LE(task.outputEnd, reference.size());
    ASSERT_EQ(task.outputSize(), task.outputEnd - task.outputBegin);
    if (task.outputEnd != reference.size()) {
      ASSERT_EQ(task.outputSize(), targetRowsPerTask);
    }

    uint64_t sliceRows = 0;
    std::vector<MergePathEntry> actualTaskRows;
    for (const auto& slice : task.slices) {
      ASSERT_LT(slice.runIndex, runValues.size());
      ASSERT_EQ(slice.runId, runIds[slice.runIndex]);
      ASSERT_EQ(slice.begin, previousOffsets[slice.runIndex]);
      ASSERT_LE(slice.end, runValues[slice.runIndex].size());
      previousOffsets[slice.runIndex] = slice.end;
      sliceRows += slice.size();
      for (auto ordinal = slice.begin; ordinal < slice.end; ++ordinal) {
        actualTaskRows.push_back(MergePathEntry{
            .key = runValues[slice.runIndex][ordinal],
            .runId = slice.runId,
            .ordinal = ordinal});
      }
    }
    ASSERT_EQ(sliceRows, task.outputSize());
    std::sort(actualTaskRows.begin(), actualTaskRows.end(), mergePathEntryLess);

    const auto expectedBegin = reference.begin() + task.outputBegin;
    const auto expectedEnd = reference.begin() + task.outputEnd;
    ASSERT_EQ(
        actualTaskRows,
        std::vector<MergePathEntry>(expectedBegin, expectedEnd));
    expectedOutputBegin = task.outputEnd;
  }

  ASSERT_EQ(expectedOutputBegin, reference.size());
  for (size_t runIndex = 0; runIndex < runValues.size(); ++runIndex) {
    ASSERT_EQ(previousOffsets[runIndex], runValues[runIndex].size());
  }
}

} // namespace

TEST_F(SortBufferTest, singleKey) {
  struct {
    std::vector<CompareFlags> sortCompareFlags;
    std::vector<int32_t> expectedResult;
    bool hybridSortEnabled;

    std::string debugString() const {
      const std::string expectedResultStr = folly::join(",", expectedResult);
      std::stringstream sortCompareFlagsStr;
      for (const auto sortCompareFlag : sortCompareFlags) {
        sortCompareFlagsStr << sortCompareFlag.toString();
      }
      return fmt::format(
          "sortCompareFlags:{}, expectedResult:{}, hybridSortEnabled:{}",
          sortCompareFlagsStr.str(),
          expectedResultStr,
          hybridSortEnabled);
    }
  } testSettings[] = {
      {{{true,
         true,
         false,
         CompareFlags::NullHandlingMode::kNullAsValue}}, // Ascending
       {1, 2, 3, 4, 5},
       false},
      {{{true,
         true,
         false,
         CompareFlags::NullHandlingMode::kNullAsValue}}, // Ascending with
                                                         // hybrid
       {1, 2, 3, 4, 5},
       true},
      {{{true,
         false,
         false,
         CompareFlags::NullHandlingMode::kNullAsValue}}, // Descending
       {5, 4, 3, 2, 1},
       false},
      {{{true,
         false,
         false,
         CompareFlags::NullHandlingMode::kNullAsValue}}, // Descending with
                                                         // hybrid
       {5, 4, 3, 2, 1},
       true}};

  // Specifies the sort columns ["c1"].
  sortColumnIndices_ = {1};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    auto sortBuffer = std::make_unique<SortBuffer>(
        inputType_,
        sortColumnIndices_,
        testData.sortCompareFlags,
        pool_.get(),
        &nonReclaimableSection_,
        nullptr,
        0,
        nullptr,
        testData.hybridSortEnabled);

    RowVectorPtr data = makeRowVector(
        {makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
         makeFlatVector<int32_t>({5, 4, 3, 2, 1}), // sorted column
         makeFlatVector<int16_t>({1, 2, 3, 4, 5}),
         makeFlatVector<float>({1.1, 2.2, 3.3, 4.4, 5.5}),
         makeFlatVector<double>({1.1, 2.2, 2.2, 5.5, 5.5}),
         makeFlatVector<std::string>(
             {"hello", "world", "today", "is", "great"})});

    sortBuffer->addInput(data);
    sortBuffer->noMoreInput();
    auto output = sortBuffer->getOutput(10000);
    ASSERT_EQ(output->size(), 5);
    int resultIndex = 0;
    for (int expectedValue : testData.expectedResult) {
      ASSERT_EQ(
          output->childAt(1)->asFlatVector<int32_t>()->valueAt(resultIndex++),
          expectedValue);
    }
  }
}

TEST_F(SortBufferTest, parallelSortQueryConfigDefaults) {
  const core::QueryConfig config({});

  ASSERT_FALSE(config.orderByParallelSortEnabled());
  ASSERT_EQ(0, config.orderByParallelMergeThreads());
  ASSERT_EQ(0, config.orderByParallelMergeTargetRows());
  ASSERT_EQ(2, config.orderByParallelMergeLookaheadTasks());
  ASSERT_EQ(4UL << 10, config.orderByIndexedSpillBlockRows());
  ASSERT_EQ(1UL << 20, config.orderByIndexedSpillBlockBytes());
}

TEST_F(SortBufferTest, parallelSortQueryConfigOverrides) {
  const core::QueryConfig config({
      {core::QueryConfig::kOrderByParallelSortEnabled, "true"},
      {core::QueryConfig::kOrderByParallelMergeThreads, "7"},
      {core::QueryConfig::kOrderByParallelMergeTargetRows, "12345"},
      {core::QueryConfig::kOrderByParallelMergeLookaheadTasks, "3"},
      {core::QueryConfig::kOrderByIndexedSpillBlockRows, "321"},
      {core::QueryConfig::kOrderByIndexedSpillBlockBytes, "654321"},
  });

  ASSERT_TRUE(config.orderByParallelSortEnabled());
  ASSERT_EQ(7, config.orderByParallelMergeThreads());
  ASSERT_EQ(12345, config.orderByParallelMergeTargetRows());
  ASSERT_EQ(3, config.orderByParallelMergeLookaheadTasks());
  ASSERT_EQ(321, config.orderByIndexedSpillBlockRows());
  ASSERT_EQ(654321, config.orderByIndexedSpillBlockBytes());
}

TEST_F(SortBufferTest, legacySortBufferImplementsISortBuffer) {
  auto sortBuffer = std::make_unique<SortBuffer>(
      inputType_,
      sortColumnIndices_,
      sortCompareFlags_,
      pool_.get(),
      &nonReclaimableSection_);

  ISortBuffer* interface = sortBuffer.get();
  ASSERT_EQ(0, interface->numInputRows());
  ASSERT_EQ(0, interface->numOutputRows());
  ASSERT_EQ(std::nullopt, interface->estimateOutputRowSize());
}

TEST_F(SortBufferTest, sortedRunPositionDefaults) {
  const SortedRunPosition position;
  ASSERT_EQ(0, position.runId);
  ASSERT_EQ(0, position.ordinal);
}

TEST_F(SortBufferTest, inMemorySortedRunEmpty) {
  auto container = std::make_unique<RowContainer>(
      std::vector<TypePtr>{INTEGER()},
      std::vector<TypePtr>{VARCHAR()},
      true,
      pool_.get());

  auto run = InMemorySortedRun::createSorted(
      11,
      std::move(container),
      {{true, true, false, CompareFlags::NullHandlingMode::kNullAsValue}});

  ASSERT_EQ(11, run->id());
  ASSERT_EQ(0, run->numRows());
  ASSERT_FALSE(run->spilled());
  ASSERT_TRUE(run->sortedRows().empty());
}

TEST_F(SortBufferTest, inMemorySortedRunSortsRows) {
  auto container = std::make_unique<RowContainer>(
      std::vector<TypePtr>{INTEGER()},
      std::vector<TypePtr>{VARCHAR()},
      true,
      pool_.get());
  container->store(makeRowVector(
      {makeFlatVector<int32_t>({3, 1, 4, 2}),
       makeFlatVector<std::string>({"three", "one", "four", "two"})}));

  auto run = InMemorySortedRun::createSorted(
      17,
      std::move(container),
      {{true, true, false, CompareFlags::NullHandlingMode::kNullAsValue}});

  ASSERT_EQ(17, run->id());
  ASSERT_EQ(4, run->numRows());
  ASSERT_FALSE(run->spilled());
  ASSERT_EQ(4, run->sortedRows().size());
  ASSERT_LE(run->compare(0, 1), 0);
  ASSERT_LE(run->compare(1, 2), 0);
  ASSERT_LE(run->compare(2, 3), 0);

  auto output = BaseVector::create(INTEGER(), run->numRows(), pool_.get());
  run->container().extractColumn(
      run->sortedRows().data(), run->numRows(), 0, output);
  auto flatOutput = output->asFlatVector<int32_t>();
  ASSERT_EQ(1, flatOutput->valueAt(0));
  ASSERT_EQ(2, flatOutput->valueAt(1));
  ASSERT_EQ(3, flatOutput->valueAt(2));
  ASSERT_EQ(4, flatOutput->valueAt(3));
}

TEST_F(SortBufferTest, parallelSortBufferSerialMergeMultipleRuns) {
  ParallelSortBuffer sortBuffer(
      inputType_,
      {1},
      {{true, true, false, CompareFlags::NullHandlingMode::kNullAsValue}},
      pool_.get());

  sortBuffer.addInput(makeRowVector(
      {makeFlatVector<int64_t>({1, 2, 3}),
       makeFlatVector<int32_t>({6, 2, 4}),
       makeFlatVector<int16_t>({1, 2, 3}),
       makeFlatVector<float>({1.1, 2.2, 3.3}),
       makeFlatVector<double>({1.1, 2.2, 3.3}),
       makeFlatVector<std::string>({"six", "two", "four"})}));
  sortBuffer.addInput(makeRowVector(
      {makeFlatVector<int64_t>({4, 5, 6}),
       makeFlatVector<int32_t>({5, 1, 3}),
       makeFlatVector<int16_t>({4, 5, 6}),
       makeFlatVector<float>({4.4, 5.5, 6.6}),
       makeFlatVector<double>({4.4, 5.5, 6.6}),
       makeFlatVector<std::string>({"five", "one", "three"})}));
  sortBuffer.noMoreInput();

  ASSERT_EQ(1, sortBuffer.numRuns());
  ASSERT_EQ(6, sortBuffer.numInputRows());
  ASSERT_NE(std::nullopt, sortBuffer.estimateOutputRowSize());

  std::vector<int32_t> results;
  while (auto output = sortBuffer.getOutput(2)) {
    auto values = output->childAt(1)->asFlatVector<int32_t>();
    for (auto i = 0; i < output->size(); ++i) {
      results.push_back(values->valueAt(i));
    }
  }

  ASSERT_EQ(results, std::vector<int32_t>({1, 2, 3, 4, 5, 6}));
  ASSERT_EQ(6, sortBuffer.numOutputRows());
  ASSERT_EQ(std::nullopt, sortBuffer.spilledStats());
  ASSERT_EQ(std::nullopt, sortBuffer.spillReadStats());
}

TEST_F(SortBufferTest, MergePathBoundaryPlannerTwoRuns) {
  std::vector<std::unique_ptr<InMemorySortedRun>> ownedRuns;
  ownedRuns.push_back(makeBigintMemoryRun(0, {1, 3, 5, 7}));
  ownedRuns.push_back(makeBigintMemoryRun(1, {2, 4, 6, 8}));

  std::vector<SortedRun*> runs{ownedRuns[0].get(), ownedRuns[1].get()};
  MergePathBoundaryPlanner planner(runs, pool_.get());
  const auto tasks = planner.plan(3);

  assertMergeTasksMatchReference(
      tasks, {{1, 3, 5, 7}, {2, 4, 6, 8}}, {0, 1}, 3);
}

TEST_F(SortBufferTest, MergePathBoundaryPlannerDuplicateHeavyAndAllEqual) {
  struct Scenario {
    std::vector<std::vector<int64_t>> runs;
    uint64_t targetRowsPerTask;
  } scenarios[] = {
      {{{1, 1, 1, 1, 1}, {2, 2, 2, 2, 2}}, 3},
      {{{1, 1, 1, 1}, {1, 1, 1, 1}, {1, 1, 1, 1}}, 5},
      {{{1, 1, 2, 2, 2}, {1, 1, 1, 3}, {2, 2, 2, 2}}, 4}};

  for (const auto& scenario : scenarios) {
    std::vector<std::unique_ptr<InMemorySortedRun>> ownedRuns;
    std::vector<SortedRun*> runs;
    std::vector<uint32_t> runIds;
    for (size_t i = 0; i < scenario.runs.size(); ++i) {
      ownedRuns.push_back(makeBigintMemoryRun(i, scenario.runs[i]));
      runs.push_back(ownedRuns.back().get());
      runIds.push_back(i);
    }

    MergePathBoundaryPlanner planner(runs, pool_.get());
    const auto tasks = planner.plan(scenario.targetRowsPerTask);
    assertMergeTasksMatchReference(
        tasks, scenario.runs, runIds, scenario.targetRowsPerTask);
  }
}

TEST_F(SortBufferTest, MergePathBoundaryPlannerSkewedAndEmptyRuns) {
  std::vector<std::vector<int64_t>> values = {
      {}, {1}, {2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}, {}, {13, 14}};
  std::vector<std::unique_ptr<InMemorySortedRun>> ownedRuns;
  std::vector<SortedRun*> runs;
  std::vector<uint32_t> runIds;
  for (size_t i = 0; i < values.size(); ++i) {
    ownedRuns.push_back(makeBigintMemoryRun(i, values[i]));
    runs.push_back(ownedRuns.back().get());
    runIds.push_back(i);
  }

  MergePathBoundaryPlanner planner(runs, pool_.get());
  const auto tasks = planner.plan(4);
  assertMergeTasksMatchReference(tasks, values, runIds, 4);
}

TEST_F(SortBufferTest, MergePathBoundaryPlannerMixedMemoryAndSpilledRuns) {
  auto tempDirectory = exec::test::TempDirectoryPath::create();
  std::vector<std::vector<int64_t>> values = {
      {1, 1, 1, 4}, {1, 2, 2, 8}, {3, 3, 3, 3, 9}};

  auto memoryRun0 = makeBigintMemoryRun(0, values[0]);
  auto spilledRun1 = makeBigintSpilledRun(1, values[1], tempDirectory->path, 2);
  auto memoryRun2 = makeBigintMemoryRun(2, values[2]);
  std::vector<SortedRun*> runs{
      memoryRun0.get(), spilledRun1.get(), memoryRun2.get()};

  MergePathBoundaryPlanner planner(runs, pool_.get());
  const auto tasks = planner.plan(3);
  assertMergeTasksMatchReference(tasks, values, {0, 1, 2}, 3);
}

TEST_F(SortBufferTest, MergePathBoundaryPlannerRandomizedCornerCases) {
  for (int iteration = 0; iteration < 100; ++iteration) {
    const auto numRuns = 1 + folly::Random::rand32(6, rng_);
    std::vector<std::vector<int64_t>> values(numRuns);
    for (auto& run : values) {
      const auto runSize = folly::Random::rand32(16, rng_);
      run.reserve(runSize);
      for (auto row = 0; row < runSize; ++row) {
        // Small value domain intentionally creates duplicate-heavy and
        // all-equal cases while the random run sizes cover empty and skewed
        // runs.
        run.push_back(folly::Random::rand32(4, rng_));
      }
      std::sort(run.begin(), run.end());
    }

    std::vector<std::unique_ptr<InMemorySortedRun>> ownedRuns;
    std::vector<SortedRun*> runs;
    std::vector<uint32_t> runIds;
    for (size_t i = 0; i < values.size(); ++i) {
      ownedRuns.push_back(makeBigintMemoryRun(i, values[i]));
      runs.push_back(ownedRuns.back().get());
      runIds.push_back(i);
    }

    MergePathBoundaryPlanner planner(runs, pool_.get());
    const auto targetRowsPerTask = 1 + folly::Random::rand32(10, rng_);
    const auto tasks = planner.plan(targetRowsPerTask);
    assertMergeTasksMatchReference(tasks, values, runIds, targetRowsPerTask);
  }
}

TEST_F(SortBufferTest, RunSliceReaderReadsBoundedSlices) {
  auto tempDirectory = exec::test::TempDirectoryPath::create();
  auto memoryRun = makeBigintMemoryRun(3, {1, 2, 3, 4});
  auto spilledRun =
      makeBigintSpilledRun(4, {10, 20, 30, 40, 50}, tempDirectory->path, 2);
  const auto outputType = ROW({"c0"}, {BIGINT()});

  RunSliceReader memoryReader(
      memoryRun.get(),
      MergeRunSlice{.runIndex = 0, .runId = 3, .begin = 1, .end = 3});
  auto memoryOutput = std::static_pointer_cast<RowVector>(
      BaseVector::create(outputType, 2, pool_.get()));
  memoryOutput->childAt(0)->resize(2);
  ASSERT_TRUE(memoryReader.hasNext());
  ASSERT_EQ(memoryReader.currentOrdinal(), 1);
  memoryReader.copyCurrentRowTo(memoryOutput, 0);
  memoryReader.advance();
  ASSERT_EQ(memoryReader.rowsRead(), 1);
  memoryReader.copyCurrentRowTo(memoryOutput, 1);
  memoryReader.advance();
  ASSERT_FALSE(memoryReader.hasNext());
  auto memoryValues = memoryOutput->childAt(0)->asFlatVector<int64_t>();
  ASSERT_EQ(memoryValues->valueAt(0), 2);
  ASSERT_EQ(memoryValues->valueAt(1), 3);

  RunSliceReader spilledReader(
      spilledRun.get(),
      MergeRunSlice{.runIndex = 1, .runId = 4, .begin = 2, .end = 4});
  auto spilledOutput = std::static_pointer_cast<RowVector>(
      BaseVector::create(outputType, 2, pool_.get()));
  spilledOutput->childAt(0)->resize(2);
  spilledReader.copyCurrentRowTo(spilledOutput, 0);
  spilledReader.advance();
  spilledReader.copyCurrentRowTo(spilledOutput, 1);
  spilledReader.advance();
  ASSERT_EQ(spilledReader.rowsRead(), 2);
  ASSERT_FALSE(spilledReader.hasNext());
  auto spilledValues = spilledOutput->childAt(0)->asFlatVector<int64_t>();
  ASSERT_EQ(spilledValues->valueAt(0), 30);
  ASSERT_EQ(spilledValues->valueAt(1), 40);
}

TEST_F(SortBufferTest, parallelSortBufferParallelMergeEndToEnd) {
  ParallelSortBuffer sortBuffer(
      ROW({"key", "payload"}, {BIGINT(), BIGINT()}),
      {0},
      {{true, true, false, CompareFlags::NullHandlingMode::kNullAsValue}},
      pool_.get(),
      3,
      2,
      executor_.get());

  sortBuffer.addInput(makeRowVector(
      {makeFlatVector<int64_t>({5, 1, 3}),
       makeFlatVector<int64_t>({50, 10, 30})}));
  sortBuffer.addInput(makeRowVector(
      {makeFlatVector<int64_t>({2, 6, 4}),
       makeFlatVector<int64_t>({20, 60, 40})}));
  sortBuffer.addInput(makeRowVector(
      {makeFlatVector<int64_t>({0, 7}), makeFlatVector<int64_t>({0, 70})}));
  sortBuffer.noMoreInput();

  std::vector<int64_t> keys;
  std::vector<int64_t> payloads;
  std::vector<vector_size_t> maxOutputRowsSequence{1, 2, 5};
  size_t sequenceIndex = 0;
  while (auto output = sortBuffer.getOutput(
             maxOutputRowsSequence
                 [sequenceIndex++ % maxOutputRowsSequence.size()])) {
    auto outputKeys = output->childAt(0)->asFlatVector<int64_t>();
    auto outputPayloads = output->childAt(1)->asFlatVector<int64_t>();
    for (auto row = 0; row < output->size(); ++row) {
      keys.push_back(outputKeys->valueAt(row));
      payloads.push_back(outputPayloads->valueAt(row));
    }
  }

  ASSERT_EQ(keys, std::vector<int64_t>({0, 1, 2, 3, 4, 5, 6, 7}));
  ASSERT_EQ(payloads, std::vector<int64_t>({0, 10, 20, 30, 40, 50, 60, 70}));
  ASSERT_EQ(sortBuffer.numInputRows(), 8);
  ASSERT_EQ(sortBuffer.numOutputRows(), 8);
}

TEST_F(SortBufferTest, parallelSortBufferParallelMergeDuplicateHeavy) {
  ParallelSortBuffer sortBuffer(
      ROW({"key", "payload"}, {BIGINT(), BIGINT()}),
      {0},
      {{true, true, false, CompareFlags::NullHandlingMode::kNullAsValue}},
      pool_.get(),
      2,
      2,
      executor_.get());

  sortBuffer.addInput(makeRowVector(
      {makeFlatVector<int64_t>({1, 1, 1}),
       makeFlatVector<int64_t>({0, 1, 2})}));
  sortBuffer.addInput(makeRowVector(
      {makeFlatVector<int64_t>({1, 1}),
       makeFlatVector<int64_t>({1'000, 1'001})}));
  sortBuffer.addInput(makeRowVector(
      {makeFlatVector<int64_t>({1, 1, 1, 1}),
       makeFlatVector<int64_t>({2'000, 2'001, 2'002, 2'003})}));
  sortBuffer.noMoreInput();

  std::vector<int64_t> keys;
  std::vector<int64_t> payloads;
  while (auto output = sortBuffer.getOutput(1)) {
    auto outputKeys = output->childAt(0)->asFlatVector<int64_t>();
    auto outputPayloads = output->childAt(1)->asFlatVector<int64_t>();
    for (auto row = 0; row < output->size(); ++row) {
      keys.push_back(outputKeys->valueAt(row));
      payloads.push_back(outputPayloads->valueAt(row));
    }
  }

  ASSERT_EQ(keys, std::vector<int64_t>({1, 1, 1, 1, 1, 1, 1, 1, 1}));
  ASSERT_EQ(
      payloads,
      std::vector<int64_t>(
          {0, 1, 2, 1'000, 1'001, 2'000, 2'001, 2'002, 2'003}));
  ASSERT_EQ(sortBuffer.numInputRows(), 9);
  ASSERT_EQ(sortBuffer.numOutputRows(), 9);
}

TEST_F(SortBufferTest, multipleKeys) {
  struct {
    bool hybridSortEnabled;

    std::string debugString() const {
      return fmt::format("hybridSortEnabled:{}", hybridSortEnabled);
    }
  } testSettings[] = {{false}, {true}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    auto sortBuffer = std::make_unique<SortBuffer>(
        inputType_,
        sortColumnIndices_,
        sortCompareFlags_,
        pool_.get(),
        &nonReclaimableSection_,
        nullptr,
        0,
        nullptr,
        testData.hybridSortEnabled);

    RowVectorPtr data = makeRowVector(
        {makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
         makeFlatVector<int32_t>({5, 4, 3, 2, 1}), // sorted-2 column
         makeFlatVector<int16_t>({1, 2, 3, 4, 5}),
         makeFlatVector<float>({1.1, 2.2, 3.3, 4.4, 5.5}),
         makeFlatVector<double>({1.1, 2.2, 2.2, 5.5, 5.5}), // sorted-1 column
         makeFlatVector<std::string>(
             {"hello", "world", "today", "is", "great"})});

    sortBuffer->addInput(data);
    sortBuffer->noMoreInput();
    auto output = sortBuffer->getOutput(10000);
    ASSERT_EQ(output->size(), 5);
    ASSERT_EQ(output->childAt(1)->asFlatVector<int32_t>()->valueAt(0), 5);
    ASSERT_EQ(output->childAt(1)->asFlatVector<int32_t>()->valueAt(1), 3);
    ASSERT_EQ(output->childAt(1)->asFlatVector<int32_t>()->valueAt(2), 4);
    ASSERT_EQ(output->childAt(1)->asFlatVector<int32_t>()->valueAt(3), 1);
    ASSERT_EQ(output->childAt(1)->asFlatVector<int32_t>()->valueAt(4), 2);
  }
}

// TODO: enable it later with test utility to compare the sorted result.
TEST_F(SortBufferTest, DISABLED_randomData) {
  struct {
    RowTypePtr inputType;
    std::vector<column_index_t> sortColumnIndices;
    std::vector<CompareFlags> sortCompareFlags;
    bool hybridSortEnabled;

    std::string debugString() const {
      const std::string sortColumnIndicesStr =
          folly::join(",", sortColumnIndices);
      std::stringstream sortCompareFlagsStr;
      for (auto sortCompareFlag : sortCompareFlags) {
        sortCompareFlagsStr << sortCompareFlag.toString() << ";";
      }
      return fmt::format(
          "inputType:{}, sortColumnIndices:{}, sortCompareFlags:{}, hybridSortEnabled:{}",
          inputType,
          sortColumnIndicesStr,
          sortCompareFlagsStr.str(),
          hybridSortEnabled);
    }
  } testSettings[] = {
      {ROW(
           {{"c0", BIGINT()},
            {"c1", INTEGER()},
            {"c2", SMALLINT()},
            {"c3", REAL()},
            {"c4", DOUBLE()},
            {"c5", VARCHAR()}}),
       {2},
       {{true, true, false, CompareFlags::NullHandlingMode::kNullAsValue}},
       false},
      {ROW(
           {{"c0", BIGINT()},
            {"c1", INTEGER()},
            {"c2", SMALLINT()},
            {"c3", REAL()},
            {"c4", DOUBLE()},
            {"c5", VARCHAR()}}),
       {2},
       {{true, true, false, CompareFlags::NullHandlingMode::kNullAsValue}},
       true},
      {ROW(
           {{"c0", BIGINT()},
            {"c1", INTEGER()},
            {"c2", SMALLINT()},
            {"c3", REAL()},
            {"c4", DOUBLE()},
            {"c5", VARCHAR()}}),
       {4, 1},
       {{true, true, false, CompareFlags::NullHandlingMode::kNullAsValue},
        {true, true, false, CompareFlags::NullHandlingMode::kNullAsValue}},
       false},
      {ROW(
           {{"c0", BIGINT()},
            {"c1", INTEGER()},
            {"c2", SMALLINT()},
            {"c3", REAL()},
            {"c4", DOUBLE()},
            {"c5", VARCHAR()}}),
       {4, 1},
       {{true, true, false, CompareFlags::NullHandlingMode::kNullAsValue},
        {true, true, false, CompareFlags::NullHandlingMode::kNullAsValue}},
       true},
      {ROW(
           {{"c0", BIGINT()},
            {"c1", INTEGER()},
            {"c2", SMALLINT()},
            {"c3", REAL()},
            {"c4", DOUBLE()},
            {"c5", VARCHAR()}}),
       {4, 1},
       {{true, true, false, CompareFlags::NullHandlingMode::kNullAsValue},
        {false, false, false, CompareFlags::NullHandlingMode::kNullAsValue}},
       false},
      {ROW(
           {{"c0", BIGINT()},
            {"c1", INTEGER()},
            {"c2", SMALLINT()},
            {"c3", REAL()},
            {"c4", DOUBLE()},
            {"c5", VARCHAR()}}),
       {4, 1},
       {{true, true, false, CompareFlags::NullHandlingMode::kNullAsValue},
        {false, false, false, CompareFlags::NullHandlingMode::kNullAsValue}},
       true}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    auto sortBuffer = std::make_unique<SortBuffer>(
        testData.inputType,
        testData.sortColumnIndices,
        testData.sortCompareFlags,
        pool_.get(),
        &nonReclaimableSection_,
        nullptr,
        0,
        nullptr,
        testData.hybridSortEnabled);

    const std::shared_ptr<memory::MemoryPool> fuzzerPool =
        memory::memoryManager()->addLeafPool("VectorFuzzer");

    std::vector<RowVectorPtr> inputVectors;
    inputVectors.reserve(3);
    for (size_t inputRows : {1000, 1000, 1000}) {
      VectorFuzzer fuzzer({.vectorSize = inputRows}, fuzzerPool.get());
      RowVectorPtr input = fuzzer.fuzzRow(inputType_);
      sortBuffer->addInput(input);
      inputVectors.push_back(input);
    }
    sortBuffer->noMoreInput();
    // todo: have a utility function buildExpectedSortResult and verify the
    // sorting result for random data.
  }
}

TEST_F(SortBufferTest, batchOutput) {
  struct {
    bool triggerSpill;
    std::vector<size_t> numInputRows;
    size_t maxOutputRows;
    std::vector<size_t> expectedOutputRowCount;
    bool hybridSortEnabled;

    std::string debugString() const {
      const std::string numInputRowsStr = folly::join(",", numInputRows);
      const std::string expectedOutputRowCountStr =
          folly::join(",", expectedOutputRowCount);
      return fmt::format(
          "triggerSpill:{}, numInputRows:{}, maxOutputRows:{}, expectedOutputRowCount:{}, hybridSortEnabled:{}",
          triggerSpill,
          numInputRowsStr,
          maxOutputRows,
          expectedOutputRowCountStr,
          hybridSortEnabled);
    }
  } testSettings[] = {
      {false, {2, 3, 3}, 1, {1, 1, 1, 1, 1, 1, 1, 1}, false},
      {false, {2, 3, 3}, 1, {1, 1, 1, 1, 1, 1, 1, 1}, true},
      {true, {2, 3, 3}, 1, {1, 1, 1, 1, 1, 1, 1, 1}, false},
      {true, {2, 3, 3}, 1, {1, 1, 1, 1, 1, 1, 1, 1}, true},
      {false, {2000, 2000}, 10000, {4000}, false},
      {false, {2000, 2000}, 10000, {4000}, true},
      {true, {2000, 2000}, 10000, {4000}, false},
      {true, {2000, 2000}, 10000, {4000}, true},
      {false, {2000, 2000}, 2000, {2000, 2000}, false},
      {false, {2000, 2000}, 2000, {2000, 2000}, true},
      {true, {2000, 2000}, 2000, {2000, 2000}, false},
      {true, {2000, 2000}, 2000, {2000, 2000}, true},
      {false, {1024, 1024, 1024}, 1000, {1000, 1000, 1000, 72}, false},
      {false, {1024, 1024, 1024}, 1000, {1000, 1000, 1000, 72}, true},
      {true, {1024, 1024, 1024}, 1000, {1000, 1000, 1000, 72}, false},
      {true, {1024, 1024, 1024}, 1000, {1000, 1000, 1000, 72}, true}};

  TestScopedSpillInjection scopedSpillInjection(100);
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    auto spillDirectory = exec::test::TempDirectoryPath::create();
    auto spillConfig = common::SpillConfig(
        [&]() -> const std::string& { return spillDirectory->path; },
        [&](uint64_t) {},
        "0.0.0",
        1000,
        false,
        0,
        executor_.get(),
        5,
        10,
        0,
        0,
        0,
        0,
        0,
        100, //  testSpillPct
        "none");
    auto sortBuffer = std::make_unique<SortBuffer>(
        inputType_,
        sortColumnIndices_,
        sortCompareFlags_,
        pool_.get(),
        &nonReclaimableSection_,
        testData.triggerSpill ? &spillConfig : nullptr,
        0,
        nullptr,
        testData.hybridSortEnabled);
    ASSERT_EQ(sortBuffer->canSpill(), testData.triggerSpill);

    const std::shared_ptr<memory::MemoryPool> fuzzerPool =
        memory::memoryManager()->addLeafPool("VectorFuzzer");

    std::vector<RowVectorPtr> inputVectors;
    inputVectors.reserve(testData.numInputRows.size());
    uint64_t totalNumInput = 0;
    for (size_t inputRows : testData.numInputRows) {
      VectorFuzzer fuzzer({.vectorSize = inputRows}, fuzzerPool.get());
      RowVectorPtr input = fuzzer.fuzzRow(inputType_);
      sortBuffer->addInput(input);
      inputVectors.push_back(input);
      totalNumInput += inputRows;
    }
    sortBuffer->noMoreInput();
    auto spillStats = sortBuffer->spilledStats();

    int expectedOutputBufferIndex = 0;
    RowVectorPtr output = sortBuffer->getOutput(testData.maxOutputRows);
    while (output != nullptr) {
      ASSERT_EQ(
          output->size(),
          testData.expectedOutputRowCount[expectedOutputBufferIndex++]);
      output = sortBuffer->getOutput(testData.maxOutputRows);
    }

    if (!testData.triggerSpill) {
      ASSERT_FALSE(spillStats.has_value());
    } else {
      ASSERT_TRUE(spillStats.has_value());
      ASSERT_GT(spillStats->spilledRows, 0);
      ASSERT_LE(spillStats->spilledRows, totalNumInput);
      ASSERT_GT(spillStats->spilledBytes, 0);
      ASSERT_EQ(spillStats->spilledPartitions, 1);
      ASSERT_GT(spillStats->spilledFiles, 0);
    }
  }
}

TEST_F(SortBufferTest, spill) {
  struct {
    bool spillEnabled;
    bool memoryReservationFailure;
    uint64_t spillMemoryThreshold;
    bool spillTriggered;
    bool hybridSortEnabled;

    std::string debugString() const {
      return fmt::format(
          "spillEnabled:{}, memoryReservationFailure:{}, spillMemoryThreshold:{}, spillTriggered:{}, hybridSortEnabled:{}",
          spillEnabled,
          memoryReservationFailure,
          spillMemoryThreshold,
          spillTriggered,
          hybridSortEnabled);
    }
  } testSettings[] = {
      {false, true, 0, false, false}, // spilling is not enabled.
      {false, true, 0, false, true}, // spilling is not enabled, hybrid enabled.
      {true,
       true,
       0,
       false,
       false}, // memory reservation failure won't trigger spilling.
      {true, true, 0, false, true}, // memory reservation failure won't trigger
                                    // spilling, hybrid enabled.
      {true,
       false,
       1000,
       true,
       false}, // threshold is small, spilling is triggered.
      {true,
       false,
       1000,
       true,
       true}, // threshold is small, spilling is triggered, hybrid enabled.
      {true,
       false,
       1000000,
       false,
       false} // threshold is too large, not triggered
  };

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    auto spillDirectory = exec::test::TempDirectoryPath::create();
    // memory pool limit is 20M
    // Set 'kSpillableReservationGrowthPct' to an extreme large value to trigger
    // memory reservation failure and thus trigger disk spilling.
    auto spillableReservationGrowthPct =
        testData.memoryReservationFailure ? 100000 : 100;
    auto spillConfig = common::SpillConfig(
        [&]() -> const std::string& { return spillDirectory->path; },
        [&](uint64_t) {},
        "0.0.0",
        1000,
        false,
        0,
        executor_.get(),
        100,
        spillableReservationGrowthPct,
        0,
        0,
        0,
        0,
        0,
        0,
        "none");
    auto sortBuffer = std::make_unique<SortBuffer>(
        inputType_,
        sortColumnIndices_,
        sortCompareFlags_,
        pool_.get(),
        &nonReclaimableSection_,
        testData.spillEnabled ? &spillConfig : nullptr,
        testData.spillMemoryThreshold,
        nullptr,
        testData.hybridSortEnabled);

    const std::shared_ptr<memory::MemoryPool> fuzzerPool =
        memory::memoryManager()->addLeafPool("spillSource");
    VectorFuzzer fuzzer({.vectorSize = 1024}, fuzzerPool.get());
    uint64_t totalNumInput = 0;

    ASSERT_EQ(memory::spillMemoryPool()->stats().currentBytes, 0);
    const auto peakSpillMemoryUsage =
        memory::spillMemoryPool()->stats().peakBytes;

    for (int i = 0; i < 3; ++i) {
      sortBuffer->addInput(fuzzer.fuzzRow(inputType_));
      totalNumInput += 1024;
    }
    sortBuffer->noMoreInput();
    const auto spillStats = sortBuffer->spilledStats();

    if (!testData.spillTriggered) {
      ASSERT_FALSE(spillStats.has_value());
      if (!testData.spillEnabled) {
        BOLT_ASSERT_THROW(sortBuffer->spill(), "spill config is null");
      }
    } else {
      ASSERT_TRUE(spillStats.has_value());
      ASSERT_GT(spillStats->spilledRows, 0);
      ASSERT_LE(spillStats->spilledRows, totalNumInput);
      ASSERT_GT(spillStats->spilledBytes, 0);
      ASSERT_EQ(spillStats->spilledPartitions, 1);
      // SortBuffer shall not respect maxFileSize. Total files should be num
      // addInput() calls minus one which is the first one that has nothing to
      // spill.
      ASSERT_EQ(spillStats->spilledFiles, 3);
      sortBuffer.reset();
      ASSERT_EQ(memory::spillMemoryPool()->stats().currentBytes, 0);
      if (memory::spillMemoryPool()->trackUsage()) {
        ASSERT_GT(memory::spillMemoryPool()->stats().peakBytes, 0);
        ASSERT_GE(
            memory::spillMemoryPool()->stats().peakBytes, peakSpillMemoryUsage);
      }
    }
  }
}

DEBUG_ONLY_TEST_F(SortBufferTest, reserveMemoryGetOutput) {
  auto spillDirectory = exec::test::TempDirectoryPath::create();
  auto spillConfig = common::SpillConfig(
      [&]() -> const std::string& { return spillDirectory->getPath(); },
      [&](uint64_t) {},
      "0.0.0",
      1000,
      false,
      1 << 20,
      executor_.get(),
      100,
      100000,
      0,
      0,
      0,
      0,
      0,
      0,
      "none",
      "",
      "disabled",
      "",
      true);
  folly::Synchronized<common::SpillStats> spillStats;
  auto sortBuffer = std::make_unique<SortBuffer>(
      inputType_,
      sortColumnIndices_,
      sortCompareFlags_,
      pool_.get(),
      &nonReclaimableSection_,
      &spillConfig);

  const std::shared_ptr<memory::MemoryPool> fuzzerPool =
      memory::memoryManager()->addLeafPool("spillSource");
  VectorFuzzer fuzzer({.vectorSize = 1024}, fuzzerPool.get());

  TestScopedSpillInjection scopedSpillInjection(0);
  for (int i = 0; i < 3; ++i) {
    sortBuffer->addInput(fuzzer.fuzzRow(inputType_));
  }

  std::atomic_bool noMoreInput{false};
  SCOPED_TESTVALUE_SET(
      "bytedance::bolt::exec::SortBuffer::noMoreInput",
      std::function<void(SortBuffer*)>(
          ([&](SortBuffer* sortBuffer) { noMoreInput.store(true); })));

  std::atomic_int numInputs{0};
  SCOPED_TESTVALUE_SET(
      "bytedance::bolt::common::memory::MemoryPoolImpl::maybeReserve",
      std::function<void(memory::MemoryPoolImpl*)>(
          ([&](memory::MemoryPoolImpl* pool) {
            if (noMoreInput) {
              ++numInputs;
            }
          })));

  sortBuffer->noMoreInput();
  sortBuffer->getOutput(10000);
  ASSERT_EQ(numInputs, 1);
}

TEST_F(SortBufferTest, emptySpill) {
  const std::shared_ptr<memory::MemoryPool> fuzzerPool =
      memory::memoryManager()->addLeafPool("emptySpillSource");

  struct {
    bool hasPostSpillData;
    bool hybridSortEnabled;

    std::string debugString() const {
      return fmt::format(
          "hasPostSpillData:{}, hybridSortEnabled:{}",
          hasPostSpillData,
          hybridSortEnabled);
    }
  } testSettings[] = {
      {false, false}, {false, true}, {true, false}, {true, true}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    auto spillDirectory = exec::test::TempDirectoryPath::create();
    auto spillConfig = getSpillConfig(spillDirectory->path);
    auto sortBuffer = std::make_unique<SortBuffer>(
        inputType_,
        sortColumnIndices_,
        sortCompareFlags_,
        pool_.get(),
        &nonReclaimableSection_,
        &spillConfig,
        0,
        nullptr,
        testData.hybridSortEnabled);

    sortBuffer->spill();
    if (testData.hasPostSpillData) {
      VectorFuzzer fuzzer({.vectorSize = 1024}, fuzzerPool.get());
      sortBuffer->addInput(fuzzer.fuzzRow(inputType_));
    }
    sortBuffer->noMoreInput();
    ASSERT_FALSE(sortBuffer->spilledStats());
  }
}

TEST_F(SortBufferTest, rowBasedSpillMemory) {
  auto spillDirectory = exec::test::TempDirectoryPath::create();
  // memory pool limit is 20M
  // Set 'kSpillableReservationGrowthPct' to an extreme large value to trigger
  // memory reservation failure and thus trigger disk spilling.
  auto spillableReservationGrowthPct = 100000;
  auto spillConfig = common::SpillConfig(
      [&]() -> const std::string& { return spillDirectory->path; },
      [&](uint64_t) {},
      "0.0.0",
      1000,
      false,
      0,
      executor_.get(),
      100,
      spillableReservationGrowthPct,
      0,
      0,
      0,
      0,
      0,
      0,
      "none",
      "",
      "raw");
  auto sortBuffer = std::make_unique<SortBuffer>(
      inputType_,
      sortColumnIndices_,
      sortCompareFlags_,
      pool_.get(),
      &nonReclaimableSection_,
      &spillConfig,
      1000);

  const std::shared_ptr<memory::MemoryPool> fuzzerPool =
      memory::memoryManager()->addLeafPool("spillSource");
  VectorFuzzer fuzzer(
      {.vectorSize = 1024, .stringLength = 1024}, fuzzerPool.get());
  uint64_t totalNumInput = 0;

  ASSERT_EQ(memory::spillMemoryPool()->stats().currentBytes, 0);
  const auto peakSpillMemoryUsage =
      memory::spillMemoryPool()->stats().peakBytes;

  for (int i = 0; i < 5; ++i) {
    sortBuffer->addInput(fuzzer.fuzzRow(inputType_));
    totalNumInput += 1024;
  }
  sortBuffer->noMoreInput();
  const auto spillStats = sortBuffer->spilledStats();

  ASSERT_TRUE(spillStats.has_value());
  ASSERT_GT(spillStats->spilledRows, 0);
  ASSERT_LE(spillStats->spilledRows, totalNumInput);
  ASSERT_GT(spillStats->spilledBytes, 0);
  ASSERT_EQ(spillStats->spilledPartitions, 1);
  // SortBuffer shall not respect maxFileSize. Total files should be num
  // addInput() calls minus one which is the first one that has nothing to
  // spill.
  ASSERT_EQ(spillStats->spilledFiles, 5);
  auto rowVector = sortBuffer->getOutput(1024);
  ASSERT_LT(sortBuffer->pool()->currentBytes(), 12 * 1024 * 1024);
  sortBuffer.reset();
  ASSERT_EQ(memory::spillMemoryPool()->stats().currentBytes, 0);
  if (memory::spillMemoryPool()->trackUsage()) {
    ASSERT_GT(memory::spillMemoryPool()->stats().peakBytes, 0);
    ASSERT_GE(
        memory::spillMemoryPool()->stats().peakBytes, peakSpillMemoryUsage);
  }
}

TEST_F(SortBufferTest, spillWithHybridModeValidateOutput) {
  // Test hybrid sort mode with spilling enabled and validate output
  // correctness.
  struct {
    bool hybridSortEnabled;
    std::string debugString() const {
      return fmt::format("hybridSortEnabled:{}", hybridSortEnabled);
    }
  } testSettings[] = {
      {false}, // Spill without hybrid mode
      {true} // Spill with hybrid mode
  };

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    auto spillDirectory = exec::test::TempDirectoryPath::create();
    auto spillConfig = common::SpillConfig(
        [&]() -> const std::string& { return spillDirectory->path; },
        [&](uint64_t) {},
        "0.0.0",
        1000,
        false,
        0,
        executor_.get(),
        5,
        100, // spillableReservationGrowthPct to trigger spilling
        0,
        0,
        0,
        0,
        0,
        100, // testSpillPct
        "none");

    auto sortBuffer = std::make_unique<SortBuffer>(
        inputType_,
        sortColumnIndices_,
        sortCompareFlags_,
        pool_.get(),
        &nonReclaimableSection_,
        &spillConfig,
        0,
        nullptr,
        testData.hybridSortEnabled);

    TestScopedSpillInjection scopedSpillInjection(100);

    // Create multiple batches to trigger spilling.
    for (int batch = 0; batch < 2; ++batch) {
      RowVectorPtr data = makeRowVector(
          {makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
           makeFlatVector<int32_t>({5, 4, 3, 2, 1}), // sorted-2 column
           makeFlatVector<int16_t>({1, 2, 3, 4, 5}),
           makeFlatVector<float>({1.1, 2.2, 3.3, 4.4, 5.5}),
           makeFlatVector<double>({1.1, 2.2, 2.2, 5.5, 5.5}), // sorted-1 column
           makeFlatVector<std::string>(
               {"hello", "world", "today", "is", "great"})});
      sortBuffer->addInput(data);
    }

    sortBuffer->noMoreInput();
    const auto spillStats = sortBuffer->spilledStats();

    // Validate spilling occurred
    ASSERT_TRUE(spillStats.has_value());
    ASSERT_GT(spillStats->spilledRows, 0);
    ASSERT_GT(spillStats->spilledBytes, 0);
    ASSERT_EQ(spillStats->spilledPartitions, 1);

    // // Log spill statistics for verification
    // std::cout << "\n=== Spill Statistics ===" << std::endl;
    // std::cout << "Hybrid Mode Enabled: " << testData.hybridSortEnabled <<
    // std::endl; std::cout << "Spilled Rows: " << spillStats->spilledRows <<
    // std::endl; std::cout << "Spilled Bytes: " << spillStats->spilledBytes <<
    // std::endl; std::cout << "Spilled Files: " << spillStats->spilledFiles <<
    // std::endl; std::cout << "========================\n" << std::endl;

    // Validate output correctness: check that all columns are correctly sorted
    std::vector<int64_t> expectedC0 = {
        1, 1, 3, 3, 2, 2, 5, 5, 4, 4}; // sorted by c4, then c1
    std::vector<int32_t> expectedC1 = {
        5, 5, 3, 3, 4, 4, 1, 1, 2, 2}; // sorted by c4, then c1
    std::vector<int16_t> expectedC2 = {1, 1, 3, 3, 2, 2, 5, 5, 4, 4};
    std::vector<float> expectedC3 = {
        1.1, 1.1, 3.3, 3.3, 2.2, 2.2, 5.5, 5.5, 4.4, 4.4};
    std::vector<double> expectedC4 = {
        1.1, 1.1, 2.2, 2.2, 2.2, 2.2, 5.5, 5.5, 5.5, 5.5};
    std::vector<std::string> expectedC5 = {
        "hello",
        "hello",
        "today",
        "today",
        "world",
        "world",
        "great",
        "great",
        "is",
        "is"};

    RowVectorPtr output;
    int totalRowsVerified = 0;
    while ((output = sortBuffer->getOutput(10000)) != nullptr) {
      auto c0Column = output->childAt(0)->asFlatVector<int64_t>();
      auto c1Column = output->childAt(1)->asFlatVector<int32_t>();
      auto c2Column = output->childAt(2)->asFlatVector<int16_t>();
      auto c3Column = output->childAt(3)->asFlatVector<float>();
      auto c4Column = output->childAt(4)->asFlatVector<double>();
      auto c5Column = output->childAt(5)->asFlatVector<StringView>();

      for (int i = 0; i < output->size(); ++i) {
        ASSERT_EQ(c0Column->valueAt(i), expectedC0[totalRowsVerified]);
        ASSERT_EQ(c1Column->valueAt(i), expectedC1[totalRowsVerified]);
        ASSERT_EQ(c2Column->valueAt(i), expectedC2[totalRowsVerified]);
        ASSERT_FLOAT_EQ(c3Column->valueAt(i), expectedC3[totalRowsVerified]);
        ASSERT_DOUBLE_EQ(c4Column->valueAt(i), expectedC4[totalRowsVerified]);
        ASSERT_EQ(c5Column->valueAt(i), expectedC5[totalRowsVerified]);
        totalRowsVerified++;
      }
    }

    // Verify we got all 10 rows (2 batches × 5 rows)
    ASSERT_EQ(totalRowsVerified, 10);
  }
}

} // namespace bytedance::bolt::functions::test

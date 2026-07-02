/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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

#include <sys/resource.h>
#include <algorithm>
#include <cctype>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include <fmt/format.h>
#include <folly/Benchmark.h>
#include <folly/String.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/init/Init.h>
#include <folly/json.h>
#include <gflags/gflags.h>

#include "bolt/common/base/Portability.h"
#include "bolt/common/caching/AsyncDataCache.h"
#include "bolt/common/file/FileSystems.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/SharedArbitrator.h"
#include "bolt/connectors/hive/HiveConnector.h"
#include "bolt/connectors/hive/HiveConnectorSplit.h"
#include "bolt/core/QueryConfig.h"
#include "bolt/dwio/common/Options.h"
#include "bolt/dwio/common/FileSink.h"
#include "bolt/dwio/parquet/RegisterParquetReader.h"
#include "bolt/dwio/parquet/RegisterParquetWriter.h"
#include "bolt/dwio/parquet/writer/Writer.h"
#include "bolt/exec/PlanNodeStats.h"
#include "bolt/exec/MemoryReclaimer.h"
#include "bolt/exec/Task.h"
#include "bolt/exec/tests/utils/Cursor.h"
#include "bolt/exec/tests/utils/HiveConnectorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "bolt/functions/prestosql/registration/RegistrationFunctions.h"
#include "bolt/parse/TypeResolver.h"
#include "bolt/serializers/PrestoSerializer.h"
#include "bolt/type/Type.h"
#include "bolt/vector/tests/utils/VectorMaker.h"

DEFINE_string(
    data_gb_values,
    "1GB,5GB,10GB,20GB",
    "Comma-separated logical input data sizes. Values may use B, KB, MB, or "
    "GB suffixes; suffix-less values are GiB.");
DEFINE_string(
    memory_gb_values,
    "5GB,10GB,20GB",
    "Comma-separated memory limits. Values may use B, KB, MB, or GB suffixes; "
    "suffix-less values are GiB. Each value is used as this benchmark run's "
    "allocator capacity and ORDER BY spill threshold.");
DEFINE_uint64(output_rows, 64UL << 10, "Rows requested per output batch.");
DEFINE_uint64(
    parallel_merge_target_rows,
    0,
    "Target rows per parallel merge task. 0 computes an automatic value inside "
    "the ORDER BY operator.");
DEFINE_uint64(
    parallel_merge_lookahead_tasks,
    0,
    "Maximum merge output tasks to keep scheduled ahead. 0 chooses a "
    "memory-budgeted default based on the ORDER BY memory limit and CPU count.");
DEFINE_uint32(
    parallel_threads,
    0,
    "Parallel executor threads. 0 uses CPU count.");
DEFINE_string(
    parallel_thread_values,
    "",
    "Optional comma-separated thread counts to run in one process, e.g. "
    "1,4,8,16. Empty uses --parallel_threads.");
DEFINE_string(
    thread_values,
    "",
    "Alias for --parallel_thread_values. If set, this takes precedence and "
    "allows comma-separated thread counts to run in one process, e.g. "
    "1,4,8,16.");
DEFINE_uint32(string_key_bytes, 32, "Bytes generated for each string key.");
DEFINE_string(
    parallel_values,
    "both",
    "Which implementations to run: both, on, off, parallel, or serial.");
DEFINE_string(
    key_shape_values,
    "all",
    "Comma-separated key shapes to run: all, all_int, all_string, mixed.");
DEFINE_string(
    num_key_values,
    "1,3,5",
    "Comma-separated number of sort keys to run.");
DEFINE_uint32(
    num_payload_fields,
    2,
    "Number of payload columns. Payload columns alternate VARCHAR, BIGINT, "
    "VARCHAR, BIGINT, ... and are not part of the ORDER BY keys.");
DEFINE_string(
    spill_dir,
    "",
    "Directory for spill files. Empty creates one under the system temp dir.");
DEFINE_string(
    json_output,
    "",
    "Optional path to write benchmark results as JSON.");
DEFINE_bool(
    dump_metrics,
    false,
    "Include detailed benchmark and ORDER BY metrics in the streamed JSON output.");
DEFINE_bool(
    enable_async_data_cache,
    false,
    "Enable AsyncDataCache for table scan input. Disabled by default so the "
    "benchmark memory limit measures the ORDER BY workload instead of cached "
    "Parquet read pages pinned by the active scan.");
DEFINE_string(
    split_size,
    "64MB",
    "Hive connector split size for the generated Parquet input file. Values may "
    "use B, KB, MB, or GB suffixes; suffix-less values are GiB.");

namespace bytedance::bolt::exec {
namespace {

constexpr uint64_t kGiB = 1UL << 30;
constexpr uint64_t kMiB = 1UL << 20;
constexpr uint64_t kKiB = 1UL << 10;
constexpr uint64_t kMergeOutputBatchRows = 64UL << 10;
constexpr uint64_t kAutoMergeTargetRowCap = 4 * kMergeOutputBatchRows;
constexpr uint64_t kTargetMergeTasksPerThread = 32;
constexpr uint64_t kWriteBatchRows = 16384;
constexpr uint32_t kWriteDrivers = 8;

uint64_t orderBySpillThresholdBytes(uint64_t queryMemoryBytes) {
  // Leave 20% of the benchmark memory limit for TableScan and transient
  // allocations. TableScan pages are not spillable/reclaimable, so using the
  // full query memory as the ORDER BY threshold lets ORDER BY consume memory
  // that the scan still needs and can still OOM before the arbitrator can make
  // progress.
  return queryMemoryBytes * 8 / 10;
}

enum class ColumnKind {
  kBigint,
  kVarchar,
};

enum class KeyShape {
  kAllInt,
  kAllString,
  kMixed,
};

struct BenchmarkParams {
  bool parallel;
  uint64_t dataBytes;
  uint64_t memoryBytes;
  KeyShape keyShape;
  int32_t numKeys;
  uint32_t numPayloadFields;

  std::string name() const {
    return fmt::format(
        "{}_data{}_mem{}_{}_{}keys_{}payload",
        parallel ? "parallel_on" : "parallel_off",
        formatBytes(dataBytes),
        formatBytes(memoryBytes),
        keyShapeName(),
        numKeys,
        numPayloadFields);
  }

  static std::string formatBytes(uint64_t bytes) {
    if (bytes % kGiB == 0) {
      return fmt::format("{}GB", bytes / kGiB);
    }
    if (bytes % kMiB == 0) {
      return fmt::format("{}MB", bytes / kMiB);
    }
    if (bytes % kKiB == 0) {
      return fmt::format("{}KB", bytes / kKiB);
    }
    return fmt::format("{}B", bytes);
  }

  std::string keyShapeName() const {
    switch (keyShape) {
      case KeyShape::kAllInt:
        return "all_int";
      case KeyShape::kAllString:
        return "all_string";
      case KeyShape::kMixed:
        return "mixed";
    }
    BOLT_UNREACHABLE();
  }
};

struct BenchmarkResult {
  std::string name;
  uint64_t inputRows{0};
  uint64_t outputRows{0};
  uint64_t inputBytes{0};
  uint64_t memoryBytes{0};
  uint64_t mergeTargetRows{0};
  uint64_t elapsedUs{0};
  uint64_t cpuUs{0};
  uint64_t spilledBytes{0};
  uint64_t spillRuns{0};
  uint64_t spillReadIOTimeUs{0};
  uint64_t spillWriteTimeUs{0};
  uint64_t sortColToRowTimeUs{0};
  uint64_t sortInSortTimeUs{0};
  uint64_t sortOutputTimeUs{0};
  bool parallel{false};
  std::string keyShape;
  uint64_t dataBytes{0};
  uint64_t memoryBytesRaw{0};
  uint32_t parallelThreads{0};
  int32_t numKeys{0};
  uint32_t numPayloadFields{0};
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
  uint64_t splitSizeBytes{0};
  uint64_t numSplits{0};
  uint64_t fileBytes{0};

  static std::string header() {
    return fmt::format(
        "{:<48} {:>12} {:>12} {:>12} {:>12} {:>8} {:>11} {:>12} {:>10} {:>14} {:>10} {:>12} {:>12} {:>14} {:>14} {:>14}",
        "benchmark",
        "inputRows",
        "outputRows",
        "inputSize",
        "memoryLimit",
        "splits",
        "mergeRows",
        "elapsedMs",
        "cpu%",
        "spilledSize",
        "spillRuns",
        "readIOMs",
        "writeMs",
        "colToRowMs",
        "sortMs",
        "outputMs");
  }

  std::string toString() const {
    return fmt::format(
        "{:<48} {:>12} {:>12} {:>12} {:>12} {:>8} {:>11} {:>12.2f} {:>10.1f} {:>14} {:>10} {:>12.2f} {:>12.2f} {:>14.2f} {:>14.2f} {:>14.2f}",
        name,
        inputRows,
        outputRows,
        BenchmarkParams::formatBytes(inputBytes),
        BenchmarkParams::formatBytes(memoryBytes),
        numSplits,
        mergeTargetRows,
        elapsedUs / 1000.0,
        elapsedUs == 0 ? 0.0 : (cpuUs * 100.0 / elapsedUs),
        BenchmarkParams::formatBytes(spilledBytes),
        spillRuns,
        spillReadIOTimeUs / 1000.0,
        spillWriteTimeUs / 1000.0,
        sortColToRowTimeUs / 1000.0,
        sortInSortTimeUs / 1000.0,
        sortOutputTimeUs / 1000.0);
  }

  folly::dynamic statsJson() const {
    folly::dynamic stats = folly::dynamic::object;
    stats["parallel"] = parallel;
    stats["parallelThreads"] = parallelThreads;
    stats["keyShape"] = keyShape;
    stats["numKeys"] = numKeys;
    stats["numPayloadFields"] = numPayloadFields;
    stats["inputRows"] = inputRows;
    stats["outputRows"] = outputRows;
    stats["dataBytes"] = dataBytes;
    stats["inputBytes"] = inputBytes;
    stats["memoryBytes"] = memoryBytes;
    stats["mergeTargetRows"] = mergeTargetRows;
    stats["elapsedUs"] = elapsedUs;
    stats["cpuUs"] = cpuUs;
    stats["cpuPct"] = elapsedUs == 0 ? 0.0 : cpuUs * 100.0 / elapsedUs;
    stats["spilledBytes"] = spilledBytes;
    stats["spillRuns"] = spillRuns;
    stats["spillReadIOTimeUs"] = spillReadIOTimeUs;
    stats["spillWriteTimeUs"] = spillWriteTimeUs;
    stats["sortColToRowTimeUs"] = sortColToRowTimeUs;
    stats["sortInSortTimeUs"] = sortInSortTimeUs;
    stats["sortOutputTimeUs"] = sortOutputTimeUs;
    stats["inputRunTargetBytes"] = inputRunTargetBytes;
    stats["inputRunsCreated"] = inputRunsCreated;
    stats["inputRunsScheduledSync"] = inputRunsScheduledSync;
    stats["inputRunsScheduledAsync"] = inputRunsScheduledAsync;
    stats["maxPendingInputRuns"] = maxPendingInputRuns;
    stats["maxRunningInputRuns"] = maxRunningInputRuns;
    stats["inputRunCollects"] = inputRunCollects;
    stats["inputRunWaitTimeUs"] = inputRunWaitTimeUs;
    stats["spillRunsScheduled"] = spillRunsScheduled;
    stats["maxRunningSpillRuns"] = maxRunningSpillRuns;
    stats["mergeTasks"] = mergeTasks;
    stats["mergeTaskLookahead"] = mergeTaskLookahead;
    stats["mergeBatchesScheduled"] = mergeBatchesScheduled;
    stats["mergeBatchesCompleted"] = mergeBatchesCompleted;
    stats["maxRunningMergeBatches"] = maxRunningMergeBatches;
    stats["maxActiveMergeTasks"] = maxActiveMergeTasks;
    stats["maxBufferedOutputRows"] = maxBufferedOutputRows;
    stats["estimatedInputRowBytes"] = estimatedInputRowBytes;
    stats["mergePlanningTimeUs"] = mergePlanningTimeUs;
    stats["mergeBoundaryPlanningTimeUs"] = mergeBoundaryPlanningTimeUs;
    stats["mergeTaskBuildTimeUs"] = mergeTaskBuildTimeUs;
    stats["mergePlanningTotalRowsTimeUs"] = mergePlanningTotalRowsTimeUs;
    stats["mergePlanningBoundaryIterations"] =
        mergePlanningBoundaryIterations;
    stats["mergePlanningCursorComparisons"] =
        mergePlanningCursorComparisons;
    stats["mergePlanningRowReferenceLoads"] =
        mergePlanningRowReferenceLoads;
    stats["mergePlanningBoundaries"] = mergePlanningBoundaries;
    stats["mergeExecutionTimeUs"] = mergeExecutionTimeUs;
    stats["mergeWaitTimeUs"] = mergeWaitTimeUs;
    stats["mergeOutputQueueTimeUs"] = mergeOutputQueueTimeUs;
    stats["outputProjectionTimeUs"] = outputProjectionTimeUs;
    stats["splitSizeBytes"] = splitSizeBytes;
    stats["numSplits"] = numSplits;
    stats["fileBytes"] = fileBytes;
    return stats;
  }

  folly::dynamic jsonObject(bool includeStats) const {
    folly::dynamic result = folly::dynamic::object("runName", name)(
        "elapsedUs", elapsedUs);
    if (includeStats) {
      result["stats"] = statsJson();
    }
    return result;
  }

  folly::dynamic jsonObject() const {
    return folly::dynamic::object("runName", name)(
        "elapsedUs", elapsedUs)("stats", statsJson());
  }
};

uint64_t parseDataSize(const std::string& value) {
  std::string token;
  token.reserve(value.size());
  for (auto c : value) {
    if (!std::isspace(static_cast<unsigned char>(c))) {
      token.push_back(c);
    }
  }
  BOLT_CHECK(!token.empty(), "Empty size value in benchmark size list");

  size_t unitOffset = 0;
  while (unitOffset < token.size() &&
         (std::isdigit(static_cast<unsigned char>(token[unitOffset])) ||
          token[unitOffset] == '.')) {
    ++unitOffset;
  }
  BOLT_CHECK_GT(unitOffset, 0, "Invalid size value: {}", value);

  const auto amount = std::stod(token.substr(0, unitOffset));
  auto unit = token.substr(unitOffset);
  std::transform(unit.begin(), unit.end(), unit.begin(), [](unsigned char c) {
    return std::tolower(c);
  });

  uint64_t multiplier = kGiB;
  if (unit.empty() || unit == "g" || unit == "gb" || unit == "gib") {
    multiplier = kGiB;
  } else if (unit == "m" || unit == "mb" || unit == "mib") {
    multiplier = kMiB;
  } else if (unit == "k" || unit == "kb" || unit == "kib") {
    multiplier = kKiB;
  } else if (unit == "b") {
    multiplier = 1;
  } else {
    BOLT_FAIL(
        "Invalid size unit '{}' in benchmark size value '{}'", unit, value);
  }

  const auto bytes = static_cast<uint64_t>(std::ceil(amount * multiplier));
  BOLT_CHECK_GT(
      bytes,
      0,
      "Benchmark size must be greater than 0. Use values like 1MB, 0.01GB, or 1GB.");
  return bytes;
}

std::vector<uint64_t> parseDataSizeList(const std::string& values) {
  std::vector<std::string> tokens;
  folly::split(',', values, tokens);

  std::vector<uint64_t> result;
  for (const auto& value : tokens) {
    if (!value.empty()) {
      result.push_back(parseDataSize(value));
    }
  }
  BOLT_CHECK(!result.empty(), "Benchmark size list must not be empty");
  return result;
}

std::vector<int32_t> parseInt32List(const std::string& values) {
  std::vector<std::string> tokens;
  folly::split(',', values, tokens);

  std::vector<int32_t> result;
  for (const auto& value : tokens) {
    std::string token;
    token.reserve(value.size());
    for (auto c : value) {
      if (!std::isspace(static_cast<unsigned char>(c))) {
        token.push_back(c);
      }
    }
    if (!token.empty()) {
      const auto parsed = std::stoi(token);
      BOLT_CHECK_GT(parsed, 0, "Integer values must be greater than 0");
      result.push_back(parsed);
    }
  }
  BOLT_CHECK(!result.empty(), "Integer value list must not be empty");
  return result;
}

std::vector<uint32_t> parseThreadValues() {
  const auto& values = FLAGS_thread_values.empty() ? FLAGS_parallel_thread_values
                                                   : FLAGS_thread_values;
  if (values.empty()) {
    return {FLAGS_parallel_threads == 0
                ? std::max<uint32_t>(1, std::thread::hardware_concurrency())
                : FLAGS_parallel_threads};
  }
  std::vector<uint32_t> result;
  for (auto value : parseInt32List(values)) {
    BOLT_CHECK_GT(value, 0, "Parallel thread count must be greater than 0");
    result.push_back(static_cast<uint32_t>(value));
  }
  return result;
}

const std::string& threadValuesFlag() {
  return FLAGS_thread_values.empty() ? FLAGS_parallel_thread_values
                                     : FLAGS_thread_values;
}

std::vector<bool> parseParallelValues(const std::string& values) {
  std::vector<std::string> tokens;
  folly::split(',', values, tokens);

  std::vector<bool> result;
  for (auto token : tokens) {
    token.erase(
        std::remove_if(
            token.begin(),
            token.end(),
            [](unsigned char c) { return std::isspace(c); }),
        token.end());
    std::transform(
        token.begin(), token.end(), token.begin(), [](unsigned char c) {
          return std::tolower(c);
        });
    if (token.empty()) {
      continue;
    }
    if (token == "both" || token == "all") {
      return {true, false};
    }
    if (token == "on" || token == "true" || token == "parallel") {
      result.push_back(true);
    } else if (token == "off" || token == "false" || token == "serial") {
      result.push_back(false);
    } else {
      BOLT_FAIL(
          "Invalid parallel value '{}'. Use both, on, off, parallel, or serial.",
          token);
    }
  }
  BOLT_CHECK(!result.empty(), "Parallel value list must not be empty");
  return result;
}

std::vector<KeyShape> parseKeyShapeValues(const std::string& values) {
  std::vector<std::string> tokens;
  folly::split(',', values, tokens);

  std::vector<KeyShape> result;
  for (auto token : tokens) {
    token.erase(
        std::remove_if(
            token.begin(),
            token.end(),
            [](unsigned char c) { return std::isspace(c); }),
        token.end());
    std::transform(
        token.begin(), token.end(), token.begin(), [](unsigned char c) {
          return std::tolower(c);
        });
    if (token.empty()) {
      continue;
    }
    if (token == "all") {
      return {KeyShape::kAllInt, KeyShape::kAllString, KeyShape::kMixed};
    }
    if (token == "all_int" || token == "int") {
      result.push_back(KeyShape::kAllInt);
    } else if (token == "all_string" || token == "string") {
      result.push_back(KeyShape::kAllString);
    } else if (token == "mixed") {
      result.push_back(KeyShape::kMixed);
    } else {
      BOLT_FAIL(
          "Invalid key shape '{}'. Use all, all_int, all_string, or mixed.",
          token);
    }
  }
  BOLT_CHECK(!result.empty(), "Key shape list must not be empty");
  return result;
}

std::vector<ColumnKind> columnKinds(
    KeyShape shape,
    int32_t numKeys,
    uint32_t numPayloadFields) {
  std::vector<ColumnKind> kinds;
  kinds.reserve(numKeys + numPayloadFields);
  for (auto i = 0; i < numKeys; ++i) {
    switch (shape) {
      case KeyShape::kAllInt:
        kinds.push_back(ColumnKind::kBigint);
        break;
      case KeyShape::kAllString:
        kinds.push_back(ColumnKind::kVarchar);
        break;
      case KeyShape::kMixed:
        kinds.push_back(
            i % 2 == 0 ? ColumnKind::kBigint : ColumnKind::kVarchar);
        break;
    }
  }

  for (uint32_t i = 0; i < numPayloadFields; ++i) {
    kinds.push_back(i % 2 == 0 ? ColumnKind::kVarchar : ColumnKind::kBigint);
  }
  return kinds;
}

uint64_t estimatedBytesPerRow(const std::vector<ColumnKind>& kinds) {
  return std::accumulate(
      kinds.begin(), kinds.end(), uint64_t{0}, [](auto total, auto kind) {
        return total +
            (kind == ColumnKind::kBigint
                 ? sizeof(int64_t)
                 : sizeof(StringView) + FLAGS_string_key_bytes);
      });
}

uint64_t processCpuTimeUs() {
  rusage usage;
  BOLT_CHECK_EQ(::getrusage(RUSAGE_SELF, &usage), 0);
  const auto userUs =
      usage.ru_utime.tv_sec * 1'000'000UL + usage.ru_utime.tv_usec;
  const auto systemUs =
      usage.ru_stime.tv_sec * 1'000'000UL + usage.ru_stime.tv_usec;
  return userUs + systemUs;
}

uint64_t automaticMergeTargetRows(
    uint64_t memoryBytes,
    uint64_t inputFlatBytes,
    uint32_t concurrency,
    uint64_t inputRows) {
  BOLT_CHECK_GT(inputRows, 0);
  const auto threads = std::max<uint32_t>(1, concurrency);
  const auto bytesPerRow =
      std::max<uint64_t>(1, (inputFlatBytes + inputRows - 1) / inputRows);
  const auto targetBytes = static_cast<uint64_t>(
      std::floor((static_cast<long double>(memoryBytes) * 0.9L) / threads));
  const auto memoryRows = std::max<uint64_t>(1, targetBytes / (bytesPerRow * 2));
  const auto targetTasks = std::max<uint64_t>(1, threads * kTargetMergeTasksPerThread);
  const auto rowsByOutputSize = (inputRows + targetTasks - 1) / targetTasks;
  const auto targetRows = std::min<uint64_t>(
      kAutoMergeTargetRowCap,
      std::max<uint64_t>(kMergeOutputBatchRows, rowsByOutputSize));
  return std::min(memoryRows, targetRows);
}

RowTypePtr rowType(const std::vector<ColumnKind>& kinds) {
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  names.reserve(kinds.size());
  types.reserve(kinds.size());
  for (auto i = 0; i < kinds.size(); ++i) {
    names.push_back(fmt::format("c{}", i));
    types.push_back(
        kinds[i] == ColumnKind::kBigint ? TypePtr{BIGINT()}
                                        : TypePtr{VARCHAR()});
  }
  return ROW(std::move(names), std::move(types));
}

uint64_t mix64(uint64_t value) {
  value += 0x9e3779b97f4a7c15ULL;
  value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ULL;
  value = (value ^ (value >> 27)) * 0x94d049bb133111ebULL;
  return value ^ (value >> 31);
}

std::string stringValue(uint64_t row, int32_t column) {
  auto value = fmt::format(
      "s{:02x}_{:016x}_{:016x}", column, mix64(row), mix64(row + column));
  if (value.size() < FLAGS_string_key_bytes) {
    value.append(
        FLAGS_string_key_bytes - value.size(),
        static_cast<char>('a' + (column % 26)));
  } else if (value.size() > FLAGS_string_key_bytes) {
    value.resize(FLAGS_string_key_bytes);
  }
  return value;
}

RowVectorPtr makeInput(
    memory::MemoryPool* pool,
    const RowTypePtr& type,
    const std::vector<ColumnKind>& kinds,
    vector_size_t rows,
    uint64_t firstRow) {
  bytedance::bolt::test::VectorMaker maker(pool);
  std::vector<VectorPtr> children;
  children.reserve(kinds.size());
  for (auto column = 0; column < kinds.size(); ++column) {
    if (kinds[column] == ColumnKind::kBigint) {
      children.push_back(maker.flatVector<int64_t>(
          rows,
          [firstRow, column](auto row) {
            return static_cast<int64_t>(mix64(firstRow + row + (column * 131)));
          },
          nullptr,
          BIGINT()));
    } else {
      children.push_back(maker.flatVector<std::string>(
          rows,
          [firstRow, column](auto row) {
            return stringValue(firstRow + row, column);
          },
          nullptr,
          VARCHAR()));
    }
  }
  return std::make_shared<RowVector>(pool, type, nullptr, rows, children);
}

std::filesystem::path makeRunDirectory(const std::string& benchmarkName) {
  auto root = FLAGS_spill_dir.empty() ? std::filesystem::temp_directory_path() /
          fmt::format("bolt-parallel-order-by-pb-benchmark-{}", ::getpid())
                                      : std::filesystem::path(FLAGS_spill_dir);
  auto path = root / benchmarkName;
  std::filesystem::remove_all(path);
  std::filesystem::create_directories(path);
  return path;
}

uint64_t runtimeStatSum(
    const OperatorStats& stats,
    const std::string& name) {
  auto it = stats.runtimeStats.find(name);
  return it == stats.runtimeStats.end() ? 0 : it->second.sum;
}

OperatorStats findOrderByStats(const std::shared_ptr<Task>& task) {
  OperatorStats result;
  for (const auto& pipelineStats : task->taskStats().pipelineStats) {
    for (const auto& operatorStats : pipelineStats.operatorStats) {
      if (operatorStats.operatorType == "OrderBy") {
        result.add(operatorStats);
      }
    }
  }
  return result;
}

std::shared_ptr<cache::AsyncDataCache> asyncDataCache;

uint64_t allocatorCapacityBytes(uint64_t memoryBytes) {
  return memoryBytes;
}

void resetMemoryForRun(uint64_t memoryBytes) {
  if (asyncDataCache != nullptr) {
    asyncDataCache->shutdown();
    asyncDataCache.reset();
  }

  memory::MemoryManager::Options options;
  options.useMmapAllocator = true;
  options.allocatorCapacity = allocatorCapacityBytes(memoryBytes);
  options.arbitratorCapacity = memoryBytes;
  options.arbitratorKind = "SHARED";
  options.arbitrationStateCheckCb = memoryArbitrationStateCheck;
  using ExtraConfig = memory::SharedArbitrator::ExtraConfig;
  options.extraArbitratorConfigs = {
      {std::string(ExtraConfig::kGlobalArbitrationEnabled), "false"},
      {std::string(ExtraConfig::kMemoryPoolInitialCapacity), "16MB"},
      {std::string(ExtraConfig::kMaxMemoryArbitrationTime), "120000ms"},
  };
  options.useMmapArena = true;
  options.mmapArenaCapacityRatio = 1;
  memory::MemoryManager::testingSetInstance(options);
  if (FLAGS_enable_async_data_cache) {
    asyncDataCache =
        cache::AsyncDataCache::create(memory::memoryManager()->allocator());
    cache::AsyncDataCache::setInstance(asyncDataCache.get());
  } else {
    cache::AsyncDataCache::setInstance(nullptr);
  }
}

void cleanupMemory() {
  if (asyncDataCache != nullptr) {
    asyncDataCache->shutdown();
    asyncDataCache.reset();
  }
  cache::AsyncDataCache::setInstance(nullptr);
}

void registerHiveConnector(folly::Executor* ioExecutor) {
  connector::hive::CheckHiveConnectorFactoryInit<
      connector::hive::HiveConnectorFactory>();
  if (connector::isConnectorRegistered(test::kHiveConnectorId)) {
    connector::unregisterConnector(test::kHiveConnectorId);
  }

  auto hiveConnector =
      connector::getConnectorFactory(connector::kHiveConnectorName)
          ->newConnector(
              test::kHiveConnectorId,
              std::make_shared<config::ConfigBase>(
                  std::unordered_map<std::string, std::string>()),
              ioExecutor);
  connector::registerConnector(hiveConnector);
}

class ParallelOrderByPlanBuilderBenchmark {
 public:
  ParallelOrderByPlanBuilderBenchmark(
      folly::Executor* executor,
      folly::Executor* ioExecutor,
      uint32_t numThreads)
      : executor_(executor), ioExecutor_(ioExecutor), numThreads_(numThreads) {}

  BenchmarkResult run(const BenchmarkParams& params) {
    registerHiveConnector(ioExecutor_);

    const auto kinds =
        columnKinds(params.keyShape, params.numKeys, params.numPayloadFields);
    const auto type = rowType(kinds);
    const auto bytesPerRow = estimatedBytesPerRow(kinds);
    const auto benchmarkName =
        fmt::format("{}_{}threads", params.name(), numThreads_);
    auto runDir = makeRunDirectory(benchmarkName);
    const auto inputPath = (runDir / "input").string();
    const auto spillPath = (runDir / "spill").string();

    auto rootPool = memory::memoryManager()->addRootPool(benchmarkName);
    auto inputPool = rootPool->addLeafChild("input");

    uint64_t generatedBytes = 0;
    uint64_t generatedRows = 0;
    uint64_t generatedFlatBytes = 0;
    writeInputFile(
        inputPath,
        rootPool,
        inputPool.get(),
        type,
        kinds,
        bytesPerRow,
        params.dataBytes,
        kWriteDrivers,
        generatedRows,
        generatedBytes,
        generatedFlatBytes);
    inputPool->release();

    const auto splitSizeBytes = parseDataSize(FLAGS_split_size);
    const auto fileBytes = inputDirectoryBytes(inputPath);
    auto splits = makeSplits(inputPath, splitSizeBytes);
    std::filesystem::create_directories(spillPath);

    core::PlanNodeId scanNodeId;
    core::PlanNodeId orderByNodeId;
    std::vector<std::string> orderByKeys;
    orderByKeys.reserve(params.numKeys);
    for (auto i = 0; i < params.numKeys; ++i) {
      orderByKeys.push_back(fmt::format("c{} ASC NULLS LAST", i));
    }

    auto plan = test::PlanBuilder()
                    .tableScan(type)
                    .capturePlanNodeId(scanNodeId)
                    .orderBy(orderByKeys, false)
                    .capturePlanNodeId(orderByNodeId)
                    .planFragment();

    auto queryCtx = core::QueryCtx::create(executor_);
    const auto orderByMemoryBytes = orderBySpillThresholdBytes(params.memoryBytes);
    queryCtx->testingOverrideConfigUnsafe({
        {core::QueryConfig::kSpillEnabled, "true"},
        {core::QueryConfig::kOrderBySpillEnabled, "true"},
        {core::QueryConfig::kOrderByParallelSortEnabled,
         params.parallel ? "true" : "false"},
        {core::QueryConfig::kOrderBySpillMemoryThreshold,
         std::to_string(orderByMemoryBytes)},
        {core::QueryConfig::kOrderByParallelMergeThreads,
         std::to_string(numThreads_)},
        {core::QueryConfig::kOrderByParallelMergeTargetRows,
         std::to_string(FLAGS_parallel_merge_target_rows)},
        {core::QueryConfig::kOrderByParallelMergeLookaheadTasks,
         std::to_string(FLAGS_parallel_merge_lookahead_tasks)},
        {core::QueryConfig::kPreferredOutputBatchRows,
         std::to_string(FLAGS_output_rows)},
        {core::QueryConfig::kMaxOutputBatchRows, std::to_string(FLAGS_output_rows)},
        {core::QueryConfig::kJitLevel, "-1"},
    });

    auto task = Task::create(
        benchmarkName,
        std::move(plan),
        0,
        queryCtx,
        Task::ExecutionMode::kSerial,
        Consumer{},
        0,
        common::SpillDiskOptions{
            .spillDirPath = spillPath,
            .spillDirCreated = true,
            .spillDirCreateCb = nullptr});

    for (auto& split : splits) {
      task->addSplit(scanNodeId, std::move(split));
    }
    task->noMoreSplits(scanNodeId);

    const auto startCpuUs = processCpuTimeUs();
    const auto startUs = getCurrentTimeMicro();
    uint64_t outputRows = 0;
    while (auto output = task->next()) {
      outputRows += output->size();
      folly::doNotOptimizeAway(output);
    }
    const auto elapsedUs = getCurrentTimeMicro() - startUs;
    const auto cpuUs = processCpuTimeUs() - startCpuUs;

    const auto orderByStats = findOrderByStats(task);

    BenchmarkResult result;
    result.name = benchmarkName;
    result.parallel = params.parallel;
    result.keyShape = params.keyShapeName();
    result.dataBytes = params.dataBytes;
    result.parallelThreads = numThreads_;
    result.numKeys = params.numKeys;
    result.numPayloadFields = params.numPayloadFields;
    result.inputRows = generatedRows;
    result.outputRows = outputRows;
    result.inputBytes = generatedBytes;
    result.memoryBytes = params.memoryBytes;
    result.memoryBytesRaw = params.memoryBytes;
    result.mergeTargetRows = FLAGS_parallel_merge_target_rows == 0
        ? automaticMergeTargetRows(
              orderByMemoryBytes, generatedFlatBytes, numThreads_, generatedRows)
        : FLAGS_parallel_merge_target_rows;
    result.elapsedUs = elapsedUs;
    result.cpuUs = cpuUs;
    result.spilledBytes = orderByStats.spilledBytes;
    result.spillRuns = runtimeStatSum(orderByStats, "spillRuns");
    result.spillReadIOTimeUs =
        runtimeStatSum(orderByStats, "spillReadIOTotalTime") / 1'000;
    result.spillWriteTimeUs =
        runtimeStatSum(orderByStats, "spillWriteTime") / 1'000;
    result.sortColToRowTimeUs = orderByStats.sortColToRowTime / 1'000;
    result.sortInSortTimeUs = orderByStats.sortInSortTime / 1'000;
    result.sortOutputTimeUs = orderByStats.sortOutputTime / 1'000;
    result.inputRunTargetBytes =
        runtimeStatSum(orderByStats, "parallelInputRunTargetBytes");
    result.inputRunsCreated =
        runtimeStatSum(orderByStats, "parallelInputRunsCreated");
    result.inputRunsScheduledSync =
        runtimeStatSum(orderByStats, "parallelInputRunsScheduledSync");
    result.inputRunsScheduledAsync =
        runtimeStatSum(orderByStats, "parallelInputRunsScheduledAsync");
    result.maxPendingInputRuns =
        runtimeStatSum(orderByStats, "parallelMaxPendingInputRuns");
    result.maxRunningInputRuns =
        runtimeStatSum(orderByStats, "parallelMaxRunningInputRuns");
    result.inputRunCollects =
        runtimeStatSum(orderByStats, "parallelInputRunCollects");
    result.inputRunWaitTimeUs =
        runtimeStatSum(orderByStats, "parallelInputRunWaitTime") / 1'000;
    result.spillRunsScheduled =
        runtimeStatSum(orderByStats, "parallelSpillRunsScheduled");
    result.maxRunningSpillRuns =
        runtimeStatSum(orderByStats, "parallelMaxRunningSpillRuns");
    result.mergeTasks = runtimeStatSum(orderByStats, "parallelMergeTasks");
    result.mergeTaskLookahead =
        runtimeStatSum(orderByStats, "parallelMergeTaskLookahead");
    result.mergeBatchesScheduled =
        runtimeStatSum(orderByStats, "parallelMergeBatchesScheduled");
    result.mergeBatchesCompleted =
        runtimeStatSum(orderByStats, "parallelMergeBatchesCompleted");
    result.maxRunningMergeBatches =
        runtimeStatSum(orderByStats, "parallelMaxRunningMergeBatches");
    result.maxActiveMergeTasks =
        runtimeStatSum(orderByStats, "parallelMaxActiveMergeTasks");
    result.maxBufferedOutputRows =
        runtimeStatSum(orderByStats, "parallelMaxBufferedOutputRows");
    result.estimatedInputRowBytes =
        runtimeStatSum(orderByStats, "parallelEstimatedInputRowBytes");
    result.mergePlanningTimeUs =
        runtimeStatSum(orderByStats, "parallelMergePlanningTime") / 1'000;
    result.mergeBoundaryPlanningTimeUs =
        runtimeStatSum(orderByStats, "parallelMergeBoundaryPlanningTime") /
        1'000;
    result.mergeTaskBuildTimeUs =
        runtimeStatSum(orderByStats, "parallelMergeTaskBuildTime") / 1'000;
    result.mergePlanningTotalRowsTimeUs =
        runtimeStatSum(orderByStats, "parallelMergePlanningTotalRowsTime") /
        1'000;
    result.mergePlanningBoundaryIterations =
        runtimeStatSum(orderByStats, "parallelMergePlanningBoundaryIterations");
    result.mergePlanningCursorComparisons =
        runtimeStatSum(orderByStats, "parallelMergePlanningCursorComparisons");
    result.mergePlanningRowReferenceLoads =
        runtimeStatSum(orderByStats, "parallelMergePlanningRowReferenceLoads");
    result.mergePlanningBoundaries =
        runtimeStatSum(orderByStats, "parallelMergePlanningBoundaries");
    result.mergeExecutionTimeUs =
        runtimeStatSum(orderByStats, "parallelMergeExecutionTime") / 1'000;
    result.mergeWaitTimeUs =
        runtimeStatSum(orderByStats, "parallelMergeWaitTime") / 1'000;
    result.mergeOutputQueueTimeUs =
        runtimeStatSum(orderByStats, "parallelMergeOutputQueueTime") / 1'000;
    result.outputProjectionTimeUs =
        runtimeStatSum(orderByStats, "parallelOutputProjectionTime") / 1'000;
    if (result.mergeTargetRows == 0) {
      result.mergeTargetRows =
          runtimeStatSum(orderByStats, "parallelMergeTargetRows");
    }
    result.fileBytes = fileBytes;
    result.splitSizeBytes = splitSizeBytes;
    result.numSplits =
        fileBytes == 0 ? 0 : (fileBytes + splitSizeBytes - 1) / splitSizeBytes;

    task.reset();
    std::filesystem::remove_all(runDir);
    if (connector::isConnectorRegistered(test::kHiveConnectorId)) {
      connector::unregisterConnector(test::kHiveConnectorId);
    }
    folly::doNotOptimizeAway(outputRows);
    return result;
  }

 private:
  void writeInputFile(
      const std::string& outputDirectoryPath,
      std::shared_ptr<memory::MemoryPool> rootPool,
      memory::MemoryPool* pool,
      const RowTypePtr& type,
      const std::vector<ColumnKind>& kinds,
      uint64_t bytesPerRow,
      uint64_t targetBytes,
      uint32_t writerDrivers,
      uint64_t& generatedRows,
      uint64_t& generatedBytes,
      uint64_t& generatedFlatBytes) {
    std::filesystem::create_directories(outputDirectoryPath);
    const auto chunkTargetBytes = std::max<uint64_t>(
        parseDataSize(FLAGS_split_size), kWriteBatchRows * bytesPerRow);

    std::vector<RowVectorPtr> chunk;
    uint64_t chunkBytes = 0;
    auto flushChunk = [&]() {
      if (chunk.empty()) {
        return;
      }
      writeInputChunkWithPlanBuilder(
          outputDirectoryPath, std::move(chunk), writerDrivers);
      chunk.clear();
      chunkBytes = 0;
    };

    while (generatedFlatBytes < targetBytes) {
      const auto remainingRows =
          (targetBytes - generatedFlatBytes + bytesPerRow - 1) / bytesPerRow;
      const auto rows = static_cast<vector_size_t>(
          std::min<uint64_t>(kWriteBatchRows, remainingRows));
      auto input = makeInput(pool, type, kinds, rows, generatedRows);
      const auto flatBytes = input->estimateFlatSize();
      chunkBytes += flatBytes;
      chunk.push_back(std::move(input));
      generatedFlatBytes += flatBytes;
      generatedRows += rows;
      generatedBytes += flatBytes;
      if (chunkBytes >= chunkTargetBytes) {
        flushChunk();
      }
    }
    flushChunk();
  }

  void writeInputChunkWithPlanBuilder(
      const std::string& outputDirectoryPath,
      std::vector<RowVectorPtr> chunk,
      uint32_t writerDrivers) {
    auto plan = test::PlanBuilder()
                    .values(chunk)
                    .localPartitionRoundRobin()
                    .tableWrite(
                        outputDirectoryPath,
                        dwio::common::FileFormat::PARQUET)
                    .localPartition(std::vector<std::string>{})
                    .tableWriteMerge()
                    .planNode();

    test::CursorParameters params;
    params.planNode = plan;
    params.maxDrivers = writerDrivers;
    params.copyResult = false;
    auto cursor = test::TaskCursor::create(params);
    while (cursor->moveNext()) {
      folly::doNotOptimizeAway(cursor->current());
    }
  }

  static std::vector<Split> makeSplits(
      const std::string& inputDirectoryPath,
      uint64_t splitSizeBytes) {
    BOLT_CHECK_GT(splitSizeBytes, 0);
    std::vector<Split> splits;
    for (const auto& path : std::filesystem::recursive_directory_iterator(
             inputDirectoryPath)) {
      if (!path.is_regular_file()) {
        continue;
      }
      const auto inputPath = path.path().string();
      const auto fileBytes = path.file_size();
      for (uint64_t offset = 0; offset < fileBytes; offset += splitSizeBytes) {
        const auto length = std::min(splitSizeBytes, fileBytes - offset);
        splits.emplace_back(
            connector::hive::HiveConnectorSplitBuilder(inputPath)
                .connectorId(test::kHiveConnectorId)
                .fileFormat(dwio::common::FileFormat::PARQUET)
                .start(offset)
                .length(length)
                .build());
      }
    }
    BOLT_CHECK(!splits.empty(), "Generated input directory must produce splits");
    return splits;
  }

  static uint64_t inputDirectoryBytes(const std::string& inputDirectoryPath) {
    uint64_t bytes = 0;
    for (const auto& path : std::filesystem::recursive_directory_iterator(
             inputDirectoryPath)) {
      if (path.is_regular_file()) {
        bytes += path.file_size();
      }
    }
    return bytes;
  }

  folly::Executor* const executor_;
  folly::Executor* const ioExecutor_;
  const uint32_t numThreads_;
};

std::vector<BenchmarkParams> benchmarkParams() {
  std::vector<BenchmarkParams> params;
  const auto parallelValues = parseParallelValues(FLAGS_parallel_values);
  const auto keyShapeValues = parseKeyShapeValues(FLAGS_key_shape_values);
  const auto numKeyValues = parseInt32List(FLAGS_num_key_values);
  for (auto parallel : parallelValues) {
    for (auto dataBytes : parseDataSizeList(FLAGS_data_gb_values)) {
      for (auto memoryBytes : parseDataSizeList(FLAGS_memory_gb_values)) {
        for (auto keyShape : keyShapeValues) {
          for (auto numKeys : numKeyValues) {
            params.push_back(BenchmarkParams{
                .parallel = parallel,
                .dataBytes = dataBytes,
                .memoryBytes = memoryBytes,
                .keyShape = keyShape,
                .numKeys = numKeys,
                .numPayloadFields = FLAGS_num_payload_fields});
          }
        }
      }
    }
  }
  return params;
}

std::string serializeJsonResult(
    const BenchmarkResult& result,
    bool includeStats) {
  return folly::toPrettyJson(result.jsonObject(includeStats));
}

class JsonResultStreamer {
 public:
  JsonResultStreamer(std::ostream& out, bool includeStats)
      : out_(out), includeStats_(includeStats) {
    out_ << "[\n";
  }

  ~JsonResultStreamer() {
    close();
  }

  void write(const BenchmarkResult& result) {
    if (!first_) {
      out_ << ",\n";
    }
    first_ = false;
    out_ << serializeJsonResult(result, includeStats_);
    out_.flush();
  }

  void close() {
    if (!closed_) {
      out_ << "\n]\n";
      out_.flush();
      closed_ = true;
    }
  }

 private:
  std::ostream& out_;
  const bool includeStats_;
  bool first_{true};
  bool closed_{false};
};

class OptionalFileJsonResultStreamer {
 public:
  OptionalFileJsonResultStreamer(const std::string& path, bool includeStats) {
    if (!path.empty()) {
      file_.open(path);
      BOLT_CHECK(file_.is_open(), "Failed to open JSON output file: {}", path);
      streamer_ = std::make_unique<JsonResultStreamer>(file_, includeStats);
    }
  }

  void write(const BenchmarkResult& result) {
    if (streamer_ != nullptr) {
      streamer_->write(result);
    }
  }

  void close() {
    if (streamer_ != nullptr) {
      streamer_->close();
    }
  }

 private:
  std::ofstream file_;
  std::unique_ptr<JsonResultStreamer> streamer_;
};

void writeJsonResult(
    const BenchmarkResult& result,
    JsonResultStreamer& stdoutStreamer,
    OptionalFileJsonResultStreamer& fileStreamer) {
  stdoutStreamer.write(result);
  fileStreamer.write(result);
}

void initializeProcess() {
  memory::SharedArbitrator::registerFactory();
  filesystems::registerLocalFileSystem();
  serializer::presto::PrestoVectorSerde::registerVectorSerde();
  serializer::presto::PrestoVectorSerde::registerNamedVectorSerde();
  parquet::registerParquetReaderFactory();
  parquet::registerParquetWriterFactory();
  functions::prestosql::registerAllScalarFunctions();
  aggregate::prestosql::registerAllAggregateFunctions();
  parse::registerTypeResolver();
}

} // namespace
} // namespace bytedance::bolt::exec

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  bytedance::bolt::exec::initializeProcess();

  const auto threadValues = bytedance::bolt::exec::parseThreadValues();
  const auto params = bytedance::bolt::exec::benchmarkParams();

  std::cerr << fmt::format(
      "Input data sizes: {}; memory limits: {}; parallel values: {}; thread values: {}; key shapes: {}; key counts: {}; payload fields: {}; merge target rows: {}; split size: {}\n",
      FLAGS_data_gb_values,
      FLAGS_memory_gb_values,
      FLAGS_parallel_values,
      bytedance::bolt::exec::threadValuesFlag().empty()
          ? std::to_string(threadValues.front())
          : bytedance::bolt::exec::threadValuesFlag(),
      FLAGS_key_shape_values,
      FLAGS_num_key_values,
      FLAGS_num_payload_fields,
      FLAGS_parallel_merge_target_rows == 0
          ? std::string("auto")
          : std::to_string(FLAGS_parallel_merge_target_rows),
      FLAGS_split_size);

  bytedance::bolt::exec::JsonResultStreamer stdoutStreamer(
      std::cout, FLAGS_dump_metrics);
  bytedance::bolt::exec::OptionalFileJsonResultStreamer fileStreamer(
      FLAGS_json_output, FLAGS_dump_metrics);

  for (const auto numThreads : threadValues) {
    folly::CPUThreadPoolExecutor executor(numThreads);
    folly::IOThreadPoolExecutor ioExecutor(std::max<uint32_t>(1, numThreads));
    for (const auto& benchmarkParams : params) {
      bytedance::bolt::exec::resetMemoryForRun(benchmarkParams.memoryBytes);
      bytedance::bolt::exec::ParallelOrderByPlanBuilderBenchmark benchmark(
          &executor, &ioExecutor, numThreads);
      auto result = benchmark.run(benchmarkParams);
      bytedance::bolt::exec::writeJsonResult(
          result, stdoutStreamer, fileStreamer);
    }
  }

  stdoutStreamer.close();
  fileStreamer.close();
  bytedance::bolt::exec::cleanupMemory();
  return 0;
}

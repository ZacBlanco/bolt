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
#include <folly/Random.h>
#include <folly/String.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>

#include "bolt/common/base/Portability.h"
#include "bolt/common/base/SpillConfig.h"
#include "bolt/common/file/FileSystems.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/common/time/Timer.h"
#include "bolt/exec/ISortBuffer.h"
#include "bolt/exec/ParallelSortBuffer.h"
#include "bolt/exec/SortBuffer.h"
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
    "suffix-less values are GiB. Each value is passed to the ORDER BY buffer "
    "as the spill threshold, and the benchmark initializes the allocator "
    "capacity to the largest value in this list. Values smaller than the input "
    "data size force spilling.");
DEFINE_uint64(batch_rows, 64UL << 10, "Input rows generated per batch.");
DEFINE_uint64(output_rows, 64UL << 10, "Rows requested per output batch.");
DEFINE_uint64(
    parallel_merge_target_rows,
    0,
    "Target rows per parallel merge task. 0 computes an automatic value that "
    "fits each active merge task into approximately 90% of the memory limit "
    "divided by the parallel thread count.");
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
    "Dump detailed benchmark and sort-buffer metrics to stderr after each run.");

namespace bytedance::bolt::exec {
namespace {

constexpr uint64_t kGiB = 1UL << 30;
constexpr uint64_t kMiB = 1UL << 20;
constexpr uint64_t kKiB = 1UL << 10;
constexpr uint64_t kEstimatedRowOverheadBytes = 512;
constexpr uint64_t kAllocatorHeadroomMinBytes = 64 * kMiB;
constexpr uint64_t kMergeOutputBatchRows = 64UL << 10;
constexpr uint64_t kAutoMergeTargetRowCap = 4 * kMergeOutputBatchRows;
constexpr uint64_t kTargetMergeTasksPerThread = 32;

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
  uint64_t maxPendingInputRuns{0};
  uint64_t maxRunningInputRuns{0};
  uint64_t inputRunCollects{0};
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
  uint64_t mergeExecutionTimeUs{0};

  static std::string header() {
    return fmt::format(
        "{:<48} {:>12} {:>12} {:>12} {:>12} {:>11} {:>12} {:>10} {:>14} {:>10} {:>12} {:>12} {:>14} {:>14} {:>14}",
        "benchmark",
        "inputRows",
        "outputRows",
        "inputSize",
        "memoryLimit",
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
        "{:<48} {:>12} {:>12} {:>12} {:>12} {:>11} {:>12.2f} {:>10.1f} {:>14} {:>10} {:>12.2f} {:>12.2f} {:>14.2f} {:>14.2f} {:>14.2f}",
        name,
        inputRows,
        outputRows,
        BenchmarkParams::formatBytes(inputBytes),
        BenchmarkParams::formatBytes(memoryBytes),
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

  std::string jsonObject() const {
    return fmt::format(
        R"({{"name":"{}","parallel":{},"parallelThreads":{},"keyShape":"{}","numKeys":{},"numPayloadFields":{},"inputRows":{},"outputRows":{},"dataBytes":{},"inputBytes":{},"memoryBytes":{},"mergeTargetRows":{},"elapsedUs":{},"cpuUs":{},"cpuPct":{},"spilledBytes":{},"spillRuns":{},"spillReadIOTimeUs":{},"spillWriteTimeUs":{},"sortColToRowTimeUs":{},"sortInSortTimeUs":{},"sortOutputTimeUs":{},"inputRunTargetBytes":{},"inputRunsCreated":{},"maxPendingInputRuns":{},"maxRunningInputRuns":{},"inputRunCollects":{},"spillRunsScheduled":{},"maxRunningSpillRuns":{},"mergeTasks":{},"mergeTaskLookahead":{},"mergeBatchesScheduled":{},"mergeBatchesCompleted":{},"maxRunningMergeBatches":{},"maxActiveMergeTasks":{},"maxBufferedOutputRows":{},"estimatedInputRowBytes":{},"mergePlanningTimeUs":{},"mergeExecutionTimeUs":{}}})",
        name,
        parallel ? "true" : "false",
        parallelThreads,
        keyShape,
        numKeys,
        numPayloadFields,
        inputRows,
        outputRows,
        dataBytes,
        inputBytes,
        memoryBytes,
        mergeTargetRows,
        elapsedUs,
        cpuUs,
        elapsedUs == 0 ? 0.0 : cpuUs * 100.0 / elapsedUs,
        spilledBytes,
        spillRuns,
        spillReadIOTimeUs,
        spillWriteTimeUs,
        sortColToRowTimeUs,
        sortInSortTimeUs,
        sortOutputTimeUs,
        inputRunTargetBytes,
        inputRunsCreated,
        maxPendingInputRuns,
        maxRunningInputRuns,
        inputRunCollects,
        spillRunsScheduled,
        maxRunningSpillRuns,
        mergeTasks,
        mergeTaskLookahead,
        mergeBatchesScheduled,
        mergeBatchesCompleted,
        maxRunningMergeBatches,
        maxActiveMergeTasks,
        maxBufferedOutputRows,
        estimatedInputRowBytes,
        mergePlanningTimeUs,
        mergeExecutionTimeUs);
  }
};

void dumpMetricsToStderr(
    const BenchmarkResult& result,
    const ISortBuffer& sortBuffer) {
  std::cerr << fmt::format("\n[metrics] {}\n", result.name);
  std::cerr << fmt::format(
      "  inputRows={} outputRows={} inputBytes={} memoryBytes={} elapsedUs={} cpuUs={} cpuPct={:.1f}\n",
      result.inputRows,
      result.outputRows,
      result.inputBytes,
      result.memoryBytes,
      result.elapsedUs,
      result.cpuUs,
      result.elapsedUs == 0 ? 0.0 : result.cpuUs * 100.0 / result.elapsedUs);
  std::cerr << fmt::format(
      "  spilledBytes={} spillRuns={} spillReadIOTimeUs={} spillWriteTimeUs={}\n",
      result.spilledBytes,
      result.spillRuns,
      result.spillReadIOTimeUs,
      result.spillWriteTimeUs);
  std::cerr << fmt::format(
      "  sortColToRowTimeUs={} sortInSortTimeUs={} sortOutputTimeUs={}\n",
      result.sortColToRowTimeUs,
      result.sortInSortTimeUs,
      result.sortOutputTimeUs);
  std::cerr << fmt::format(
      "  numInputRows={} numOutputRows={} estimateOutputRowSize={}\n",
      sortBuffer.numInputRows(),
      sortBuffer.numOutputRows(),
      sortBuffer.estimateOutputRowSize().has_value()
          ? std::to_string(sortBuffer.estimateOutputRowSize().value())
          : std::string("null"));
  if (const auto* parallel =
          dynamic_cast<const ParallelSortBuffer*>(&sortBuffer)) {
    const auto stats = parallel->debugStats();
    std::cerr << fmt::format(
        "  parallel.inputRunTargetBytes={} inputRunsCreated={} maxPendingInputRuns={} maxRunningInputRuns={} inputRunCollects={}\n",
        stats.inputRunTargetBytes,
        stats.inputRunsCreated,
        stats.maxPendingInputRuns,
        stats.maxRunningInputRuns,
        stats.inputRunCollects);
    std::cerr << fmt::format(
        "  parallel.spillRunsScheduled={} maxRunningSpillRuns={} estimatedInputRowBytes={}\n",
        stats.spillRunsScheduled,
        stats.maxRunningSpillRuns,
        stats.estimatedInputRowBytes);
    std::cerr << fmt::format(
        "  parallel.mergeTargetRows={} mergeTasks={} mergeTaskLookahead={} mergeBatchesScheduled={} mergeBatchesCompleted={}\n",
        stats.mergeTargetRows,
        stats.mergeTasks,
        stats.mergeTaskLookahead,
        stats.mergeBatchesScheduled,
        stats.mergeBatchesCompleted);
    std::cerr << fmt::format(
        "  parallel.maxRunningMergeBatches={} maxActiveMergeTasks={} maxBufferedOutputRows={}\n",
        stats.maxRunningMergeBatches,
        stats.maxActiveMergeTasks,
        stats.maxBufferedOutputRows);
    std::cerr << fmt::format(
        "  parallel.mergePlanningTimeUs={} mergeExecutionTimeUs={}\n",
        stats.mergePlanningTimeUs,
        stats.mergeExecutionTimeUs);
  }
}

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
      BOLT_CHECK_GT(parsed, 0, "Number of sort keys must be greater than 0");
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
  const auto rawBytes = std::accumulate(
      kinds.begin(), kinds.end(), uint64_t{0}, [](auto total, auto kind) {
        return total +
            (kind == ColumnKind::kBigint
                 ? sizeof(int64_t)
                 : sizeof(StringView) + FLAGS_string_key_bytes);
      });
  return std::max(rawBytes, kEstimatedRowOverheadBytes);
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
  test::VectorMaker maker(pool);
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

std::filesystem::path makeSpillDirectory(const std::string& benchmarkName) {
  auto root = FLAGS_spill_dir.empty() ? std::filesystem::temp_directory_path() /
          fmt::format("bolt-parallel-order-by-benchmark-{}", ::getpid())
                                      : std::filesystem::path(FLAGS_spill_dir);
  auto path = root / benchmarkName;
  std::filesystem::remove_all(path);
  std::filesystem::create_directories(path);
  return path;
}

common::SpillConfig makeSpillConfig(
    const std::string& spillDir,
    folly::Executor* executor) {
  return common::SpillConfig(
      [spillDir]() -> const std::string& {
        static thread_local std::string path;
        path = spillDir;
        return path;
      },
      [](uint64_t) {},
      "parallel-order-by-benchmark",
      0,
      false,
      0,
      executor,
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

class ParallelOrderByBenchmark {
 public:
  ParallelOrderByBenchmark(folly::Executor* executor, uint32_t numThreads)
      : executor_(executor), numThreads_(numThreads) {}

  BenchmarkResult run(const BenchmarkParams& params) {
    auto pool = memory::memoryManager()->addLeafPool();
    const auto kinds =
        columnKinds(params.keyShape, params.numKeys, params.numPayloadFields);
    const auto type = rowType(kinds);
    const auto bytesPerRow = estimatedBytesPerRow(kinds);
    const auto benchmarkName = fmt::format("{}_{}threads", params.name(), numThreads_);
    auto spillDir = makeSpillDirectory(benchmarkName);
    auto spillDirString = spillDir.string();
    auto spillConfig = makeSpillConfig(spillDirString, executor_);

    std::vector<column_index_t> sortColumnIndices;
    std::vector<CompareFlags> sortCompareFlags;
    sortColumnIndices.reserve(params.numKeys);
    sortCompareFlags.reserve(params.numKeys);
    for (auto i = 0; i < params.numKeys; ++i) {
      sortColumnIndices.push_back(i);
      sortCompareFlags.push_back(
          {true, true, false, CompareFlags::NullHandlingMode::kNullAsValue});
    }

    tsan_atomic<bool> nonReclaimableSection{false};
    std::unique_ptr<ISortBuffer> sortBuffer;
    if (params.parallel) {
      sortBuffer = std::make_unique<ParallelSortBuffer>(
          type,
          sortColumnIndices,
          sortCompareFlags,
          pool.get(),
          FLAGS_parallel_merge_target_rows,
          FLAGS_parallel_merge_lookahead_tasks,
          executor_,
          &spillConfig,
          params.memoryBytes,
          numThreads_);
    } else {
      sortBuffer = std::make_unique<SortBuffer>(
          type,
          sortColumnIndices,
          sortCompareFlags,
          pool.get(),
          &nonReclaimableSection,
          &spillConfig,
          params.memoryBytes,
          nullptr,
          false,
          false);
    }

    const auto startCpuUs = processCpuTimeUs();
    const auto startUs = getCurrentTimeMicro();
    uint64_t generatedBytes = 0;
    uint64_t generatedRows = 0;
    uint64_t generatedFlatBytes = 0;
    uint64_t nextSpillBytes = params.memoryBytes;
    while (generatedBytes < params.dataBytes) {
      const auto remainingRows =
          (params.dataBytes - generatedBytes + bytesPerRow - 1) / bytesPerRow;
      const auto rows = static_cast<vector_size_t>(
          std::min<uint64_t>(FLAGS_batch_rows, remainingRows));
      const auto nextBatchBytes = rows * bytesPerRow;
      if (generatedBytes > 0 &&
          generatedBytes + nextBatchBytes >= nextSpillBytes &&
          generatedBytes < params.dataBytes) {
        sortBuffer->reclaim(0);
        nextSpillBytes = generatedBytes + params.memoryBytes;
      }
      auto input = makeInput(pool.get(), type, kinds, rows, generatedRows);
      generatedFlatBytes += input->estimateFlatSize();
      sortBuffer->addInput(input);
      generatedRows += rows;
      generatedBytes += nextBatchBytes;
    }

    sortBuffer->noMoreInput();

    uint64_t outputRows = 0;
    while (auto output = sortBuffer->getOutput(FLAGS_output_rows)) {
      outputRows += output->size();
      folly::doNotOptimizeAway(output);
    }
    const auto elapsedUs = getCurrentTimeMicro() - startUs;
    const auto cpuUs = processCpuTimeUs() - startCpuUs;

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
              params.memoryBytes,
              generatedFlatBytes,
              numThreads_,
              generatedRows)
        : FLAGS_parallel_merge_target_rows;
    result.elapsedUs = elapsedUs;
    result.cpuUs = cpuUs;
    if (auto spilledStats = sortBuffer->spilledStats()) {
      result.spilledBytes = spilledStats->spilledBytes;
      result.spillRuns = spilledStats->spillRuns;
      result.spillWriteTimeUs = spilledStats->spillWriteTimeUs;
    }
    if (auto spillReadStats = sortBuffer->spillReadStats()) {
      result.spillReadIOTimeUs = spillReadStats->spillReadIOTimeUs;
    }
    if (auto sortStats = sortBuffer->sortStats()) {
      result.sortColToRowTimeUs = sortStats->sortColToRowTimeUs;
      result.sortInSortTimeUs = sortStats->sortInSortTimeUs;
      result.sortOutputTimeUs = sortStats->sortOutputTimeUs;
    }

    if (const auto* parallelBuffer =
            dynamic_cast<const ParallelSortBuffer*>(sortBuffer.get())) {
      const auto stats = parallelBuffer->debugStats();
      result.mergeTargetRows = stats.mergeTargetRows;
      result.inputRunTargetBytes = stats.inputRunTargetBytes;
      result.inputRunsCreated = stats.inputRunsCreated;
      result.maxPendingInputRuns = stats.maxPendingInputRuns;
      result.maxRunningInputRuns = stats.maxRunningInputRuns;
      result.inputRunCollects = stats.inputRunCollects;
      result.spillRunsScheduled = stats.spillRunsScheduled;
      result.maxRunningSpillRuns = stats.maxRunningSpillRuns;
      result.mergeTasks = stats.mergeTasks;
      result.mergeTaskLookahead = stats.mergeTaskLookahead;
      result.mergeBatchesScheduled = stats.mergeBatchesScheduled;
      result.mergeBatchesCompleted = stats.mergeBatchesCompleted;
      result.maxRunningMergeBatches = stats.maxRunningMergeBatches;
      result.maxActiveMergeTasks = stats.maxActiveMergeTasks;
      result.maxBufferedOutputRows = stats.maxBufferedOutputRows;
      result.estimatedInputRowBytes = stats.estimatedInputRowBytes;
      result.mergePlanningTimeUs = stats.mergePlanningTimeUs;
      result.mergeExecutionTimeUs = stats.mergeExecutionTimeUs;
    }

    if (FLAGS_dump_metrics) {
      dumpMetricsToStderr(result, *sortBuffer);
    }

    sortBuffer.reset();
    pool->release();
    std::filesystem::remove_all(spillDir);
    folly::doNotOptimizeAway(outputRows);
    return result;
  }

 private:
  folly::Executor* const executor_;
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

void writeJsonResults(
    const std::string& path,
    const std::vector<BenchmarkResult>& results) {
  std::ofstream out(path);
  BOLT_CHECK(out.is_open(), "Failed to open JSON output file: {}", path);
  out << "{\n  \"results\": [\n";
  for (size_t i = 0; i < results.size(); ++i) {
    out << "    " << results[i].jsonObject();
    out << (i + 1 == results.size() ? "\n" : ",\n");
  }
  out << "  ]\n}\n";
}

uint64_t allocatorCapacityBytes() {
  const auto memoryValues = parseDataSizeList(FLAGS_memory_gb_values);
  const auto maxMemoryBytes =
      *std::max_element(memoryValues.begin(), memoryValues.end());
  return maxMemoryBytes +
      std::max(maxMemoryBytes / 10, kAllocatorHeadroomMinBytes);
}

} // namespace
} // namespace bytedance::bolt::exec

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  bytedance::bolt::filesystems::registerLocalFileSystem();
  bytedance::bolt::serializer::presto::PrestoVectorSerde::registerVectorSerde();

  bytedance::bolt::memory::MemoryManager::Options options;
  options.useMmapAllocator = true;
  options.allocatorCapacity = bytedance::bolt::exec::allocatorCapacityBytes();
  options.useMmapArena = true;
  options.mmapArenaCapacityRatio = 1;
  bytedance::bolt::memory::MemoryManager::initialize(options);

  const auto threadValues = bytedance::bolt::exec::parseThreadValues();
  std::vector<bytedance::bolt::exec::BenchmarkResult> results;

  std::cout << fmt::format(
      "Input data sizes: {}; memory limits: {}; allocator capacity: {} "
      "(derived from max memory limit plus spill/output overhead); "
      "parallel values: {}; thread values: {}; key shapes: {}; key counts: {}; "
      "payload fields: {}; merge target rows: {}\n",
      FLAGS_data_gb_values,
      FLAGS_memory_gb_values,
      bytedance::bolt::exec::BenchmarkParams::formatBytes(
          options.allocatorCapacity),
      FLAGS_parallel_values,
      bytedance::bolt::exec::threadValuesFlag().empty()
          ? std::to_string(threadValues.front())
          : bytedance::bolt::exec::threadValuesFlag(),
      FLAGS_key_shape_values,
      FLAGS_num_key_values,
      FLAGS_num_payload_fields,
      FLAGS_parallel_merge_target_rows == 0
          ? std::string("auto")
          : std::to_string(FLAGS_parallel_merge_target_rows));
  std::cout << bytedance::bolt::exec::BenchmarkResult::header() << std::endl;
  for (const auto numThreads : threadValues) {
    folly::CPUThreadPoolExecutor executor(numThreads);
    bytedance::bolt::exec::ParallelOrderByBenchmark benchmark(
        &executor, numThreads);
    for (const auto& params : bytedance::bolt::exec::benchmarkParams()) {
      auto result = benchmark.run(params);
      results.push_back(result);
      std::cout << result.toString() << std::endl;
    }
  }
  if (!FLAGS_json_output.empty()) {
    bytedance::bolt::exec::writeJsonResults(FLAGS_json_output, results);
  }
  return 0;
}

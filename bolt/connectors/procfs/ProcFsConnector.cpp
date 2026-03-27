/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/connectors/procfs/ProcFsConnector.h"

#include <folly/Conv.h>
#include <algorithm>
#include <cctype>
#include <fstream>
#include <sstream>
#include <utility>

#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::connector::procfs {
namespace {
std::string trim(std::string_view input) {
  size_t begin = 0;
  while (begin < input.size() &&
         std::isspace(static_cast<unsigned char>(input[begin]))) {
    ++begin;
  }
  size_t end = input.size();
  while (end > begin &&
         std::isspace(static_cast<unsigned char>(input[end - 1]))) {
    --end;
  }
  return std::string(input.substr(begin, end - begin));
}

std::vector<std::string> splitWhitespace(const std::string& line) {
  std::vector<std::string> tokens;
  std::istringstream in(line);
  for (std::string token; in >> token;) {
    tokens.push_back(std::move(token));
  }
  return tokens;
}

std::string toLower(std::string value) {
  std::transform(
      value.begin(), value.end(), value.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
      });
  return value;
}

std::ifstream openFileOrThrow(const std::string& path) {
  std::ifstream in(path);
  BOLT_USER_CHECK(in.good(), "Failed to open procfs file '{}'", path);
  return in;
}

int64_t parseInt64OrDefault(const std::string& value, int64_t defaultValue = 0) {
  try {
    return folly::to<int64_t>(value);
  } catch (...) {
    return defaultValue;
  }
}
} // namespace

ProcFsTableKind ProcFsDataSource::parseTableKind(const std::string& tableName) {
  const auto normalized = toLower(tableName);
  if (normalized == "meminfo") {
    return ProcFsTableKind::kMeminfo;
  }
  if (normalized == "cpuinfo") {
    return ProcFsTableKind::kCpuinfo;
  }
  if (normalized == "modules") {
    return ProcFsTableKind::kModules;
  }
  BOLT_USER_FAIL(
      "Unknown ProcFs table '{}'. Supported tables: meminfo, cpuinfo, modules.",
      tableName);
  BOLT_UNREACHABLE("Unknown ProcFs table '{}'", tableName);
}

ProcFsDataSource::ProcFsDataSource(
    const std::shared_ptr<const RowType>& outputType,
    const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
    bolt::memory::MemoryPool* FOLLY_NONNULL pool)
    : outputType_(outputType), pool_(pool), tableName_([&]() -> std::string {
        auto procfsTable =
            std::dynamic_pointer_cast<ProcFsTableHandle>(tableHandle);
        BOLT_CHECK_NOT_NULL(
            procfsTable,
            "TableHandle must be an instance of ProcFsTableHandle.");
        return procfsTable->name();
      }()),
      tableKind_(parseTableKind(tableName_)) {}

void ProcFsDataSource::addSplit(std::shared_ptr<ConnectorSplit> split) {
  BOLT_CHECK(
      currentSplit_ == nullptr,
      "Previous split has not been processed yet. Call next() to process the split.");
  currentSplit_ = std::dynamic_pointer_cast<ProcFsConnectorSplit>(split);
  BOLT_CHECK_NOT_NULL(
      currentSplit_,
      "Wrong split type for ProcFsDataSource. Expected ProcFsConnectorSplit.");
  rowOffset_ = 0;
  parseCurrentTable();
}

std::optional<RowVectorPtr> ProcFsDataSource::next(
    uint64_t size,
    bolt::ContinueFuture& /*future*/) {
  BOLT_CHECK_NOT_NULL(
      currentSplit_, "No split to process. Call addSplit() first.");
  if (size == 0) {
    size = 1;
  }

  RowVectorPtr output;
  switch (tableKind_) {
    case ProcFsTableKind::kMeminfo:
      output = nextMeminfoBatch(size);
      break;
    case ProcFsTableKind::kCpuinfo:
      output = nextCpuinfoBatch(size);
      break;
    case ProcFsTableKind::kModules:
      output = nextModulesBatch(size);
      break;
  }
  if (output == nullptr) {
    currentSplit_ = nullptr;
    rowOffset_ = 0;
    return nullptr;
  }
  completedRows_ += output->size();
  completedBytes_ += output->retainedSize();
  return output;
}

void ProcFsDataSource::parseCurrentTable() {
  meminfoRows_.clear();
  cpuinfoRows_.clear();
  modulesRows_.clear();
  switch (tableKind_) {
    case ProcFsTableKind::kMeminfo:
      parseMeminfo();
      break;
    case ProcFsTableKind::kCpuinfo:
      parseCpuinfo();
      break;
    case ProcFsTableKind::kModules:
      parseModules();
      break;
  }
}

void ProcFsDataSource::parseMeminfo() {
  auto in = openFileOrThrow("/proc/meminfo");
  for (std::string line; std::getline(in, line);) {
    const auto sep = line.find(':');
    if (sep == std::string::npos) {
      continue;
    }
    MeminfoRow row;
    row.key = trim(std::string_view(line).substr(0, sep));
    const auto rest = trim(std::string_view(line).substr(sep + 1));
    const auto tokens = splitWhitespace(rest);
    row.value = tokens.empty() ? 0 : parseInt64OrDefault(tokens[0]);
    row.unit = tokens.size() > 1 ? tokens[1] : "";
    meminfoRows_.push_back(std::move(row));
  }
}

void ProcFsDataSource::parseCpuinfo() {
  auto in = openFileOrThrow("/proc/cpuinfo");
  int64_t currentProcessor = -1;
  for (std::string line; std::getline(in, line);) {
    if (line.empty()) {
      continue;
    }
    const auto sep = line.find(':');
    if (sep == std::string::npos) {
      continue;
    }
    auto field = trim(std::string_view(line).substr(0, sep));
    auto value = trim(std::string_view(line).substr(sep + 1));
    if (field == "processor") {
      currentProcessor = parseInt64OrDefault(value, -1);
    }
    cpuinfoRows_.push_back(CpuinfoRow{
        .processor = currentProcessor,
        .field = std::move(field),
        .value = std::move(value),
    });
  }
}

void ProcFsDataSource::parseModules() {
  auto in = openFileOrThrow("/proc/modules");
  for (std::string line; std::getline(in, line);) {
    if (line.empty()) {
      continue;
    }
    const auto tokens = splitWhitespace(line);
    if (tokens.size() < 6) {
      continue;
    }
    modulesRows_.push_back(ModulesRow{
        .module = tokens[0],
        .size = parseInt64OrDefault(tokens[1]),
        .instances = parseInt64OrDefault(tokens[2]),
        .dependencies = tokens[3],
        .state = tokens[4],
        .address = tokens[5],
    });
  }
}

RowVectorPtr ProcFsDataSource::nextMeminfoBatch(uint64_t batchSize) {
  if (rowOffset_ >= meminfoRows_.size()) {
    return nullptr;
  }
  const auto end =
      std::min<uint64_t>(rowOffset_ + batchSize, meminfoRows_.size());
  const auto outputSize = end - rowOffset_;

  auto key = BaseVector::create<FlatVector<StringView>>(
      VARCHAR(), outputSize, pool_);
  auto value = BaseVector::create<FlatVector<int64_t>>(BIGINT(), outputSize, pool_);
  auto unit = BaseVector::create<FlatVector<StringView>>(
      VARCHAR(), outputSize, pool_);

  for (vector_size_t i = 0; i < outputSize; ++i) {
    const auto& row = meminfoRows_[rowOffset_ + i];
    key->set(i, StringView(row.key));
    value->set(i, row.value);
    unit->set(i, StringView(row.unit));
  }
  rowOffset_ = end;
  return std::make_shared<RowVector>(
      pool_, outputType_, BufferPtr(), outputSize, std::vector<VectorPtr>{
                                                   key, value, unit});
}

RowVectorPtr ProcFsDataSource::nextCpuinfoBatch(uint64_t batchSize) {
  if (rowOffset_ >= cpuinfoRows_.size()) {
    return nullptr;
  }
  const auto end =
      std::min<uint64_t>(rowOffset_ + batchSize, cpuinfoRows_.size());
  const auto outputSize = end - rowOffset_;

  auto processor = BaseVector::create<FlatVector<int64_t>>(
      BIGINT(), outputSize, pool_);
  auto field = BaseVector::create<FlatVector<StringView>>(
      VARCHAR(), outputSize, pool_);
  auto value = BaseVector::create<FlatVector<StringView>>(
      VARCHAR(), outputSize, pool_);

  for (vector_size_t i = 0; i < outputSize; ++i) {
    const auto& row = cpuinfoRows_[rowOffset_ + i];
    processor->set(i, row.processor);
    field->set(i, StringView(row.field));
    value->set(i, StringView(row.value));
  }
  rowOffset_ = end;
  return std::make_shared<RowVector>(
      pool_, outputType_, BufferPtr(), outputSize, std::vector<VectorPtr>{
                                                   processor, field, value});
}

RowVectorPtr ProcFsDataSource::nextModulesBatch(uint64_t batchSize) {
  if (rowOffset_ >= modulesRows_.size()) {
    return nullptr;
  }
  const auto end =
      std::min<uint64_t>(rowOffset_ + batchSize, modulesRows_.size());
  const auto outputSize = end - rowOffset_;

  auto module = BaseVector::create<FlatVector<StringView>>(
      VARCHAR(), outputSize, pool_);
  auto size = BaseVector::create<FlatVector<int64_t>>(BIGINT(), outputSize, pool_);
  auto instances = BaseVector::create<FlatVector<int64_t>>(
      BIGINT(), outputSize, pool_);
  auto dependencies = BaseVector::create<FlatVector<StringView>>(
      VARCHAR(), outputSize, pool_);
  auto state = BaseVector::create<FlatVector<StringView>>(
      VARCHAR(), outputSize, pool_);
  auto address = BaseVector::create<FlatVector<StringView>>(
      VARCHAR(), outputSize, pool_);

  for (vector_size_t i = 0; i < outputSize; ++i) {
    const auto& row = modulesRows_[rowOffset_ + i];
    module->set(i, StringView(row.module));
    size->set(i, row.size);
    instances->set(i, row.instances);
    dependencies->set(i, StringView(row.dependencies));
    state->set(i, StringView(row.state));
    address->set(i, StringView(row.address));
  }
  rowOffset_ = end;
  return std::make_shared<RowVector>(
      pool_,
      outputType_,
      BufferPtr(),
      outputSize,
      std::vector<VectorPtr>{
          module, size, instances, dependencies, state, address});
}

} // namespace bytedance::bolt::connector::procfs

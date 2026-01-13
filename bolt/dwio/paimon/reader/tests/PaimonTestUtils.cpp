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
#include "PaimonTestUtils.h"

#include "bolt/connectors/hive/PaimonConstants.h"
#include "bolt/connectors/hive/TableHandle.h"
#include "bolt/dwio/common/Options.h"
#include "bolt/exec/Task.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/vector/tests/utils/VectorMaker.h"

namespace bytedance::bolt::dwio::paimon::test {

using namespace bytedance::bolt::connector;
using namespace bytedance::bolt::dwio;
using namespace bytedance::bolt::exec;

RowTypePtr createPaimonFile(
    bolt::test::VectorMaker vectorMaker,
    memory::MemoryPool* pool,
    std::string outputDirectoryPath,
    std::shared_ptr<folly::Executor> executor,
    std::vector<int> primaryKeyIndices,
    RowTypePtr rowType,
    std::vector<VectorPtr> data,
    std::vector<int64_t> sequenceNumber,
    std::vector<int8_t> valueKind,
    bool withRowId,
    std::unordered_map<std::string, VectorPtr> extraColumns) {
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  std::vector<VectorPtr> fileData;

  const auto& colNames = rowType->names();
  const auto& colTypes = rowType->children();
  BOLT_CHECK(!data.empty(), "data must not be empty");
  const auto inputSize = data[0]->size();

  for (int i : primaryKeyIndices) {
    names.push_back(connector::paimon::kKEY_FIELD_PREFIX + colNames[i]);
    types.push_back(colTypes[i]);
    fileData.push_back(data[i]);
  }

  auto primaryKeyNames = colNames;

  // only write sequence numbers if non-empty
  if (!sequenceNumber.empty()) {
    names.emplace_back(connector::paimon::kSEQUENCE_NUMBER);
    types.push_back(BIGINT());
    fileData.push_back(vectorMaker.flatVector<int64_t>(sequenceNumber));
  }

  names.emplace_back(connector::paimon::kVALUE_KIND);
  types.push_back(TINYINT());
  fileData.push_back(vectorMaker.flatVector<int8_t>(valueKind));

  names.insert(names.end(), colNames.begin(), colNames.end());
  types.insert(types.end(), colTypes.begin(), colTypes.end());
  fileData.insert(fileData.end(), data.begin(), data.end());
  if (withRowId) {
    names.emplace_back(connector::paimon::kColumnNameRowID);
    types.push_back(BIGINT());
    std::vector<int64_t> rowId(inputSize);
    std::iota(rowId.begin(), rowId.end(), 9001);
    fileData.push_back(vectorMaker.flatVector<int64_t>(rowId));
  }

  for (const auto& [colName, colVector] : extraColumns) {
    names.push_back(colName);
    types.push_back(colVector->type());
    fileData.push_back(colVector);
  }

  auto fileRowType =
      std::make_shared<RowType>(std::move(names), std::move(types));

  auto rowVector = std::make_shared<RowVector>(
      pool, fileRowType, BufferPtr(nullptr), fileData[0]->size(), fileData);

  std::vector<RowVectorPtr> rowVectors;
  for (int i = 0; i < inputSize; i += 64) {
    rowVectors.emplace_back(std::static_pointer_cast<RowVector>(
        rowVector->slice(i, std::min(64, rowVector->size() - i))));
  }

  auto writerPlanFragment =
      exec::test::PlanBuilder()
          .values(rowVectors)
          .orderBy(primaryKeyNames, false)
          .tableWrite(outputDirectoryPath, dwio::common::FileFormat::PARQUET)
          .planFragment();
  auto writeTask = exec::Task::create(
      "my_write_task",
      writerPlanFragment,
      0,
      core::QueryCtx::create(executor.get()),
      exec::Task::ExecutionMode::kSerial);

  while (auto result = writeTask->next())
    ;

  return fileRowType;
}

RowTypePtr createPaimonFile(
    bolt::test::VectorMaker vectorMaker,
    memory::MemoryPool* pool,
    std::string outputDirectoryPath,
    std::shared_ptr<folly::Executor> executor,
    std::vector<int> primaryKeyIndices,
    RowTypePtr rowType,
    std::vector<std::vector<int32_t>> data,
    std::vector<int64_t> sequenceNumber,
    std::vector<int8_t> valueKind,
    bool withRowId,
    std::unordered_map<std::string, VectorPtr> extraColumns) {
  std::vector<VectorPtr> fileData;
  for (const auto& colData : data) {
    fileData.push_back(vectorMaker.flatVector<int32_t>(colData));
  }
  return createPaimonFile(
      vectorMaker,
      pool,
      outputDirectoryPath,
      executor,
      primaryKeyIndices,
      rowType,
      fileData,
      sequenceNumber,
      valueKind,
      withRowId,
      extraColumns);
}

std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
getIdentityAssignment(
    RowTypePtr rowType,
    std::unordered_map<std::string, TypePtr> partitionKeys) {
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      assignments;

  for (int i = 0; i < rowType->size(); i++) {
    const auto& colType = rowType->childAt(i);
    const auto& colName = rowType->nameOf(i);
    auto columnHandleType = hive::HiveColumnHandle::ColumnType::kRegular;
    if (partitionKeys.find(colName) != partitionKeys.end()) {
      columnHandleType = hive::HiveColumnHandle::ColumnType::kPartitionKey;
    } else if (
        colName == connector::paimon::kColumnNameRowIndex ||
        colName == connector::paimon::kColumnNameFilePath ||
        colName == connector::paimon::kColumnNamePartition ||
        colName == connector::paimon::kColumnNameBucket ||
        colName == connector::paimon::kColumnNameRowID) {
      columnHandleType = hive::HiveColumnHandle::ColumnType::kSynthesized;
    }
    assignments[colName] = std::make_shared<connector::hive::HiveColumnHandle>(
        colName, columnHandleType, colType, colType);
  }

  for (const auto& [colName, colType] : partitionKeys) {
    assignments[colName] = std::make_shared<connector::hive::HiveColumnHandle>(
        colName,
        hive::HiveColumnHandle::ColumnType::kPartitionKey,
        colType,
        colType);
  }

  return assignments;
}

} // namespace bytedance::bolt::dwio::paimon::test

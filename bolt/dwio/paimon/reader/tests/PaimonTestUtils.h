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
#include <folly/Executor.h>

#include <string>

#include "bolt/connectors/Connector.h"
#include "bolt/type/Type.h"
#include "bolt/vector/BaseVector.h"
#include "bolt/vector/tests/utils/VectorMaker.h"

namespace bytedance::bolt::dwio::paimon::test {

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
    bool withRowId = false,
    std::unordered_map<std::string, VectorPtr> extraColumns = {});

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
    bool withRowId = false,
    std::unordered_map<std::string, VectorPtr> extraColumns = {});

std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
getIdentityAssignment(
    RowTypePtr rowType,
    std::unordered_map<std::string, TypePtr> partitionKeys = {});

} // namespace bytedance::bolt::dwio::paimon::test

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

#pragma once

#include <stdint.h>
namespace bytedance::bolt::connector::paimon {

static constexpr const char* kColumnNameRowIndex = "__paimon_row_index";
static constexpr const char* kColumnNameFilePath = "__paimon_file_path";
static constexpr const char* kColumnNamePartition = "__paimon_partition";
static constexpr const char* kColumnNameBucket = "__paimon_bucket";
static constexpr const char* kColumnNameRowID = "_ROW_ID";
static constexpr const char* kSEQUENCE_NUMBER = "_SEQUENCE_NUMBER";
static constexpr const char* kVALUE_KIND = "_VALUE_KIND";
static constexpr const char* kKEY_FIELD_PREFIX = "_KEY_";
static constexpr uint32_t kMAX_BATCH_SIZE = 1024;

static constexpr const char* kFileMetaFirstRowID = "_FIRST_ROW_ID";
static constexpr const char* kFileMetaMaxSequenceNumber =
    "_MAX_SEQUENCE_NUMBER";

static constexpr const char* kPrimaryKey = "primary-key";
static constexpr const char* kSequenceField = "sequence.field";
static constexpr const char* kRowKindField = "rowkind.field";
static constexpr const char* kMergeEngine = "merge-engine";
static constexpr const char* kDeduplicateMergeEngine = "deduplicate";
static constexpr const char* kRowTrackingEnabled = "row-tracking.enabled";
static constexpr const char* kPartialUpdateMergeEngine = "partial-update";
static constexpr const char* kAggregateMergeEngine = "aggregation";
static constexpr const char* kFirstRowMergeEngine = "first-row";
static constexpr const char* kIgnoreDelete = "ignore-delete";
static constexpr const char* kAggregateEngineKeyPrefix = "fields.";
static constexpr const char* kAggregateEngineKeyPostfix = ".aggregate-function";
static constexpr const char* kAggregateSumName = "sum";
static constexpr const char* kAggregateProductName = "product";
static constexpr const char* kAggregateMaxName = "max";
static constexpr const char* kAggregateMinName = "min";
static constexpr const char* kAggregateLastValueName = "last_value";
static constexpr const char* kAggregateLastNonNullValueName =
    "last_non_null_value";
static constexpr const char* kAggregateListAggName = "listagg";
static constexpr const char* kAggregateBoolAndName = "bool_and";
static constexpr const char* kAggregateBoolOrName = "bool_or";
static constexpr const char* kAggregateFirstValueName = "first_value";
static constexpr const char* kAggregateFirstNonNullValueName =
    "first_non_null_value";
static constexpr const char* kAggregateNestedUpdateName = "nested_update";
static constexpr const char* kAggregateCollectName = "collect";
static constexpr const char* kAggregateMergeMapName = "merge_map";
static constexpr const char* kAggregateListAggDelimiter = ".list-agg-delimiter";
static constexpr const char* kAggregateCollectDistinct = ".distinct";

static constexpr const char* kPartialUpdateKeyPrefix = "fields.";
static constexpr const char* kPartialUpdateSequenceGroup = ".sequence-group";
static constexpr const char* kPartialUpdateDefaultAggregateFunctionKey =
    "fields.default-aggregate-function";

} // namespace bytedance::bolt::connector::paimon

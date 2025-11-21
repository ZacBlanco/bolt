/*
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates
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

#include "velox/expression/VectorFunction.h"
#include "velox/functions/Registerer.h"
#include "velox/functions/prestosql/DateTimeFunctions.h"
namespace bytedance::bolt::functions {
namespace {
void registerArithmeticFunctionsInternal(const std::string& prefix) {
  registerFunction<DateMinusIntervalDayTime, Date, Date, IntervalDayTime>(
      {prefix + "minus"});
  registerFunction<DatePlusIntervalDayTime, Date, Date, IntervalDayTime>(
      {prefix + "plus"});
  registerFunction<
      TimestampMinusIntervalDayTime,
      Timestamp,
      Timestamp,
      IntervalDayTime>({prefix + "minus"});
  registerFunction<
      TimestampPlusIntervalDayTime,
      Timestamp,
      Timestamp,
      IntervalDayTime>({prefix + "plus"});
  registerFunction<
      IntervalDayTimePlusTimestamp,
      Timestamp,
      IntervalDayTime,
      Timestamp>({prefix + "plus"});
  registerFunction<
      TimestampMinusFunction,
      IntervalDayTime,
      Timestamp,
      Timestamp>({prefix + "minus"});

  registerFunction<HiveDateDiffFunction, int32_t, Date, Date>(
      {prefix + "datediff"});
  registerFunction<DateDiffFunction, int64_t, Varchar, Timestamp, Timestamp>(
      {prefix + "timestampdiff"});

#ifndef SPARK_COMPATIBLE
  registerFunction<
      DateDiffFunction,
      int64_t,
      Varchar,
      TimestampWithTimezone,
      TimestampWithTimezone>({prefix + "date_diff"});
  registerFunction<DateFormatFunction, Varchar, Timestamp, Varchar>(
      {prefix + "date_format"});
#endif
}
} // namespace

void registerDateTimeArithmeticFunctions(const std::string& prefix) {
  registerTimestampWithTimeZoneType();
  registerArithmeticFunctionsInternal(prefix);
}
} // namespace bytedance::bolt::functions
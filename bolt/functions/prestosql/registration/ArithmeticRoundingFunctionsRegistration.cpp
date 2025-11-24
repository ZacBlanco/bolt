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
#include "velox/functions/Registerer.h"
#include "velox/functions/lib/RegistrationHelpers.h"
#include "velox/functions/prestosql/Arithmetic.h"

namespace bytedance::bolt::functions {

namespace {

void registerRoundingFunctionsInternal(const std::string& prefix) {
  registerUnaryNumeric<CeilFunction>(
      {prefix + "ceil", prefix + "ceiling", prefix + "bytedance_ceil"});
  registerFunction<CeilFunction, int64_t, double>({prefix + "bytedance_ceil"});
  registerFunction<CeilFunction, int64_t, float>({prefix + "bytedance_ceil"});
  registerUnaryNumeric<FloorFunction>(
      {prefix + "floor", prefix + "bytedance_floor"});
  registerFunction<FloorFunction, int64_t, double>(
      {prefix + "bytedance_floor"});
  registerUnaryNumeric<RoundFunction>({prefix + "round"});
  registerFunction<RoundFunction, int8_t, int8_t, int32_t>({prefix + "round"});
  registerFunction<RoundFunction, int16_t, int16_t, int32_t>(
      {prefix + "round"});
  registerFunction<RoundFunction, int32_t, int32_t, int32_t>(
      {prefix + "round"});
  registerFunction<RoundFunction, int64_t, int64_t, int32_t>(
      {prefix + "round"});
  registerFunction<RoundFunction, double, double, int32_t>({prefix + "round"});
  registerFunction<RoundFunction, float, float, int32_t>({prefix + "round"});
  registerFunction<TruncateFunction, double, double>({prefix + "truncate"});
  registerFunction<TruncateFunction, double, double, int32_t>(
      {prefix + "truncate"});
}

} // namespace

void registerRoundingFunctions(const std::string& prefix = "") {
  registerRoundingFunctionsInternal(prefix);
}

} // namespace bytedance::bolt::functions
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
#include "velox/functions/sparksql/Arithmetic.h"
#include "velox/functions/sparksql/String.h"

namespace bytedance::bolt::functions {

namespace {

void registerMiscArithmeticFunctionsInternal(const std::string& prefix) {
  registerFunction<ClampFunction, int8_t, int8_t, int8_t, int8_t>(
      {prefix + "clamp"});
  registerFunction<ClampFunction, int16_t, int16_t, int16_t, int16_t>(
      {prefix + "clamp"});
  registerFunction<ClampFunction, int32_t, int32_t, int32_t, int32_t>(
      {prefix + "clamp"});
  registerFunction<ClampFunction, int64_t, int64_t, int64_t, int64_t>(
      {prefix + "clamp"});
  registerFunction<ClampFunction, double, double, double, double>(
      {prefix + "clamp"});
  registerFunction<ClampFunction, float, float, float, float>(
      {prefix + "clamp"});
  registerFunction<
      WidthBucketFunction,
      int64_t,
      double,
      double,
      double,
      int64_t>({prefix + "width_bucket"});
  registerUnaryNumeric<SignFunction>({prefix + "sign"});
  registerFunction<FromBaseFunction, int64_t, Varchar, int64_t>(
      {prefix + "from_base"});
  registerFunction<ToBaseFunction, Varchar, int64_t, int64_t>(
      {prefix + "to_base"});
  registerFunction<ToBigintFunction, int64_t, double>({prefix + "to_bigint"});
  registerFunction<ToBigintFunction, int64_t, float>({prefix + "to_bigint"});
  registerFunction<ToIntegerFunction, int32_t, double>({prefix + "to_integer"});
  registerFunction<ToIntegerFunction, int32_t, float>({prefix + "to_integer"});
  registerFunction<
      functions::sparksql::FindInSetFunction,
      int32_t,
      Varchar,
      Varchar>({prefix + "find_in_set"});
  registerFunction<
      functions::sparksql::TranslateFunction,
      Varchar,
      Varchar,
      Varchar,
      Varchar>({prefix + "translate"});
  registerBinaryIntegral<sparksql::PModIntFunction>({prefix + "pmod"});
  registerFunction<
      CosineSimilarityFunction,
      double,
      Map<Varchar, double>,
      Map<Varchar, double>>({prefix + "cosine_similarity"});
}

} // namespace

void registerMiscArithmeticFunctions(const std::string& prefix = "") {
  registerMiscArithmeticFunctionsInternal(prefix);
}

} // namespace bytedance::bolt::functions
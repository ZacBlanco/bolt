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
#include "velox/functions/prestosql/Arithmetic.h"
#include "velox/functions/sparksql/Arithmetic.h"

namespace bytedance::bolt::functions {

namespace {

void registerExponentialLogarithmicFunctionsInternal(
    const std::string& prefix) {
  registerFunction<PowerFunction, double, double, double>(
      {prefix + "power", prefix + "pow"});
  registerFunction<PowerFunction, double, int64_t, int64_t>(
      {prefix + "power", prefix + "pow"});
  registerFunction<ExpFunction, double, double>({prefix + "exp"});
  registerFunction<LnFunction, double, double>({prefix + "ln", prefix + "log"});
  registerFunction<LnFunction, double, int32_t>(
      {prefix + "ln", prefix + "log"});
  registerFunction<LnFunction, double, int64_t>(
      {prefix + "ln", prefix + "log"});
  registerFunction<sparksql::LnFunction, double, double>(
      {prefix + "bytedance_ln"});
  registerFunction<Log2Function, double, double>({prefix + "log2"});
  registerFunction<Log10Function, double, double>({prefix + "log10"});
  registerFunction<sparksql::LogFunction, double, double, double>(
      {prefix + "log"});
  registerFunction<SqrtFunction, double, double>({prefix + "sqrt"});
  registerFunction<CbrtFunction, double, double>({prefix + "cbrt"});
}

} // namespace

void registerExponentialLogarithmicFunctions(const std::string& prefix = "") {
  registerExponentialLogarithmicFunctionsInternal(prefix);
}

} // namespace bytedance::bolt::functions
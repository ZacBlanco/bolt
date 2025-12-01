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
#include "bolt/functions/Registerer.h"
#include "bolt/functions/prestosql/Arithmetic.h"

namespace bytedance::bolt::functions {

namespace {

void registerTrigonometricFunctionsInternal(const std::string& prefix) {
  registerFunction<RadiansFunction, double, double>({prefix + "radians"});
  registerFunction<DegreesFunction, double, double>({prefix + "degrees"});
  registerFunction<CosFunction, double, double>({prefix + "cos"});
  registerFunction<CoshFunction, double, double>({prefix + "cosh"});
  registerFunction<AcosFunction, double, double>({prefix + "acos"});
  registerFunction<SinFunction, double, double>({prefix + "sin"});
  registerFunction<AsinFunction, double, double>({prefix + "asin"});
  registerFunction<TanFunction, double, double>({prefix + "tan"});
  registerFunction<TanhFunction, double, double>({prefix + "tanh"});
  registerFunction<AtanFunction, double, double>({prefix + "atan"});
  registerFunction<Atan2Function, double, double, double>({prefix + "atan2"});
}

} // namespace

void registerTrigonometricFunctions(const std::string& prefix = "") {
  registerTrigonometricFunctionsInternal(prefix);
}

} // namespace bytedance::bolt::functions
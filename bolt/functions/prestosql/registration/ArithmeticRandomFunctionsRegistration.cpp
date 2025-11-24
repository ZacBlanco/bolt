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
#include "velox/functions/prestosql/Rand.h"

namespace bytedance::bolt::functions {

namespace {

void registerBytedanceRandFunctions(const std::string& prefix) {
  registerFunction<RandFunction, double>({prefix + "bytedance_rand"});
  registerFunction<RandFunction, double, int8_t>({prefix + "bytedance_rand"});
  registerFunction<RandFunction, double, int16_t>({prefix + "bytedance_rand"});
  registerFunction<RandFunction, double, int32_t>({prefix + "bytedance_rand"});
  registerFunction<RandFunction, double, int64_t>({prefix + "bytedance_rand"});
}

void registerRandomFunctionsInternal(const std::string& prefix) {
  registerFunction<PiFunction, double>({prefix + "pi"});
  registerFunction<EulerConstantFunction, double>({prefix + "e"});
  registerFunction<InfinityFunction, double>({prefix + "infinity"});
  registerFunction<IsFiniteFunction, bool, double>({prefix + "is_finite"});
  registerFunction<IsInfiniteFunction, bool, double>({prefix + "is_infinite"});
  registerFunction<IsNanFunction, bool, double>({prefix + "is_nan"});
  registerFunction<NanFunction, double>({prefix + "nan"});
  registerFunction<RandFunction, double>({prefix + "rand", prefix + "random"});
  registerUnaryIntegral<RandFunction>({prefix + "rand", prefix + "random"});
  registerBytedanceRandFunctions(prefix);
}

} // namespace

void registerRandomFunctions(const std::string& prefix = "") {
  registerRandomFunctionsInternal(prefix);
}

} // namespace bytedance::bolt::functions
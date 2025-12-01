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

#include "bolt/functions/lib/RegistrationHelpers.h"
#include "bolt/functions/prestosql/Arithmetic.h"
#include "bolt/functions/sparksql/Arithmetic.h"

namespace bytedance::bolt::functions::sparksql {

void registerMathTrigonometricFunctions(const std::string& prefix) {
  registerFunction<AcosFunction, double, double>({prefix + "acos"});
  registerFunction<AcoshFunction, double, double>({prefix + "acosh"});
  registerFunction<AsinhFunction, double, double>({prefix + "asinh"});
  registerFunction<AtanhFunction, double, double>({prefix + "atanh"});
  registerFunction<SecFunction, double, double>({prefix + "sec"});
  registerFunction<CscFunction, double, double>({prefix + "csc"});
  registerFunction<SinhFunction, double, double>({prefix + "sinh"});
  registerFunction<CoshFunction, double, double>({prefix + "cosh"});
  registerFunction<CotFunction, double, double>({prefix + "cot"});
  registerFunction<Atan2Function, double, double, double>({prefix + "atan2"});
  registerFunction<HypotFunction, double, double, double>({prefix + "hypot"});
  registerFunction<sparksql::Atan2Function, double, double, double>(
      {prefix + "atan2"});
}
} // namespace bytedance::bolt::functions::sparksql

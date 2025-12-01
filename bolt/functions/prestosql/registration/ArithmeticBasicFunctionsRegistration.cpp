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
#include "bolt/functions/lib/RegistrationHelpers.h"
#include "bolt/functions/prestosql/Arithmetic.h"

namespace bytedance::bolt::functions {

namespace {

void registerBasicArithmeticFunctionsInternal(const std::string& prefix) {
  registerBinaryFloatingPoint<PlusFunction>({prefix + "plus"});
  registerFunction<
      PlusFunction,
      IntervalDayTime,
      IntervalDayTime,
      IntervalDayTime>({prefix + "plus"});
  registerBinaryFloatingPoint<MinusFunction>({prefix + "minus"});
  registerFunction<
      MinusFunction,
      IntervalDayTime,
      IntervalDayTime,
      IntervalDayTime>({prefix + "minus"});
  registerBinaryFloatingPoint<MultiplyFunction>({prefix + "multiply"});
  registerFunction<MultiplyFunction, IntervalDayTime, IntervalDayTime, int64_t>(
      {prefix + "multiply"});
  registerFunction<MultiplyFunction, IntervalDayTime, int64_t, IntervalDayTime>(
      {prefix + "multiply"});
  registerFunction<
      IntervalMultiplyFunction,
      IntervalDayTime,
      IntervalDayTime,
      double>({prefix + "multiply"});
  registerFunction<
      IntervalMultiplyFunction,
      IntervalDayTime,
      double,
      IntervalDayTime>({prefix + "multiply"});
  registerBinaryFloatingPoint<DivideFunction>({prefix + "divide"});
  registerFunction<
      IntervalDivideFunction,
      IntervalDayTime,
      IntervalDayTime,
      double>({prefix + "divide"});
  registerBinaryFloatingPoint<ModulusFunction>({prefix + "mod"});
  registerUnaryNumeric<AbsFunction>({prefix + "abs"});
  registerUnaryFloatingPoint<NegateFunction>({prefix + "negate"});
}

} // namespace

void registerBasicArithmeticFunctions(const std::string& prefix = "") {
  registerBasicArithmeticFunctionsInternal(prefix);
  BOLT_REGISTER_VECTOR_FUNCTION(udf_not, prefix + "not");
}

} // namespace bytedance::bolt::functions
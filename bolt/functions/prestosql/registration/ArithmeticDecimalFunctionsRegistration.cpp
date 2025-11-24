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

namespace bytedance::bolt::functions {

void registerDecimalArithmeticFunctions(const std::string& prefix = "") {
  BOLT_REGISTER_VECTOR_FUNCTION(udf_decimal_add, prefix + "plus");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_decimal_sub, prefix + "minus");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_decimal_mul, prefix + "multiply");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_decimal_div, prefix + "divide");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_decimal_round, prefix + "round");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_decimal_abs, prefix + "abs");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_decimal_negate, prefix + "negate");
}

} // namespace bytedance::bolt::functions
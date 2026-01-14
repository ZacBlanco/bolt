# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# --------------------------------------------------------------------------
# Copyright (c) ByteDance Ltd. and/or its affiliates.
# SPDX-License-Identifier: Apache-2.0
#
# This file has been modified by ByteDance Ltd. and/or its affiliates on
# 2025-11-11.
#
# Original file was released under the Apache License 2.0,
# with the full license text available at:
#     http://www.apache.org/licenses/LICENSE-2.0
#
# This modified file is released under the same license.
# --------------------------------------------------------------------------

add_subdirectory(dbgen)

add_library(bolt_tpch_gen DBGenIterator.cpp TpchGen.cpp)

target_include_directories(bolt_tpch_gen PRIVATE dbgen/include)

target_link_libraries(bolt_tpch_gen bolt_memory bolt_vector bolt_dbgen)

add_executable(bolt_tpch_generate TpchGenerateMain.cpp)
# Link against generator library and required runtime libs for plan execution and Hive writes.
target_link_libraries(
  bolt_tpch_generate
  bolt_tpch_gen
  bolt_exec
  bolt_exec_test_lib
  bolt_connector
  bolt_hive_connector
  bolt_dwio_common
  bolt_dwio_parquet_reader
  bolt_memory
  bolt_type
  bolt_vector
  Folly::folly
  fmt::fmt
  gflags::gflags)

if(${BOLT_BUILD_TESTING})
  add_subdirectory(tests)
endif()

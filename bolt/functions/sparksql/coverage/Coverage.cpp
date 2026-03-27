/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include <gflags/gflags.h>
#include "bolt/core/QueryCtx.h"
#include "bolt/functions/CoverageUtil.h"
#include "bolt/plugin/builtin/SparkFunctionsPlugin.h"

DEFINE_bool(all, false, "Generate coverage map for all Spark functions");
using namespace bytedance::bolt;

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  auto queryCtx = core::QueryCtx::create();
  queryCtx->addPlugin(plugin::builtin::createSparkFunctionsPlugin());

  if (FLAGS_all) {
    functions::printCoverageMapForAll(":spark");
  } else {
    functions::printBoltFunctions({}, ":spark");
  }

  return 0;
}

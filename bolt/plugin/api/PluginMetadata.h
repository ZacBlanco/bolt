/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <string>

namespace bytedance::bolt::plugin {

struct PluginMetadata {
  std::string name;
  std::string version;
  std::string boltAbi;
  std::string vendor;
};

} // namespace bytedance::bolt::plugin

/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <string>

namespace bytedance::bolt::plugin {

class PluginRegistrar;

class IPlugin {
 public:
  virtual ~IPlugin() = default;

  virtual const std::string& name() const = 0;
  virtual void registerInto(PluginRegistrar& registrar) = 0;
};

} // namespace bytedance::bolt::plugin

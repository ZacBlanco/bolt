/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <memory>
#include <vector>

#include "bolt/plugin/api/PluginSpec.h"

namespace bytedance::bolt::plugin {

class OperatorInstaller {
 public:
  virtual ~OperatorInstaller() = default;
  virtual void install(const OperatorSpec& spec) = 0;
};

void registerOperatorSpec(OperatorSpec spec);

std::vector<OperatorSpec> listOperatorSpecs();

void setOperatorInstaller(std::shared_ptr<OperatorInstaller> installer);

} // namespace bytedance::bolt::plugin

/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <memory>
#include <string>

#include "bolt/functions/sparksql/aggregates/Register.h"
#include "bolt/functions/sparksql/registration/Register.h"
#include "bolt/functions/sparksql/window/WindowFunctionsRegistration.h"
#include "bolt/plugin/api/IPlugin.h"
#include "bolt/plugin/api/PluginRegistrar.h"

namespace bytedance::bolt::plugin::builtin {

class SparkFunctionsPlugin final : public IPlugin {
 public:
  const std::string& name() const override {
    static const std::string kName = "builtin.spark_functions";
    return kName;
  }

  void registerInto(PluginRegistrar& registrar) override {
    PluginMetadata metadata;
    metadata.name = name();
    metadata.version = "1.0.0";
    metadata.boltAbi = "bolt-plugin-v1";
    metadata.vendor = "bolt";
    registrar.setMetadata(metadata);
    ::bytedance::bolt::functions::sparksql::registerFunctions("");
    ::bytedance::bolt::functions::aggregate::sparksql::registerAggregateFunctions(
        "");
    ::bytedance::bolt::functions::window::sparksql::registerWindowFunctions("");
  }
};

inline std::shared_ptr<IPlugin> createSparkFunctionsPlugin() {
  return std::make_shared<SparkFunctionsPlugin>();
}

} // namespace bytedance::bolt::plugin::builtin

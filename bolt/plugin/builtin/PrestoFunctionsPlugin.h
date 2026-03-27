/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <memory>
#include <string>

#include "bolt/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "bolt/functions/prestosql/registration/RegistrationFunctions.h"
#include "bolt/plugin/api/IPlugin.h"
#include "bolt/plugin/api/PluginRegistrar.h"

namespace bytedance::bolt::plugin::builtin {

class PrestoFunctionsPlugin final : public IPlugin {
 public:
  explicit PrestoFunctionsPlugin(std::string prefix = "")
      : prefix_(std::move(prefix)) {}

  const std::string& name() const override {
    static const std::string kName = "builtin.presto_functions";
    return kName;
  }

  void registerInto(PluginRegistrar& registrar) override {
    PluginMetadata metadata;
    metadata.name = name();
    metadata.version = "1.0.0";
    metadata.boltAbi = "bolt-plugin-v1";
    metadata.vendor = "bolt";
    registrar.setMetadata(metadata);
    ::bytedance::bolt::functions::prestosql::registerAllScalarFunctions(prefix_);
    ::bytedance::bolt::aggregate::prestosql::registerAllAggregateFunctions(
        prefix_);
  }

 private:
  const std::string prefix_;
};

inline std::shared_ptr<IPlugin> createPrestoFunctionsPlugin(
    std::string prefix = "") {
  return std::make_shared<PrestoFunctionsPlugin>(std::move(prefix));
}

} // namespace bytedance::bolt::plugin::builtin

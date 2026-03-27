/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <memory>
#include <string>

#include "bolt/connectors/Connector.h"
#include "bolt/connectors/hive/HiveConnector.h"
#include "bolt/plugin/api/IPlugin.h"
#include "bolt/plugin/api/PluginRegistrar.h"

namespace bytedance::bolt::plugin::builtin {

class HiveConnectorPlugin final : public IPlugin {
 public:
  const std::string& name() const override {
    static const std::string kName = "builtin.hive_connector_factory";
    return kName;
  }

  void registerInto(PluginRegistrar& registrar) override {
    PluginMetadata metadata;
    metadata.name = name();
    metadata.version = "1.0.0";
    metadata.boltAbi = "bolt-plugin-v1";
    metadata.vendor = "bolt";
    registrar.setMetadata(metadata);

    if (!connector::hasConnectorFactory(connector::kHiveConnectorName)) {
      registrar.addConnectorFactory(plugin::ConnectorFactorySpec{
          .connectorName = connector::kHiveConnectorName,
          .factory = std::make_shared<connector::hive::HiveConnectorFactory>()});
    }
  }
};

inline std::shared_ptr<IPlugin> createHiveConnectorPlugin() {
  return std::make_shared<HiveConnectorPlugin>();
}

} // namespace bytedance::bolt::plugin::builtin

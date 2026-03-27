/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <memory>
#include <string>

#include "bolt/connectors/Connector.h"
#include "bolt/connectors/procfs/ProcFsConnector.h"
#include "bolt/plugin/api/IPlugin.h"
#include "bolt/plugin/api/PluginRegistrar.h"

namespace bytedance::bolt::plugin::builtin {

class ProcFsConnectorPlugin final : public IPlugin {
 public:
  const std::string& name() const override {
    static const std::string kName = "builtin.procfs_connector_factory";
    return kName;
  }

  void registerInto(PluginRegistrar& registrar) override {
    PluginMetadata metadata;
    metadata.name = name();
    metadata.version = "1.0.0";
    metadata.boltAbi = "bolt-plugin-v1";
    metadata.vendor = "bolt";
    registrar.setMetadata(metadata);

    if (!connector::hasConnectorFactory(connector::kProcFsConnectorName)) {
      registrar.addConnectorFactory(plugin::ConnectorFactorySpec{
          .connectorName = connector::kProcFsConnectorName,
          .factory =
              std::make_shared<connector::procfs::ProcFsConnectorFactory>()});
    }
  }
};

inline std::shared_ptr<IPlugin> createProcFsConnectorPlugin() {
  return std::make_shared<ProcFsConnectorPlugin>();
}

} // namespace bytedance::bolt::plugin::builtin


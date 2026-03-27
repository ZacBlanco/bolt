/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <memory>
#include <string>

#include "bolt/plugin/api/IPlugin.h"
#include "bolt/plugin/api/PluginRegistrar.h"
#include "bolt/serializers/ArrowSerializer.h"
#include "bolt/serializers/CompactRowSerializer.h"
#include "bolt/serializers/PrestoSerializer.h"
#include "bolt/serializers/UnsafeRowSerializer.h"
#include "bolt/vector/VectorStream.h"

namespace bytedance::bolt::plugin::builtin {

class StandardVectorFormatsPlugin final : public IPlugin {
 public:
  const std::string& name() const override {
    static const std::string kName = "builtin.standard_vector_formats";
    return kName;
  }

  void registerInto(PluginRegistrar& registrar) override {
    PluginMetadata metadata;
    metadata.name = name();
    metadata.version = "1.0.0";
    metadata.boltAbi = "bolt-plugin-v1";
    metadata.vendor = "bolt";
    registrar.setMetadata(metadata);

    if (!isRegisteredNamedVectorSerde(VectorSerde::Kind::kPresto)) {
      registrar.addVectorFormat(plugin::VectorFormatSpec{
          .kind = VectorSerde::Kind::kPresto,
          .serde = std::make_unique<serializer::presto::PrestoVectorSerde>()});
    }
    if (!isRegisteredNamedVectorSerde(VectorSerde::Kind::kCompactRow)) {
      registrar.addVectorFormat(plugin::VectorFormatSpec{
          .kind = VectorSerde::Kind::kCompactRow,
          .serde = std::make_unique<serializer::CompactRowVectorSerde>()});
    }
    if (!isRegisteredNamedVectorSerde(VectorSerde::Kind::kUnsafeRow)) {
      registrar.addVectorFormat(plugin::VectorFormatSpec{
          .kind = VectorSerde::Kind::kUnsafeRow,
          .serde = std::make_unique<serializer::spark::UnsafeRowVectorSerde>()});
    }
    if (!isRegisteredNamedVectorSerde(VectorSerde::Kind::kArrow)) {
      registrar.addVectorFormat(plugin::VectorFormatSpec{
          .kind = VectorSerde::Kind::kArrow,
          .serde = std::make_unique<serializer::arrowserde::ArrowVectorSerde>()});
    }
  }
};

inline std::shared_ptr<IPlugin> createStandardVectorFormatsPlugin() {
  return std::make_shared<StandardVectorFormatsPlugin>();
}

} // namespace bytedance::bolt::plugin::builtin

/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "bolt/plugin/api/PluginMetadata.h"
#include "bolt/plugin/api/PluginSpec.h"

namespace bytedance::bolt::plugin {

class PluginRegistrar {
 public:
  virtual ~PluginRegistrar() = default;

  virtual void setMetadata(PluginMetadata metadata) = 0;

  virtual void addScalarFunction(ScalarFunctionSpec spec) = 0;
  virtual void addAggregateFunction(AggregateFunctionSpec spec) = 0;
  virtual void addOperator(OperatorSpec spec) = 0;
  virtual void addOptimizerPass(OptimizerPassSpec spec) = 0;
  virtual void addLogicalType(LogicalTypeSpec spec) = 0;
  virtual void addVectorFormat(VectorFormatSpec spec) = 0;
  virtual void addConnectorFactory(ConnectorFactorySpec spec) = 0;
};

} // namespace bytedance::bolt::plugin

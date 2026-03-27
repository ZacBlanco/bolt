/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "bolt/connectors/Connector.h"
#include "bolt/exec/Aggregate.h"
#include "bolt/expression/VectorFunction.h"
#include "bolt/type/Type.h"
#include "bolt/vector/VectorStream.h"

namespace bytedance::bolt::plugin {

struct ScalarFunctionSpec {
  std::string name;
  std::vector<exec::FunctionSignaturePtr> signatures;
  exec::VectorFunctionFactory factory;
  exec::VectorFunctionMetadata metadata;
  bool overwrite{true};
};

struct AggregateFunctionSpec {
  std::string name;
  std::vector<exec::AggregateFunctionSignaturePtr> signatures;
  exec::AggregateFunctionFactory factory;
  bool registerCompanionFunctions{false};
  bool overwrite{false};
};

struct LogicalTypeSpec {
  std::string name;
  std::unique_ptr<const CustomTypeFactories> factories;
};

struct ConnectorFactorySpec {
  std::string connectorName;
  std::shared_ptr<connector::ConnectorFactory> factory;
};

struct OperatorSpec {
  std::string name;
};

enum class OptimizerStage {
  kLogical,
  kPhysical,
  kPostPhysical,
};

struct OptimizerContext {};

class OptimizerPass {
 public:
  virtual ~OptimizerPass() = default;
  virtual core::PlanNodePtr transform(
      const core::PlanNodePtr& inputPlan,
      const OptimizerContext& context) = 0;
};

struct OptimizerPassSpec {
  std::string name;
  OptimizerStage stage;
  std::shared_ptr<OptimizerPass> pass;
};

struct VectorFormatSpec {
  VectorSerde::Kind kind;
  std::unique_ptr<VectorSerde> serde;
};

} // namespace bytedance::bolt::plugin

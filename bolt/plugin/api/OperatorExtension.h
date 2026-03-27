/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <cstdint>
#include <memory>
#include <optional>

#include "bolt/core/PlanNode.h"

namespace bytedance::bolt::exec {
class DriverCtx;
class ExchangeClient;
class JoinBridge;
class Operator;
} // namespace bytedance::bolt::exec

namespace bytedance::bolt::plugin {

/// Public operator translator interface used by plugins.
///
/// This mirrors the core behavior needed by exec::Operator::PlanNodeTranslator
/// while remaining decoupled from exec headers in the public plugin API.
class OperatorTranslator {
 public:
  virtual ~OperatorTranslator() = default;

  virtual std::unique_ptr<exec::Operator> toOperator(
      exec::DriverCtx* ctx,
      int32_t id,
      const core::PlanNodePtr& node) {
    return nullptr;
  }

  virtual std::unique_ptr<exec::Operator> toOperator(
      exec::DriverCtx* ctx,
      int32_t id,
      const core::PlanNodePtr& node,
      std::shared_ptr<exec::ExchangeClient> exchangeClient) {
    return nullptr;
  }

  virtual std::unique_ptr<exec::JoinBridge> toJoinBridge(
      const core::PlanNodePtr& node) {
    return nullptr;
  }

  virtual std::optional<uint32_t> maxDrivers(const core::PlanNodePtr& node) {
    return std::nullopt;
  }
};

} // namespace bytedance::bolt::plugin

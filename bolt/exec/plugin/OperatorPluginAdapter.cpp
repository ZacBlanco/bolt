/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/exec/plugin/OperatorPluginAdapter.h"

#include <mutex>
#include <utility>

#include "bolt/exec/Operator.h"
#include "bolt/plugin/OperatorRegistry.h"

namespace bytedance::bolt::exec::plugin {
namespace {

class PluginPlanNodeTranslatorAdapter final : public exec::Operator::PlanNodeTranslator {
 public:
  explicit PluginPlanNodeTranslatorAdapter(
      std::shared_ptr<::bytedance::bolt::plugin::OperatorTranslator> translator)
      : translator_(std::move(translator)) {}

  std::unique_ptr<exec::Operator> toOperator(
      exec::DriverCtx* ctx,
      int32_t id,
      const core::PlanNodePtr& node) override {
    return translator_->toOperator(ctx, id, node);
  }

  std::unique_ptr<exec::Operator> toOperator(
      exec::DriverCtx* ctx,
      int32_t id,
      const core::PlanNodePtr& node,
      std::shared_ptr<exec::ExchangeClient> exchangeClient) override {
    return translator_->toOperator(ctx, id, node, std::move(exchangeClient));
  }

  std::unique_ptr<exec::JoinBridge> toJoinBridge(
      const core::PlanNodePtr& node) override {
    return translator_->toJoinBridge(node);
  }

  std::optional<uint32_t> maxDrivers(const core::PlanNodePtr& node) override {
    return translator_->maxDrivers(node);
  }

 private:
  std::shared_ptr<::bytedance::bolt::plugin::OperatorTranslator> translator_;
};

void installOperatorSpec(const ::bytedance::bolt::plugin::OperatorSpec& spec) {
  exec::Operator::registerOperator(
      std::make_unique<PluginPlanNodeTranslatorAdapter>(spec.translator));
}

class ExecOperatorInstaller final : public ::bytedance::bolt::plugin::OperatorInstaller {
 public:
  void install(const ::bytedance::bolt::plugin::OperatorSpec& spec) override {
    installOperatorSpec(spec);
  }
};

} // namespace

void initializeOperatorPluginAdapter() {
  static std::once_flag initOnce;
  std::call_once(initOnce, [] {
    static std::shared_ptr<::bytedance::bolt::plugin::OperatorInstaller> installer =
        std::make_shared<ExecOperatorInstaller>();
    ::bytedance::bolt::plugin::setOperatorInstaller(installer);
  });
}

} // namespace bytedance::bolt::exec::plugin

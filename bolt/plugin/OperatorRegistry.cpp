/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/plugin/OperatorRegistry.h"

#include <mutex>
#include <utility>

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::plugin {
namespace {
std::mutex& operatorSpecsMutex() {
  static std::mutex m;
  return m;
}

std::vector<OperatorSpec>& operatorSpecs() {
  static std::vector<OperatorSpec> specs;
  return specs;
}

std::shared_ptr<OperatorInstaller>& operatorInstaller() {
  static std::shared_ptr<OperatorInstaller> installer;
  return installer;
}
} // namespace

void registerOperatorSpec(OperatorSpec spec) {
  BOLT_USER_CHECK_NOT_NULL(
      spec.translator,
      "Operator '{}' registration requires non-null translator",
      spec.name);
  std::shared_ptr<OperatorInstaller> installer;
  {
    std::lock_guard<std::mutex> l(operatorSpecsMutex());
    operatorSpecs().push_back(spec);
    installer = operatorInstaller();
  }
  if (installer != nullptr) {
    installer->install(spec);
  }
}

std::vector<OperatorSpec> listOperatorSpecs() {
  std::lock_guard<std::mutex> l(operatorSpecsMutex());
  return operatorSpecs();
}

void setOperatorInstaller(std::shared_ptr<OperatorInstaller> installer) {
  std::vector<OperatorSpec> specs;
  {
    std::lock_guard<std::mutex> l(operatorSpecsMutex());
    operatorInstaller() = std::move(installer);
    specs = operatorSpecs();
  }
  auto currentInstaller = operatorInstaller();
  if (currentInstaller == nullptr) {
    return;
  }
  for (const auto& spec : specs) {
    currentInstaller->install(spec);
  }
}

} // namespace bytedance::bolt::plugin

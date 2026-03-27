/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/plugin/PluginManager.h"
#include <dlfcn.h>
#include <utility>
#include "bolt/common/base/Exceptions.h"
#include "bolt/connectors/Connector.h"
#include "bolt/exec/Aggregate.h"
#include "bolt/expression/VectorFunction.h"
#include "bolt/plugin/api/PluginRegistrar.h"
#include "bolt/type/Type.h"
#include "bolt/vector/VectorStream.h"

namespace bytedance::bolt::plugin {
namespace {
constexpr const char* kInitSymbol = "boltPluginInitV1";
constexpr const char* kShutdownSymbol = "boltPluginShutdownV1";

using InitFn = bool (*)(PluginRegistrar&);

class RegistryBackedRegistrar final : public PluginRegistrar {
 public:
  void setMetadata(PluginMetadata metadata) override {
    metadata_ = std::move(metadata);
  }

  void addScalarFunction(ScalarFunctionSpec spec) override {
    exec::registerStatefulVectorFunction(
        spec.name,
        std::move(spec.signatures),
        std::move(spec.factory),
        std::move(spec.metadata),
        spec.overwrite);
  }

  void addAggregateFunction(AggregateFunctionSpec spec) override {
    exec::registerAggregateFunction(
        spec.name,
        std::move(spec.signatures),
        spec.factory,
        spec.registerCompanionFunctions,
        spec.overwrite);
  }

  void addOperator(OperatorSpec spec) override {
    BOLT_NYI(
        "Operator registration is declared in the plugin API but not wired to "
        "execution registration yet. Operator: {}",
        spec.name);
  }

  void addOptimizerPass(OptimizerPassSpec spec) override {
    optimizerPasses_.push_back(std::move(spec));
  }

  void addLogicalType(LogicalTypeSpec spec) override {
    registerCustomType(spec.name, std::move(spec.factories));
  }

  void addVectorFormat(VectorFormatSpec spec) override {
    registerNamedVectorSerde(spec.kind, std::move(spec.serde));
  }

  void addConnectorFactory(ConnectorFactorySpec spec) override {
    connector::registerConnectorFactory(std::move(spec.factory));
  }

  const PluginMetadata& metadata() const {
    return metadata_;
  }

 private:
  PluginMetadata metadata_;
  std::vector<OptimizerPassSpec> optimizerPasses_;
};
} // namespace

PluginManager::~PluginManager() {
  // Intentionally no-op for v1 runtime lifecycle.
  //
  // Registries used by plugin registration are process-global today (e.g.
  // function/type/connector registries) and do not support scoped unregister.
  // Unloading dynamic libraries here would leave dangling function pointers in
  // those registries and can crash later during query planning/execution.
  //
  // Once unregister is implemented across all plugin surfaces, destructor can
  // invoke per-plugin shutdown + dlclose in reverse load order.
}

bool PluginManager::addPlugin(const std::shared_ptr<IPlugin>& plugin) {
  BOLT_USER_CHECK_NOT_NULL(plugin, "Plugin cannot be null");
  BOLT_USER_CHECK(!plugin->name().empty(), "Plugin name cannot be empty");
  std::lock_guard<std::mutex> l(mutex_);
  if (loadedPluginNames_.count(plugin->name()) > 0) {
    return false;
  }
  RegistryBackedRegistrar registrar;
  plugin->registerInto(registrar);
  loadedPluginNames_.insert(plugin->name());
  loadedPluginObjects_.push_back(plugin);
  loadedPluginMetadata_[plugin->name()] = registrar.metadata();
  return true;
}

bool PluginManager::loadPlugin(const std::string& path) {
  BOLT_USER_CHECK(!path.empty(), "Plugin path cannot be empty");
  {
    std::lock_guard<std::mutex> l(mutex_);
    for (const auto& loaded : loadedPluginPaths_) {
      if (loaded == path) {
        return false;
      }
    }
  }

  void* handle = ::dlopen(path.c_str(), RTLD_NOW | RTLD_LOCAL);
  if (handle == nullptr) {
    const char* error = ::dlerror();
    BOLT_USER_FAIL(
        "Failed to load plugin '{}': {}",
        path,
        error != nullptr ? error : "unknown");
  }

  ::dlerror();
  auto* init = reinterpret_cast<InitFn>(::dlsym(handle, kInitSymbol));
  const char* initError = ::dlerror();
  if (init == nullptr || initError != nullptr) {
    ::dlclose(handle);
    BOLT_USER_FAIL(
        "Plugin '{}' missing init symbol '{}': {}",
        path,
        kInitSymbol,
        initError != nullptr ? initError : "not found");
  }

  ::dlerror();
  auto* shutdown = reinterpret_cast<ShutdownFn>(::dlsym(handle, kShutdownSymbol));
  const char* shutdownError = ::dlerror();
  if (shutdownError != nullptr) {
    shutdown = nullptr;
  }

  RegistryBackedRegistrar registrar;
  if (!init(registrar)) {
    ::dlclose(handle);
    BOLT_USER_FAIL(
        "Plugin '{}' init symbol '{}' returned false", path, kInitSymbol);
  }

  auto metadata = registrar.metadata();
  std::string pluginName = std::string(metadata.name);

  {
    std::lock_guard<std::mutex> l(mutex_);
    for (const auto& loaded : loadedPluginPaths_) {
      if (loaded == path) {
        if (shutdown != nullptr) {
          shutdown();
        }
        ::dlclose(handle);
        return false;
      }
    }
    if (!pluginName.empty() && loadedPluginNames_.count(pluginName) > 0) {
      if (shutdown != nullptr) {
        shutdown();
      }
      ::dlclose(handle);
      return false;
    }
    loadedPluginPaths_.push_back(path);
    if (!pluginName.empty()) {
      loadedPluginNames_.insert(pluginName);
      loadedPluginMetadata_[pluginName] = std::move(metadata);
    }
    loadedLibraries_.push_back(
        LoadedLibrary{path, std::move(pluginName), handle, shutdown});
  }
  return true;
}

size_t PluginManager::loadPlugins(const std::vector<std::string>& paths) {
  size_t numLoaded = 0;
  for (const auto& path : paths) {
    if (loadPlugin(path)) {
      ++numLoaded;
    }
  }
  return numLoaded;
}

bool PluginManager::isLoaded(const std::string& path) const {
  std::lock_guard<std::mutex> l(mutex_);
  if (loadedPluginNames_.count(path) > 0) {
    return true;
  }
  for (const auto& loaded : loadedPluginPaths_) {
    if (loaded == path) {
      return true;
    }
  }
  return false;
}

std::vector<std::string> PluginManager::loadedPlugins() const {
  std::lock_guard<std::mutex> l(mutex_);
  std::vector<std::string> result;
  result.reserve(loadedPluginNames_.size() + loadedPluginPaths_.size());
  result.insert(result.end(), loadedPluginNames_.begin(), loadedPluginNames_.end());
  result.insert(result.end(), loadedPluginPaths_.begin(), loadedPluginPaths_.end());
  return result;
}

} // namespace bytedance::bolt::plugin

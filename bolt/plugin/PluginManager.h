/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "bolt/plugin/api/IPlugin.h"
#include "bolt/plugin/api/PluginMetadata.h"

namespace bytedance::bolt::plugin {

/// Query/session scoped plugin manager.
///
/// NOTE: This is the initial API surface for downstream integration.
/// Actual dynamic loading and plugin lifecycle wiring will be added incrementally.
class PluginManager {
 public:
  PluginManager() = default;
  ~PluginManager();

  bool addPlugin(const std::shared_ptr<IPlugin>& plugin);

  /// Loads plugin by path. Returns true if newly loaded, false if it was
  /// already loaded in this manager.
  bool loadPlugin(const std::string& path);

  size_t loadPlugins(const std::vector<std::string>& paths);

  bool isLoaded(const std::string& path) const;

  std::vector<std::string> loadedPlugins() const;

 private:
  using ShutdownFn = void (*)();

  struct LoadedLibrary {
    std::string path;
    std::string pluginName;
    void* handle{nullptr};
    ShutdownFn shutdown{nullptr};
  };

  mutable std::mutex mutex_;
  std::vector<std::string> loadedPluginPaths_;
  std::unordered_set<std::string> loadedPluginNames_;
  std::vector<std::shared_ptr<IPlugin>> loadedPluginObjects_;
  std::unordered_map<std::string, PluginMetadata> loadedPluginMetadata_;
  std::vector<LoadedLibrary> loadedLibraries_;
};

} // namespace bytedance::bolt::plugin

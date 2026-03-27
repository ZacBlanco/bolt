/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include "bolt/core/QueryCtx.h"
#include <algorithm>
#include <cctype>
#include <sstream>
#include <vector>
#include "bolt/common/base/SpillConfig.h"
#include "bolt/common/config/Config.h"
#include "bolt/exec/TraceConfig.h"
#include "bolt/plugin/PluginManager.h"
namespace bytedance::bolt::core {
namespace {
std::vector<std::string> parsePluginLoadPaths(const std::string& input) {
  std::vector<std::string> paths;
  if (input.empty()) {
    return paths;
  }
  std::istringstream ss(input);
  std::string token;
  while (std::getline(ss, token, ',')) {
    token.erase(
        token.begin(),
        std::find_if(
            token.begin(),
            token.end(),
            [](unsigned char ch) { return !std::isspace(ch); }));
    token.erase(
        std::find_if(
            token.rbegin(),
            token.rend(),
            [](unsigned char ch) { return !std::isspace(ch); })
            .base(),
        token.end());
    if (!token.empty()) {
      paths.push_back(std::move(token));
    }
  }
  return paths;
}
} // namespace

// static
std::shared_ptr<QueryCtx> QueryCtx::create(
    folly::Executor* executor,
    QueryConfig&& queryConfig,
    std::unordered_map<std::string, std::shared_ptr<config::ConfigBase>>
        connectorConfigs,
    cache::AsyncDataCache* cache,
    std::shared_ptr<memory::MemoryPool> pool,
    folly::Executor* spillExecutor,
    const std::string& queryId) {
  std::shared_ptr<QueryCtx> queryCtx(new QueryCtx(
      executor,
      std::move(queryConfig),
      std::move(connectorConfigs),
      cache,
      std::move(pool),
      spillExecutor,
      queryId));
  queryCtx->maybeSetReclaimer();
  return queryCtx;
}

QueryCtx::QueryCtx(
    folly::Executor* executor,
    QueryConfig&& queryConfig,
    std::unordered_map<std::string, std::shared_ptr<config::ConfigBase>>
        connectorSessionProperties,
    cache::AsyncDataCache* cache,
    std::shared_ptr<memory::MemoryPool> pool,
    folly::Executor* spillExecutor,
    const std::string& queryId)
    : queryId_(queryId),
      executor_(executor),
      spillExecutor_(spillExecutor),
      cache_(cache),
      connectorSessionProperties_(connectorSessionProperties),
      pool_(std::move(pool)),
      queryConfig_{std::move(queryConfig)},
      pluginManager_(std::make_shared<plugin::PluginManager>()) {
  initPool(queryId);
  for (const auto& path : parsePluginLoadPaths(queryConfig_.pluginLoadPaths())) {
    pluginManager_->loadPlugin(path);
  }
}

const std::shared_ptr<plugin::PluginManager>& QueryCtx::pluginManager() const {
  return pluginManager_;
}

void QueryCtx::setPluginManager(
    std::shared_ptr<plugin::PluginManager> pluginManager) {
  BOLT_USER_CHECK_NOT_NULL(pluginManager, "PluginManager cannot be null");
  pluginManager_ = std::move(pluginManager);
}

bool QueryCtx::loadPlugin(const std::string& path) {
  return pluginManager_->loadPlugin(path);
}

bool QueryCtx::addPlugin(const std::shared_ptr<plugin::IPlugin>& plugin) {
  return pluginManager_->addPlugin(plugin);
}

/*static*/ std::string QueryCtx::generatePoolName(const std::string& queryId) {
  // We attach a monotonically increasing sequence number to ensure the pool
  // name is unique.
  static std::atomic<int64_t> seqNum{0};
  return fmt::format("query.{}.{}", queryId.c_str(), seqNum++);
}

void QueryCtx::maybeSetReclaimer() {
  BOLT_CHECK_NOT_NULL(pool_);
  BOLT_CHECK(!underArbitration_);
  if (pool_->reclaimer() != nullptr) {
    return;
  }
  pool_->setReclaimer(QueryCtx::MemoryReclaimer::create(this, pool_.get()));
}

void QueryCtx::updateSpilledBytesAndCheckLimit(uint64_t bytes) {
  const auto numSpilledBytes = numSpilledBytes_.fetch_add(bytes) + bytes;
  if (queryConfig_.maxSpillBytes() > 0 &&
      numSpilledBytes > queryConfig_.maxSpillBytes()) {
    BOLT_SPILL_LIMIT_EXCEEDED(fmt::format(
        "Query exceeded per-query local spill limit of {}",
        succinctBytes(queryConfig_.maxSpillBytes())));
  }
}

void QueryCtx::updateTracedBytesAndCheckLimit(uint64_t bytes) {
  if (numTracedBytes_.fetch_add(bytes) + bytes >=
      queryConfig_.queryTraceMaxBytes()) {
    BOLT_TRACE_LIMIT_EXCEEDED(fmt::format(
        "Query exceeded per-query local trace limit of {}",
        succinctBytes(queryConfig_.queryTraceMaxBytes())));
  }
}

std::unique_ptr<memory::MemoryReclaimer> QueryCtx::MemoryReclaimer::create(
    QueryCtx* queryCtx,
    memory::MemoryPool* pool) {
  return std::unique_ptr<memory::MemoryReclaimer>(new QueryCtx::MemoryReclaimer(
      queryCtx->shared_from_this(),
      pool,
      queryCtx->queryConfig().queryMemoryReclaimerPriority()));
}

uint64_t QueryCtx::MemoryReclaimer::reclaim(
    memory::MemoryPool* pool,
    uint64_t targetBytes,
    uint64_t maxWaitMs,
    memory::MemoryReclaimer::Stats& stats) {
  auto queryCtx = ensureQueryCtx();
  if (queryCtx == nullptr) {
    return 0;
  }
  BOLT_CHECK_EQ(pool->name(), pool_->name());

  const auto leaveGuard =
      folly::makeGuard([&]() { queryCtx->finishArbitration(); });
  queryCtx->startArbitration();
  return memory::MemoryReclaimer::reclaim(pool, targetBytes, maxWaitMs, stats);
}

bool QueryCtx::checkUnderArbitration(ContinueFuture* future) {
  BOLT_CHECK_NOT_NULL(future);
  if (!underArbitration_) {
    return false;
  }

  std::lock_guard<std::mutex> l(mutex_);
  // Check again under the lock to avoid data race.
  if (!underArbitration_) {
    BOLT_CHECK(arbitrationPromises_.empty());
    return false;
  }
  arbitrationPromises_.emplace_back("QueryCtx::waitArbitration");
  *future = arbitrationPromises_.back().getSemiFuture();
  return true;
}

void QueryCtx::startArbitration() {
  std::lock_guard<std::mutex> l(mutex_);
  BOLT_CHECK(!underArbitration_);
  BOLT_CHECK(arbitrationPromises_.empty());
  underArbitration_ = true;
}

void QueryCtx::finishArbitration() {
  std::vector<ContinuePromise> promises;
  {
    std::lock_guard<std::mutex> l(mutex_);
    BOLT_CHECK(underArbitration_);
    underArbitration_ = false;
    promises.swap(arbitrationPromises_);
  }
  for (auto& promise : promises) {
    promise.setValue();
  }
}
} // namespace bytedance::bolt::core

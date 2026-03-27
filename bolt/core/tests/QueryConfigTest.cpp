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

#include <gtest/gtest.h>

#include <algorithm>

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/core/QueryCtx.h"
#include "bolt/exec/Aggregate.h"
#include "bolt/exec/AggregateFunctionRegistry.h"
#include "bolt/expression/Expr.h"
#include "bolt/expression/EvalCtx.h"
#include "bolt/parse/Expressions.h"
#include "bolt/parse/ExpressionsParser.h"
#include "bolt/parse/TypeResolver.h"
#include "bolt/plugin/api/IPlugin.h"
#include "bolt/plugin/api/PluginRegistrar.h"
#include "bolt/plugin/PluginManager.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"
namespace bytedance::bolt::core::test {
namespace {
constexpr const char* kRuntimePlusOneInprocFunction = "runtime_plus_one_inproc";
constexpr const char* kRuntimeCountInprocFunction = "runtime_count_inproc";

class RuntimePlusOneInprocFunction : public exec::VectorFunction {
 public:
  RuntimePlusOneInprocFunction(
      const std::string& /*name*/,
      const std::vector<exec::VectorFunctionArg>& /*inputArgs*/) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /*outputType*/,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    BOLT_CHECK_EQ(args.size(), 1);
    BOLT_CHECK(args[0]->typeKind() == TypeKind::BIGINT);

    BaseVector::ensureWritable(rows, BIGINT(), context.pool(), result);
    auto* output = result->as<FlatVector<int64_t>>();
    auto* rawOutput = output->mutableRawValues();

    exec::LocalDecodedVector inputDecoded(context, *args[0], rows);
    auto* decoded = inputDecoded.get();

    rows.applyToSelected([&](vector_size_t row) {
      if (decoded->isNullAt(row)) {
        output->setNull(row, true);
      } else {
        output->setNull(row, false);
        rawOutput[row] = decoded->valueAt<int64_t>(row) + 1;
      }
    });
  }
};

std::vector<exec::FunctionSignaturePtr> runtimePlusOneInprocSignatures() {
  return {exec::FunctionSignatureBuilder()
              .returnType("bigint")
              .argumentType("bigint")
              .build()};
}

std::vector<exec::AggregateFunctionSignaturePtr> runtimeCountInprocSignatures();
exec::AggregateFunctionFactory runtimeCountInprocFactory();

class RuntimeFunctionPlugin final : public plugin::IPlugin {
 public:
  const std::string& name() const override {
    return name_;
  }

  void registerInto(plugin::PluginRegistrar& registrar) override {
    plugin::PluginMetadata metadata;
    metadata.name = name_;
    metadata.version = "1.0.0";
    metadata.boltAbi = "bolt-plugin-v1";
    metadata.vendor = "bolt-tests";
    registrar.setMetadata(std::move(metadata));

    registrar.addScalarFunction(plugin::ScalarFunctionSpec{
        .name = kRuntimePlusOneInprocFunction,
        .signatures = runtimePlusOneInprocSignatures(),
        .factory = exec::makeVectorFunctionFactory<RuntimePlusOneInprocFunction>(),
        .metadata = {},
        .overwrite = true});

    registrar.addAggregateFunction(plugin::AggregateFunctionSpec{
        .name = kRuntimeCountInprocFunction,
        .signatures = runtimeCountInprocSignatures(),
        .factory = runtimeCountInprocFactory(),
        .registerCompanionFunctions = false,
        .overwrite = true});
  }

 private:
  const std::string name_{"test.runtime.inproc.plugin"};
};

class RuntimeCountInprocAggregate : public exec::Aggregate {
 public:
  explicit RuntimeCountInprocAggregate(const TypePtr& resultType)
      : Aggregate(resultType) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(int64_t);
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    for (auto index : indices) {
      *value(groups[index]) = 0;
    }
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& /*args*/,
      bool /*mayPushdown*/) override {
    rows.applyToSelected([&](vector_size_t row) { ++(*value(groups[row])); });
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& /*args*/,
      bool /*mayPushdown*/) override {
    rows.applyToSelected([&](vector_size_t row) { ++(*value(groups[row])); });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& /*args*/,
      bool /*mayPushdown*/) override {
    *value(group) += rows.countSelected();
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& /*args*/,
      bool /*mayPushdown*/) override {
    *value(group) += rows.countSelected();
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result) override {
    extract(groups, numGroups, result);
  }

  void extractAccumulators(
      char** groups,
      int32_t numGroups,
      VectorPtr* result) override {
    extract(groups, numGroups, result);
  }

 private:
  int64_t* value(char* group) const {
    return reinterpret_cast<int64_t*>(group + offset_);
  }

  void extract(char** groups, int32_t numGroups, VectorPtr* result) const {
    BaseVector::ensureWritable(
        SelectivityVector(numGroups), BIGINT(), pool_, *result);
    auto* flat = (*result)->as<FlatVector<int64_t>>();
    for (int32_t i = 0; i < numGroups; ++i) {
      flat->set(i, *value(groups[i]));
    }
  }
};

std::vector<exec::AggregateFunctionSignaturePtr> runtimeCountInprocSignatures() {
  return {exec::AggregateFunctionSignatureBuilder()
              .returnType("bigint")
              .intermediateType("bigint")
              .argumentType("bigint")
              .build()};
}

exec::AggregateFunctionFactory runtimeCountInprocFactory() {
  return [](core::AggregationNode::Step /*step*/,
            const std::vector<TypePtr>& /*argTypes*/,
            const TypePtr& resultType,
            const core::QueryConfig& /*config*/) {
    return std::make_unique<RuntimeCountInprocAggregate>(resultType);
  };
}
} // namespace

class QueryConfigTest : public testing::Test, public bolt::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    parse::registerTypeResolver();
  }
};

TEST_F(QueryConfigTest, emptyConfig) {
  auto queryCtx = QueryCtx::create(nullptr, QueryConfig{{}});
  const QueryConfig& config = queryCtx->queryConfig();

  ASSERT_FALSE(config.isLegacyCast());
}

TEST_F(QueryConfigTest, setConfig) {
  std::string path = "/tmp/setConfig";
  std::unordered_map<std::string, std::string> configData(
      {{QueryConfig::kLegacyCast, "true"}});
  auto queryCtx = QueryCtx::create(nullptr, QueryConfig{std::move(configData)});
  const QueryConfig& config = queryCtx->queryConfig();

  ASSERT_TRUE(config.isLegacyCast());
}

TEST_F(QueryConfigTest, taskWriterCountConfig) {
  struct {
    std::optional<int> numWriterCounter;
    std::optional<int> numPartitionedWriterCounter;
    int expectedWriterCounter;
    int expectedPartitionedWriterCounter;

    std::string debugString() const {
      return fmt::format(
          "numWriterCounter[{}] numPartitionedWriterCounter[{}] expectedWriterCounter[{}] expectedPartitionedWriterCounter[{}]",
          numWriterCounter.value_or(0),
          numPartitionedWriterCounter.value_or(0),
          expectedWriterCounter,
          expectedPartitionedWriterCounter);
    }
  } testSettings[] = {
      {std::nullopt, std::nullopt, 4, 4},
      {std::nullopt, 1, 4, 1},
      {std::nullopt, 6, 4, 6},
      {2, 4, 2, 4},
      {4, 2, 4, 2},
      {4, 6, 4, 6},
      {6, 5, 6, 5},
      {6, 4, 6, 4},
      {6, std::nullopt, 6, 6}};
  for (const auto& testConfig : testSettings) {
    SCOPED_TRACE(testConfig.debugString());
    std::unordered_map<std::string, std::string> configData;
    if (testConfig.numWriterCounter.has_value()) {
      configData.emplace(
          QueryConfig::kTaskWriterCount,
          std::to_string(testConfig.numWriterCounter.value()));
    }
    if (testConfig.numPartitionedWriterCounter.has_value()) {
      configData.emplace(
          QueryConfig::kTaskPartitionedWriterCount,
          std::to_string(testConfig.numPartitionedWriterCounter.value()));
    }
    auto queryCtx =
        QueryCtx::create(nullptr, QueryConfig{std::move(configData)});
    const QueryConfig& config = queryCtx->queryConfig();
    ASSERT_EQ(config.taskWriterCount(), testConfig.expectedWriterCounter);
    ASSERT_EQ(
        config.taskPartitionedWriterCount(),
        testConfig.expectedPartitionedWriterCounter);
  }
}

TEST_F(QueryConfigTest, enableExpressionEvaluationCacheConfig) {
  std::shared_ptr<memory::MemoryPool> rootPool{
      memory::memoryManager()->addRootPool()};
  std::shared_ptr<memory::MemoryPool> pool{rootPool->addLeafChild("leaf")};

  auto testConfig = [&](bool enableExpressionEvaluationCache) {
    std::unordered_map<std::string, std::string> configData(
        {{core::QueryConfig::kEnableExpressionEvaluationCache,
          enableExpressionEvaluationCache ? "true" : "false"}});
    auto queryCtx =
        core::QueryCtx::create(nullptr, QueryConfig{std::move(configData)});
    const core::QueryConfig& config = queryCtx->queryConfig();
    ASSERT_EQ(
        config.isExpressionEvaluationCacheEnabled(),
        enableExpressionEvaluationCache);

    auto execCtx = std::make_shared<core::ExecCtx>(pool.get(), queryCtx.get());
    ASSERT_EQ(execCtx->exprEvalCacheEnabled(), enableExpressionEvaluationCache);
    ASSERT_EQ(
        execCtx->vectorPool() != nullptr, enableExpressionEvaluationCache);

    auto evalCtx = std::make_shared<exec::EvalCtx>(execCtx.get());
    ASSERT_EQ(evalCtx->cacheEnabled(), enableExpressionEvaluationCache);

    // Test ExecCtx::selectivityVectorPool_.
    auto rows = execCtx->getSelectivityVector(100);
    ASSERT_NE(rows, nullptr);
    ASSERT_EQ(
        execCtx->releaseSelectivityVector(std::move(rows)),
        enableExpressionEvaluationCache);

    // Test ExecCtx::decodedVectorPool_.
    auto decoded = execCtx->getDecodedVector();
    ASSERT_NE(decoded, nullptr);
    ASSERT_EQ(
        execCtx->releaseDecodedVector(std::move(decoded)),
        enableExpressionEvaluationCache);
  };

  testConfig(true);
  testConfig(false);
}

TEST_F(QueryConfigTest, pluginLoadPathsAutoLoad) {
  std::unordered_map<std::string, std::string> configData{
      {QueryConfig::kPluginLoadPaths, BOLT_TEST_RUNTIME_PLUGIN_PATH}};

  auto queryCtx = QueryCtx::create(nullptr, QueryConfig{std::move(configData)});
  auto pluginManager = queryCtx->pluginManager();

  ASSERT_TRUE(pluginManager->isLoaded(BOLT_TEST_RUNTIME_PLUGIN_PATH));
}

TEST_F(QueryConfigTest, pluginLoadPathsDeduplicates) {
  const std::string duplicatedPaths = fmt::format(
      "  {} , {},  ",
      BOLT_TEST_RUNTIME_PLUGIN_PATH,
      BOLT_TEST_RUNTIME_PLUGIN_PATH);
  std::unordered_map<std::string, std::string> configData{
      {QueryConfig::kPluginLoadPaths, duplicatedPaths}};

  auto queryCtx = QueryCtx::create(nullptr, QueryConfig{std::move(configData)});
  auto pluginManager = queryCtx->pluginManager();
  const auto loaded = pluginManager->loadedPlugins();

  ASSERT_TRUE(pluginManager->isLoaded(BOLT_TEST_RUNTIME_PLUGIN_PATH));
  ASSERT_EQ(
      std::count(loaded.begin(), loaded.end(), BOLT_TEST_RUNTIME_PLUGIN_PATH),
      1);
}

TEST_F(QueryConfigTest, queryCtxLoadPluginIsIdempotent) {
  auto queryCtx = QueryCtx::create(nullptr, QueryConfig{{}});
  auto pluginManager = queryCtx->pluginManager();

  ASSERT_TRUE(queryCtx->loadPlugin(BOLT_TEST_RUNTIME_PLUGIN_PATH));
  ASSERT_FALSE(queryCtx->loadPlugin(BOLT_TEST_RUNTIME_PLUGIN_PATH));
  ASSERT_TRUE(pluginManager->isLoaded(BOLT_TEST_RUNTIME_PLUGIN_PATH));

  const auto loaded = pluginManager->loadedPlugins();
  ASSERT_EQ(
      std::count(loaded.begin(), loaded.end(), BOLT_TEST_RUNTIME_PLUGIN_PATH),
      1);
}

TEST_F(QueryConfigTest, addPluginRegistersScalarFunctionForExecution) {
  auto queryCtx = QueryCtx::create(nullptr, QueryConfig{{}});
  ASSERT_TRUE(queryCtx->addPlugin(std::make_shared<RuntimeFunctionPlugin>()));
  ASSERT_FALSE(queryCtx->addPlugin(std::make_shared<RuntimeFunctionPlugin>()));
  core::ExecCtx execCtx(pool(), queryCtx.get());

  auto input = makeRowVector({makeFlatVector<int64_t>({1, 2, 3})});

  parse::ParseOptions options;
  auto untyped = parse::parseExpr("runtime_plus_one_inproc(c0)", options);
  auto typed =
      core::Expressions::inferTypes(untyped, asRowType(input->type()), pool());

  exec::ExprSet exprSet({typed}, &execCtx);
  exec::EvalCtx evalCtx(&execCtx, &exprSet, input.get());
  std::vector<VectorPtr> results(1);
  SelectivityVector rows(input->size());
  exprSet.eval(rows, evalCtx, results);

  auto* output = results[0]->as<FlatVector<int64_t>>();
  ASSERT_EQ(output->valueAt(0), 2);
  ASSERT_EQ(output->valueAt(1), 3);
  ASSERT_EQ(output->valueAt(2), 4);
}

TEST_F(QueryConfigTest, addPluginRegistersAggregateFunctionForPlanning) {
  auto queryCtx = QueryCtx::create(nullptr, QueryConfig{{}});
  ASSERT_TRUE(queryCtx->addPlugin(std::make_shared<RuntimeFunctionPlugin>()));

  auto resolved = exec::resolveAggregateFunction(
      kRuntimeCountInprocFunction, {BIGINT()});
  ASSERT_NE(resolved.first, nullptr);
  ASSERT_NE(resolved.second, nullptr);
  ASSERT_TRUE(resolved.first->equivalent(*BIGINT()));
  ASSERT_TRUE(resolved.second->equivalent(*BIGINT()));

  auto aggregate = exec::Aggregate::create(
      kRuntimeCountInprocFunction,
      core::AggregationNode::Step::kSingle,
      {BIGINT()},
      BIGINT(),
      queryCtx->queryConfig());
  ASSERT_NE(aggregate, nullptr);
}

TEST_F(QueryConfigTest, pluginLoadPathsRegistersScalarFunctionForExecution) {
  std::unordered_map<std::string, std::string> configData{
      {QueryConfig::kPluginLoadPaths, BOLT_TEST_RUNTIME_PLUGIN_PATH}};
  auto queryCtx = QueryCtx::create(nullptr, QueryConfig{std::move(configData)});
  core::ExecCtx execCtx(pool(), queryCtx.get());

  auto input = makeRowVector({makeFlatVector<int64_t>({1, 2, 3})});

  parse::ParseOptions options;
  auto untyped = parse::parseExpr("runtime_plus_one_dynamic(c0)", options);
  auto typed =
      core::Expressions::inferTypes(untyped, asRowType(input->type()), pool());

  exec::ExprSet exprSet({typed}, &execCtx);
  exec::EvalCtx evalCtx(&execCtx, &exprSet, input.get());
  std::vector<VectorPtr> results(1);
  SelectivityVector rows(input->size());
  exprSet.eval(rows, evalCtx, results);

  auto* output = results[0]->as<FlatVector<int64_t>>();
  ASSERT_EQ(output->valueAt(0), 2);
  ASSERT_EQ(output->valueAt(1), 3);
  ASSERT_EQ(output->valueAt(2), 4);
}

TEST_F(QueryConfigTest, pluginLoadPathsRegistersAggregateFunctionForPlanning) {
  std::unordered_map<std::string, std::string> configData{
      {QueryConfig::kPluginLoadPaths, BOLT_TEST_RUNTIME_PLUGIN_PATH}};
  auto queryCtx = QueryCtx::create(nullptr, QueryConfig{std::move(configData)});

  auto resolved = exec::resolveAggregateFunction("runtime_count_dynamic", {BIGINT()});
  ASSERT_NE(resolved.first, nullptr);
  ASSERT_NE(resolved.second, nullptr);
  ASSERT_TRUE(resolved.first->equivalent(*BIGINT()));
  ASSERT_TRUE(resolved.second->equivalent(*BIGINT()));

  auto aggregate = exec::Aggregate::create(
      "runtime_count_dynamic",
      core::AggregationNode::Step::kSingle,
      {BIGINT()},
      BIGINT(),
      queryCtx->queryConfig());
  ASSERT_NE(aggregate, nullptr);
}

} // namespace bytedance::bolt::core::test

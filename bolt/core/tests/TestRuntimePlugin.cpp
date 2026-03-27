/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/plugin/api/PluginRegistrar.h"

namespace bytedance::bolt::core::test {
namespace {
constexpr const char* kRuntimePlusOneDynamicFunction = "runtime_plus_one_dynamic";
constexpr const char* kRuntimeCountDynamicFunction = "runtime_count_dynamic";

class RuntimePlusOneDynamicFunction : public exec::VectorFunction {
 public:
  RuntimePlusOneDynamicFunction(
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

std::vector<exec::FunctionSignaturePtr> runtimePlusOneDynamicSignatures() {
  return {exec::FunctionSignatureBuilder()
              .returnType("bigint")
              .argumentType("bigint")
              .build()};
}

class RuntimeCountDynamicAggregate : public exec::Aggregate {
 public:
  explicit RuntimeCountDynamicAggregate(const TypePtr& resultType)
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

std::vector<exec::AggregateFunctionSignaturePtr> runtimeCountDynamicSignatures() {
  return {exec::AggregateFunctionSignatureBuilder()
              .returnType("bigint")
              .intermediateType("bigint")
              .argumentType("bigint")
              .build()};
}

exec::AggregateFunctionFactory runtimeCountDynamicFactory() {
  return [](core::AggregationNode::Step /*step*/,
            const std::vector<TypePtr>& /*argTypes*/,
            const TypePtr& resultType,
            const core::QueryConfig& /*config*/) {
    return std::make_unique<RuntimeCountDynamicAggregate>(resultType);
  };
}
} // namespace
} // namespace bytedance::bolt::core::test

extern "C" bool boltPluginInitV1(bytedance::bolt::plugin::PluginRegistrar& registrar) {
  bytedance::bolt::plugin::PluginMetadata metadata;
  metadata.name = "test.runtime.plugin";
  metadata.version = "1.0.0";
  metadata.boltAbi = "bolt-plugin-v1";
  metadata.vendor = "bolt-tests";
  registrar.setMetadata(std::move(metadata));

  registrar.addScalarFunction(bytedance::bolt::plugin::ScalarFunctionSpec{
      .name = bytedance::bolt::core::test::kRuntimePlusOneDynamicFunction,
      .signatures =
          bytedance::bolt::core::test::runtimePlusOneDynamicSignatures(),
      .factory = bytedance::bolt::exec::makeVectorFunctionFactory<
          bytedance::bolt::core::test::RuntimePlusOneDynamicFunction>(),
      .metadata = {},
      .overwrite = true});

  registrar.addAggregateFunction(bytedance::bolt::plugin::AggregateFunctionSpec{
      .name = bytedance::bolt::core::test::kRuntimeCountDynamicFunction,
      .signatures = bytedance::bolt::core::test::runtimeCountDynamicSignatures(),
      .factory = bytedance::bolt::core::test::runtimeCountDynamicFactory(),
      .registerCompanionFunctions = false,
      .overwrite = true});
  return true;
}

extern "C" void boltPluginShutdownV1() {}

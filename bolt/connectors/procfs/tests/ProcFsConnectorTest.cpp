/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <filesystem>
#include <folly/init/Init.h>

#include "bolt/common/base/BoltException.h"
#include "bolt/connectors/Connector.h"
#include "bolt/connectors/procfs/ProcFsConnector.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/OperatorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/plugin/builtin/ProcFsConnectorPlugin.h"
#include "gtest/gtest.h"

namespace bytedance::bolt::connector::procfs::test {
using bytedance::bolt::exec::test::PlanBuilder;

class ProcFsConnectorTest : public exec::test::OperatorTestBase {
 protected:
  static constexpr const char* kConnectorId = "test-procfs";

  void SetUp() override {
    OperatorTestBase::SetUp();
    queryCtx_ = core::QueryCtx::create(driverExecutor_.get());
    ASSERT_TRUE(queryCtx_->addPlugin(plugin::builtin::createProcFsConnectorPlugin()));

    std::shared_ptr<const config::ConfigBase> config;
    auto connector = connector::getConnectorFactory(connector::kProcFsConnectorName)
                         ->newConnector(kConnectorId, config);
    connector::registerConnector(connector);
  }

  void TearDown() override {
    connector::unregisterConnector(kConnectorId);
    queryCtx_.reset();
    OperatorTestBase::TearDown();
  }

  static void maybeSkipIfProcMissing() {
    if (!std::filesystem::exists("/proc/meminfo") ||
        !std::filesystem::exists("/proc/cpuinfo") ||
        !std::filesystem::exists("/proc/modules")) {
      GTEST_SKIP() << "procfs files are unavailable on this platform.";
    }
  }

  std::shared_ptr<ProcFsTableHandle> makeTableHandle(
      const std::string& tableName) const {
    return std::make_shared<ProcFsTableHandle>(kConnectorId, tableName);
  }

  exec::Split makeSplit() const {
    return exec::Split(std::make_shared<ProcFsConnectorSplit>(kConnectorId));
  }

  std::shared_ptr<core::QueryCtx> queryCtx_;
};

TEST_F(ProcFsConnectorTest, meminfoTable) {
  maybeSkipIfProcMissing();
  const auto type = ROW({"key", "value", "unit"}, {VARCHAR(), BIGINT(), VARCHAR()});
  auto plan = PlanBuilder()
                  .startTableScan()
                  .outputType(type)
                  .tableHandle(makeTableHandle("meminfo"))
                  .endTableScan()
                  .planNode();

  auto output = exec::test::AssertQueryBuilder(plan)
                    .queryCtx(queryCtx_)
                    .split(makeSplit())
                    .copyResults(pool());
  ASSERT_GT(output->size(), 0);
}

TEST_F(ProcFsConnectorTest, cpuinfoTable) {
  maybeSkipIfProcMissing();
  const auto type = ROW(
      {"processor", "field", "value"},
      {BIGINT(), VARCHAR(), VARCHAR()});
  auto plan = PlanBuilder()
                  .startTableScan()
                  .outputType(type)
                  .tableHandle(makeTableHandle("cpuinfo"))
                  .endTableScan()
                  .planNode();

  auto output = exec::test::AssertQueryBuilder(plan)
                    .queryCtx(queryCtx_)
                    .split(makeSplit())
                    .copyResults(pool());
  ASSERT_GT(output->size(), 0);
}

TEST_F(ProcFsConnectorTest, modulesTable) {
  maybeSkipIfProcMissing();
  const auto type = ROW(
      {"module", "size", "instances", "dependencies", "state", "address"},
      {VARCHAR(), BIGINT(), BIGINT(), VARCHAR(), VARCHAR(), VARCHAR()});
  auto plan = PlanBuilder()
                  .startTableScan()
                  .outputType(type)
                  .tableHandle(makeTableHandle("modules"))
                  .endTableScan()
                  .planNode();

  auto output = exec::test::AssertQueryBuilder(plan)
                    .queryCtx(queryCtx_)
                    .split(makeSplit())
                    .copyResults(pool());
  ASSERT_GE(output->size(), 0);
}

TEST_F(ProcFsConnectorTest, multipleSplitsRepeatFullRead) {
  maybeSkipIfProcMissing();
  const auto type = ROW({"key", "value", "unit"}, {VARCHAR(), BIGINT(), VARCHAR()});
  auto plan = PlanBuilder()
                  .startTableScan()
                  .outputType(type)
                  .tableHandle(makeTableHandle("meminfo"))
                  .endTableScan()
                  .planNode();

  auto oneSplitRows = exec::test::AssertQueryBuilder(plan)
                          .queryCtx(queryCtx_)
                          .split(makeSplit())
                          .copyResults(pool())
                          ->size();
  auto twoSplitRows = exec::test::AssertQueryBuilder(plan)
                          .queryCtx(queryCtx_)
                          .split(makeSplit())
                          .split(makeSplit())
                          .copyResults(pool())
                          ->size();

  ASSERT_GT(oneSplitRows, 0);
  ASSERT_GT(twoSplitRows, oneSplitRows);
}

TEST_F(ProcFsConnectorTest, unknownTableThrows) {
  maybeSkipIfProcMissing();
  const auto type = ROW({"key", "value", "unit"}, {VARCHAR(), BIGINT(), VARCHAR()});
  auto plan = PlanBuilder()
                  .startTableScan()
                  .outputType(type)
                  .tableHandle(makeTableHandle("unknown_proc_table"))
                  .endTableScan()
                  .planNode();

  EXPECT_THROW(
      {
        exec::test::AssertQueryBuilder(plan)
            .queryCtx(queryCtx_)
            .split(makeSplit())
            .copyResults(pool());
      },
      bytedance::bolt::BoltException);
}

} // namespace bytedance::bolt::connector::procfs::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}

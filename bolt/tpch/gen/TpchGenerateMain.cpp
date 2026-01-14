/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
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
 */

#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <filesystem>
#include <iostream>

#include "bolt/common/file/FileSystems.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/connectors/hive/HiveConnector.h"
#include "bolt/exec/Task.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/tpch/gen/TpchGen.h"

using namespace bytedance::bolt;
using namespace bytedance::bolt::tpch;

DEFINE_double(
    tpch_generator_scale_factor,
    1.0,
    "TPC-H scale factor (e.g., 1, 10, 100)");
DEFINE_string(
    tpch_generator_output_dir,
    "",
    "Output directory to write tables");

int main(int argc, char** argv) {
  folly::init(&argc, &argv, true);
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});

  // Register local filesystem and Hive connector.
  filesystems::registerLocalFileSystem();
  const std::string kHiveConnectorId = "test-hive";
  auto hiveConnector =
      connector::getConnectorFactory(
          connector::hive::HiveConnectorFactory::kHiveConnectorName)
          ->newConnector(
              kHiveConnectorId,
              std::make_shared<config::ConfigBase>(
                  std::unordered_map<std::string, std::string>()));
  connector::registerConnector(hiveConnector);

  // Validate output directory.
  if (FLAGS_tpch_generator_output_dir.empty()) {
    std::cerr << "-tpch_generator_output_dir must be specified" << std::endl;
    return 1;
  }
  std::filesystem::create_directories(FLAGS_tpch_generator_output_dir);

  auto pool = memory::memoryManager()->addLeafPool("tpch-generate");
  const double sf = FLAGS_tpch_generator_scale_factor;

  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency()));

  for (auto table : tables) {
    const auto name = toTableName(table);
    const auto tableDir =
        (std::filesystem::path(FLAGS_tpch_generator_output_dir) / name)
            .string();
    std::filesystem::create_directories(tableDir);
    try {
      RowVectorPtr vec;
      switch (table) {
        case Table::TBL_PART: {
          const auto rows = getRowCount(Table::TBL_PART, sf);
          vec = genTpchPart(pool.get(), rows, /*offset*/ 0, sf);
          break;
        }
        case Table::TBL_SUPPLIER: {
          const auto rows = getRowCount(Table::TBL_SUPPLIER, sf);
          vec = genTpchSupplier(pool.get(), rows, /*offset*/ 0, sf);
          break;
        }
        case Table::TBL_PARTSUPP: {
          const auto rows = getRowCount(Table::TBL_PARTSUPP, sf);
          vec = genTpchPartSupp(pool.get(), rows, /*offset*/ 0, sf);
          break;
        }
        case Table::TBL_CUSTOMER: {
          const auto rows = getRowCount(Table::TBL_CUSTOMER, sf);
          vec = genTpchCustomer(pool.get(), rows, /*offset*/ 0, sf);
          break;
        }
        case Table::TBL_ORDERS: {
          const auto rows = getRowCount(Table::TBL_ORDERS, sf);
          vec = genTpchOrders(pool.get(), rows, /*offset*/ 0, sf);
          break;
        }
        case Table::TBL_LINEITEM: {
          // Lineitem generation is based on orders rows.
          const auto ordersRows = getRowCount(Table::TBL_ORDERS, sf);
          vec = genTpchLineItem(pool.get(), ordersRows, /*ordersOffset*/ 0, sf);
          break;
        }
        case Table::TBL_NATION: {
          const auto rows = getRowCount(Table::TBL_NATION, sf);
          vec = genTpchNation(pool.get(), rows, /*offset*/ 0, sf);
          break;
        }
        case Table::TBL_REGION: {
          const auto rows = getRowCount(Table::TBL_REGION, sf);
          vec = genTpchRegion(pool.get(), rows, /*offset*/ 0, sf);
          break;
        }
        default:
          BOLT_UNSUPPORTED("Unsupported TPC-H Table name");
      }
      std::cout << "Generated " << name << ": " << vec->size() << " rows\n";

      // Write to Parquet via TableWrite.
      auto writerPlanFragment =
          exec::test::PlanBuilder()
              .values({vec})
              .tableWrite(tableDir, dwio::common::FileFormat::PARQUET)
              .planFragment();
      auto writeTask = exec::Task::create(
          fmt::format("tpch_write_{}", name),
          writerPlanFragment,
          /*destination=*/0,
          core::QueryCtx::create(executor.get()),
          exec::Task::ExecutionMode::kSerial);
      while (auto result = writeTask->next()) {
        (void)result;
      }
    } catch (const std::exception& e) {
      std::cerr << "Failed to generate/write table '" << name
                << "': " << e.what() << "\n";
      return 1;
    }
  }

  std::cout << "Done." << std::endl;
  return 0;
}

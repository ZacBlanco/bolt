/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <folly/init/Init.h>
#include <gflags/gflags.h>

#include <algorithm>
#include <cctype>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "bolt/common/memory/Memory.h"
#include "bolt/connectors/Connector.h"
#include "bolt/connectors/procfs/ProcFsConnector.h"
#include "bolt/core/Expressions.h"
#include "bolt/core/PlanFragment.h"
#include "bolt/core/PlanNode.h"
#include "bolt/core/QueryCtx.h"
#include "bolt/exec/Task.h"
#include "bolt/plugin/builtin/ProcFsConnectorPlugin.h"

DEFINE_string(table, "meminfo", "ProcFs table name: meminfo|cpuinfo|modules");
DEFINE_int32(
    splits,
    1,
    "Number of splits to run. Each split performs a full read of the procfs table.");
DEFINE_string(
    orderby,
    "",
    "Optional ORDER BY keys, comma-separated. "
    "Examples: key | key DESC | processor ASC NULLS LAST, field");

namespace bytedance::bolt::connector::procfs {
namespace {
std::string toLower(std::string input) {
  std::transform(input.begin(), input.end(), input.begin(), [](unsigned char c) {
    return static_cast<char>(std::tolower(c));
  });
  return input;
}

RowTypePtr outputTypeForTable(const std::string& table) {
  const auto name = toLower(table);
  if (name == "meminfo") {
    return ROW({"key", "value", "unit"}, {VARCHAR(), BIGINT(), VARCHAR()});
  }
  if (name == "cpuinfo") {
    return ROW({"processor", "field", "value"}, {BIGINT(), VARCHAR(), VARCHAR()});
  }
  if (name == "modules") {
    return ROW(
        {"module", "size", "instances", "dependencies", "state", "address"},
        {VARCHAR(), BIGINT(), BIGINT(), VARCHAR(), VARCHAR(), VARCHAR()});
  }
  BOLT_USER_FAIL(
      "Unknown table '{}'. Supported: meminfo, cpuinfo, modules.", table);
  BOLT_UNREACHABLE("Unknown table '{}'", table);
}

std::vector<std::string> splitOrderByKeys(const std::string& orderBy) {
  std::vector<std::string> keys;
  if (orderBy.empty()) {
    return keys;
  }
  size_t begin = 0;
  while (begin <= orderBy.size()) {
    const auto comma = orderBy.find(',', begin);
    const size_t end = comma == std::string::npos ? orderBy.size() : comma;
    auto key = orderBy.substr(begin, end - begin);
    auto isSpace = [](unsigned char c) { return std::isspace(c); };
    key.erase(key.begin(), std::find_if(key.begin(), key.end(), [&](char c) {
                return !isSpace(static_cast<unsigned char>(c));
              }));
    key.erase(
        std::find_if(key.rbegin(), key.rend(), [&](char c) {
          return !isSpace(static_cast<unsigned char>(c));
        }).base(),
        key.end());
    if (!key.empty()) {
      keys.push_back(std::move(key));
    }
    if (comma == std::string::npos) {
      break;
    }
    begin = comma + 1;
  }
  return keys;
}

std::vector<std::string> splitWhitespace(const std::string& text) {
  std::vector<std::string> tokens;
  std::istringstream in(text);
  for (std::string token; in >> token;) {
    tokens.push_back(std::move(token));
  }
  return tokens;
}

std::pair<
    std::vector<bytedance::bolt::core::FieldAccessTypedExprPtr>,
    std::vector<bytedance::bolt::core::SortOrder>>
parseOrderBy(
    const std::vector<std::string>& clauses,
    const RowTypePtr& inputType) {
  std::vector<bytedance::bolt::core::FieldAccessTypedExprPtr> sortingKeys;
  std::vector<bytedance::bolt::core::SortOrder> sortingOrders;

  sortingKeys.reserve(clauses.size());
  sortingOrders.reserve(clauses.size());

  for (const auto& clause : clauses) {
    auto tokens = splitWhitespace(clause);
    BOLT_USER_CHECK(!tokens.empty(), "Invalid order-by clause: '{}'", clause);

    const auto& column = tokens[0];
    BOLT_USER_CHECK(
        inputType->containsChild(column),
        "Unknown order-by column '{}' for type {}",
        column,
        inputType->toString());

    bool ascending = true;
    bool nullsFirst = false; // Match PlanBuilder default: ASC NULLS LAST.

    size_t i = 1;
    if (i < tokens.size()) {
      const auto direction = toLower(tokens[i]);
      if (direction == "asc") {
        ascending = true;
        ++i;
      } else if (direction == "desc") {
        ascending = false;
        ++i;
      }
    }

    if (i < tokens.size()) {
      BOLT_USER_CHECK_EQ(
          toLower(tokens[i]),
          "nulls",
          "Invalid order-by clause '{}'. Expected NULLS FIRST|LAST.",
          clause);
      BOLT_USER_CHECK_LT(
          i + 1,
          tokens.size(),
          "Invalid order-by clause '{}'. Expected NULLS FIRST|LAST.",
          clause);
      const auto nullsToken = toLower(tokens[i + 1]);
      if (nullsToken == "first") {
        nullsFirst = true;
      } else if (nullsToken == "last") {
        nullsFirst = false;
      } else {
        BOLT_USER_FAIL(
            "Invalid order-by clause '{}'. Expected NULLS FIRST|LAST.",
            clause);
      }
      i += 2;
    }

    BOLT_USER_CHECK_EQ(
        i,
        tokens.size(),
        "Invalid order-by clause '{}'. Supported format: "
        "<column> [ASC|DESC] [NULLS FIRST|LAST].",
        clause);

    const auto childIdx = inputType->getChildIdx(column);
    sortingKeys.push_back(
        std::make_shared<const bytedance::bolt::core::FieldAccessTypedExpr>(
            inputType->childAt(childIdx), column));
    sortingOrders.emplace_back(ascending, nullsFirst);
  }
  return {sortingKeys, sortingOrders};
}

void printTable(
    const RowTypePtr& outputType,
    const std::vector<std::vector<std::string>>& rows) {
  const auto& headers = outputType->names();
  std::vector<size_t> widths(headers.size(), 0);
  for (size_t i = 0; i < headers.size(); ++i) {
    widths[i] = headers[i].size();
  }
  for (const auto& row : rows) {
    for (size_t i = 0; i < row.size(); ++i) {
      widths[i] = std::max(widths[i], row[i].size());
    }
  }

  auto printSeparator = [&]() {
    std::cout << "+";
    for (const auto width : widths) {
      std::cout << std::string(width + 2, '-') << "+";
    }
    std::cout << "\n";
  };

  printSeparator();
  std::cout << "|";
  for (size_t i = 0; i < headers.size(); ++i) {
    std::cout << " " << std::left << std::setw(widths[i]) << headers[i] << " |";
  }
  std::cout << "\n";
  printSeparator();

  for (const auto& row : rows) {
    std::cout << "|";
    for (size_t i = 0; i < row.size(); ++i) {
      std::cout << " " << std::left << std::setw(widths[i]) << row[i] << " |";
    }
    std::cout << "\n";
  }
  printSeparator();
}

class RegisteredConnector {
 public:
  RegisteredConnector(std::string connectorId, std::shared_ptr<connector::Connector> connector)
      : connectorId_(std::move(connectorId)), connector_(std::move(connector)) {
    connector::registerConnector(connector_);
  }

  ~RegisteredConnector() {
    if (!connectorId_.empty()) {
      connector::unregisterConnector(connectorId_);
    }
  }

 private:
  std::string connectorId_;
  std::shared_ptr<connector::Connector> connector_;
};

class TaskCursor {
 public:
  explicit TaskCursor(std::shared_ptr<bytedance::bolt::exec::Task> task)
      : task_(std::move(task)) {}

  void start() {
    started_ = true;
  }

  bool moveNext() {
    if (!started_) {
      start();
    }
    current_ = task_->next();
    return current_ != nullptr;
  }

  RowVectorPtr& current() {
    return current_;
  }

  const std::shared_ptr<bytedance::bolt::exec::Task>& task() const {
    return task_;
  }

 private:
  bool started_{false};
  RowVectorPtr current_;
  std::shared_ptr<bytedance::bolt::exec::Task> task_;
};
} // namespace
} // namespace bytedance::bolt::connector::procfs

int main(int argc, char** argv) {
  folly::init(&argc, &argv, false);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  BOLT_USER_CHECK_GT(FLAGS_splits, 0, "--splits must be > 0");

  if (!bytedance::bolt::memory::MemoryManager::testInstance()) {
    bytedance::bolt::memory::initializeMemoryManager({});
  }

  const auto outputType = bytedance::bolt::connector::procfs::outputTypeForTable(
      FLAGS_table);
  const auto orderByKeys =
      bytedance::bolt::connector::procfs::splitOrderByKeys(FLAGS_orderby);
  const std::string connectorId = "procfs-printer";

  auto queryCtx = bytedance::bolt::core::QueryCtx::create();
  queryCtx->addPlugin(
      bytedance::bolt::plugin::builtin::createProcFsConnectorPlugin());
  auto connector =
      bytedance::bolt::connector::getConnectorFactory(
          bytedance::bolt::connector::kProcFsConnectorName)
          ->newConnector(
              connectorId,
              std::shared_ptr<const bytedance::bolt::config::ConfigBase>{});
  bytedance::bolt::connector::procfs::RegisteredConnector registeredConnector(
      connectorId, connector);

  const bytedance::bolt::core::PlanNodeId scanNodeId = "0";
  bytedance::bolt::core::PlanNodePtr planNode =
      std::make_shared<const bytedance::bolt::core::TableScanNode>(
          scanNodeId,
          outputType,
          std::make_shared<bytedance::bolt::connector::procfs::ProcFsTableHandle>(
              connectorId, FLAGS_table),
          std::unordered_map<
              std::string,
              std::shared_ptr<bytedance::bolt::connector::ColumnHandle>>{});
  if (!orderByKeys.empty()) {
    const auto [sortingKeys, sortingOrders] =
        bytedance::bolt::connector::procfs::parseOrderBy(
            orderByKeys, outputType);
    planNode = std::make_shared<const bytedance::bolt::core::OrderByNode>(
        "1", sortingKeys, sortingOrders, false, planNode);
  }

  auto task = bytedance::bolt::exec::Task::create(
      "procfs_printer_task",
      bytedance::bolt::core::PlanFragment(planNode),
      0,
      queryCtx,
      bytedance::bolt::exec::Task::ExecutionMode::kSerial);
  bytedance::bolt::connector::procfs::TaskCursor cursor(task);
  cursor.start();
  for (int32_t i = 0; i < FLAGS_splits; ++i) {
    task->addSplit(
        scanNodeId,
        bytedance::bolt::exec::Split(std::make_shared<
            bytedance::bolt::connector::procfs::ProcFsConnectorSplit>(
            connectorId)));
  }
  task->noMoreSplits(scanNodeId);

  std::vector<std::vector<std::string>> allRows;
  while (cursor.moveNext()) {
    const auto& batch = cursor.current();
    for (bytedance::bolt::vector_size_t row = 0; row < batch->size(); ++row) {
      std::vector<std::string> values;
      values.reserve(batch->childrenSize());
      for (bytedance::bolt::vector_size_t col = 0; col < batch->childrenSize();
           ++col) {
        values.push_back(batch->childAt(col)->toString(row));
      }
      allRows.push_back(std::move(values));
    }
  }
  task->taskCompletionFuture().wait();

  std::cout << "table=" << FLAGS_table << " splits=" << FLAGS_splits
            << " orderby="
            << (FLAGS_orderby.empty() ? "<none>" : FLAGS_orderby)
            << " rows=" << allRows.size() << "\n";
  bytedance::bolt::connector::procfs::printTable(outputType, allRows);
  return 0;
}

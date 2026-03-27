/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <optional>

#include "bolt/common/config/Config.h"
#include "bolt/connectors/Connector.h"
#include "bolt/connectors/ConnectorNames.h"

namespace bytedance::bolt::connector::procfs {

enum class ProcFsTableKind {
  kMeminfo,
  kCpuinfo,
  kModules,
};

struct ProcFsConnectorSplit : public connector::ConnectorSplit {
  explicit ProcFsConnectorSplit(const std::string& connectorId)
      : ConnectorSplit(connectorId) {}

  std::string toString() const override {
    return fmt::format("[ProcFsConnectorSplit connectorId={}]", connectorId);
  }
};

class ProcFsTableHandle : public ConnectorTableHandle {
 public:
  ProcFsTableHandle(std::string connectorId, std::string tableName)
      : ConnectorTableHandle(std::move(connectorId)),
        tableName_(std::move(tableName)) {}

  const std::string& name() const override {
    return tableName_;
  }

  std::string toString() const override {
    return fmt::format(
        "ProcFsTableHandle(connectorId={}, table={})",
        connectorId(),
        tableName_);
  }

 private:
  std::string tableName_;
};

struct MeminfoRow {
  std::string key;
  int64_t value;
  std::string unit;
};

struct CpuinfoRow {
  int64_t processor;
  std::string field;
  std::string value;
};

struct ModulesRow {
  std::string module;
  int64_t size;
  int64_t instances;
  std::string dependencies;
  std::string state;
  std::string address;
};

class ProcFsDataSource : public DataSource {
 public:
  ProcFsDataSource(
      const std::shared_ptr<const RowType>& outputType,
      const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
      bolt::memory::MemoryPool* FOLLY_NONNULL pool);

  void addSplit(std::shared_ptr<ConnectorSplit> split) override;

  std::optional<RowVectorPtr> next(uint64_t size, bolt::ContinueFuture& future)
      override;

  void addDynamicFilter(
      column_index_t /*outputChannel*/,
      const std::shared_ptr<common::Filter>& /*filter*/) override {
    BOLT_NYI("Dynamic filters are not supported by ProcFsConnector.");
  }

  uint64_t getCompletedBytes() override {
    return completedBytes_;
  }

  uint64_t getCompletedRows() override {
    return completedRows_;
  }

  std::unordered_map<std::string, RuntimeCounter> runtimeStats() override {
    return {};
  }

 private:
  static ProcFsTableKind parseTableKind(const std::string& tableName);

  void parseCurrentTable();
  void parseMeminfo();
  void parseCpuinfo();
  void parseModules();

  RowVectorPtr nextMeminfoBatch(uint64_t batchSize);
  RowVectorPtr nextCpuinfoBatch(uint64_t batchSize);
  RowVectorPtr nextModulesBatch(uint64_t batchSize);

  RowTypePtr outputType_;
  memory::MemoryPool* FOLLY_NONNULL pool_;
  const std::string tableName_;
  const ProcFsTableKind tableKind_;

  std::shared_ptr<ProcFsConnectorSplit> currentSplit_;
  uint64_t rowOffset_{0};
  uint64_t completedRows_{0};
  uint64_t completedBytes_{0};

  std::vector<MeminfoRow> meminfoRows_;
  std::vector<CpuinfoRow> cpuinfoRows_;
  std::vector<ModulesRow> modulesRows_;
};

class ProcFsConnector final : public Connector {
 public:
  ProcFsConnector(
      const std::string& id,
      std::shared_ptr<const config::ConfigBase> /*config*/,
      folly::Executor* /*executor*/)
      : Connector(id) {}

  std::unique_ptr<DataSource> createDataSource(
      const std::shared_ptr<const RowType>& outputType,
      const std::shared_ptr<ConnectorTableHandle>& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& /*columnHandles*/,
      std::shared_ptr<ConnectorQueryCtx> connectorQueryCtx,
      const core::QueryConfig& /*queryConfig*/) override {
    return std::make_unique<ProcFsDataSource>(
        outputType, tableHandle, connectorQueryCtx->memoryPool());
  }

  std::unique_ptr<DataSink> createDataSink(
      RowTypePtr /*inputType*/,
      std::shared_ptr<
          ConnectorInsertTableHandle> /*connectorInsertTableHandle*/,
      ConnectorQueryCtx* /*connectorQueryCtx*/,
      CommitStrategy /*commitStrategy*/,
      const core::QueryConfig& /*queryConfig*/) override {
    BOLT_NYI("ProcFsConnector does not support write.");
  }
};

class ProcFsConnectorFactory : public ConnectorFactory {
 public:
  ProcFsConnectorFactory() : ConnectorFactory(kProcFsConnectorName) {}

  std::shared_ptr<Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const config::ConfigBase> config,
      folly::Executor* executor = nullptr) override {
    return std::make_shared<ProcFsConnector>(id, config, executor);
  }

  std::shared_ptr<Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const Config> config,
      folly::Executor* executor = nullptr) override {
    std::shared_ptr<const config::ConfigBase> convertedConfig;
    convertedConfig = config == nullptr
        ? nullptr
        : std::make_shared<config::ConfigBase>(config->valuesCopy());
    return newConnector(id, convertedConfig, executor);
  }
};

} // namespace bytedance::bolt::connector::procfs

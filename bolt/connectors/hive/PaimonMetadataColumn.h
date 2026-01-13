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
#pragma once

#include "bolt/type/Type.h"
#include "bolt/vector/BaseVector.h"
#include "bolt/vector/ComplexVector.h"

namespace bytedance::bolt::connector::paimon {

class MetadataColumn {
 public:
  virtual ~MetadataColumn() = default;
  MetadataColumn() = default;
  MetadataColumn(const MetadataColumn&) = delete;
  MetadataColumn& operator=(const MetadataColumn&) = delete;
  MetadataColumn& operator=(MetadataColumn&&) = default;

  virtual TypePtr type() const = 0;
  virtual void populateVector(VectorPtr& vector) = 0;
};

class MetadataColumnFilePath : public MetadataColumn {
 public:
  explicit MetadataColumnFilePath(const std::string& filePath)
      : filePath_(filePath) {}
  TypePtr type() const override;
  void populateVector(VectorPtr& vector) override;

 private:
  const std::string filePath_;
};

class MetadataColumnBucket : public MetadataColumn {
 public:
  explicit MetadataColumnBucket(int32_t bucket) : bucket_(bucket) {}
  TypePtr type() const override;
  void populateVector(VectorPtr& vector) override;

 private:
  const int32_t bucket_;
};

class MetadataColumnPartition : public MetadataColumn {
 public:
  explicit MetadataColumnPartition(
      TypePtr partitionType,
      const std::unordered_map<std::string, std::optional<std::string>>&
          partitionKeys,
      memory::MemoryPool* pool);
  TypePtr type() const override;
  void populateVector(VectorPtr& vector) override;

 private:
  const TypePtr partitionType_;
  RowVectorPtr partitionValue_;
};

class MetadataColumnSequenceNumber : public MetadataColumn {
 public:
  explicit MetadataColumnSequenceNumber(int64_t maxSequenceNumber)
      : maxSequenceNumber_(maxSequenceNumber) {}
  TypePtr type() const override;
  void populateVector(VectorPtr& vector) override;

 private:
  const int64_t maxSequenceNumber_;
};

} // namespace bytedance::bolt::connector::paimon

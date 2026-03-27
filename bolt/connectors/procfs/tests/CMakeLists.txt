# Copyright (c) ByteDance Ltd. and/or its affiliates.
# SPDX-License-Identifier: Apache-2.0

add_executable(bolt_procfs_connector_test ProcFsConnectorTest.cpp)
add_test(bolt_procfs_connector_test bolt_procfs_connector_test)

target_link_libraries(
  bolt_procfs_connector_test
  bolt_procfs_connector
  bolt_testutils
  GTest::gtest
  GTest::gtest_main
)


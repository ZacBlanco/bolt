# Copyright (c) ByteDance Ltd. and/or its affiliates.
# SPDX-License-Identifier: Apache-2.0

bolt_add_library(bolt_procfs_connector ProcFsConnector.cpp)

target_link_libraries(bolt_procfs_connector bolt_connector)

add_executable(bolt_procfs_printer ProcFsPrinterMain.cpp)
target_link_libraries(
  bolt_procfs_printer
  bolt_engine
  bolt_procfs_connector
  gflags::gflags
  Folly::folly
)

if(${BOLT_BUILD_TESTING})
  add_subdirectory(tests)
endif()

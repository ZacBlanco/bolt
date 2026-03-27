/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

namespace bytedance::bolt::exec::plugin {

/// Registers exec-side callback that installs plugin operator translators into
/// exec translation runtime when plugins are added.
void initializeOperatorPluginAdapter();

} // namespace bytedance::bolt::exec::plugin

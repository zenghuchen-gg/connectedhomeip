/*
 *
 *    Copyright (c) 2026 Project CHIP Authors
 *    All rights reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

#include "MountedDimmableLoadControl.h"

#include <devices/Types.h>

namespace chip {
namespace app {

namespace {

constexpr DataModel::DeviceTypeEntry kMountedDimmableOnly[] = {
    Device::Type::kMountedDimmableLoadControl,
};

constexpr DataModel::DeviceTypeEntry kMountedDimmableWithPlug[] = {
    Device::Type::kMountedDimmableLoadControl,
    Device::Type::kDimmablePlugInUnit,
};

} // namespace

MountedDimmableLoadControl::MountedDimmableLoadControl(const Context & context) : MountedDimmableLoadControl(context, Config{}) {}

MountedDimmableLoadControl::MountedDimmableLoadControl(const Context & context, const Config & config) :
    DimmableLoad(config.includeDimmablePlugInUnit ? Span<const DataModel::DeviceTypeEntry>(kMountedDimmableWithPlug)
                                                  : Span<const DataModel::DeviceTypeEntry>(kMountedDimmableOnly),
                 context,
                 Delegates{
                     .onOff        = *this,
                     .levelControl = *this,
                     .effect       = *this,
                     .identify     = context.identifyDelegate,
                 },
                 DimmableLoad::Config{
                     .levelControl = config.levelControl,
                     .tagList      = config.tagList,
                 })
{}

} // namespace app
} // namespace chip

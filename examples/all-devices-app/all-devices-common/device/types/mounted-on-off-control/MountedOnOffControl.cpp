/*
 *    Copyright (c) 2026 Project CHIP Authors
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

#include "MountedOnOffControl.h"

#include <devices/Types.h>

namespace chip::app {

namespace {

constexpr DataModel::DeviceTypeEntry kMountedOnOffOnly[] = {
    Device::Type::kMountedOnOffControl,
};

constexpr DataModel::DeviceTypeEntry kMountedOnOffWithPlug[] = {
    Device::Type::kMountedOnOffControl,
    Device::Type::kOnOffPlugInUnit,
};

} // namespace

MountedOnOffControl::MountedOnOffControl(const Context & context, const Config & config, Clusters::OnOffDelegate * customOnOff,
                                         Clusters::OnOffEffectDelegate * customEffect,
                                         Clusters::IdentifyDelegate * customIdentify) :
    OnOffLoad(config.includeOnOffPlugInUnit ? Span<const DataModel::DeviceTypeEntry>(kMountedOnOffWithPlug)
                                            : Span<const DataModel::DeviceTypeEntry>(kMountedOnOffOnly),
              customOnOff ? *customOnOff : static_cast<Clusters::OnOffDelegate &>(*this),
              customEffect ? *customEffect : static_cast<Clusters::OnOffEffectDelegate &>(*this),
              customIdentify ? *customIdentify : context.identifyDelegate, context)
{}

MountedOnOffControl::MountedOnOffControl(const Context & context, Clusters::OnOffDelegate * customOnOff,
                                         Clusters::OnOffEffectDelegate * customEffect,
                                         Clusters::IdentifyDelegate * customIdentify) :
    MountedOnOffControl(context, Config{}, customOnOff, customEffect, customIdentify)
{}

} // namespace chip::app

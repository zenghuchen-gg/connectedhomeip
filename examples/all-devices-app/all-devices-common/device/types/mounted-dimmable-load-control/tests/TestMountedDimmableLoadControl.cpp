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

#include <device/types/mounted-dimmable-load-control/MountedDimmableLoadControl.h>
#include <pw_unit_test/framework.h>

#include <app/server-cluster/testing/TestServerClusterContext.h>
#include <credentials/FabricTable.h>
#include <credentials/GroupDataProviderImpl.h>
#include <data-model-providers/codedriven/CodeDrivenDataModelProvider.h>
#include <device/api/allocator/DynamicEndpointIdAllocator.h>
#include <device/capabilities/identify/LoggingIdentifyDelegate.h>
#include <devices/Types.h>
#include <lib/support/TimerDelegateMock.h>
#include <lib/support/tests/ExtraPwTestMacros.h>

namespace {

using namespace chip;
using namespace chip::app;

class TestActionContext : public DataModel::ActionContext
{
public:
    Messaging::ExchangeContext * CurrentExchange() override { return nullptr; }
};

class TestMountedDimmableLoadControl : public ::testing::Test
{
public:
    static void SetUpTestSuite() { ASSERT_SUCCESS(Platform::MemoryInit()); }
    static void TearDownTestSuite() { Platform::MemoryShutdown(); }

protected:
    static constexpr EndpointId kTestEndpoint = 1;

    void SetUp() override
    {
        mServerClusterTestContext.StorageDelegate().ClearStorage();
        ASSERT_SUCCESS(mProvider.Startup(mInteractionModelContext));
    }

    void TearDown() override { EXPECT_SUCCESS(mProvider.Shutdown()); }

    MountedDimmableLoadControl::Context MakeContext()
    {
        return MountedDimmableLoadControl::Context{
            .groupDataProvider = mGroupDataProvider,
            .fabricTable       = mFabricTable,
            .timerDelegate     = mTimerDelegate,
            .identifyDelegate  = mIdentifyDelegate,
        };
    }

    Testing::LogOnlyEvents mEventGenerator;
    TestActionContext mActionContext;
    DataModel::InteractionModelContext mInteractionModelContext{
        .eventsGenerator = mEventGenerator,
        .actionContext   = mActionContext,
    };
    Testing::TestServerClusterContext mServerClusterTestContext;
    CodeDrivenDataModelProvider mProvider{ mServerClusterTestContext.StorageDelegate(),
                                           mServerClusterTestContext.AttributePersistenceProvider() };
    Credentials::GroupDataProviderImpl mGroupDataProvider;
    FabricTable mFabricTable;
    TimerDelegateMock mTimerDelegate;
    LoggingIdentifyDelegate mIdentifyDelegate;
};

TEST_F(TestMountedDimmableLoadControl, DefaultExposesOnlyMountedDimmableLoadControlDeviceType)
{
    MountedDimmableLoadControl device(MakeContext());
    DeviceInterface & iface = device;

    DynamicEndpointIdAllocator allocator({ kRootEndpointId });
    allocator.ForceNext(kTestEndpoint);

    ASSERT_SUCCESS(iface.Register(allocator, mProvider));

    ReadOnlyBufferBuilder<DataModel::DeviceTypeEntry> builder;
    ASSERT_SUCCESS(mProvider.DeviceTypes(kTestEndpoint, builder));
    auto deviceTypes = builder.TakeBuffer();

    ASSERT_EQ(deviceTypes.size(), 1u);
    EXPECT_EQ(deviceTypes[0].deviceTypeId, Device::Type::kMountedDimmableLoadControl.deviceTypeId);

    iface.Unregister(mProvider);
}

TEST_F(TestMountedDimmableLoadControl, IncludeDimmablePlugInUnitExposesBothDeviceTypesOnSameEndpoint)
{
    MountedDimmableLoadControl device(MakeContext(), MountedDimmableLoadControl::Config{ .includeDimmablePlugInUnit = true });
    DeviceInterface & iface = device;

    DynamicEndpointIdAllocator allocator({ kRootEndpointId });
    allocator.ForceNext(kTestEndpoint);

    ASSERT_SUCCESS(iface.Register(allocator, mProvider));

    ReadOnlyBufferBuilder<DataModel::DeviceTypeEntry> builder;
    ASSERT_SUCCESS(mProvider.DeviceTypes(kTestEndpoint, builder));
    auto deviceTypes = builder.TakeBuffer();

    ASSERT_EQ(deviceTypes.size(), 2u);
    EXPECT_EQ(deviceTypes[0].deviceTypeId, Device::Type::kMountedDimmableLoadControl.deviceTypeId);
    EXPECT_EQ(deviceTypes[1].deviceTypeId, Device::Type::kDimmablePlugInUnit.deviceTypeId);

    iface.Unregister(mProvider);
}

} // namespace

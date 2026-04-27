#
#    Copyright (c) 2024 Project CHIP Authors
#    All rights reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#

import base64
import json
import logging
import sys
import time
import asyncio

import matter.clusters as Clusters
from matter.clusters import ClusterObjects as ClusterObjects
from matter.clusters.Attribute import EventReadResult, SubscriptionTransaction, TypedAttributePath
from matter.interaction_model import Status
from matter.testing.matter_testing import MatterBaseTest
from matter.testing.decorators import async_test_body
from matter.testing.runner import TestStep, default_matter_test_main

DM_DUMP_FILE = None
JSON_LOG_FILE = None
TEXT_LOG_FILE = None


class DeviceStateListenerTool(MatterBaseTest):
    @property
    def default_timeout(self) -> int:
        return 2**63  # Effectively infinite timeout

    def steps_DeviceStateListener(self) -> list[TestStep]:
        steps = [TestStep(1, "Commissioning, already done", is_commissioning=True),
                 TestStep(2, "Listen to device activity"),
                 ]
        return steps

    def desc_DeviceStateListener(self) -> str:
        return 'List to entire device state changes over time'

    @async_test_body
    async def test_DeviceStateListener(self):
        dev_ctrl = self.default_controller

        def event_callback(res: EventReadResult, transaction: SubscriptionTransaction) -> None:
            if res.Status == Status.Success:
                cluster_id = res.Header.ClusterId
                cluster = ClusterObjects.ALL_CLUSTERS.get(cluster_id)
                cluster_name = cluster.__name__ if cluster is not None else f"Cluster 0x{cluster_id:x}"

                event_id = res.Header.EventId
                event = ClusterObjects.ALL_EVENTS.get(cluster_id, {}).get(event_id)
                event_name = event.__name__ if event is not None else f"Event 0x{event_id:x}"

                log_msg = f'@@@ EVENT: EP{res.Header.EndpointId}/{cluster_name}/{event_name}: {res.Data}'
                logging.info(log_msg)
                if TEXT_LOG_FILE:
                    with open(TEXT_LOG_FILE, 'a') as f:
                        f.write(f"[{time.time()}] {log_msg}\n")

        def attribute_callback(path: TypedAttributePath, transaction: SubscriptionTransaction):
            cluster_id = path.ClusterId
            cluster = ClusterObjects.ALL_CLUSTERS.get(cluster_id)
            cluster_name = cluster.__name__ if cluster is not None else f"Cluster 0x{cluster_id:x}"

            attribute_id = path.AttributeId
            attribute = ClusterObjects.ALL_ATTRIBUTES.get(cluster_id, {}).get(attribute_id)
            attribute_name = attribute.__name__ if attribute is not None else f"Attribute 0x{attribute_id:x}"
            attribute_data = transaction.GetAttribute(path)

            log_msg = f'@@@ ATTRIB: EP{path.Path.EndpointId}/{cluster_name}/{attribute_name}: {attribute_data}'
            logging.info(log_msg)
            if TEXT_LOG_FILE:
                with open(TEXT_LOG_FILE, 'a') as f:
                    f.write(f"[{time.time()}] {log_msg}\n")

        def report_begin_callback(transaction: SubscriptionTransaction):
            log_msg = '@@@ REPORT BEGIN'
            logging.info(log_msg)
            if TEXT_LOG_FILE:
                with open(TEXT_LOG_FILE, 'a') as f:
                    f.write(f"[{time.time()}] {log_msg}\n")

        def report_end_callback(transaction: SubscriptionTransaction):
            log_msg = '@@@ REPORT END'
            logging.info(log_msg)
            if TEXT_LOG_FILE:
                with open(TEXT_LOG_FILE, 'a') as f:
                    f.write(f"[{time.time()}] {log_msg}\n")

        def raw_report_callback(data: bytes, transaction: SubscriptionTransaction):
            report_type = "priming_report" if getattr(transaction, 'is_priming', False) else "subscription_report"
            tlv_b64 = base64.b64encode(data).decode("utf-8")
            logging.info(f'@@@  RAW REPORT [{report_type}] ({len(data)} bytes): {tlv_b64}')
            if JSON_LOG_FILE:
                with open(JSON_LOG_FILE, 'a') as f:
                    log_entry = {
                        "timestamp": time.time(),
                        "node_id": self.dut_node_id,
                        "type": report_type,
                        "size": len(data),
                        "tlv": tlv_b64
                    }
                    f.write(json.dumps(log_entry) + "\n")

        urgent = True
        sub = await dev_ctrl.Read(self.dut_node_id, attributes=[("*")],
                                  events=[("*", urgent)],
                                  reportInterval=(1, 3600),
                                  fabricFiltered=True, keepSubscriptions=True, autoResubscribe=True)
        sub.SetEventUpdateCallback(event_callback)
        sub.SetAttributeUpdateCallback(attribute_callback)
        sub.SetReportBeginCallback(report_begin_callback)
        sub.SetReportEndCallback(report_end_callback)
        sub.SetReadRawReportCallback(raw_report_callback)

        if DM_DUMP_FILE:
            logging.info(f"Saving data model dump to {DM_DUMP_FILE}...")
            with open(DM_DUMP_FILE, 'w') as f:
                f.write("ATTRIBUTES:\n")
                for ep, clusters in sorted(sub.GetAttributes().items()):
                    f.write(f"Endpoint {ep}:\n")
                    for cluster, attrs in sorted(clusters.items(), key=lambda item: item[0].id):
                        cluster_name = cluster.__name__ if hasattr(cluster, '__name__') else str(cluster)
                        f.write(f"  Cluster {cluster_name}:\n")
                        if isinstance(attrs, dict):
                            for attr, val in sorted(attrs.items(), key=lambda item: item[0].attribute_id if hasattr(item[0], 'attribute_id') else -1):
                                attr_name = attr.__name__ if hasattr(attr, '__name__') else str(attr)
                                f.write(f"    {attr_name}: {val}\n")
                        else:
                            f.write(f"    {attrs}\n")

                f.write("\nEVENTS:\n")
                events_by_ep = {}
                for event_result in sub.GetEvents():
                    if event_result.Status == Status.Success:
                        ep = event_result.Header.EndpointId
                        events_by_ep.setdefault(ep, []).append(event_result)

                for ep, event_list in sorted(events_by_ep.items()):
                    f.write(f"Endpoint {ep}:\n")
                    for event_result in sorted(event_list, key=lambda e: (e.Header.ClusterId, e.Header.EventNumber)):
                        cluster = ClusterObjects.ALL_CLUSTERS.get(event_result.Header.ClusterId)
                        cluster_name = cluster.__name__ if cluster is not None else f"Cluster 0x{event_result.Header.ClusterId:x}"
                        event_type = ClusterObjects.ALL_EVENTS.get(
                            event_result.Header.ClusterId, {}).get(event_result.Header.EventId)
                        event_name = event_type.__name__ if event_type is not None else f"Event 0x{event_result.Header.EventId:x}"
                        f.write(f"  {cluster_name} - Event {event_name} (Num: {event_result.Header.EventNumber}): {event_result.Data}\n")
            logging.info(f"Data model dump saved to {DM_DUMP_FILE}")

        async def keep_session_alive():
            """Periodically ping the device to keep the secure session alive without draining battery."""
            while True:
                await asyncio.sleep(50)
                try:
                    # Read a lightweight attribute to refresh the session timers
                    await dev_ctrl.ReadAttribute(self.dut_node_id, [(0, Clusters.BasicInformation.Attributes.VendorID)])
                except Exception as e:
                    logging.debug(f"Keep-alive ping failed: {e}")

        # Start the background ping task
        keep_alive_task = asyncio.create_task(keep_session_alive())

        while True:
            await asyncio.sleep(0.1)

        return

        async def write_fan_mode(fan_mode):
            logging.info(f"@@@ WRITE FanMode to {fan_mode}")
            endpoint = 1
            await dev_ctrl.WriteAttribute(self.dut_node_id, [(endpoint, Clusters.FanControl.Attributes.FanMode(fan_mode))])

        async def write_percent_setting(percent):
            logging.info(f"@@@ WRITE PercentSetting to {percent}")
            endpoint = 1
            await dev_ctrl.WriteAttribute(self.dut_node_id, [(endpoint, Clusters.FanControl.Attributes.PercentSetting(percent))])

        async def write_speed_setting(speed):
            logging.info(f"@@@ WRITE SpeedSetting to {speed}")
            endpoint = 1
            await dev_ctrl.WriteAttribute(self.dut_node_id, [(endpoint, Clusters.FanControl.Attributes.SpeedSetting(speed))])

        for fan_mode in [Clusters.FanControl.Enums.FanModeEnum.kOff,
                         Clusters.FanControl.Enums.FanModeEnum.kLow,
                         Clusters.FanControl.Enums.FanModeEnum.kMedium,
                         Clusters.FanControl.Enums.FanModeEnum.kHigh,
                         Clusters.FanControl.Enums.FanModeEnum.kOn,
                         Clusters.FanControl.Enums.FanModeEnum.kAuto,
                         Clusters.FanControl.Enums.FanModeEnum.kSmart]:
            await write_fan_mode(fan_mode)
            time.sleep(5.0)

        for percent_setting in [100, 80, 60, 42, 25, 10, 5, 2, 0]:
            await write_percent_setting(percent_setting)
            time.sleep(2.5)

        for speed_setting in [1, 3, 5, 7, 9, 10]:
            await write_speed_setting(speed_setting)
            time.sleep(2.5)

        await write_speed_setting(0)
        time.sleep(5.0)

        await write_fan_mode(Clusters.FanControl.Enums.FanModeEnum.kLow)
        time.sleep(5.0)

        await write_fan_mode(Clusters.FanControl.Enums.FanModeEnum.kAuto)
        time.sleep(5.0)

        await write_fan_mode(Clusters.FanControl.Enums.FanModeEnum.kSmart)
        time.sleep(5.0)


if __name__ == "__main__":
    if "--dm-dump" in sys.argv:
        idx = sys.argv.index("--dm-dump")
        DM_DUMP_FILE = sys.argv[idx + 1]
        sys.argv.pop(idx)  # Remove the argument
        sys.argv.pop(idx)  # Remove the value
        open(DM_DUMP_FILE, 'w').close()

    if "--json-log" in sys.argv:
        idx = sys.argv.index("--json-log")
        JSON_LOG_FILE = sys.argv[idx + 1]
        sys.argv.pop(idx)  # Remove the argument
        sys.argv.pop(idx)  # Remove the value
        open(JSON_LOG_FILE, 'w').close()

    if "--text-log" in sys.argv:
        idx = sys.argv.index("--text-log")
        TEXT_LOG_FILE = sys.argv[idx + 1]
        sys.argv.pop(idx)  # Remove the argument
        sys.argv.pop(idx)  # Remove the value
        open(TEXT_LOG_FILE, 'w').close()

    default_matter_test_main()

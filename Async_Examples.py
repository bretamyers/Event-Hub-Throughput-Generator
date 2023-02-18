#!/usr/bin/env python

# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for license information.
# --------------------------------------------------------------------------------------------

"""
Examples to show sending events with different options to an Event Hub asynchronously.
"""

import time
import asyncio
import os

from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub.exceptions import EventHubError
from azure.eventhub import EventData

import main.DetermineNodes


async def send_event_data_list(producer, eventDataList):
    await producer.send_batch(eventDataList)


async def gen_data(NodeSpecDict:dict) -> None:
    producer = EventHubProducerClient.from_connection_string(
        conn_str=NodeSpecDict['EventHubConnection']
        ,eventhub_name=NodeSpecDict['EventHubName']
    )

    gen_start_time = time.time()
    while int(time.time() - gen_start_time) < NodeSpecDict['RunDurationMin']*60:
        print(int(time.time() - gen_start_time), int(time.time())%60)
    
        async with producer:
            start_time = time.time()
            eventDataList = list()
            for _ in range(NodeSpecDict['NodeThroughput']):
                eventDataList.append(EventData(main.DetermineNodes.gen_payload(jsonAttributePathList=[_ for _ in NodeSpecDict['PayloadDefinitionList']], maxValueFlag=False)))
            
            time.sleep(1) #get the difference in time
            # await producer.send_batch(eventDataList)
            await producer.send_batch(eventDataList)
            # send_event_data_list(producer, eventDataList)
        
        print(f'Sent in {str(round(time.time() - start_time, 2))} seconds')

if __name__ == '__main__':

    import tomllib
    import json

    with open('main/config.toml', 'rb') as f:
        config = tomllib.load(f)
        print(json.dumps(config, indent=4))

    import main.DetermineNodes

    batchSpecDict = main.DetermineNodes.get_batch_specs(throughput=config['AzureBatch']['ThroughputMessagesPerSec'])

    NodeSpecDict = {
        'RunDurationMin': config['AzureBatch']['RunDurationMin']
        ,'EventHubConnection': config['AzureBatch']['EventHubConnection']
        ,'EventHubName': config['AzureBatch']['EventHubName']
        ,'NodeThroughput': batchSpecDict['NodeThroughput']
        ,'PayloadDefinitionList': batchSpecDict['PayloadDefinitionList']
    }

    start_time = time.time()
    asyncio.run(gen_data(NodeSpecDict=NodeSpecDict))
    print("Send messages in {} seconds.".format(time.time() - start_time))


import time
import Batch.DetermineNodes
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
import tomllib
import json
import asyncio

def sync_time():
    time.sleep(1-(time.time()%1)) #time.time() = epoch time

# https://github.com/Azure/azure-sdk-for-python/blob/main/sdk/eventhub/azure-eventhub/samples/async_samples/send_async.py
async def send_event_data_batch(producer, eventDataList):
    # Without specifying partition_id or partition_key
    # the events will be distributed to available partitions via round-robin.
    await producer.send_batch(eventDataList)
    print(f"Batch Count {len(eventDataList)}")
            

async def gen_data(NodeSpecDict:dict) -> None:
    producer = EventHubProducerClient.from_connection_string(
        conn_str=NodeSpecDict['EventHubConnection']
        ,eventhub_name=NodeSpecDict['EventHubName']
    )

    gen_start_time = time.time()
    while int(time.time() - gen_start_time) < NodeSpecDict['RunDurationMin']*60:
        print(int(time.time() - gen_start_time), int(time.time())%60)
        print(int(time.time()%60 * 1000)/1000) #millisec

        async with producer:
            start_time = time.time()
            eventDataList = list()
            for _ in range(NodeSpecDict['NodeThroughput']):
                eventDataList.append(EventData(Batch.DetermineNodes.gen_payload(jsonAttributePathList=[_ for _ in NodeSpecDict['PayloadDefinitionList']], maxValueFlag=False)))

            # send_event_data_batch(producer=producer, eventDataList=eventDataList)
            sync_time()
            await producer.send_batch(eventDataList)
            print(f'Sent in {str(round(time.time() - start_time, 2))} seconds')

        

if __name__ == '__main__':

    with open('main/config.toml', 'rb') as f:
        config = tomllib.load(f)
        print(json.dumps(config, indent=4))

    import Batch.DetermineNodes

    batchSpecDict = Batch.DetermineNodes.get_batch_specs(throughput=config['AzureBatch']['ThroughputMessagesPerSec'])

    NodeSpecDict = {
        'RunDurationMin': config['AzureBatch']['RunDurationMin']
        ,'EventHubConnection': config['AzureBatch']['EventHubConnection']
        ,'EventHubName': config['AzureBatch']['EventHubName']
        ,'NodeThroughput': batchSpecDict['NodeThroughput']
        ,'PayloadDefinitionList': batchSpecDict['PayloadDefinitionList']
    }

    print(batchSpecDict['PayloadDefinitionList'])
    asyncio.run(gen_data(NodeSpecDict=NodeSpecDict))
    

import time, datetime
import DetermineNodes
from azure.eventhub import EventHubProducerClient, EventData
import tomllib
import json
import sys

def sync_time():
    time.sleep(1-(time.time()%1)) #time.time() = epoch time

def gen_data(NodeSpecDict:dict) -> None:
    producer = EventHubProducerClient.from_connection_string(
        conn_str=NodeSpecDict['EventHubConnection'],
        eventhub_name=NodeSpecDict['EventHubName']
    )

    sync_time()
    gen_start_time = time.time()
    while int(time.time() - gen_start_time) < NodeSpecDict['RunDurationMin']*60:
        # print(int(time.time() - gen_start_time))

        with producer:
            event_data_batch = producer.create_batch()

            start_time = time.time()
            for _ in range(NodeSpecDict['NodeThroughput']):
                eventString = DetermineNodes.gen_payload(jsonAttributePathList=[_ for _ in NodeSpecDict['PayloadDefinitionList']], maxValueFlag=False)
                event_data = EventData(eventString)
                event_data_batch.add(event_data)

            sync_time()
            # if int(time.time())%NodeSpecDict['NumberOfNodes'] == NodeSpecDict['NodeNum']:
            if int(time.time())%NodeSpecDict['NumberOfNodes'] == NodeSpecDict['NodeSec']:
                producer.send_batch(event_data_batch)
                print(f"Batch Count {len(event_data_batch)} - Total Sent {event_data_batch} messagess in {str(round(time.time() - start_time, 2))} seconds - {time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())}")
                event_data_batch = producer.create_batch()
        

def regression_test():
    with open('main/config_user.toml', 'rb') as f:
        config = tomllib.load(f)
        print(json.dumps(config, indent=4))

    import DetermineNodes

    batchSpecMasterDict = DetermineNodes.get_batch_specs(TargetThroughput=config['GeneratorInput']['ThroughputMessagesPerSec'])

    #Create a job on the pool and use this loop to create tasks within the job
    for nodeSpec in batchSpecMasterDict['NodeMessageSpecList']:
        NodeSpecDict = {
            'RunDurationMin': config['GeneratorInput']['RunDurationMin']
            ,'EventHubConnection': config['AzureEventHub']['EventHubConnection']
            ,'EventHubName': config['AzureEventHub']['EventHubName']
            ,'NumberOfNodes': batchSpecMasterDict['NumberOfNodes']
            ,'PayloadDefinitionList': batchSpecMasterDict['PayloadDefinitionList']
            ,'NodeSec': nodeSpec['NodeSec']
            ,'NodeThroughput': nodeSpec['NodeThroughput']
        }

        print(f'Running Node Num {nodeSpec["NodeNum"]}')
        gen_data(NodeSpecDict)
        
if __name__ == '__main__':


    NodeSpecDict = json.loads(sys.argv[1])
    # NodeSpecDict = {
    #     'RunDurationMin': config['GeneratorInput']['RunDurationMin']
    #     ,'EventHubConnection': config['AzureEventHub']['EventHubConnection']
    #     ,'EventHubName': config['AzureEventHub']['EventHubName']
    #     ,'NumberOfNodes': batchSpecMasterDict['NumberOfNodes']
    #     ,'PayloadDefinitionList': batchSpecMasterDict['PayloadDefinitionList']
    #     ,'NodeSec': nodeSpec['NodeSec']
    #     ,'NodeThroughput': nodeSpec['NodeThroughput']
    # }

    print(f'Running Node Num {NodeSpecDict["NodeNum"]}')
    gen_data(NodeSpecDict)
        
    
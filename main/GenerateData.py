import time
import DetermineNodes
from azure.eventhub import EventHubProducerClient, EventData
import json
import os, sys


def sync_time():
    #sleep to the until the nearest second.
    time.sleep(1-(time.time()%1)) #time.time() = epoch time

def gen_data(NodeSpecDict:dict) -> None:
    producer = EventHubProducerClient.from_connection_string(
        conn_str=NodeSpecDict['EventHubConnection'],
        eventhub_name=NodeSpecDict['EventHubName']
    )

    sync_time()
    gen_start_time = time.time()
    while int(time.time() - gen_start_time) < NodeSpecDict['RunDurationMin']*60:
        # print(f'Running for {int(time.time() - gen_start_time)} sec')

        start_message_time = time.time()
        with producer:
            event_data_batch = producer.create_batch()

            start_datagen_time = time.time()
            for _ in range(int(NodeSpecDict['NodeThroughput'])):
                eventString = DetermineNodes.gen_payload(jsonAttributePathList=[_ for _ in NodeSpecDict['PayloadDefinitionList']], maxValueFlag=False)
                event_data = EventData(eventString)
                event_data_batch.add(event_data)
            end_datagen_time = time.time()

            while int(time.time())%NodeSpecDict['NumberOfNodes'] != int(NodeSpecDict['NodeSec']):
                # print(f"{int(time.time())%NodeSpecDict['NumberOfNodes']} - {int(NodeSpecDict['NodeSec'])}")
                sync_time()

            start_send_time = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())
            producer.send_batch(event_data_batch)
            print(f"Batch Count {len(event_data_batch)} - Start {start_send_time} - End {time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())} - Message Details {event_data_batch} messagess - Data Gen Duration - {str(round(end_datagen_time - start_datagen_time, 4))} - Total Duration {str(round(time.time() - start_message_time, 2))}")
            event_data_batch = producer.create_batch()
            sync_time() #sync time to the next nearest sec to avoid double sending
        

def regression_test():
    config = TomlHelper.read_toml_file('main/config_uers.toml')
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

    nodeSpec = json.loads(sys.argv[1])
    
    myNodeNum = nodeSpec['NodeMessageSpecList']['NodeNum'] 
    # myNodeNum = 1

    # config = TomlHelper.read_toml_file(FileName=os.path.join(os.path.split(os.path.join(os.path.dirname(os.path.abspath(__file__))))[0], 'config_user.toml'))

    baseMetrics = DetermineNodes.get_batch_specs(TargetThroughput=config_user['GeneratorInput']['ThroughputMessagesPerSec'])
    
    NodeSpecDict = {'EventHubConnection': config_user['AzureEventHub']['EventHubConnection']
                ,'EventHubName': config_user['AzureEventHub']['EventHubName']
                ,'RunDurationMin': config_user['GeneratorInput']['RunDurationMin']
                }
    for key, value in baseMetrics.items():
        if key in ['NodeMessageSpecList', 'PayloadDefinitionList', 'NumberOfNodes']:
            if key == 'NodeMessageSpecList':
                for nodeSpec in value:
                    if nodeSpec['NodeNum'] == str(myNodeNum):
                        NodeSpecDict.update(nodeSpec)
            else:
                NodeSpecDict[key] = value
    print(f'{json.dumps(NodeSpecDict, indent=4)}')
    

    # NodeSpecDict = {
    #     'RunDurationMin': config['GeneratorInput']['RunDurationMin']
    #     ,'EventHubConnection': config['AzureEventHub']['EventHubConnection']
    #     ,'EventHubName': config['AzureEventHub']['EventHubName']
    #     ,'NumberOfNodes': batchSpecMasterDict['NumberOfNodes']
    #     ,'PayloadDefinitionList': batchSpecMasterDict['PayloadDefinitionList']
    #     ,'NodeSec': nodeSpec['NodeSec']
    #     ,'NodeThroughput': nodeSpec['NodeThroughput']
    # }

    # print(f'Running Node Num {NodeSpecDict["NodeNum"]}')
    gen_data(NodeSpecDict)
        
    
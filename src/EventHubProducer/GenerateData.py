import time
# import Batch.DetermineNodes
import src.DataFactory.PayloadFactory as DataFactory_PayloadFactory
import src.DataFactory.DataTypeFactory as DataFactory_DataTypeFactory
import src.Helpers.TomlHelper as Helpers_TomlHelper
import src.Batch.BatchPoolWait as Batch_BatchPoolWait
import src.Batch.DetermineNodes as Batch_DetermineNodes
from azure.eventhub import EventHubProducerClient, EventData
import json
import os, sys
import faker
import copy
import random


def sync_time():
    #sleep to the until the nearest second.
    time.sleep(1-(time.time()%1)) #time.time() = epoch time

def gen_data(NodeSpecDict:dict) -> None:

    print('Starting sync time to nearest minute')
    #sleep to the until the nearest minute.
    time.sleep(60-(time.time()%60)) 

    print('Starting wait for nodes to be ready')
    #wait until all nodes are ready before generating the data
    Batch_BatchPoolWait.wait_until_pool_is_ready_state(NodeSpecDict=NodeSpecDict)

    producer = EventHubProducerClient.from_connection_string(
        conn_str=NodeSpecDict['EventHubConnection'],
        eventhub_name=NodeSpecDict['EventHubName']
    )

    fake = faker.Faker()
    faker.Faker.seed(NodeSpecDict['NodeNum'])

    #Build the pool of unique values
    # datasetDict = {("keyValue"): [{"element1": "1"}, {"element2": "1"}] }
    # datasetDict = dict() #dict.fromkeys([_ for _ in range(int(NodeSpecDict['NodeThroughput'])) ], dict())
    # for _ in range(int(NodeSpecDict['NodeThroughput'])):
    datasetDict = dict()
    while len(datasetDict.keys()) < int(NodeSpecDict['NodeThroughput']):
        keyValue = list()
        for key, value in NodeSpecDict['UniqueElements'][0].items():
            print(key)
            if value == '{product_code}':
                elementValue = DataFactory_DataTypeFactory.gen_product_code()
                keyValue.append(elementValue)
        datasetDict[tuple(keyValue)] = dict()
    # datasetDict = dict.fromkeys([(_,) for _ in range(int(NodeSpecDict['NodeThroughput'])) ], dict())
    print(json.dumps(datasetDict, indent=4))

    sync_time()
    gen_start_time = time.time()
    while int(time.time() - gen_start_time) < NodeSpecDict['RunDurationMin']*60:
        # print(f'Running for {int(time.time() - gen_start_time)} sec')

        start_message_time = time.time()
        with producer:
            event_data_batch = producer.create_batch()

            start_datagen_time = time.time()
            for _ in range(int(NodeSpecDict['NodeThroughput'])):
                keyTuple = (_, ) 
                # datasetDict[keyTuple] = dic
                eventString = json.dumps(DataFactory_PayloadFactory.gen_payload(jsonAttributePathDict=copy.deepcopy(NodeSpecDict['PayloadDefinitionDict'])
                                                                                ,keyTuple=keyTuple, datasetDict=datasetDict, maxValueFlag=False, fake=fake))
                print(keyTuple, '-', eventString)
                event_data = EventData(eventString)
                event_data_batch.add(event_data)
                # print(f'Event data batch size in bytes = {event_data_batch.size_in_bytes}')
            # print('\n')
            # print(datasetDict)
            # print('\n')
            end_datagen_time = time.time()

            while int(time.time())%4 != int(NodeSpecDict['NodeSec']):
                # print(f"{int(time.time())%NodeSpecDict['NumberOfNodes']} - {int(NodeSpecDict['NodeSec'])}")
                sync_time()

            start_send_time = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())
            producer.send_batch(event_data_batch)
            print(f"Batch Count {len(event_data_batch)} - Start {start_send_time} - End {time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())} - Message Details {event_data_batch} messagess - Data Gen Duration - {str(round(end_datagen_time - start_datagen_time, 4))} - Total Duration {str(round(time.time() - start_message_time, 2))}")
            event_data_batch = producer.create_batch()
            sync_time() #sync time to the next nearest sec to avoid double sending


        
if __name__ == '__main__':

    project_name = 'Event-Hub-Throughput-Generator'
    os_path_base = os.path.abspath(__file__).split(project_name)[0]

    config_global = Helpers_TomlHelper.read_toml_file(FileName=os.path.join(os_path_base, project_name, 'config_global_local.toml'))

    config_user = Helpers_TomlHelper.read_toml_file(FileName=os.path.join(os_path_base, project_name, config_global['DataGeneration']['ConfigFilePath']))

    node_spec_dict = Batch_DetermineNodes.get_batch_specs(TargetThroughput=config_user['GeneratorInput']['ThroughputMessagesPerSec'], JsonFilePath=config_user['GeneratorInput']['JsonTemplate'])
    
    nodeSpec = node_spec_dict['NodeMessageSpecList'][0]

    my_pool_id = f'{config_global["AzureBatch"]["PoolNameBase"]}-{config_global["AzureBatch"]["PoolVMSku"]}-{node_spec_dict["NumberOfNodes"]}-{config_global["AzureBatch"]["TaskSlotsPerTask"]}'[:64] #limited to 64 characters

    NodeSpecDict = {'EventHubConnection': config_user['AzureEventHub']['EventHubConnection']
            ,'EventHubName': config_user['AzureEventHub']['EventHubName']
            ,'RunDurationMin': config_user['GeneratorInput']['RunDurationMin']
            ,'NumberOfNodes': node_spec_dict['NumberOfNodes']
            ,'NodeNum': nodeSpec['NodeNum']
            ,'NodeSec': nodeSpec['NodeSec']
            ,'NodeThroughput': nodeSpec['NodeThroughput']
            ,'PayloadDefinitionDict': node_spec_dict['PayloadDefinitionDict']
            ,'BatchAccountKey': config_user['AzureBatch']['BatchAccountKey']
            ,'BatchAccountName': config_user['AzureBatch']['BatchAccountName']
            ,'BatchServiceUrl': config_user['AzureBatch']['BatchServiceUrl']
            ,'PoolId': my_pool_id
            ,'UniqueElements': ({'deviceId': '{product_code}'}, 100000)
            }

    # print(NodeSpecDict)
    
    # item = '{datetime} 2020-01-01 increasing'
    item = '{product_code} unique 100000'
    dataType = item[item.index('{')+1:item.index('}')]
    properties = item[item.index('}')+1:].strip().split(' ')
    print(dataType)
    print(properties)
    # print(json.dumps(NodeSpecDict, indent=4))

    gen_data(NodeSpecDict)
    



    
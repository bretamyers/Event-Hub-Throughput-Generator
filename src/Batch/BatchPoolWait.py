from azure.batch import BatchServiceClient
from azure.batch.batch_auth import SharedKeyCredentials
import azure.batch.models as batchmodels
import time
import Helpers.TomlHelper
import os
import Batch.DetermineNodes


def get_node_list(NodeSpecDict: dict) -> None:
    # nodes = list(batch_client.compute_node.list(pool.id))
    pass


def reboot_nodes_with_startup_error(NodeSpecDict: dict) -> None:

    credentials = SharedKeyCredentials(
        NodeSpecDict['BatchAccountName'],
        NodeSpecDict['BatchAccountKey']
        )

    batch_client = BatchServiceClient(
        credentials,
        batch_url=NodeSpecDict['BatchServiceUrl']
        )
    
    nodes = list(batch_client.compute_node.list(NodeSpecDict['PoolId']))
    for node in nodes:
        # https://learn.microsoft.com/en-us/python/api/azure-batch/azure.batch.models.computenodestate?view=azure-python
        if node.state.value == 'starttaskfailed':
            print(f'Rebooting node {node.id}')
            batch_client.compute_node.reboot(pool_id=NodeSpecDict['PoolId'], node_id=node.id)

    # print(PoolReadyTotalNodes)


def wait_until_pool_is_ready_state(NodeSpecDict: dict) -> None:
    credentials = SharedKeyCredentials(
        NodeSpecDict['BatchAccountName'],
        NodeSpecDict['BatchAccountKey']
        )

    batch_client = BatchServiceClient(
        credentials,
        batch_url=NodeSpecDict['BatchServiceUrl']
        )

    poolInfo = batch_client.pool.get(pool_id=NodeSpecDict['PoolId'])
    # print(poolInfo)
    PoolTargetTotalNodes = poolInfo.target_dedicated_nodes + poolInfo.target_low_priority_nodes
    # PoolCurrentTotalNodes = poolInfo.current_dedicated_nodes + poolInfo.current_low_priority_nodes
    # PoolAllocationState = poolInfo.allocation_state.value
    # PoolState = poolInfo.state.value
    
    # print(PoolTargetTotalNodes)
    # print(PoolCurrentTotalNodes)
    # print(PoolAllocationState)
    # print(PoolState)
    
    # print(type(poolInfo.allocation_state.value))

    attemptCnt = 0
    startTime = time.time()
    while True:
        attemptCnt += 1
        PoolReadyTotalNodes = 0
        nodes = list(batch_client.compute_node.list(NodeSpecDict['PoolId']))
        for node in nodes:
            # https://learn.microsoft.com/en-us/python/api/azure-batch/azure.batch.models.computenodestate?view=azure-python
            if node.state.value in ['idle', 'running']:
                PoolReadyTotalNodes += 1
        print(f'{NodeSpecDict["PoolId"]} - waiting for nodes to start... total duration - {int(time.time() - startTime)} seconds - {PoolReadyTotalNodes} out of {PoolTargetTotalNodes} are ready - check count - {attemptCnt}')
        if PoolReadyTotalNodes == PoolTargetTotalNodes:
            break
        else:
            while True:
                if int(NodeSpecDict['NodeNum']*4) == int(time.time()%(NodeSpecDict['NumberOfNodes']*4)):
                    print(f'{NodeSpecDict["PoolId"]} - Attempting to reboot failed nodes.')
                    reboot_nodes_with_startup_error(NodeSpecDict=NodeSpecDict)
                else:
                    # Every 60 seconds, break and check if all nodes are ready
                    if int(time.time()%60) == 59:
                        break
                    else:
                        time.sleep(1-(time.time()%1)) #sleep until the next nearest second
                
            # time.sleep(60-(time.time()%60)) #sleep until the nearest minute

    time.sleep(60-(time.time()%60)) #sleep until the nearest minute


if __name__ == '__main__':

    os_path_base = os.path.abspath(os.path.join(__file__, "../../.."))

    config_global = Helpers.TomlHelper.read_toml_file(FileName=os.path.join(os_path_base, 'config_global_local.toml'))

    config_user = Helpers.TomlHelper.read_toml_file(FileName=os.path.join(os_path_base, config_global['DataGeneration']['ConfigFilePath']))

    node_spec_dict = Batch.DetermineNodes.get_batch_specs(TargetThroughput=config_user['GeneratorInput']['ThroughputMessagesPerSec'], JsonFilePath=config_user['GeneratorInput']['JsonTemplate'])
    

    NodeSpecDict = {'EventHubConnection': config_user['AzureEventHub']['EventHubConnection']
            ,'EventHubName': config_user['AzureEventHub']['EventHubName']
            ,'RunDurationMin': config_user['GeneratorInput']['RunDurationMin']
            ,'NumberOfNodes': node_spec_dict['NumberOfNodes']
            # ,'NodeNum': nodeSpec['NodeNum']
            # ,'NodeSec': nodeSpec['NodeSec']
            # ,'NodeThroughput': nodeSpec['NodeThroughput']
            ,'PayloadDefinitionDict': node_spec_dict['PayloadDefinitionDict']
            ,'BatchAccountKey': config_user['AzureBatch']['BatchAccountKey']
            ,'BatchAccountName': config_user['AzureBatch']['BatchAccountName']
            ,'BatchServiceUrl': config_user['AzureBatch']['BatchServiceUrl']
            ,'PoolId': 'pl-lolt-STANDARD_A2_V2-4-1'
            }
    
    # wait_until_pool_is_ready_state(NodeSpecDict)


# import os
# print(os.environ.get('PYTHONPATH', '').split(os.pathsep))

from azure.batch import BatchServiceClient
from azure.batch.batch_auth import SharedKeyCredentials
import azure.batch.models as batchmodels
import time
import src.Helpers.TomlHelper
import os
import src.Batch.DetermineNodes
import json

def get_node_list(NodeSpecDict: dict) -> None:
    # nodes = list(batch_client.compute_node.list(pool.id))
    pass

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
    # PoolTargetTotalNodes = poolInfo.target_dedicated_nodes + poolInfo.target_low_priority_nodes
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
    while poolInfo.state.value != 'active' and poolInfo.allocation_state.value != 'steady':
        PoolTargetTotalNodes = poolInfo.target_dedicated_nodes + poolInfo.target_low_priority_nodes
        PoolCurrentTotalNodes = poolInfo.current_dedicated_nodes + poolInfo.current_low_priority_nodes
        print(f'{NodeSpecDict["PoolId"]} - waiting for nodes to start... total duration - {int(time.time() - startTime)} seconds - {PoolCurrentTotalNodes} out of {PoolTargetTotalNodes} are ready - check count - {attemptCnt}')
        time.sleep(60)


if __name__ == '__main__':

    os_path_base = os.path.abspath(os.path.join(__file__, "../../.."))

    config_global = src.Helpers.TomlHelper.read_toml_file(FileName=os.path.join(os_path_base, 'config_global.toml'))

    config_user = src.Helpers.TomlHelper.read_toml_file(FileName=os.path.join(os_path_base, config_global['DataGeneration']['ConfigFilePath']))

    node_spec_dict = src.Batch.DetermineNodes.get_batch_specs(TargetThroughput=config_user['GeneratorInput']['ThroughputMessagesPerSec'], JsonFilePath=config_user['GeneratorInput']['JsonTemplate'])
    

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
            ,'PoolId': 'pl-ev2d-STANDARD_A2_V2-4-1'
            }
    
    wait_until_pool_is_ready_state(NodeSpecDict)


# import os
# print(os.environ.get('PYTHONPATH', '').split(os.pathsep))

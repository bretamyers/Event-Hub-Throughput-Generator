import datetime
import os
from random import sample
import math
import time 
import tomllib, json

from azure.batch import BatchServiceClient
from azure.batch.batch_auth import SharedKeyCredentials
import azure.batch.models as batchmodels
import azure.batch.operations as batchoperations
import common.helpers
import DetermineNodes

def execute_sample(global_config: dict):
    """Executes the sample with the specified configurations.
    :param global_config: The global configuration to use.
    """
    # Set up the configuration
    batch_account_key = global_config['AzureBatch']['BatchAccountKey']
    batch_account_name = global_config['AzureBatch']['BatchAccountName']
    batch_service_url = global_config['AzureBatch']['BatchServiceUrl']
    pool_name = global_config['AzureBatch']['PoolNameBase']

    node_spec_dict = DetermineNodes.get_batch_specs(global_config['AzureBatch']['ThroughputMessagesPerSec'])
    task_slots_per_task = global_config['AzureBatch']['TaskSlotsPerTask']
    pool_vm_sku = global_config['AzureBatch']['PoolVMSku']
    pool_vm_spot_count = 0
    pool_vm_dedicated_count = node_spec_dict['NumberOfNodes']
    pool_vm_count = node_spec_dict['NumberOfNodes'] + pool_vm_spot_count
    node_throughput_per_sec = node_spec_dict['NodeThroughput']
    node_spec_dict['EventHubConnection'] = global_config['AzureBatch']['EventHubConnection']
    node_spec_dict['EventHubName'] = global_config['AzureBatch']['EventHubName']

    python_run_file = global_config['PythonCommands']['PythonRunFilePath']

    # Print the settings we are running with
    print(json.dumps(global_config, indent=4))

    credentials = SharedKeyCredentials(
        batch_account_name,
        batch_account_key)

    batch_client = BatchServiceClient(
        credentials,
        batch_url=batch_service_url)

    # Retry 5 times -- default is 3
    batch_client.config.retry_policy.retries = 5

    vm_config = batchmodels.VirtualMachineConfiguration(
        image_reference=batchmodels.ImageReference(
            publisher="microsoft-azure-batch",
            offer="ubuntu-server-container",
            sku="20-04-lts"
        ),
        node_agent_sku_id="batch.node.ubuntu 20.04"
    )
    
    #https://docs.microsoft.com/en-us/azure/batch/quick-run-python
    my_pool_id = f'{pool_name}-{pool_vm_sku}-{pool_vm_count}-{task_slots_per_task}'[:64] #limited to 64 characters

    user_admin = batchmodels.UserIdentity(
        auto_user=batchmodels.AutoUserSpecification(
            elevation_level=batchmodels.ElevationLevel.admin,
            scope=batchmodels.AutoUserScope.pool) #batchmodels.AutoUserScope.task
        )

    if not batch_client.pool.exists(pool_id=my_pool_id):
        #https://docs.microsoft.com/en-us/azure/batch/batch-user-accounts
        new_pool = batchmodels.PoolAddParameter(
            id=my_pool_id,
            virtual_machine_configuration=vm_config,
            vm_size=pool_vm_sku,
            target_dedicated_nodes=pool_vm_dedicated_count,
            target_low_priority_nodes=pool_vm_spot_count,
            task_slots_per_node=task_slots_per_task, #added to run task in parallel on a node
            start_task=batchmodels.StartTask(
                user_identity=user_admin,
                max_task_retry_count=2,
                command_line=common.helpers.wrap_commands_in_shell(
                    'linux', commands = global_config['PythonCommands']['VMSetup']['commands']
                    ) 
                )
            )
        batch_client.pool.add(new_pool)

    # Code used to monitor and reboot a node if it fails to start
    # This is here due to a locking issue with ubuntu when to update 
    # and install the required applications.
    pool = batch_client.pool.get(my_pool_id)
    startTime = time.time()
    nodeReadyCnt = 0
    retryCnt = 2
    attemptCnt = 0
    while int(nodeReadyCnt) < int(pool_vm_count) and retryCnt < attemptCnt:
        nodes = list(batch_client.compute_node.list(pool.id))
        nodeReadyCnt = 0
        for node in nodes:
            if node.state in batchmodels.ComputeNodeState.start_task_failed:            
                print(f'Node Rebooting - {node.id} - {int(time.time() - startTime)}')
                batch_client.compute_node.reboot(pool_id=my_pool_id, node_id=node.id)
            #https://docs.microsoft.com/en-us/python/api/azure-batch/azure.batch.models.computenodestate?view=azure-python
            if node.state in [batchmodels.ComputeNodeState.idle, batchmodels.ComputeNodeState.running]:
                nodeReadyCnt += 1
        print(f'{my_pool_id} - waiting for nodes to start... total duration - {int(time.time() - startTime)} seconds - {nodeReadyCnt} out of {pool_vm_count} are ready')
        if int(nodeReadyCnt) < int(pool_vm_count):
            attemptCnt += 1
            time.sleep(30)

        
    pool_info = batchmodels.PoolInformation(pool_id=my_pool_id)

    job_id = common.helpers.generate_unique_resource_name(f"{my_pool_id}-{python_run_file.split('/')[-1].split('.py')[0]}")[:64]
    print(f'Adding Job job_id={job_id}')
    job = batchmodels.JobAddParameter(
        id=job_id
        ,pool_info=pool_info
        # A task that runs before other tasks that downloads the github artifacts
        ,job_preparation_task=batchmodels.JobPreparationTask( 
            id='JobPreparationTask_DownloadGithubArtifacts'
            ,user_identity=user_admin
            ,command_line=common.helpers.wrap_commands_in_shell(
                'linux', commands = global_config['PythonCommands']['CodeSetup']['commands']
                )
        )
    )
    batch_client.job.add(job)

    python_run_file_path = global_config['PythonCommands']['PythonRunFilePath']
    batch_add_app_tasks(batch_client, job_id, pool_vm_count, task_slots_per_task, python_run_file_path)


def batch_add_app_tasks(batch_client, job_id, pool_vm_count, task_slots_per_task, python_run_file_path):

    print(f'Adding Tasks to Job job_id={job_id}')
    tasks = list()
    # https://docs.microsoft.com/en-us/python/api/azure-batch/azure.batch.models?view=azure-python
    for idx in range(2004, 2023):
        tasks.append(batchmodels.TaskAddParameter(
            id=f'Task-{python_run_file_path[:-3]}-{str(idx).zfill(2)}',
            # command_line=f"/bin/bash -c \'set -e; set -o pipefail; echo \"test-{str(idx).zfill(2)}\"; wait\'"
            command_line=f"""/bin/bash -c 'PYTHONPATH=/mnt/batch/tasks/shared/StockAPIParser/StockAPIParser-main python3.11 /mnt/batch/tasks/shared/StockAPIParser/StockAPIParser-main/{python_run_file_path} {idx}
                '"""
            # ,constraints=batchmodels.TaskConstraints(max_task_retry_count=3)
            )
        )
        # break
    batch_client.task.add_collection(job_id, tasks)


if __name__ == '__main__':

    with open('main/config.toml', 'rb') as f:
        config = tomllib.load(f)
        print(json.dumps(config, indent=4))
    
    execute_sample(config)

    

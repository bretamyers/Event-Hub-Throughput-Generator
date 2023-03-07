import json, time, os, random, string
from azure.batch import BatchServiceClient
from azure.batch.batch_auth import SharedKeyCredentials
import azure.batch.models as batchmodels
import azure.batch.operations as batchoperations
import Batch.common.helpers
import Batch.DetermineNodes 
import Helpers.TomlHelper

def execute_batch_build(config_user:dict, config_global:dict, node_spec_dict:dict) -> None:
    """Executes the sample with the specified configurations.
    :param config_user: The user configuration to use.
    :param config_global: The global configuration to use.
    :param node_spec_dict: The node spec dictionary that contains the info on the request like on how many nodes need to be created.
    """
    # Set up the configuration
    batch_account_key = config_user['AzureBatch']['BatchAccountKey']
    batch_account_name = config_user['AzureBatch']['BatchAccountName']
    batch_service_url = config_user['AzureBatch']['BatchServiceUrl']
    pool_name = config_global['AzureBatch']['PoolNameBase']
    unique_pool_name_flag = config_global['AzureBatch']['UniquePoolNameFlag']
    drop_pool_on_completion_flag = config_global['AzureBatch']['DropPoolOnCompletionFlag']
    pool_os_publisher = config_global['AzureBatch']['Publisher']
    pool_os_offer = config_global['AzureBatch']['Offer']
    pool_os_sku = config_global['AzureBatch']['Sku']


    node_spec_dict = Batch.DetermineNodes.get_batch_specs(config_user['GeneratorInput']['ThroughputMessagesPerSec'])
    task_slots_per_task = config_global['AzureBatch']['TaskSlotsPerTask']
    pool_vm_sku = config_global['AzureBatch']['PoolVMSku']
    pool_vm_spot_count = 0
    pool_vm_dedicated_count = node_spec_dict['NumberOfNodes']
    pool_vm_count = node_spec_dict['NumberOfNodes'] + pool_vm_spot_count
    node_spec_dict['EventHubConnection'] = config_user['AzureEventHub']['EventHubConnection']
    node_spec_dict['EventHubName'] = config_user['AzureEventHub']['EventHubName']

    python_run_file = config_global['PythonCommands']['PythonProgramFilePath']

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
            publisher=pool_os_publisher,
            offer=pool_os_offer,
            sku=pool_os_sku
        ),
        node_agent_sku_id="batch.node.ubuntu 20.04"
    )
    
    #https://docs.microsoft.com/en-us/azure/batch/quick-run-python
    my_pool_id = f'{pool_name}-{"".join(random.choices(string.ascii_lowercase + string.digits, k=4)) if unique_pool_name_flag == "true" else ""}-{pool_vm_sku}-{pool_vm_count}-{task_slots_per_task}'[:64] #limited to 64 characters

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
                command_line=Batch.common.helpers.wrap_commands_in_shell(
                    'linux', commands = config_global['PythonCommands']['NodeSetup']['commands']
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

    job_id = Batch.common.helpers.generate_unique_resource_name(f"{my_pool_id}-{python_run_file.split('/')[-1].split('.py')[0]}")[:64]
    print(f'Adding Job job_id={job_id}')
    job = batchmodels.JobAddParameter(
        id=job_id
        ,pool_info=pool_info
        # A task that runs before other tasks that downloads the github artifacts
        ,job_preparation_task=batchmodels.JobPreparationTask( 
            id='JobPreparationTask-NodePreparation'
            ,user_identity=user_admin
            ,command_line=Batch.common.helpers.wrap_commands_in_shell(
                'linux', commands = config_global['PythonCommands']['CodeSetup']['commands']
                )
        )
        ,on_all_tasks_complete=batchmodels.OnAllTasksComplete.terminate_job
        ,job_release_task=batchmodels.JobReleaseTask(
            id=f'JobReleaseTask-DeletePool-{my_pool_id}'
            ,command_line=f"""/bin/bash -c 'PYTHONPATH={config_global['PythonCommands']['PythonRepoPath']} python3.11 {config_global['PythonCommands']['PythonRepoPath']}/main/BatchDropPool.py \"{my_pool_id}\" \"{batch_account_key}\" \"{batch_account_name}\" \"{batch_service_url}\"
                '""" if drop_pool_on_completion_flag == 'true' else f"""/bin/bash echo ''"""
         ) 
    )
    batch_client.job.add(job)

    #Add tasks to job to generate the data
    python_run_file_path = config_global['PythonCommands']['PythonProgramFilePath']
    batch_add_app_tasks(batch_client, job_id, python_run_file_path, config_global, config_user, node_spec_dict)

    # # time.sleep(1)

    # #Add job to delete the pool
    # job_id = Batch.common.helpers.generate_unique_resource_name(f"{my_pool_id}-DROP-{python_run_file.split('/')[-1].split('.py')[0]}")[:64]
    # print(f'Adding Job job_id={job_id}')
    # job = batchmodels.JobAddParameter(
    #     id=job_id
    #     ,pool_info=pool_info
    #     # A task that runs before other tasks that downloads the github artifacts
    #     ,job_preparation_task=batchmodels.JobPreparationTask( 
    #         id='JobPreparationTask_DeletePool'
    #         ,user_identity=user_admin
    #         ,command_line=f"""/bin/bash echo 'Delete Job'"""
    #     )
    #     ,on_all_tasks_complete=batchmodels.OnAllTasksComplete.terminate_job
    # )
    # batch_client.job.add(job)
    # task = batchmodels.TaskAddParameter(
    #         id=f'Task-Delete-Pool-{my_pool_id}',
    #         command_line=f"""/bin/bash -c 'PYTHONPATH=/mnt/batch/tasks/shared/EventHub-Throughput-Generator/EventHub-Throughput-Generator-main python3.11 /mnt/batch/tasks/shared/EventHub-Throughput-Generator/EventHub-Throughput-Generator-main/main/BatchDropPool.py \"{my_pool_id}\"
    #             '"""
    #         )
    # batch_client.task.add(job_id=job_id, task=task)

    
def batch_add_app_tasks(batch_client, job_id, python_run_file_path, config_global, config_user, node_spec_dict):

    print(f'Adding Tasks to Job job_id={job_id}')
    tasks = list()
    # https://docs.microsoft.com/en-us/python/api/azure-batch/azure.batch.models?view=azure-python
    for nodeSpec in node_spec_dict['NodeMessageSpecList']:
        NodeSpecDict = {'EventHubConnection': config_user['AzureEventHub']['EventHubConnection']
            ,'EventHubName': config_user['AzureEventHub']['EventHubName']
            ,'RunDurationMin': config_user['GeneratorInput']['RunDurationMin']
            ,'NumberOfNodes': node_spec_dict['NumberOfNodes']
            ,'NodeNum': nodeSpec['NodeNum']
            ,'NodeSec': nodeSpec['NodeSec']
            ,'NodeThroughput': nodeSpec['NodeThroughput']
            ,'PayloadDefinitionDict': node_spec_dict['PayloadDefinitionDict']
            }
        tasks.append(batchmodels.TaskAddParameter(
            id=f'Task-{str(nodeSpec["NodeNum"]).zfill(4)}',
            command_line=f"""PYTHONPATH={config_global['PythonCommands']['PythonRepoPath']} python3.11 {config_global['PythonCommands']['PythonRepoPath']}/{python_run_file_path} '{json.dumps(NodeSpecDict)}' """
            )
        )
    batch_client.task.add_collection(job_id, tasks)


if __name__ == '__main__':

    os_path_base = os.path.split(os.path.join(os.path.dirname(os.path.abspath(__file__))))[0]

    config_global = Helpers.TomlHelper.read_toml_file(FileName=os.path.join(os_path_base, 'config_global.toml'))

    config_user = Helpers.TomlHelper.read_toml_file(FileName=os.path.join(os_path_base, config_global['DataGeneration']['ConfigFilePath']))

    node_spec_dict = Batch.DetermineNodes.get_batch_specs(TargetThroughput=config_user['GeneratorInput']['ThroughputMessagesPerSec'], JsonFilePath=config_user['GeneratorInput']['JsonTemplate'])
    
    
    print(json.dumps(config_global, indent=4))
    print(json.dumps(config_user, indent=4))
    print(json.dumps(node_spec_dict, indent=4))

    execute_batch_build(config_user=config_user, config_global=config_global, node_spec_dict=node_spec_dict)


    


from azure.batch import BatchServiceClient
from azure.batch.batch_auth import SharedKeyCredentials
import azure.batch.models as batchmodels
import TomlHelper
import sys, os, json


def batch_drop_pool(batch_client:BatchServiceClient, pool_id) -> None:
    batch_client.pool.delete(pool_id=pool_id)


def batch_list_jobs(batch_client:BatchServiceClient, pool_id) -> None:
    return batch_client.job.list(batchmodels.JobListOptions(filter=f"startswith(id,'{pool_id}')"))
    

def batch_delete_pool_jobs(batch_client:BatchServiceClient, pool_id) -> None:
    jobList = batch_list_jobs(batch_client=batch_client, pool_id=pool_id)
    for job in jobList:
        batch_client.job.delete(job_id=job.as_dict()['id'])


if __name__ == '__main__':

    # pool_id = sys.argv[1]
    # print(f'{pool_id}')

    os_path_base = os.path.split(os.path.join(os.path.dirname(os.path.abspath(__file__))))[0]
    config_global = TomlHelper.read_toml_file(FileName=os.path.join(os_path_base, 'config_global.toml'))
    config_user = TomlHelper.read_toml_file(FileName=os.path.join(os_path_base, config_global['DataGeneration']['ConfigFilePath']))
    print(json.dumps(config_user, indent=4))

    batch_account_key = config_user['AzureBatch']['BatchAccountKey']
    batch_account_name = config_user['AzureBatch']['BatchAccountName']
    batch_service_url = config_user['AzureBatch']['BatchServiceUrl']
    
    credentials = SharedKeyCredentials(
        batch_account_name,
        batch_account_key)

    batch_client = BatchServiceClient(
        credentials,
        batch_url=batch_service_url)

    # batch_drop_pool(batch_client=batch_client, pool_id=pool_id)
    # batch_list_jobs(batch_client=batch_client, pool_id=pool_id)
    # batch_delete_pool_jobs(batch_client=batch_client, pool_id=pool_id)

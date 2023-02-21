
from azure.batch import BatchServiceClient
from azure.batch.batch_auth import SharedKeyCredentials
import azure.batch.models as batchmodels
import azure.batch.operations as batchoperations
import common.helpers
# from main import TomlHelper
import sys, os, json, tomli


def batch_drop_pool(batch_client:BatchServiceClient, pool_id) -> None:
    batch_client.pool.delete(pool_id=pool_id)


def batch_list_jobs(batch_client:BatchServiceClient, pool_id) -> None:
    return batch_client.job.list(batchmodels.JobListOptions(filter=f"startswith(id,'{pool_id}')"))
    

def batch_delete_pool_jobs(batch_client:BatchServiceClient, pool_id) -> None:
    jobList = batch_list_jobs(batch_client=batch_client, pool_id=pool_id)
    for job in jobList:
        # print(job)
        batch_client.job.delete(job_id=job.as_dict()['id'])


if __name__ == '__main__':

    # pool_id = sys.argv[1]
    pool_id = 'pools-STANDARD_A2_V2-4-1'
    print(f'{pool_id}')

    # with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config_user_local.toml'), 'rb') as f:
    #     config_user = tomli.load(f)
    # config_user = TomlHelper.read_toml_file(FileName=os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config_user.toml'))

    # BatchAccountName = 'batchbam'
    # BatchAccountKey = 'gUsh/iFf+imtxVCbgGHbCCte0pUyoEH75M3sVDPsfIXQ3go0GhMadBJiDNZHyGWs3AME4HCaZVLh+ABaDwzw2g=='
    # BatchServiceUrl = 'https://batchbam.eastus2.batch.azure.com'

    batch_account_key = 'gUsh/iFf+imtxVCbgGHbCCte0pUyoEH75M3sVDPsfIXQ3go0GhMadBJiDNZHyGWs3AME4HCaZVLh+ABaDwzw2g==' #config_user['AzureBatch']['BatchAccountKey']
    batch_account_name = 'batchbam' #config_user['AzureBatch']['BatchAccountName']
    batch_service_url = 'https://batchbam.eastus2.batch.azure.com' #config_user['AzureBatch']['BatchServiceUrl']
    
    credentials = SharedKeyCredentials(
        batch_account_name,
        batch_account_key)

    batch_client = BatchServiceClient(
        credentials,
        batch_url=batch_service_url)

    # batch_drop_pool(batch_client=batch_client, pool_id=pool_id)
    # batch_list_jobs(batch_client=batch_client, pool_id=pool_id)
    batch_delete_pool_jobs(batch_client=batch_client, pool_id=pool_id)

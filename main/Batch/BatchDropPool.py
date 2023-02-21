
from azure.batch import BatchServiceClient
from azure.batch.batch_auth import SharedKeyCredentials
import azure.batch.models as batchmodels
import azure.batch.operations as batchoperations
import Batch.common.helpers
import main.TomlHelper as TomlHelper
import sys, os, json


def batch_drop_pool(batch_client:BatchServiceClient, pool_id) -> None:
    batch_client.pool.delete(pool_id=pool_id)


def batch_list_jobs(batch_client:BatchServiceClient, pool_id) -> None:
    
    jobList = batch_client.job.list(batchmodels.JobListOptions(filter=f"startswith(id,'{pool_id}')"))
    for job in jobList:
        batch_client.job.delete(job_id=job.as_dict()['id'])

if __name__ == '__main__':

    pool_id = sys.argv[1]
    # pool_id = 'pools-STANDARD_A2_V2-4-1'
    print(f'{pool_id}')

    config_user = TomlHelper.read_toml_file(FileName=os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config_user.toml'))

    batch_account_key = config_user['AzureBatch']['BatchAccountKey']
    batch_account_name = config_user['AzureBatch']['BatchAccountName']
    batch_service_url = config_user['AzureBatch']['BatchServiceUrl']
    
    credentials = SharedKeyCredentials(
        batch_account_name,
        batch_account_key)

    batch_client = BatchServiceClient(
        credentials,
        batch_url=batch_service_url)

    batch_drop_pool(batch_client=batch_client, pool_id=pool_id)
    # batch_list_jobs(batch_client=batch_client, pool_id=pool_id)
    

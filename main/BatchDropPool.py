
from azure.batch import BatchServiceClient
from azure.batch.batch_auth import SharedKeyCredentials
import azure.batch.models as batchmodels
import azure.batch.operations as batchoperations
import common.helpers
import TomlHelper
import sys


def batch_drop_pool(batch_client:BatchServiceClient, pool_id) -> None:
    batch_client.pool.delete(pool_id=pool_id)


if __name__ == '__main__':

    pool_id = sys.argv[1]

    config_user = TomlHelper.read_toml_file('main/config_user.toml')

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

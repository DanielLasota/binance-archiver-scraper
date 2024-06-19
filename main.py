from data_scraper import download_data
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
from dotenv import load_dotenv
import os

load_dotenv('C:/Users/daniellasota/scraper.env')

client = SecretClient(
    vault_url=os.environ.get('VAULT_URL'),
    credential=DefaultAzureCredential()
)

blob_parameters_secret_name = os.environ.get('AZURE_BLOB_PARAMETERS_WITH_KEY_SECRET_NAME')
container_name_secret_name = os.environ.get('CONTAINER_NAME_SECRET_NAME')
blob_connection_string = client.get_secret(blob_parameters_secret_name).value
container_name = client.get_secret(container_name_secret_name).value

download_data(
    '15-06-2024T12:00:00Z',
    '15-06-2024T13:00:00Z',
    blob_connection_string=blob_connection_string,
    container_name=container_name,
    file_duration_seconds=300
)

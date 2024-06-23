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
    download_directory='C:/Users/daniellasota/binance_data_main/',
    from_date='21-06-2024',
    to_date='21-06-2024',
    pairs=['BTCUSDT'],
    # markets=['SPOT', 'USD_M_FUTURES'],
    markets=['SPOT'],
    # stream_types=['ORDERBOOK', 'TRANSACTIONS', 'ORDERBOOK_SNAPSHOT'],
    stream_types=['DIFFERENCE_DEPTH'],
    blob_connection_string=blob_connection_string,
    container_name=container_name,
    single_file_duration_seconds=300,
    save_raw_jsons=False
)

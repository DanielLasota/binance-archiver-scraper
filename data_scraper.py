from azure.storage.blob import BlobServiceClient
import json
from datetime import datetime, timedelta


class DataScaper:
    def __init__(self):
        ...

    @staticmethod
    def _filter_files_by_date(file_list, from_date, to_date, file_duration_seconds: int = 60*30):
        from_date = (datetime.strptime(from_date, '%d-%m-%YT%H:%M:%SZ')
                     - timedelta(seconds=file_duration_seconds + 10))
        to_date = datetime.strptime(to_date, '%d-%m-%YT%H:%M:%SZ')

        filtered_files = []

        for file in file_list:
            date_str = file.split('_')[-1].replace('.json.zip', '')
            file_date = datetime.strptime(date_str, '%d-%m-%YT%H-%M-%SZ')

            if from_date <= file_date <= to_date:
                filtered_files.append(file)

        return filtered_files

    def download_data(self, datetime_from: str, datetime_to: str, blob_connection_string: str, container_name: str,
                      file_duration_seconds: int | None = 60*30) -> None:

        blob_service_client = BlobServiceClient.from_connection_string(blob_connection_string)

        container_client = blob_service_client.get_container_client(container_name)

        prefix = 'binance_l2lob_delta_broadcast_spot_btcusdt'

        blob_list = container_client.list_blobs(name_starts_with=prefix)

        filtered_blobs = [blob.name for blob in blob_list]

        for _ in filtered_blobs: print(_)

        filtered_blobs = self._filter_files_by_date(filtered_blobs, from_date=datetime_from, to_date=datetime_to,
                                                    file_duration_seconds=file_duration_seconds)

        print('sorted:')
        for _ in filtered_blobs: print(_)


def download_data(datetime_from: str, datetime_to: str, blob_connection_string: str, container_name: str,
                  file_duration_seconds: int | None = 60*30) -> None:
    data_scraper = DataScaper()

    data_scraper.download_data(datetime_from, datetime_to, blob_connection_string, container_name, file_duration_seconds)

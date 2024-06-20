from typing import List

from azure.storage.blob import BlobServiceClient
from datetime import datetime, timedelta
from market_enum import Market
from stream_type_enum import StreamType


class DataScaper:
    def __init__(self):
        ...

    @staticmethod
    def _return_date_list(from_: str, to: str) -> List[str]:
        from_date = datetime.strptime(from_, '%d-%m-%Y')
        to_date = datetime.strptime(to, '%d-%m-%Y')

        date_list = []
        current_date = from_date
        while current_date <= to_date:
            date_list.append(current_date.strftime('%d-%m-%Y'))
            current_date += timedelta(days=1)

        return date_list

    @staticmethod
    def _filter_files_by_date(file_list, from_date, to_date, file_duration_seconds: int = 60 * 30):
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

    @staticmethod
    def _get_file_name_prefix(instrument: str, market: Market, stream_type: StreamType) -> str:
        market_mapping = {
            Market.SPOT: 'spot',
            Market.USD_M_FUTURES: 'futures_usd_m',
            Market.COIN_M_FUTURES: 'futures_coin_m'
        }

        data_type_mapping = {
            StreamType.ORDERBOOK: 'binance_l2lob_delta_broadcast',
            StreamType.ORDERBOOK_SNAPSHOT: 'binance_l2lob_snapshot',
            StreamType.TRANSACTIONS: 'binance_transaction_broadcast'
        }

        market_short_name = market_mapping.get(market, 'unknown_market')
        prefix = data_type_mapping.get(stream_type, 'unknown_data_type')

        return f'{prefix}_{market_short_name}_{instrument}'

    def get_blob_list(self, container_client, from_date: str, to_date: str, market: Market,
                      stream_type: StreamType) -> List[str]:

        prefix = self._get_file_name_prefix('btcusdt', market, stream_type)

        blob_list = []

        for date in self._return_date_list(from_date.split('T')[0], to_date.split('T')[0]):
            single_day_blob_list = container_client.list_blobs(name_starts_with=f'{prefix}_{date}')
            single_day_blob_list = [blob.name for blob in single_day_blob_list]
            for _ in single_day_blob_list:
                blob_list.append(_)

        filtered_blobs = self._filter_files_by_date(blob_list, from_date=from_date, to_date=to_date)

        for _ in filtered_blobs: print(_)

        return filtered_blobs

    def download_data(self, from_date: str, to_date: str, market: Market, stream_type: StreamType,
                      blob_connection_string: str, container_name: str,
                      file_duration_seconds: int | None = 60 * 30) -> None:

        blob_service_client = BlobServiceClient.from_connection_string(blob_connection_string)

        container_client = blob_service_client.get_container_client(container_name)

        query_result = self.get_blob_list(container_client, from_date, to_date, market, stream_type)
        for _ in query_result: print(_)


def download_data(from_date: str, to_date: str, blob_connection_string: str, container_name: str, market: str,
                  stream_type: str, file_duration_seconds: int | None = 60 * 30) -> None:
    data_scraper = DataScaper()

    market = Market[market.upper()]
    stream_type = StreamType[stream_type.upper()]

    data_scraper.download_data(from_date=from_date,
                               to_date=to_date,
                               market=market,
                               stream_type=stream_type,
                               blob_connection_string=blob_connection_string,
                               container_name=container_name,
                               file_duration_seconds=file_duration_seconds
                               )

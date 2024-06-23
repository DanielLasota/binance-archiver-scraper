from typing import List, Callable
import os
import io
import pprint
import zipfile
import json
import threading
from azure.storage.blob import BlobServiceClient
from datetime import datetime, timedelta
from market_enum import Market
from stream_type_enum import StreamType
import time
import pandas as pd


class DataScraper:
    def __init__(
            self,
            blob_connection_string: str,
            container_name: str,
            single_file_duration_seconds: int,
            save_raw_jsons_indicator: bool = False
    ):
        self.blob_service_client = BlobServiceClient.from_connection_string(blob_connection_string)
        self.container_client = self.blob_service_client.get_container_client(container_name)
        self.container_name = container_name
        self.save_raw_jsons_indicator = save_raw_jsons_indicator
        self.single_file_duration_seconds = single_file_duration_seconds
        self.threads = []

    def download_daemon(self, download_directory: str, from_date: str, to_date: str, pair: str,
                        market: Market, stream_type: StreamType) -> None:

        stream_processors: dict[StreamType, Callable[[List[str], str], pd.DataFrame]] = {
            StreamType.DIFFERENCE_DEPTH: self.process_difference_depth_data,
            StreamType.TRADE: self.process_trade_data,
            StreamType.DEPTH_SNAPSHOT: self.process_depth_snapshot_data
        }
        processor = stream_processors.get(stream_type)

        for target_date in self._return_date_list(from_date, to_date):
            desired_blob_list = self.list_blob_for_specified_day(pair, target_date, market, stream_type)
            exact_directory = self._get_exact_download_directory(download_directory, market, stream_type, pair)

            print(f'desired_blob_list for: {target_date}')
            pprint.pprint(desired_blob_list)

            df = processor(desired_blob_list, exact_directory)

            print(df)

            df.to_csv(f'{exact_directory}/'
                      f'{pair}_{market.name.lower()}_{stream_type.name.lower()}_{target_date}.csv', index=False)
            df = None
            del df

    def process_depth_snapshot_data(self, blob_list: List[str], exact_directory: str) -> pd.DataFrame:
        ...

    def process_trade_data(self, blob_list: List[str], exact_directory: str) -> pd.DataFrame:
        records = []
        for file_name in blob_list:
            print(f'file_name: {file_name}')
            print(len(records))
            json_data = self.get_blob(blob_name=file_name, download_folder_path=f'{exact_directory}/raw_jsons')
            for record in json_data:
                event_time = record["data"]["E"]
                trade_id = record["data"]["t"]
                price = record["data"]["p"]
                quantity = record["data"]["q"]
                # seller_order_id = record["data"]["a"] if "a" in record["data"] else None
                # buyer_order_id = record["data"]["b"] if "b" in record["data"] else None
                trade_time = record["data"]["T"]
                is_buyer_market_maker = record["data"]["m"]
                timestamp_of_receive = record["_E"]

                records.append(
                    [
                        event_time,
                        trade_id,
                        price,
                        quantity,
                        # seller_order_id,
                        # buyer_order_id,
                        trade_time,
                        int(is_buyer_market_maker),
                        timestamp_of_receive
                    ]
                )

        print('creating dataframe...')

        columns = [
            "EventTime",
            "TradeId",
            "Price",
            "Quantity",
            "TradeTime",
            "IsBuyerMarketMaker",
            "TimestampOfReceive"
        ]

        df = pd.DataFrame(records, columns=columns)
        return df

    def process_difference_depth_data(self, blob_list: List[str], exact_directory: str) -> pd.DataFrame:
        records = []
        for file_name in blob_list:
            print(f'file_name: {file_name}')
            print(len(records))
            json_data = self.get_blob(blob_name=file_name, download_folder_path=f'{exact_directory}/raw_jsons')

            for record in json_data:
                event_time = record["data"]["E"]
                first_update = record["data"]["U"]
                final_update = record["data"]["u"]
                bids = record["data"]["b"]
                asks = record["data"]["a"]
                timestamp_of_receive = record["_E"]

                for bid in bids:
                    records.append([
                        event_time,
                        0,
                        float(bid[0]),
                        float(bid[1]),
                        timestamp_of_receive,
                        first_update,
                        final_update
                    ])

                for ask in asks:
                    records.append([
                        event_time,
                        1,
                        float(ask[0]),
                        float(ask[1]),
                        timestamp_of_receive,
                        first_update,
                        final_update
                    ])
            print(len(records))

        print('creating dataframe...')

        columns = [
            "EventTime",
            "IsAsk",
            "Price",
            "Quantity",
            "TimestampOfReceive",
            "FirstUpdate",
            "FinalUpdate"
        ]

        df = pd.DataFrame(records, columns=columns)
        return df

    def get_blob(self, blob_name: str, download_folder_path: str | None = None) -> dict:

        blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=blob_name)
        blob_data = blob_client.download_blob().readall()

        if self.save_raw_jsons_indicator is True:
            if download_folder_path is None:
                raise Exception('download_file_path= in .download_and_extract_json is not specified, cannot save file')
            else:
                if not os.path.exists(f'{download_folder_path}/raw_data'):
                    os.makedirs(f'{download_folder_path}/raw_data')
                download_file_path = f'{download_folder_path}/raw_data/{blob_name}'
                with open(download_file_path, "wb") as download_file:
                    download_file.write(blob_data)

        with zipfile.ZipFile(io.BytesIO(blob_data)) as z:
            for file_name in z.namelist():
                with z.open(file_name) as json_file:
                    json_file = json.load(json_file)
                    return json_file

    def list_blob_for_specified_day(self, pair: str, target_date: str, market: Market,
                                    stream_type: StreamType) -> List[str]:

        prefix = self._get_file_name_prefix(pair, market, stream_type)
        from_date_minus_one_day = self.str_date_timedelta(target_date, timedelta(days=-1))

        blob_list = []

        for date in self._return_date_list(from_date_minus_one_day, target_date):
            single_day_blob_list = self.container_client.list_blobs(name_starts_with=f'{prefix}_{date}')
            single_day_blob_list = [blob.name for blob in single_day_blob_list]

            for _ in single_day_blob_list:
                blob_list.append(_)

        filtered_blob_list = self._filter_files_to_one_day(blob_list, target_date)
        return filtered_blob_list

    def _filter_files_to_one_day(self, file_list, target_date):
        from_date = (datetime.strptime(target_date, '%d-%m-%Y')
                     - timedelta(seconds=self.single_file_duration_seconds + 10))
        to_date = datetime.strptime(target_date, '%d-%m-%Y') + timedelta(days=1)

        filtered_files = []

        for file in file_list:
            date_str = file.split('_')[-1].replace('.json.zip', '')
            file_date = datetime.strptime(date_str, '%d-%m-%YT%H-%M-%SZ')

            if from_date <= file_date <= to_date:
                filtered_files.append(file)

        return filtered_files

    @staticmethod
    def _get_exact_download_directory(download_directory, market, stream_type, pair) -> str:
        # sub_market_stream_type_instrument_download_directory = (f'{download_directory}/{pair.lower()}/'
        #                                                         f'{market.name.lower()}/{stream_type.name.lower()}')

        sub_market_stream_type_instrument_download_directory = (f'{download_directory}/{pair.lower()}/'
                                                                f'{market.name.lower()}/{stream_type.name.lower()}')

        # if not os.path.exists(sub_market_stream_type_instrument_download_directory):
        #     os.makedirs(sub_market_stream_type_instrument_download_directory)

        return download_directory

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
    def _get_file_name_prefix(pair: str, market: Market, stream_type: StreamType) -> str:
        market_mapping = {
            Market.SPOT: 'spot',
            Market.USD_M_FUTURES: 'futures_usd_m',
            Market.COIN_M_FUTURES: 'futures_coin_m'
        }

        data_type_mapping = {
            StreamType.DIFFERENCE_DEPTH: 'binance_l2lob_delta_broadcast',
            StreamType.DEPTH_SNAPSHOT: 'binance_l2lob_snapshot',
            StreamType.TRADE: 'binance_transaction_broadcast'
        }

        market_short_name = market_mapping.get(market, 'unknown_market')
        prefix = data_type_mapping.get(stream_type, 'unknown_data_type')

        return f'{prefix}_{market_short_name}_{pair}'

    @staticmethod
    def str_date_timedelta(date_str: str, x: timedelta) -> str:
        date_obj = datetime.strptime(date_str, '%d-%m-%Y')

        previous_day = (date_obj + x).strftime('%d-%m-%Y')

        previous_day_str = previous_day

        return previous_day_str


def download_data(download_directory: str, from_date: str, to_date: str, blob_connection_string: str,
                  container_name: str, pairs: List[str], markets: List[str], stream_types: List[str],
                  single_file_duration_seconds: int, save_raw_jsons: bool | None = None) -> None:

    data_scraper = DataScraper(blob_connection_string=blob_connection_string, container_name=container_name,
                               single_file_duration_seconds=single_file_duration_seconds,
                               save_raw_jsons_indicator=save_raw_jsons)

    markets = [Market[_.upper()] for _ in markets]
    stream_types = [StreamType[_.upper()] for _ in stream_types]
    pairs = [pair.lower() for pair in pairs]

    for pair in pairs:
        for market in markets:
            if market == Market.COIN_M_FUTURES:
                pair = f'{pair}_perp'
            for stream_type in stream_types:
                print(f'ought to download: {from_date} {to_date} {pair} {market} {stream_type}')
                thread = threading.Thread(target=data_scraper.download_daemon,
                                          args=(download_directory, from_date, to_date, pair, market, stream_type))
                thread.start()
                data_scraper.threads.append(thread)

                # data_scraper.download_daemon(
                #     download_directory=download_directory,
                #     from_date=from_date,
                #     to_date=to_date,
                #     market=market,
                #     stream_type=stream_type,
                #     blob_connection_string=blob_connection_string,
                #     container_name=container_name,
                #     file_duration_seconds=file_duration_seconds
                #     )

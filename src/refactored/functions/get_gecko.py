#!/usr/bin/env python

"""get_gecko.py

This script contains the GetGecko class, which gathers and processes price
and contract data from the CoinGecko API for various blockchains and tokens.

The GetGecko class provides methods to fetch and save prices and contract
information for a set of blockchains and their associated tokens. The data
is fetched from the CoinGecko API and then processed and saved to Excel and
CSV files for further analysis.

The class also provides utility methods to manipulate data, such as
copying columns with new headers and filling missing blockchain data.

Usage:
    To use this script, import the GetGecko class and instantiate it,
    then call the run() method.

    Example:
        from get_gecko import GetGecko

        gecko = GetGecko()
        gecko.run()

Classes:
    GetGecko

Exceptions:
    HTTPError: An error occurred when sending a request to the CoinGecko API.
    RequestException: A general error occurred when sending a request.
"""


import time
import pandas as pd
import numpy as np
import requests


class GetGecko:
    """
    Class to interact with the CoinGecko API.

    Attributes
    ----------
    BASE_URL : str
        Base URL for the CoinGecko API.
    SLEEP_TIME : int
        Time in seconds to sleep between requests to avoid hitting rate limit.
    prices_file : str
        File path for the file where prices will be saved.
    info_file : str
        File path for the file where contract info will be saved.

    Methods
    -------
    send_request(url, params=None):
        Send a request to the specified URL with optional parameters.
    get_monthly_prices(contract_address, blockchain):
        Get the monthly prices for a given contract address on a specified
        blockchain.
    get_contract_info(contract_address, blockchain):
        Get the ticker and precision for a given contract address on a
        specified blockchain.
    fetch_and_save_prices():
        Fetch and save the monthly prices for all contracts on all blockchains
        specified in chain_info.csv.
    fetch_and_save_contract_info():
        Fetch and save the ticker and precision for all contracts on all
        blockchains specified in chain_info.csv.
    copy_columns_with_new_headers(monthly, sheet):
        Copy specified columns in a dataframe and give them new headers.
    save_prices_to_excel():
        Save the prices to an Excel file, making necessary modifications to
        column headers.
    write_sheet_to_excel(excel_file, sheet, writer):
        Write a sheet of an Excel file to a new file, making necessary
        modifications to column headers.
    get_excel_sheets():
        Get the sheets of an Excel file.
    fill_missing_blockchain_data():
        Fill missing blockchain data in the data derived from chain_info.csv.
    fill_data_and_save(merged_df, blkchn):
        Fill data in a dataframe and save it to a CSV file.
    merge_dataframes(transactions_df, contract_df):
        Merge two dataframes on the contract_address column.
    """

    FANT = '0x9879abdea01a879644185341f7af7d8343556b7a'
    OPT_ETHER = '0x4200000000000000000000000000000000000006'
    BASE_URL = "https://api.coingecko.com/api/v3"
    SLEEP_TIME = 7

    def __init__(
        self,
        prices_file='data/monthly_prices.xlsx',
        info_file='data/contract_info.csv'
    ):
        self.prices_file = prices_file
        self.info_file = info_file
        pd.set_option('display.float_format', lambda x: f'{x:.3f}')
        self.sheet_headers_mapping = {
            "arbitrum-one": [
                ("0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9", "0x2913e812cf0dcca30fb28e6cac3d2dcff4497688"),
                ("0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9", "0xe264cb5a941f98a391b9d5244378edf79bf5c19e"),
                ("0x82af49447d8a07e3bd95bd0d56f35241523fbab1", "0x3ea9b0ab55f34fb188824ee288ceaefc63cf908e")
            ],

            "avalanche": [
                ("0xd586e7f844cea2f87f50152665bcbc2c279d8d70", "0x55904f416586b5140a0f666cf5acf320adf64846"),
                ("0xd586e7f844cea2f87f50152665bcbc2c279d8d70", "0xcfc37a6ab183dd4aed08c204d1c2773c0b1bdf46")
            ],

            "binance-smart-chain": [
                ("0xe9e7cea3dedca5984780bafc599bd69add087d56", "0xf2511b5e4fb0e5e2d123004b672ba14850478c14"),
                ("0xe9e7cea3dedca5984780bafc599bd69add087d56", "0xf0b8b631145d393a767b4387d08aa09969b2dfed"),
                ("0xe9e7cea3dedca5984780bafc599bd69add087d56", "0xdd17344f7537df99f212a08f5a5480af9f6c083a"),
                ("0x7130d2a12b9bcbfae4f2634d864a1ee1ce3ead9c", "0x54261774905f3e6e9718f2abb10ed6555cae308a"),
                ("0xe9e7cea3dedca5984780bafc599bd69add087d56", "0x23b891e5c62e0955ae2bd185990103928ab817b3"),
                ("0xe9e7cea3dedca5984780bafc599bd69add087d56", "0x049d68029688eabf473097a2fc38ef61633a3c7a")
            ],

            "fantom": [
                ("0x049d68029688eabf473097a2fc38ef61633a3c7a", "0x43cf58380e69594fa2a5682de484ae00edd83e94"),
                ("0x74b23882a30290451a17c44f4f05243b6b58c76d", "0x67c10c397dd0ba417329543c1a40eb48aaa7cd00"),
                ("0x049d68029688eabf473097a2fc38ef61633a3c7a", "0xed2a7edd7413021d440b09d654f3b87712abab66")
            ],

            "ethereum": [
                ("0xdac17f958d2ee523a2206206994597c13d831ec7", "0x1b84765de8b7566e4ceaf4d0fd3c5af52d3dde4f")
            ],

            "optimistic-ethereum": [
                ("0x4200000000000000000000000000000000000006", "0x809dc529f07651bd43a172e8db6f4a7a0d771036")
            ],

            "polygon-pos": [
                ("0x2791bca1f2de4661ed88a30c99a7a9449aa84174", "0x128a587555d1148766ef4327172129b50ec66e5d"),
                ("0x2791bca1f2de4661ed88a30c99a7a9449aa84174", "0xb6c473756050de474286bed418b77aeac39b02af")
            ]
        }


    def send_request(self, url, params=None):
        """
        Send a request to the specified URL with optional parameters.

        Args:
            url (str): The URL to send the request to.
            params (dict, optional): Optional dictionary of parameters to include in the request.

        Returns:
            dict: The JSON response from the server, or None if an error occurred.
        """
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP Error occurred: {http_err}")
        except requests.exceptions.RequestException as req_err:
            print(f"Request Exception occurred: {req_err}")
        except Exception as exc_err:
            print(f"An error occurred: {exc_err}")
        return None

    def get_monthly_prices(self, contract_address, blockchain):
        """
        Get the monthly prices for a given contract address on a specified blockchain.

        Args:
            contract_address (str): The contract address to get prices for.
            blockchain (str): The blockchain that the contract address is on.

        Returns:
            DataFrame: A DataFrame containing the monthly prices, or None if an error occurred.
        """

        url = f"{self.BASE_URL}/coins/{blockchain}/contract/{contract_address}/market_chart"
        params = {'vs_currency': 'usd', 'days': '1111'}
        data = self.send_request(url, params=params)
        if data is not None:
            prices = data['prices']
            timestamps = pd.to_datetime(np.array(prices)[:, 0], unit='ms')
            prices = np.array(prices)[:, 1].astype(float)
            df = pd.DataFrame(prices, index=timestamps, columns=[contract_address])
            monthly_prices = df.resample('M').last()
            return monthly_prices
        return None


    def get_contract_info(self, contract_addr, blkchn):
        """
        Get the ticker and precision for a given contract address on a specified blockchain.

        Args:
            contract_address (str): The contract address to get info for.
            blockchain (str): The blockchain that the contract address is on.

        Returns:
            tuple: A tuple containing the ticker and precision, or (None, None) if an error occurred.
        """
        url = f"{self.BASE_URL}/coins/{blkchn}/contract/{contract_addr}"
        data = self.send_request(url)
        if data is not None:
            ticker = data['symbol']
            precision = data['detail_platforms'][f'{blkchn}']['decimal_place']
            return ticker, precision
        return None, None

    def fetch_and_save_prices(self):
        """
        Fetch and save the monthly prices for all contracts on all blockchains specified in chain_info.csv.

        Returns:
            None
        """
        writer = pd.ExcelWriter(self.prices_file)
        data = pd.read_csv('chain_info.csv')
        for _, row in data.iterrows():
            blockchain = row['blockchain']
            addresses_file = row[' queryCSV']
            addresses_df = pd.read_csv(f'data/{addresses_file}')
            contract_addrs = addresses_df['contract_address'].unique()
            if blockchain == 'optimistic-ethereum':
                contract_addrs = np.append(contract_addrs, self.OPT_ETHER)
            if blockchain == 'fantom':
                contract_addrs = np.append(contract_addrs, self.FANT)
            blockchain_prices = pd.DataFrame()
            for address in contract_addrs:
                monthly_prices = self.get_monthly_prices(address, blockchain)
                if monthly_prices is not None:
                    blockchain_prices = pd.concat([blockchain_prices, monthly_prices], axis=1)
                time.sleep(7)
            blockchain_prices.to_excel(writer, sheet_name=blockchain)
        writer.close()


    def fetch_and_save_contract_info(self):
        """
        Fetch and save the ticker and precision for all contracts on all blockchains specified in chain_info.csv.

        Returns:
            None
        """
        contract_info = []
        data = pd.read_csv('chain_info.csv')
        for _, row in data.iterrows():
            blockchain = row['blockchain']
            addresses_file = row[' queryCSV']
            addrs_df = pd.read_csv(f'data/{addresses_file}')
            contract_addresses = addrs_df['contract_address'].unique()
            if blockchain == 'optimistic-ethereum':
                contract_addresses = np.append(contract_addresses, self.OPT_ETHER)
            if blockchain == 'fantom':
                contract_addresses = np.append(contract_addresses, self.FANT)
            for address in contract_addresses:
                ticker, precision = self.get_contract_info(address, blockchain)
                if ticker is not None and precision is not None:
                    contract_info.append({
                        'blockchain': blockchain,
                        'contract_address':address,
                        'ticker': ticker,
                        'decimal': precision
                    })
                time.sleep(7)
        contract_info_df = pd.DataFrame(contract_info)
        contract_info_df.to_csv(self.info_file, index=False)


    def copy_column_with_new_header(self, df, orig_header, new_header):
        """
        copy_column_with_new_header _summary_

        _extended_summary_

        Args:
            df (_type_): _description_
            orig_header (_type_): _description_
            new_header (_type_): _description_

        Returns:
            _type_: _description_
        """
        if orig_header in df.columns:
            df[new_header] = df[orig_header]
        return df


    def copy_columns_with_new_headers(self, monthly, sheet):
        """
        Copy specified columns in a DataFrame and give them new headers.

        Args:
            monthly (DataFrame): The DataFrame containing the columns to copy.
            sheet (str): The sheet name corresponding to the columns to copy.

        Returns:
            DataFrame: The modified DataFrame.
        """
        if sheet in self.sheet_headers_mapping:
            for orig_header, new_header in self.sheet_headers_mapping[sheet]:
                monthly = self.copy_column_with_new_header(monthly, orig_header, new_header)
        return monthly


    def save_prices_to_excel(self):
        """
        Save the prices to an Excel file, making necessary modifications to column headers.

        Returns:
            None
        """
        excel_file, sheet_names = self.get_excel_sheets()
        writer = pd.ExcelWriter('../../../data/monthly_prices_full.xlsx')
        for sheet in sheet_names:
            self.write_sheet_to_excel(excel_file, sheet, writer)
        writer.close()


    def write_sheet_to_excel(self, excel_file, sheet, writer):
        """
        Write a sheet of an Excel file to a new file, making necessary modifications to column headers.

        Args:
            excel_file (str): The Excel file to read the sheet from.
            sheet (str): The name of the sheet to write.
            writer (ExcelWriter): The ExcelWriter object to use for writing.

        Returns:
            None
        """
        monthly = pd.read_excel(excel_file, sheet)
        monthly = self.copy_columns_with_new_headers(monthly, sheet)
        monthly.to_excel(writer, sheet_name=sheet)


    def get_excel_sheets(self):
        """
        Get the sheets of an Excel file.

        Returns:
            tuple: A tuple containing the ExcelFile object and the list of sheet names.
        """
        excel_file = pd.ExcelFile(self.prices_file)
        sheet_names = excel_file.sheet_names
        return excel_file, sheet_names


    def fill_missing_blockchain_data(self):
        """
        Fill missing blockchain data in the data derived from chain_info.csv.

        Returns:
            None
        """
        contract_df = pd.read_csv(self.info_file)
        data = pd.read_csv('chain_info.csv')
        for _, row in data.iterrows():
            blockchain = row['blockchain']
            file_name = row['queryCSV']
            transactions_df = pd.read_csv(f'data/{file_name}')
            merged_df = self.merge_dataframes(transactions_df, contract_df)
            self.fill_data_and_save(merged_df, blockchain)


    def fill_data_and_save(self, merged_df, blkchn):
        """
        Fill data in a DataFrame and save it to a CSV file.

        Args:
            merged_df (DataFrame): The DataFrame to fill data in.
            blkchn (str): The name of the blockchain.

        Returns:
            None
        """
        merged_df['decimal'] = np.where(
            merged_df['decimal'].isnull(),
            merged_df['decimal'],
            merged_df['decimals']
        )
        merged_df.drop(['decimals', 'blockchain_x'], axis=1, inplace=True)
        merged_df.rename(columns={'blockchain_y': 'blockchain'}, inplace=True)
        merged_df['blockchain'] = merged_df['blockchain'].fillna(f'{blkchn}')
        merged_df.to_csv(f'data/{blkchn}.csv', index=False)


    def merge_dataframes(self, transactions_df, contract_df):
        """
        Merge two DataFrames on the contract_address column.

        Args:
            transactions_df (DataFrame): The first DataFrame.
            contract_df (DataFrame): The second DataFrame.

        Returns:
            DataFrame: The merged DataFrame.
        """
        merged_df = pd.merge(transactions_df, contract_df, on='contract_address', how='left')
        return merged_df

    def run(self):
        """
        Run the data gathering and saving process.

        This method is the main entry point for the GetGecko class. It calls
        the other methods to gather the data, and then stores the processed
        data for further analysis.
        """
        self.fetch_and_save_prices()
        self.fetch_and_save_contract_info()
        self.save_prices_to_excel()
        self.fill_missing_blockchain_data()

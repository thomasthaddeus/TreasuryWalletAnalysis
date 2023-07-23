"""
Module to process and analyze token data from various blockchains.

This module contains a class, TokenData, that is responsible for
gathering token data from various blockchains, calculating their values,
and storing the processed data for further analysis.

The main functionality of the TokenData class can be accessed by creating
an instance of the class and calling its run method.
"""

import os
import time
import pandas as pd
import numpy as np
from selenium import webdriver
#from selenium.webdriver.common.keys import Keys
from dotenv import load_dotenv
from web3 import Web3
import json


INFURA_API_KEY = os.getenv("INFURA_API_KEY")


class TokenData:
    """
    Class to process and analyze token data from various blockchains.

    The TokenData class provides methods for gathering token data, calculating
    their values, and storing the processed data for further analysis.
    """

    def __init__(self):
        """
        Initialize a new instance of the TokenData class.

        This initializes a webdriver and loads the chain data from a CSV file.
        """
        self.load_dotenv()
        self.options = webdriver.ChromeOptions()
        self.options.add_argument("--start-maximized")
        self.options.add_argument("--log-level=3")
        self.driver = webdriver.Chrome()
        self.driver.set_window_size(1920, 1080)
        self.data = pd.read_csv("chain_info.csv")

    def load_dotenv(self):
        """
        Load environment variables from a .env file using python-dotenv.

        This method uses the load_dotenv function from python-dotenv to
        load environment variables from a .env file. This allows sensitive
        information such as API keys to be stored securely and not included
        in the code or version control.
        """
        load_dotenv()

    def get_token_info(self, contract_address, w3):
        """
        Get information about a token from its contract address.

        Args:
            contract_address (str): The contract address of the token.
            w3 (Web3): The Web3 instance used to interact with the blockchain.

        Returns:
            tuple: A tuple containing the token name, token symbol, and decimal
            value.
        """
        with open(
            file='data/json/contract_abi.json',
            mode='r',
            encoding='utf-8'
        ) as file:
            contract_abi = json.load(file)

        contract = w3.eth.contract(
            address=Web3.to_checksum_address(contract_address), abi=contract_abi
        )
        try:
            token_name = contract.functions.name().call()
            token_symbol = contract.functions.symbol().call()
            decimal_value = contract.functions.decimals().call()
            return token_name, token_symbol, decimal_value
        except Exception as err:
            print("An error occurred:", str(err))
            print(contract_address)
            return None

    def process_data(self):
        """
        Process the token data.

        This method processes the token data by looping through unique contract
        addresses, getting the token details, updating the dataframe with the
        token details, and then calculating and filtering the data.
        """
        for _, row in self.data.iterrows():
            blockchain, base_url = row["blockchain"], row["blockExplorerURL"]
            print(blockchain, base_url)
            w3 = self.get_web3(blockchain)
            data = pd.read_csv(f"data/{blockchain}.csv")
            data.drop(data.columns[[0]], axis=1, inplace=True)
            contract_addresses = data["contract_address"].unique()
            for contract_address in contract_addresses:
                if data["ticker"].isnull().any() \
                and data["decimal"].isnull().any():
                    token_name, ticker, decimal = self.get_token_details(
                        blockchain, base_url, contract_address, w3
                    )
                    data = self.update_data(
                        data, contract_address, token_name, ticker, decimal
                    )
            data = self.calculate_values(data)
            data = self.filter_data(data)
            data.to_csv(f"data/{blockchain}.csv")

    def get_web3(self, blockchain):
        """
        Get a Web3 instance for a given blockchain.

        Args:
            blockchain (str): The name of the blockchain.

        Returns:
            Web3: A Web3 instance used to interact with the given blockchain.
        """
        if blockchain == "optimistic-ethereum":
            return Web3(
                Web3.HTTPProvider(
                    f"https://optimism-mainnet.infura.io/v3/{INFURA_API_KEY}"
                )
            )
        elif blockchain == "ethereum":
            return Web3(
                Web3.HTTPProvider(
                    f"https://mainnet.infura.io/v3/{INFURA_API_KEY}")
            )
        elif blockchain == "polygon-pos":
            return Web3(
                Web3.HTTPProvider(
                    f"https://polygon-mainnet.infura.io/v3/{INFURA_API_KEY}"
                )
            )

    def get_token_details(self, blockchain, base_url, contract_address, w3):
        """
        Get the details of a token.

        Args:
            blockchain (str): The name of the blockchain.
            base_url (str): The base URL of the block explorer for the
            blockchain.
            contract_address (str): The contract address of the token.
            w3 (Web3): The Web3 instance used to interact with the blockchain.

        Returns:
            tuple: A tuple containing the token name, token symbol, and decimal
            value.
        """
        self.driver.get(base_url + contract_address)
        time.sleep(15)
        if blockchain in ["arbitrum-one", "avalanche", "binance-smart-chain", "fantom"]:
            return self.get_details_from_site()
        elif blockchain in ["optimistic-ethereum", "ethereum", "polygon-pos"]:
            try:
                return self.get_token_info(contract_address, w3)
            except BaseException as err:
                print("An error occurred:", str(err))

    def get_details_from_site(self):
        """
        Get token details from the block explorer website.

        Returns:
            tuple: A tuple containing the token name, token symbol, and decimal
            value.
        """
        token_name = self.driver.find_element_by_xpath(
            '//*[@id="content"]/div[1]/div/div[1]/h1/div/span'
        ).text

        ticker = self.driver.find_element_by_xpath(
            '//*[@id="ContentPlaceHolder1_divSummary"]/div[1]/div[1]/div/div[2]/div[2]/div[2]/b'
        ).text

        decimal = self.driver.find_element_by_xpath(
            '//*[@id="ContentPlaceHolder1_trDecimals"]/div/div[2]'
        ).text

        return token_name, ticker, decimal

    def update_data(self, data, contract_address, token_name, ticker, decimal):
        """
        Update the token data with the token details.

        Args:
            data (pd.DataFrame): The DataFrame containing the token data.
            contract_address (str): The contract address of the token.
            token_name (str): The name of the token.
            ticker (str): The ticker symbol of the token.
            decimal (float): The decimal value of the token.

        Returns:
            pd.DataFrame: The updated DataFrame containing the token data.
        """
        indices = np.where(data["contract_address"] == contract_address)
        for index in indices[0]:
            data.at[index, "token"] = token_name
            data.at[index, "ticker"] = ticker
            data.at[index, "decimal"] = decimal
        return data

    def calculate_values(self, data):
        """
        Calculate the values of the tokens.

        Args:
            data (pd.DataFrame): The DataFrame containing the token data.

        Returns:
            pd.DataFrame: The DataFrame with the calculated token values.
        """
        data["value"] = data["value"].astype(float)
        data["decimal"] = data["decimal"].astype(float)
        data["calc_value"] = (data["value"] * pow(10, -data["decimal"])).astype(float)
        return data

    def filter_data(self, data):
        """
        Filter the token data.

        This method filters out rows from the DataFrame that contain certain
        strings in the 'ticker' and 'token' columns.

        Args:
            data (pd.DataFrame): The DataFrame containing the token data.

        Returns:
            pd.DataFrame: The filtered DataFrame.
        """
        data = data[
            ~data["ticker"].str.contains(
                "N/A|Visit|.com|.fi|.io|.xyz|.site|.exchange|.pro|.net"
            )
        ]
        data = data[
            ~data["token"].str.contains(
                ".org|.com|.fi|.io|.org|.xyz|.site|.exchange|.pro|.net"
            )
        ]
        return data

    def run(self):
        """
        Run the data processing and analysis.

        This method is the main entry point for the TokenData class. It calls
        the other methods to gather and process the data, calculate the token
        values, and then stores the processed data for further analysis.
        """
        self.process_data()
        self.driver.close()
        self.driver.quit()

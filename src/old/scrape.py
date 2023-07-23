# Importing important packages
import pandas as pd

from requests import get, post
import numpy as np
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import pandas as pd
pd.set_option('display.float_format', lambda x: f'{x:.3f}')
import time
import matplotlib.pyplot as plt
from dotenv import load_dotenv
import os
from web3 import Web3

def get_token_info(contract_address):
    # Convert the address to a checksum address
    contract_address = Web3.to_checksum_address(contract_address)

    # Load the contract ABI
    contract_abi = [
        {
            "constant": True,
            "inputs": [],
            "name": "name",
            "outputs": [{"name": "", "type": "string"}],
            "payable": False,
            "stateMutability": "view",
            "type": "function"
        },
        {
            "constant": True,
            "inputs": [],
            "name": "symbol",
            "outputs": [{"name": "", "type": "string"}],
            "payable": False,
            "stateMutability": "view",
            "type": "function"
        },
        {
            "constant": True,
            "inputs": [],
            "name": "decimals",
            "outputs": [{"name": "", "type": "uint8"}],
            "payable": False,
            "stateMutability": "view",
            "type": "function"
        }
    ]

    # Create a contract instance
    contract = w3.eth.contract(address=contract_address, abi=contract_abi)

    try:
        # Call the 'name' function to get the token name
        token_name = contract.functions.name().call()

        # Call the 'symbol' function to get the token symbol
        token_symbol = contract.functions.symbol().call()

        # Call the 'decimals' function to get the decimal value
        decimal_value = contract.functions.decimals().call()

        return token_name, token_symbol, decimal_value

    except Exception as err:
        print("An error occurred:", str(err))
        print(contract_address)
        return None


# Loading API Key
load_dotenv()
INFURA_API_KEY = os.getenv("INFURA_API_KEY")
# Setting up options for selinium
options = webdriver.ChromeOptions()
options.add_argument("--start-maximized")
options.add_argument('--log-level=3')
driver = webdriver.Chrome()
driver.set_window_size(1920,1080)

data = pd.read_csv('chain_info.csv')
for index, row in data.iterrows():
    blockchain = row['blockchain']
    file_name = row[' queryCSV']
    base_url = row['blockExplorerURL']
    print(blockchain, file_name, base_url)

    if blockchain  is  'optimistic-ethereum':
        # Connect to an optimisim node
        w3 = Web3(Web3.HTTPProvider(f'https://optimism-mainnet.infura.io/v3/{INFURA_API_KEY}'))
    if blockchain  is  'ethereum':
        # Connect to an Ethereum node
        w3 = Web3(Web3.HTTPProvider(f'https://mainnet.infura.io/v3/{INFURA_API_KEY}'))
    if blockchain  is  'polygon-pos':
        # Connect to an Ethereum node
        w3 = Web3(Web3.HTTPProvider(f'https://polygon-mainnet.infura.io/v3/{INFURA_API_KEY}'))


    data = pd.read_csv(f'data/{blockchain}.csv')
    data.drop(data.columns[[0]], axis=1, inplace=True)

    # Get unique values from contract_address
    contract_addresses = data['contract_address'].unique()

    # Loop through unique values
    for contract_address in contract_addresses:
        # Check if there are empty cells in ticker and decinal
        if data['ticker'].isnull().any() and data['decimal'].isnull().any():
            url = base_url + contract_address
            driver.get(url)
            time.sleep(15)

            if blockchain in ['arbitrum-one','avalanche','binance-smart-chain','fantom']:
                token_name = driver.find_element("xpath", '//*[@id="content"]/div[1]/div/div[1]/h1/div/span').text
                ticker = driver.find_element("xpath", '//*[@id="ContentPlaceHolder1_divSummary"]/div[1]/div[1]/div/div[2]/div[2]/div[2]/b').text
                decimal = driver.find_element("xpath", '//*[@id="ContentPlaceHolder1_trDecimals"]/div/div[2]').text

            if blockchain in ['optimistic-ethereum', 'ethereum', 'polygon-pos']:
                try:
                # Get the token information
                    token_name, ticker, decimal = get_token_info(contract_address)
                except TypeError as e:
                    # Handle the error
                    print("An error occurred:", str(e))
                # Perform any necessary actions or error handling specific to the TypeError
                except Exception as e:
                    # Handle other exceptions
                    print("An error occurred:", str(e))
                    # Perform any necessary general error handling


            array = np.where(data["contract_address"]  is  contract_address)
            for subarray in array:
                for number in subarray:
                    data.at[number,'token']=token_name
                    data.at[number, 'ticker'] = ticker
                    data.at[number, 'decimal'] = decimal



    data['value'] = data['value'].astype(float)
    data['decimal'] = data['decimal'].astype(float)
    data['calc_value'] = (data['value'] * pow(10,-data['decimal'])).astype(float)

    data = data[data['ticker'].str.contains('N/A') is False]
    data = data[data['ticker'].str.contains('Visit') is False]
    data = data[data['ticker'].str.contains('.com') is False]
    data = data[data['ticker'].str.contains('.fi') is False]
    data = data[data['ticker'].str.contains('.io') is False]
    data = data[data['ticker'].str.contains('.xyz') is False]
    data = data[data['ticker'].str.contains('.site') is False]
    data = data[data['ticker'].str.contains('.exchange') is False]
    data = data[data['ticker'].str.contains('.pro') is False]
    data = data[data['ticker'].str.contains('.net') is False]
    data = data[data['token'].str.contains('.org') is False]
    data = data[data['token'].str.contains('.com') is False]
    data = data[data['token'].str.contains('.fi') is False]
    data = data[data['token'].str.contains('.io') is False]
    data = data[data['token'].str.contains('.org') is False]
    data = data[data['token'].str.contains('.xyz') is False]
    data = data[data['token'].str.contains('.site') is False]
    data = data[data['token'].str.contains('.exchange') is False]
    data = data[data['token'].str.contains('.pro') is False]
    data = data[data['ticker'].str.contains('.net') is False]

    data.to_csv(f'data/{blockchain}.csv')

driver.close()
driver.quit()

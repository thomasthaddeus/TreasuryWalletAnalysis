# Importing important packages
import pandas as pd
import requests
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


def get_monthly_prices(contract_address, blockchain):
    # Define the CoinGecko API endpoint
    url = f"https://api.coingecko.com/api/v3/coins/{blockchain}/contract/{contract_address}/market_chart"

    # Set the desired parameters for the API request (e.g., time range)
    params = {
        'vs_currency': 'usd',
        'days': '1111',  # Adjust the time range as needed
    }

    try:
        # Send the API request
        response = requests.get(url, params=params)

        # Raise an exception if the response status code is not 200 (OK)
        response.raise_for_status()

        # Parse the response JSON
        data = response.json()

        # Extract the price data from the response
        prices = data['prices']

        # Convert the UNIX timestamps to datetime objects and prices to floats
        timestamps = pd.to_datetime(np.array(prices)[:, 0], unit='ms')
        prices = np.array(prices)[:, 1].astype(float)

        # Create a pandas DataFrame with timestamps as the index and prices as the column
        df = pd.DataFrame(prices, index=timestamps, columns=[contract_address])

        # Resample the DataFrame to get the last day of every month
        monthly_prices = df.resample('M').last()

        return monthly_prices

    except requests.exceptions.HTTPError as err:
        print(f"HTTP Error occurred: {err}")
    except requests.exceptions.RequestException as err:
        print(f"Request Exception occurred: {err}")
    except KeyError as err:
        print(f"KeyError occurred: {err}")
    except Exception as err:
        print(f"An error occurred: {err}")

    return None

def get_contract_info(contract_address, blockchain):
    # Define the CoinGecko API endpoint
    url = f"https://api.coingecko.com/api/v3/coins/{blockchain}/contract/{contract_address}"

    try:
        # Send the API request
        response = requests.get(url)

        # Raise an exception if the response status code is not 200 (OK)
        response.raise_for_status()

        # Parse the response JSON
        data = response.json()

        # Extract the ticker and precision from the response
        ticker = data['symbol']
        precision = data['detail_platforms'][f'{blockchain}']['decimal_place']

        return ticker, precision

    except requests.exceptions.HTTPError as err:
        print(f"HTTP Error occurred: {err}")
    except requests.exceptions.RequestException as err:
        print(f"Request Exception occurred: {err}")
    except KeyError as err:
        print(f"KeyError occurred: {err}")
    except Exception as err:
        print(f"An error occurred: {err}")

    return None, None

# Create an Excel workbook
writer = pd.ExcelWriter('data/monthly_prices.xlsx')

# Fetch monthly prices for each blockchain and contract addresses and store in separate sheets
data = pd.read_csv('chain_info.csv')
for index, row in data.iterrows():
    blockchain = row['blockchain']
    addresses_file = row[' queryCSV']
    # Read the CSV file and extract the contract addresses for the current blockchain
    addresses_df = pd.read_csv(f'data/{addresses_file}')
    contract_addresses = addresses_df['contract_address'].unique()
    if blockchain == 'optimistic-ethereum':
        contract_addresses = np.append(contract_addresses, '0x4200000000000000000000000000000000000006') # Appending the contract address for WETH
    if blockchain == 'fantom':
        contract_addresses = np.append(contract_addresses, '0x9879abdea01a879644185341f7af7d8343556b7a') # Appending the contract address for TrueUSD
    # Create an empty DataFrame to store monthly prices for the current blockchain
    blockchain_prices = pd.DataFrame()

    for address in contract_addresses:
        monthly_prices = get_monthly_prices(address, blockchain)
        if monthly_prices is not None:
            blockchain_prices = pd.concat([blockchain_prices, monthly_prices], axis=1)
        time.sleep(7)  # Add a 7-second delay between API requests

    # Write the monthly prices for the current blockchain to a sheet in the Excel workbook
    blockchain_prices.to_excel(writer, sheet_name=blockchain)

# Save and close the Excel workbook
writer.close()


# Create an empty list to store contract information
contract_info = []

# Get token info
data = pd.read_csv('chain_info.csv')
for index, row in data.iterrows():
    blockchain = row['blockchain']
    addresses_file = row[' queryCSV']
    # Read the CSV file and extract the contract addresses for the current blockchain
    addresses_df = pd.read_csv(f'data/{addresses_file}')
    contract_addresses = addresses_df['contract_address'].unique()
    if blockchain == 'optimistic-ethereum':
        contract_addresses = np.append(contract_addresses, '0x4200000000000000000000000000000000000006')
    if blockchain == 'fantom':
        contract_addresses = np.append(contract_addresses, '0x9879abdea01a879644185341f7af7d8343556b7a')
    for address in contract_addresses:
        ticker, precision = get_contract_info(address, blockchain)
        if ticker is not None and precision is not None:
            contract_info.append({'blockchain': blockchain, 'contract_address': address, 'ticker': ticker, 'decimal': precision})
        time.sleep(7)

# Save the contract information to a CSV file
contract_info_df = pd.DataFrame(contract_info)
contract_info_df.to_csv('data/contract_info.csv', index=False)



#For tokens such as nETH and nUSD, columns from similar tokens are copied and the header is changed to match the contract address of the original token
def copy_column_with_new_header(df, column_name, new_header):
    new_df = df.copy()
    new_df[new_header] = df[column_name]
    return new_df

excel_file = pd.ExcelFile('data/monthly_prices.xlsx')
sheet_names = excel_file.sheet_names

# Create an Excel workbook
writer = pd.ExcelWriter('data/monthly_prices_full.xlsx')

for sheet_name in sheet_names:
    monthly = pd.read_excel(excel_file, sheet_name)
    cols = list(monthly.columns) # Creating a list with the headers
    for col in cols:
        if sheet_name == "arbitrum-one":
            monthly = copy_column_with_new_header(monthly, "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9", "0x2913e812cf0dcca30fb28e6cac3d2dcff4497688") 
            monthly = copy_column_with_new_header(monthly, "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9", "0xe264cb5a941f98a391b9d5244378edf79bf5c19e")
            monthly = copy_column_with_new_header(monthly, "0x82af49447d8a07e3bd95bd0d56f35241523fbab1", "0x3ea9b0ab55f34fb188824ee288ceaefc63cf908e")
        if sheet_name == "avalanche":
            monthly = copy_column_with_new_header(monthly, "0xd586e7f844cea2f87f50152665bcbc2c279d8d70", "0x55904f416586b5140a0f666cf5acf320adf64846")
            monthly = copy_column_with_new_header(monthly, "0xd586e7f844cea2f87f50152665bcbc2c279d8d70", "0xcfc37a6ab183dd4aed08c204d1c2773c0b1bdf46")
        if sheet_name == "binance-smart-chain":
            monthly = copy_column_with_new_header(monthly, "0xe9e7cea3dedca5984780bafc599bd69add087d56", "0xf2511b5e4fb0e5e2d123004b672ba14850478c14") # 3NRV-LP -> BUSD
            monthly = copy_column_with_new_header(monthly, "0xe9e7cea3dedca5984780bafc599bd69add087d56", "0xf0b8b631145d393a767b4387d08aa09969b2dfed") # USD-LP -> BUSD
            monthly = copy_column_with_new_header(monthly, "0xe9e7cea3dedca5984780bafc599bd69add087d56", "0xdd17344f7537df99f212a08f5a5480af9f6c083a") # nUSD-LP -> BUSD
            monthly = copy_column_with_new_header(monthly, "0x7130d2a12b9bcbfae4f2634d864a1ee1ce3ead9c", "0x54261774905f3e6e9718f2abb10ed6555cae308a") # anyBTC -> Binance BTC
            monthly = copy_column_with_new_header(monthly, "0xe9e7cea3dedca5984780bafc599bd69add087d56", "0x23b891e5c62e0955ae2bd185990103928ab817b3") # nUSD -> BUSD
            monthly = copy_column_with_new_header(monthly, "0xe9e7cea3dedca5984780bafc599bd69add087d56", "0x049d68029688eabf473097a2fc38ef61633a3c7a") # fUSDT -> BUSD
        if sheet_name == "fantom":
            monthly = copy_column_with_new_header(monthly, "0x049d68029688eabf473097a2fc38ef61633a3c7a", "0x43cf58380e69594fa2a5682de484ae00edd83e94") # USD-LP -> TrueUSD
            monthly = copy_column_with_new_header(monthly, "0x74b23882a30290451a17c44f4f05243b6b58c76d", "0x67c10c397dd0ba417329543c1a40eb48aaa7cd00") # nETH -> WETH
            monthly = copy_column_with_new_header(monthly, "0x049d68029688eabf473097a2fc38ef61633a3c7a", "0xed2a7edd7413021d440b09d654f3b87712abab66") # nUSD -> TrueUSD

        if sheet_name == "ethereum":
            monthly = copy_column_with_new_header(monthly, "0xdac17f958d2ee523a2206206994597c13d831ec7", "0x1b84765de8b7566e4ceaf4d0fd3c5af52d3dde4f") # nUSD -> USDT
        if sheet_name == "optimistic-ethereum":
            monthly = copy_column_with_new_header(monthly, "0x4200000000000000000000000000000000000006", "0x809dc529f07651bd43a172e8db6f4a7a0d771036") # nETH -> WETH
        if sheet_name == "polygon-pos":
            monthly = copy_column_with_new_header(monthly, "0x2791bca1f2de4661ed88a30c99a7a9449aa84174", "0x128a587555d1148766ef4327172129b50ec66e5d") # USD-LP -> USDC
            monthly = copy_column_with_new_header(monthly, "0x2791bca1f2de4661ed88a30c99a7a9449aa84174", "0xb6c473756050de474286bed418b77aeac39b02af") # nUSD -> USDC
    # Write the monthly prices for the current blockchain to a sheet in the Excel workbook
    monthly.to_excel(writer, sheet_name=sheet_name)

# Save and close the Excel workbook
writer.close()



#Filling in missing blockchain and decimal data

# Read the contract addresses CSV file
contract_df = pd.read_csv('data/contract_info.csv')

# Get token info
data = pd.read_csv('chain_info.csv')
for index, row in data.iterrows():
    blockchain = row['blockchain']
    file_name = row['queryCSV']
    
    # Read the transactions CSV file
    transactions_df = pd.read_csv(f'data/{file_name}')

    # Merge the two dataframes on the 'contract_address' column
    merged_df = pd.merge(transactions_df, contract_df, on='contract_address', how='left')

    # Fill in the missing decimal values
    merged_df['decimal'] = np.where(merged_df['decimal'].isnull(), merged_df['decimal'], merged_df['decimals'])

    # Drop the redundant columns
    merged_df.drop(['decimals', 'blockchain_x'], axis=1, inplace=True)

    # Rename the remaining columns, if needed
    merged_df.rename(columns={'blockchain_y': 'blockchain'}, inplace=True)

    # standardizing the blockchain name
    merged_df['blockchain'] = merged_df['blockchain'].fillna(f'{blockchain}')

    # Save the updated dataframe to a new CSV file
    merged_df.to_csv(f'data/{blockchain}.csv', index=False)

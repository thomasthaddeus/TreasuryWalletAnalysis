## Getting Started
Create a python3.11 virtual environment
```console
user@machine:~$ python3.11 -m venv env
```

Enter into the virtual environment
```console
user@machine:~$ source env/bin/activate
```

Install necissary packages using pip
```console
user@machine:~$ pip3 install -r requirements.txt 
```
Files should be run in this order:
dune_API_pull.py &rarr; historical_prices_coingecko.py &rarr; decimals_token_coingecko.py &rarr; scrape_Web3.py &rarr; calculations.py

Data files will be stored in the data directory

---
## Using the Dune API to download the results from the Dune queries 
Below is the list of Dune quieres. Each query tracks the inflow and outflow of ERC-20 tokens from the Synapse treasury wallet for each chain.
* [Arbitrum](https://dune.com/queries/2664815) 2664815
* [Avalanche](https://dune.com/queries/2670811) 2670811
* [Binance Smart Chain](https://dune.com/queries/2670814) 2670814
* [Fantom](https://dune.com/queries/2670822) 2670822
* [Mainnet](https://dune.com/queries/2670826) 2670826
* [Optimisim](https://dune.com/queries/2670831) 2670831
* [Polygon](https://dune.com/queries/2670824) 2670824

## Historical Data, token names, tickers, and decimals
Historical price data, token names, tickers, and decimal values are gathered from Coingecko using the CoinGecko API

## Handling missing historical price data for nETH, nUSD, and liquidity pool tokens
In most cases, there is no historical price data for nETH, nUSD, or any token from a liquidy pool. In the case of nETH and nUSD, the historical and current prices are taken from similar tokens. nETH is given the same prices as WETH, and nUSD is given the same price as another stablecoin. The same is done for tokens from liquidity pools. These tokens are priced with the largest stablecoin on that chain.

## Web scraping and Web3.py
Some tokens do not have decimal or token name information on CoinGecko or Dune. To rectify this problem, web scraping of each chains block explorer is used to get that data. In the event of chains where Infura has nodes, a direct call to the contract address is used to get information.

# Treasury Wallet Analysis

## Getting Started

1. Create a python 3.11 virtual environment

    ```bash
    python3.11 -m venv env
    ```

2. Enter into the virtual environment

    ```bash
    source env/bin/activate
    ```

3. Install necessary packages using pip

    ```bash
    pip3 install -r requirements.txt
    ```

### Execution

Files should be run in this order:

1. `dune.py` &rarr;
2. `coingecko.py` &rarr;
3. `scrape.py` &rarr;
4. `calculations.py`

### API Keys

- **Dune API key**
- **Infura API key**
- Add to the `.env` file

### Database

- Data files will be stored in the `/data` directory

- Information regarding each chain is held in the `chain_info.csv` file

- Results from running these scripts will be placed in the `summed.csv` file.

## Using the Dune API

Below is the list of Dune queries. Each query tracks the inflow and outflow of ERC-20 tokens from the Synapse treasury wallet for each chain.

Blockchain|Query #
---|---
[Arbitrum](https://dune.com/queries/2664815) | 2664815
[Avalanche](https://dune.com/queries/2670811) | 2670811
[Binance Smart Chain](https://dune.com/queries/2670814) | 2670814
[Fantom](https://dune.com/queries/2670822) | 2670822
[Mainnet](https://dune.com/queries/2670826) | 2670826
[Optimisim](https://dune.com/queries/2670831) | 2670831
[Polygon](https://dune.com/queries/2670824) | 2670824

### Historical Data, token names, tickers, and decimals

Historical price data, token names, tickers, and decimal values are gathered from Coingecko using the CoinGecko API

### Handling missing historical price data for nETH, nUSD, and liquidity pool tokens

In most cases, there is no historical price data for nETH, nUSD, or any token from a liquidity pool. In the case of nETH and nUSD, the historical and current prices are taken from similar tokens. nETH is given the same prices as WETH, and nUSD is given the same price as another stablecoin. The same is done for tokens from liquidity pools. These tokens are priced with the largest stablecoin on that chain.

### Web scraping and `Web3.py`

Some tokens do not have decimal or token name information on CoinGecko or Dune. To rectify this problem, web scraping of each chains block explorer is used to get that data. In the event of chains where Infura has nodes, a direct call to the contract address is used to get information.

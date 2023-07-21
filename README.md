## Getting Started

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

## Getting historical price data from CoinGecko


## Getting decimals and token names from CoinGecko
Not all tokens have names on Dune.

## Handling missing historical price data for nETH, nUSD, and liquidity pool tokens
In most cases, there is no historical price data for nETH, nUSD, or any token from a liquidy pool. In the case of nETH and nUSD, the historical and current prices are taken from similar tokens. nETH is given the same prices as WETH, and nUSD is given the same price as another stablecoin. The same is done for tokens from liquidity pools. These tokens are priced with the largest stablecoin on that chain.

## Web scraping and Web3.py
Some tokens do not have decimal or token name information on CoinGecko or Dune. To rectify this problem, web scraping of each chains block explorer is used to get that data. In the event of chains where Infura has nodes, a direct call to the contract address is used to get information.

## Calculating monthly amounts

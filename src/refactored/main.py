#!/usr/bin/env python

"""main.py
main function of the the application

Creates an instance of the different classe and runs the querys for each of
them individually
"""

import logging
from dotenv import load_dotenv
from functions.skraper import TokenData
from functions.get_gecko import GetGecko
from functions.calculations import DataAnalysis
#from functions.get_dune import Dune

load_dotenv()

def main():
    """
    main the main program for this scraper

    _extended_summary_
    """
    # Set up logging
    logging.basicConfig(
        filename='log/app.log',
        filemode='w',
        format='%(name)s - %(levelname)s - %(message)s',
        level=logging.INFO
    )
    logging.info('Started')

    # Instantiate classes and run the program
    #get_dune = Dune()
    gecko = GetGecko()
    token_data = TokenData()
    calculated_data = DataAnalysis()

    # Run methods
    #get_dune.run_query()
    #get_dune.run()
    gecko.run()
    token_data.run()
    calculated_data.run()

    logging.info('Finished')

if __name__ == "__main__":
    main()

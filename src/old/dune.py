"""dune.py

All of the code for the Dune API is from the dune documentation. Only slight
modifications have been made to loop through the list of quiries and wait while
dune is executing the query.

_extended_summary_

Returns:
    _type_: _description_
"""

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


load_dotenv()
API_KEY = os.getenv("DUNE_API_KEY")
HEADER = {"x-dune-api-key" : API_KEY}

BASE_URL = "https://api.dune.com/api/v1/"

class Dune:
    """
     _summary_

    _extended_summary_
    """

    def __init__(self) -> None:
        self.module = module
        self.action = action



        pass

    def make_api_url(self, module, action, ID):
        """
        We shall use this function to generate a URL to call the API.
        """

        url = BASE_URL + self.module + "/" + self.ID + "/" + self.action

        return url

    def execute_query(self, query_id):
        """
        Takes in the query ID.
        Calls the API to execute the query.
        Returns the execution ID of the instance which is executing the query.
        """

        url = make_api_url("query", "execute", query_id)
        response = post(url, headers=HEADER, timeout=300)
        execution_id = response.json()['execution_id']

        return execution_id


    def get_query_status(self, execution_id):
        """
        Takes in an execution ID.
        Fetches the status of query execution using the API
        Returns the status response object
        """

        url = make_api_url("execution", "status", execution_id)
        response = get(url, headers=HEADER, timeout=300)

        return response


    def get_query_results(self, execution_id):
        """
        Takes in an execution ID.
        Fetches the results returned from the query using the API
        Returns the results response object
        """

        url = make_api_url("execution", "results", execution_id)
        response = get(url, headers=HEADER, timeout=300)

        return response


    def cancel_query_execution(self, execution_id):
        """
        Takes in an execution ID.
        Cancels the ongoing execution of the query.
        Returns the response object.
        """

        url = make_api_url("execution", "cancel", execution_id)
        response = get(url, headers=HEADER, timeout=300)

        return response

    # Function to check if the query is complete
    def stop_checking(self, execution_id):
        """
        stop_checking _summary_

        _extended_summary_

        Args:
            execution_id (_type_): _description_

        Returns:
            _type_: _description_
        """
        response = get_query_status(execution_id)
        response = response.json()
        if response['state'] == 'QUERY_STATE_COMPLETED':
            return True

        return False

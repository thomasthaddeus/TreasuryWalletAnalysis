"""get_dune.py

All of the code for the Dune API is from the dune documentation. Only slight
modifications have been made to loop through the list of queries and wait while
dune is executing the query.

This file contains the Dune class which handles interactions with the Dune API.
The Dune API is used to execute queries and retrieve the results. The class
provides methods for executing queries, getting the status of a query, getting
the results of a query, and cancelling a query execution.

Returns:
    Dune: an instance of the Dune class.
"""

# Importing important packages
import os
import time
import pandas as pd
import requests as req
from requests import get, post
pd.set_option('display.float_format', lambda x: f'{x:.3f}')
from dotenv import load_dotenv

load_dotenv()

class Dune:
    """
    A class to interact with the Dune API.

    The Dune class provides methods to execute queries on the Dune API, get the
    status of a query execution, get the results of a query execution, and
    cancel a query execution.

    Attributes:
        API_KEY (str): The API key for the Dune API.
        BASE_URL (str): The base URL for the Dune API.
        HEADER (dict): The headers to include in the API request.
        module (str): The module to use in the API URL.
        action (str): The action to use in the API URL.
        ID (str): The ID to use in the API URL.
    """

    API_KEY = os.getenv("DUNE_API_KEY")
    BASE_URL = "https://api.dune.com/api/v1/"
    HEADER = {"x-dune-api-key" : API_KEY}


    def __init__(self, module, action, ID):
        """
        Generate a URL to call the API.

        Args:
            module (str): The module to use in the API URL.
            action (str): The action to use in the API URL.
            ID (str): The ID to use in the API URL.

        Returns:
            str: The URL to call the API.
        """
        self.module = module
        self.action = action
        self.ID = ID

    def make_api_url(self, module, action, ID):
        """
        We shall use this function to generate a URL to call the API.
        """

        url = f"{self.BASE_URL}{module}/{ID}/{action}"

        return url

    def execute_query(self, query_id):
        """
        Execute a query on the Dune API.

        This method sends a POST request to the Dune API to execute the query
        with the given ID. It returns the execution ID of the instance which is
        executing the query.

        Args:
            query_id (str): The ID of the query to execute.

        Returns:
            str: The execution ID of the instance executing the query.

        Raises:
            BaseException: If there is an error executing the query.
        """
        url = self.make_api_url("query", "execute", query_id)
        try:
            response = post(url, headers=self.HEADER, timeout=300)
            response.raise_for_status()  # This will raise an exception if the response contains an HTTP error status.
        except req.exceptions.RequestException as req_err:
            print(f"Error executing query: {req_err}")
            raise  # Reraise the exception so the caller can handle it.
        except BaseException as base_err:
            print(f"An unexpected error occurred: {base_err}")
            raise  # Reraise the exception so the caller can handle it.

        execution_id = response.json()['execution_id']
        return execution_id


    def get_query_status(self, execution_id):
        """
        Get the status of a query execution.

        This method sends a GET request to the Dune API to get the status of the query execution with the given execution ID.

        Args:
            execution_id (str): The execution ID of the query.

        Returns:
            dict: The response from the API.

        Raises:
            BaseException: If there is an error getting the query status.
        """
        url = self.make_api_url("execution", "status", execution_id)
        try:
            response = req.get(url, headers=self.HEADER, timeout=300)
            response.raise_for_status()
        except req.exceptions.RequestException as req_err:
            print(f"Error getting query status: {req_err}")
            raise
        except Exception as base_err:
            print(f"An unexpected error occurred: {base_err}")
            raise

        return response.json()

    def get_query_results(self, execution_id):
        """
        Get the results of a query execution.

        This method sends a GET request to the Dune API to get the results of the query execution with the given execution ID.

        Args:
            execution_id (str): The execution ID of the query.

        Returns:
            dict: The response from the API.

        Raises:
            Exception: If there is an error getting the query results.
        """
        url = self.make_api_url("execution", "results", execution_id)
        try:
            response = req.get(url, headers=self.HEADER, timeout=300)
            response.raise_for_status()
        except req.exceptions.RequestException as req_err:
            print(f"Error getting query results: {req_err}")
            raise
        except Exception as base_err:
            print(f"An unexpected error occurred: {base_err}")
            raise

        return response.json()

    def cancel_query_execution(self, execution_id):
        """
        Cancel a query execution.

        This method sends a GET request to the Dune API to cancel the query execution with the given execution ID.

        Args:
            execution_id (str): The execution ID of the query.

        Returns:
            dict: The response from the API.

        Raises:
            Exception: If there is an error canceling the query execution.
        """
        url = self.make_api_url("execution", "cancel", execution_id)
        try:
            response = req.get(url, headers=self.HEADER, timeout=300)
            response.raise_for_status()
        except req.exceptions.RequestException as req_err:
            print(f"Error canceling query execution: {req_err}")
            raise
        except BaseException as base_err:
            print(f"An unexpected error occurred: {base_err}")
            raise

        return response.json()

    def stop_checking(self, execution_id):
        """
        Check if a query execution is complete.

        This method sends a GET request to the Dune API to get the status of the query execution with the given execution ID, and checks if the state is "QUERY_STATE_COMPLETED".

        Args:
            execution_id (str): The execution ID of the query.

        Returns:
            bool: True if the query execution is complete, False otherwise.

        Raises:
            Exception: If there is an error checking the query status.
        """
        response = self.get_query_status(execution_id)
        if response['state'] == 'QUERY_STATE_COMPLETED':
            return True

        return False


    def run_query(self, query_id):
        """
        Run a query and fetch its results.

        This method sends a POST request to execute a query, then polls the API
        until the query execution has completed. It then fetches and returns the
        results of the query.

        Args:
            query_id (str): The ID of the query to run.

        Returns:
            dict: The results of the query, or None if an error occurred.
        """
        execution_id = self.execute_query(query_id)

        while not self.stop_checking(execution_id):
            time.sleep(1)  #TODO: #1 wait for a bit before checking again

        results = self.get_query_results(execution_id)

        if 'error' in results:
            print(f"Error executing query: {results['error']}")
            return None

        return results

    def run(self, query_id):
        execution_id = self.execute_query(query_id)
        while not self.stop_checking(execution_id):
            time.sleep(1)  # wait for a bit before checking again
        results = self.get_query_results(execution_id)
        if 'error' in results:
            print(f"Error executing query: {results['error']}")
            return None
        return results

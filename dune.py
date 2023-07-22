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

# All of the code for the Dune API is from the dune documentation. Only slight modifications have been made to loop 
# through the list of quiries and wait while dune is executing the query.
load_dotenv()
API_KEY = os.getenv("DUNE_API_KEY")
HEADER = {"x-dune-api-key" : API_KEY}

BASE_URL = "https://api.dune.com/api/v1/"

def make_api_url(module, action, ID):
    """
    We shall use this function to generate a URL to call the API.
    """
    
    url = BASE_URL + module + "/" + ID + "/" + action
    
    return url

def execute_query(query_id):
    """
    Takes in the query ID.
    Calls the API to execute the query.
    Returns the execution ID of the instance which is executing the query.
    """
    
    url = make_api_url("query", "execute", query_id)
    response = post(url, headers=HEADER)
    execution_id = response.json()['execution_id']
    
    return execution_id


def get_query_status(execution_id):
    """
    Takes in an execution ID.
    Fetches the status of query execution using the API
    Returns the status response object
    """
    
    url = make_api_url("execution", "status", execution_id)
    response = get(url, headers=HEADER)
    
    return response


def get_query_results(execution_id):
    """
    Takes in an execution ID.
    Fetches the results returned from the query using the API
    Returns the results response object
    """
    
    url = make_api_url("execution", "results", execution_id)
    response = get(url, headers=HEADER)
    
    return response


def cancel_query_execution(execution_id):
    """
    Takes in an execution ID.
    Cancels the ongoing execution of the query.
    Returns the response object.
    """
    
    url = make_api_url("execution", "cancel", execution_id)
    response = get(url, headers=HEADER)
    
    return response

# Function to check if the query is complete
def stop_checking(execution_id):
    response = get_query_status(execution_id)
    response = response.json()
    if response['state'] == 'QUERY_STATE_COMPLETED':
        return True
    
    return False

# Looping through the queries 
data = pd.read_csv('chain_info.csv')
for query in data['queryID']:
    execution_id = execute_query(f"{query}")

    while not stop_checking(execution_id):
        time.sleep(180) # 180 seconds = 3 min

    response = get_query_results(execution_id)
    data = pd.DataFrame(response.json()['result']['rows'])
    data.to_csv(f"data/{query}.csv")
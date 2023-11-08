import pandas
import pandas as pd
import ray
import requests
from typing import Dict, Any
import hashlib
import json


def dict_hash(dictionary: Dict[str, Any]) -> str:
    """MD5 hash of a dictionary."""
    dhash = hashlib.md5()
    # We need to sort arguments so {'a': 1, 'b': 2} is
    # the same as {'b': 2, 'a': 1}
    encoded = json.dumps(dictionary, sort_keys=True).encode()
    dhash.update(encoded)
    return dhash.hexdigest()


# Generic helper function to query stats.nba.com endpoint and fetch details.
def fetch_data(endpoint, params: dict) -> Dict:
    reqHeaders = {
        'User-Agent': 'PythonScript',
        'Referer': 'https://www.nba.com/',
        'Origin': 'https://www.nba.com',
    }

    url = f"https://stats.nba.com/stats/{endpoint}/"
    resp = requests.get(url, headers=reqHeaders, params=params)
    retVal = {'STATUS': resp.status_code, 'RAW': resp.text}

    if resp.status_code == 200:
        jsonResp = resp.json()
        reqParams = {k: v for k, v in jsonResp['parameters'].items() if v is not None}
        retVal['HASH'] = dict_hash(reqParams)
        result_sets = jsonResp['resultSets']
        for result_set in result_sets:
            name = result_set['name']
            headers = result_set['headers']
            rows = result_set['rowSet']
            # Remove parameters with value = None
            # Create a pandas table with parameter name as column name and value as row.
            # The table has same shape as the data table
            paramsTable = pd.DataFrame(reqParams, index=range(len(rows)))
            pandaTable = pd.DataFrame(rows, columns=headers)
            # Concat both tables ie:- Add req params as columns to the beginning of the table.
            retVal[name] = pandas.concat([paramsTable, pandaTable], axis=1)
    return retVal


@ray.remote
def fetch_data_parallel(endpoint: str, params: dict):
    return fetch_data(endpoint, params)

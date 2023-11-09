import argparse
import importlib
from functools import reduce
from pathlib import Path

import pandas
import pandas as pd
from deltalake import write_deltalake
import ray
from nba_dataloader import DataFetcher as df

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='__main__.py',
        description="Downloads data from stats.nba.com and persists on disk as delta tables",
    )
    parser.add_argument("resource", help="Will make a request to --> https://stats.nba.com/stats/<endpoint>")
    parser.add_argument("--params",
                        help="A python module containing variable 'params' for the query")
    parser.add_argument("--partition_by", help="The column to partition by", default=None)
    parser.add_argument("--mode", help="The write mode", choices=['overwrite', 'append', "error", "ignore"],
                        default='append')
    parser.add_argument("--location", help="Location to write the fetched data, defaults to tmp/", default="tmp/")
    args = parser.parse_args()
    endpoint = args.resource
    paramsModule = args.params if args.params is not None else f"request_params.{endpoint}_params"
    p = importlib.import_module(paramsModule)
    params = p.params

    MAX_NUM_PENDING_TASKS = 4
    refs = list()
    ray.init()
    for param in params:
        if len(refs) > MAX_NUM_PENDING_TASKS:
            ray.wait(refs, num_returns=MAX_NUM_PENDING_TASKS)
        refs.append(df.fetch_data_parallel.remote(endpoint, param))

    tableDicts = ray.get(refs)
    ray.shutdown()

    singleTable = pd.DataFrame(tableDicts)
    for colName, values in singleTable.items():
        if colName != "STATUS" and colName != "HASH" and colName != "RAW":
            deltaPath = Path(fr"{args.location}/{colName}")
            table_to_write = pandas.concat(list(values))
            write_deltalake(deltaPath, table_to_write, mode=args.mode, partition_by=args.partition_by)
            print(table_to_write)

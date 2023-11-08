import argparse
import importlib
from pathlib import Path

from deltalake import write_deltalake
import ray
import DataFetcher as df

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

    for tableDict in tableDicts:
        # write raw resp to args.location + "/raw"
        rawPath = Path(fr"{args.location}/raw/{tableDict['HASH']}")
        rawPath.parent.mkdir(exist_ok=True, parents=True)
        rawPath.write_text(tableDict['RAW'])
        for key, table in tableDict.items():
            if key == 'STATUS' or key == 'HASH' or key == 'RAW':
                continue
            else:
                deltaPath = Path(fr"{args.location}/{key}")
                write_deltalake(deltaPath, table, mode=args.mode, partition_by=args.partition_by)
                print(table)
# nba_dataloader
A python client to query the various stats.nba.com resources and download data into delta lake tables for further analysis. I started this project to mainly explore and get familiar with a number of different technologies mainly:-
- Ray.io
- Delta Lake
- Advanced Python

A simple client capable of querying various stats.nba.com endpoints and storing the data onto disk as Delta/Parquet tables. The client can query a given endpoint with multiple different query parameters in parallel, using Ray tasks. Documentation of the various endpoints can be found [here](https://any-api.com/nba_com/nba_com/docs/API_Description)


## Usage
### Show help
```
py -m nba_dataloader --help
usage: __main__.py [-h] [--params PARAMS] [--partition_by PARTITION_BY] [--mode {overwrite,append,error,ignore}]
                   [--location LOCATION]
                   resource

Downloads data from stats.nba.com and persists on disk as delta tables

positional arguments:
  resource              Will make a request to --> https://stats.nba.com/stats/<endpoint>

options:
  -h, --help            show this help message and exit
  --params PARAMS       A python module containing variable 'params' for the query
  --partition_by PARTITION_BY
                        The column to partition by
  --mode {overwrite,append,error,ignore}
                        The write mode
  --location LOCATION   Location to write the fetched data, defaults to tmp/
```
### Fetch data from resource endpoint "CommonTeamYears"
```
py -m nba_dataloader commonTeamYears --params my_request_params
```
The ```--params``` parameter accepts a python file or module that contains a variable ```param: list[dict]``` in the above example the contents of ```my_request_params``` is
```python
params = [{
    "LeagueID":"00"
}]
```
The parameters specified in the file are used to make a web service request to ```https://stats.nba.com/commonTeamYears``` with the appropriate request headers. The response json is parsed and converted into a delta table that is stored in the folder ```tmp/``` of the directory in which the script was run. This default location can be overridden using the ```---location``` command line parameter.

### Fetch data for multiple seasons from the resource "leaguedashplayerstats"
Note that params is an array of dict, if multiple dicts are provided each of the dict objects corresponds to a separate web service request and each response is appended to a single delta table.
```
py -m nba_dataloader leaguedashplayerstats --params multiple_season_params
```
Contents of ```multiple_season_params.py``` is:
```python
base_params_dict = {
        "LastNGames": 0,
        "LeagueID": "00",
        "MeasureType": "Base",
        "Month": 0,
        "OpponentTeamID": 0,
        "PORound": 0,
        "PaceAdjust": "N",
        "PerMode": "Totals",
        "Period": 0,
        "PlusMinus": "N",
        "Rank": "N",
        "SeasonType": "Regular Season",
        "TeamID": 0
}
seasons = {'1996-97', '1997-98', '1998-99', '1999-00', '2000-01', '2001-02', '2002-03', '2003-04', '2004-05',
           '2005-06', '2006-07', '2007-08', '2008-09', '2009-10', '2010-11', '2011-12', '2012-13', '2013-14',
           '2014-15', '2015-16', '2016-17', '2017-18', '2018-19', '2019-20', '2020-21', '2021-22', '2022-23'}
params = map(lambda season: {'Season': season} | base_params_dict, seasons)
```

In the above code params is an array of dicts constructed by adding a new attribute 'Season:<value>' to the ```base_params_dict``` for each of the seasons and building a list of dicts

The resulting value of ```params``` is
```python
params = [
{
        "LastNGames": 0,
        "LeagueID": "00",
        "MeasureType": "Base",
        "Month": 0,
        "OpponentTeamID": 0,
        "PORound": 0,
        "PaceAdjust": "N",
        "PerMode": "Totals",
        "Period": 0,
        "PlusMinus": "N",
        "Rank": "N",
        "SeasonType": "Regular Season",
        "TeamID": 0,
        "Season": "1996-97" # <--- Note the new attribute season
},
{
        "LastNGames": 0,
        "LeagueID": "00",
        "MeasureType": "Base",
        "Month": 0,
        "OpponentTeamID": 0,
        "PORound": 0,
        "PaceAdjust": "N",
        "PerMode": "Totals",
        "Period": 0,
        "PlusMinus": "N",
        "Rank": "N",
        "SeasonType": "Regular Season",
        "TeamID": 0,
        "Season":"1997-98"
}
{},{}..
]

```

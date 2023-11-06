# nba_dataloader
A python client to query the various stats.nba.com resources and download data into delta lake tables for further analysis. I started this project to mainly explore and get familiar with a number of different technologies mainly:-
- Ray.io
- Delta Lake
- Advanced Python

A simple client capable of querying various stats.nba.com endpoints and storing the data onto disk as Delta/Parquet tables. The client can query a given endpoint with multiple different query parameters in parallel, using Ray tasks. Documentation of the various endpoints can be found [here](https://any-api.com/nba_com/nba_com/docs/API_Description)

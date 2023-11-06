default_params = {
    "LeagueID": "00",
    "IsOnlyCurrentSeason": 1
}

seasons = {'1996-97', '1997-98', '2022-23'}
params = map(lambda season: {'Season': season} | default_params, seasons)
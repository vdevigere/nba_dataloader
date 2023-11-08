default_params = {
    "LastNGames": 0,
    "LeagueID": "00",
    "MeasureType": "Base",
    "Month": 0,
    "OpponentTeamID": 0,
    "PORound": 0,
    "PaceAdjust": "N",
    "PerMode": "PerGame",
    "Period": 0,
    "PlayerID": 764,
    "PlusMinus": "N",
    "Rank": "N",
    "SeasonType": "Regular Season"
}
seasons = {'1996-97', '1997-98'}
params = map(lambda season: {'Season': season} | default_params, seasons)
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
    "PlusMinus": "N",
    "Rank": "N",
    "SeasonType": "Regular Season",
    "TeamID": 0,
    "TwoWay": 0,
}

seasons = {'1996-97', '1997-98', '1998-99', '1999-00', '2000-01', '2001-02', '2002-03', '2003-04', '2004-05',
           '2005-06', '2006-07', '2007-08', '2008-09', '2009-10', '2010-11', '2011-12', '2012-13', '2013-14',
           '2014-15', '2015-16', '2016-17', '2017-18', '2018-19', '2019-20', '2020-21', '2021-22', '2022-23'}
params = map(lambda season: {'Season': season} | default_params, seasons)
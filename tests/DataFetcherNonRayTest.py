import unittest

import pandas.core.frame
import pandas as pd

from nba_dataloader import DataFetcher as df


class DataFetcherNonRayTest(unittest.TestCase):

    @classmethod
    def setUp(self):
        pd.set_option('display.max_columns', None)

    def test_fetch_team_stats(self):
        season = '1997-98'
        input_params = {
            'Season': season,
            'Conference': 'East'
        }
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
            "Season": "2022-23",
            "SeasonType": "Regular Season",
            "TeamID": 0,
            "TwoWay": 0,
        }
        params = default_params | input_params
        table = df.fetch_data(endpoint='LeagueDashTeamStats', params=params)
        self.assertEqual(200, table['STATUS'])
        self.assertSequenceEqual(['STATUS', 'LeagueDashTeamStats'], list(table.keys()))
        self.assertIsInstance(table['LeagueDashTeamStats'], pandas.core.frame.DataFrame)
        self.assertEqual(len(table['LeagueDashTeamStats']), 15)
        print(table['LeagueDashTeamStats'])

    def test_fetch_player_stats(self):
        season = '1997-98'
        input_params = {'Season': season}
        default_params = {
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
            "Season": "2022-23",
            "SeasonType": "Regular Season",
            "TeamID": 0
        }
        params = default_params | input_params
        table = df.fetch_data(endpoint="leaguedashplayerstats", params=params)
        self.assertEqual(200, table['STATUS'])
        self.assertSequenceEqual(['STATUS', 'LeagueDashPlayerStats'], list(table.keys()))
        self.assertIsInstance(table['LeagueDashPlayerStats'], pandas.core.frame.DataFrame)
        self.assertEqual(len(table['LeagueDashPlayerStats']), 439)
        print(table['LeagueDashPlayerStats'])


if __name__ == '__main__':
    unittest.main()

import requests
import pandas as pd

ODDS_API_KEY = '4cdf14bb60204a84c8f98b5004de32b4'
ODDS_API_BASE = 'https://api.the-odds-api.com/v4/sports/basketball_nba/odds'

def get_nba_player_props():
    resp = requests.get(
        ODDS_API_BASE,
        params={
            'apiKey': ODDS_API_KEY,
            'regions': 'us',
            'markets': 'player_points',
            'oddsFormat': 'decimal'
        }
    )
    return pd.json_normalize(resp.json())

def get_player_props():
    # call the api, filter for nba and player_points market
    df = requests.get(
        ODDS_API_BASE,
        params={
            'apiKey': '4cdf14bb60204a84c8f98b5004de32b4',
            'regions': 'us',
            'markets': 'player_points',
            'oddsFormat': 'decimal'
        }
    )
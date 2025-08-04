import requests
import pandas as pd

ODDS_API_KEY = '4cdf14bb60204a84c8f98b5004de32b4'
ODDS_API_BASE = 'https://api.the-odds-api.com/v4/sports/basketball_nba/odds'

def get_player_props():
    res = requests.get(
        ODDS_API_BASE,
        params={
            'apiKey': ODDS_API_KEY,
            'regions': 'us',
            'markets': 'player_props',
            'oddsFormat': 'decimal'
        }
    )

    if res.status_code != 200:
        raise Exception(f"Error fetching data: {res.status_code} - {res.text}")
    
    data = res.json()
    df = pd.json_normalize(data)
    return df

if __name__ == "__main__":
    df = get_player_props()
    df.to_csv('Data/raw/nba_player_props.csv', index=False)

import requests
import pandas as pd
import os
from datetime import datetime, timedelta

NBA_API_BASE = 'https://www.balldontlie.io/api/v1/'

def get_player_game_stats(player_id, start_date, end_date):
    stats = []
    page = 1
    while True:
        resp = requests.get(
            f"{NBA_API_BASE}/stats",
            params={
                'player_ids[]': player_id,
                'start_date': start_date,
                'end_date': end_date,
                'per_page': 100,
                'page': page
            }
        ).json()
        stats.extend(resp['data'])
        if resp['meta']['next_page'] is None:
            break
        page += 1
    return pd.json_normalize(stats)

def search_players_by_name(name):
    resp = requests.get(
        f"{NBA_API_BASE}/players",
        params={'search': name}
    )
    return pd.json_normalize(resp.json()['data'])

players = search_players_by_name('LeBron James')
lebron_id = players.iloc[0]['id']

df = get_player_game_stats(lebron_id, "2023-01-01", "2025-01-01")

os.makedirs('data/raw', exist_ok=True)
df.to_csv('data/raw/lebron_game_stats.csv', index=False)

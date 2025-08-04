import requests
import pandas as pd
import os
from datetime import datetime, timedelta

NBA_API_BASE = 'https://nba.balldontlie.io/v1'

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
    url = f"{NBA_API_BASE}/players"
    resp = requests.get(
        url,
        params={
            'search': name,
        }
    )

    if resp.status_code != 200:
        print(f"Request failed:", resp.status_code, resp.text)
        return None
    
    try:
        return pd.json_normalize(resp.json()['data'])
    except Exception as e:
        print("Failed to decode JSON:", e)
        return None
    
def get_all_players():
    players = []
    page = 1

    while True:
        resp = requests.get(
            f"{NBA_API_BASE}/players",
            params={'page': page, 'per_page': 100}
        )
        data = resp.json()
        players.extend(data['data'])

        if data['meta']['next_page'] is None:
            break
        page += 1

    return pd.json_normalize(players)

if __name__ == "__main__":
    # Get all players and filter out active ones
    all_players_df = get_all_players()
    active_players_df = all_players_df[all_players_df['team.full_name'].notna()].copy()

    player_stats = []

    #loop through player and fetch game stats
    for idx, row in active_players.iterrows():
        player_id = row
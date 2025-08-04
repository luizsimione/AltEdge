import requests
import pandas as pd
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import html
import json

load_dotenv()

NBA_API_KEY = os.getenv("NBA_API_KEY")
Headers = {"Authorization": NBA_API_KEY}
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
    cursor = None

    while True:
        params = {'per_page': 100}
        if cursor:
            params['cursor'] = cursor

        response = requests.get(f"{NBA_API_BASE}/players", params=params, headers=Headers)

        print(f"Status Code: {response.status_code}")
                # Try cleaning HTML if JSON decoding fails
        if response.status_code != 200:
            # Clean out any HTML tags (e.g., <span>) if present
            soup = BeautifulSoup(response.text, "html.parser")
            cleaned_text = soup.get_text()
            print(f"Cleaned response:\n{cleaned_text}")
            break

        try:
            data = response.json()
        except ValueError:
            # Fallback: attempt to clean and re-parse
            soup = BeautifulSoup(response.text, "html.parser")
            cleaned_text = soup.get_text()
            print("Original response could not be parsed as JSON. Cleaned text:")
            print(cleaned_text)
            data = json.loads(cleaned_text)

        players.extend(data['data'])
        
        cursor = data['meta'].get('next_cursor')
        if not cursor:
            break

    return pd.json_normalize(players)

if __name__ == "__main__":
    df = get_all_players()
    print(f"Fetched {len(df)} players")

    os.makedirs('Data/raw', exist_ok=True)
    df.to_csv('Data/raw/all_players.csv', index=False)
import requests
import pandas as pd
import os
from datetime import datetime, timedelta
import html
import json
from bs4 import BeautifulSoup
from pathlib import Path

NBA_API_BASE = 'https://www.balldontlie.io/api/v1'

def get_all_players():
    players = []
    page = 1

    while True:
        try:
            resp = requests.get(f"{NBA_API_BASE}/players", params={"per_page": 100, "page": page}, timeout=10)
            if resp.status_code != 200:
                soup = BeautifulSoup(resp.text, "html.parser")
                print(f"Request failed: {resp.status_code}")
                print(soup.get_text())
                break
            try:
                data = resp.json()
            except json.JSONDecodeError:
                soup = BeautifulSoup(resp.text, "html.parser")
                cleaned_text = soup.get_text()
                data = json.loads(cleaned_text)
        except requests.RequestException as e:
            print(f"Request error: {e}")
            break

        batch = data.get('data', [])
        if not batch:
            break
        players.extend(batch)

        if page >= data['meta']['total_pages']:
            break
        page += 1

    df = pd.json_normalize(players)
    df.columns = df.columns.str.lower().str.replace(" ", "_")
    df = df.drop_duplicates(subset=["id"])
    print(f"Total players fetched: {len(df)}")
    return df

def get_player_game_stats(player_id, start_date, end_date):
    stats = []
    page = 1

    while True:
        try:
            resp = requests.get(
                f"{NBA_API_BASE}/stats",
                params={
                    'player_ids[]': player_id,
                    'start_date': start_date,
                    'end_date': end_date,
                    'per_page': 100,
                    'page': page
                },
                timeout=10
            )
            if resp.status_code != 200:
                soup = BeautifulSoup(resp.text, "html.parser")
                print(f"Request failed: {resp.status_code}")
                print(soup.get_text())
                break

            try:
                data = resp.json()
            except json.JSONDecodeError:
                soup = BeautifulSoup(resp.text, "html.parser")
                cleaned_text = soup.get_text()
                data = json.loads(cleaned_text)

        except requests.RequestException as e:
            print(f"Request error: {e}")
            break

        batch = data.get('data', [])
        stats.extend(batch)
        if page >= data['meta']['total_pages']:
            break
        page += 1

    df = pd.json_normalize(stats) if stats else pd.DataFrame()
    if not df.empty:
        df.columns = df.columns.str.lower().str.replace(" ", "_")
    return df

def get_all_game_stats(start_date, end_date):
    df_players = get_all_players()
    all_stats = []

    for idx, player in df_players.iterrows():
        df_stats = get_player_game_stats(player['id'], start_date, end_date)
        if not df_stats.empty:
            all_stats.append(df_stats)
        print(f"Fetched stats for {player['first_name']} {player['last_name']} ({idx+1}/{len(df_players)})")

    return pd.concat(all_stats, ignore_index=True) if all_stats else pd.DataFrame()

if __name__ == "__main__":
    raw_path = 'Data/raw'
    os.makedirs(raw_path, exist_ok=True)  

    df_players = get_all_players()
    df_players.to_csv(f'{raw_path}/all_players.csv', index=False)
    print("Saved all players to Data/raw/all_players.csv")

    start_date = "2023-10-01"
    end_date = "2024-04-15"
    df_stats = get_all_game_stats(start_date, end_date)
    df_stats.to_csv(f'{raw_path}/game_stats_2023_24.csv', index=False)
    print("Saved game stats to Data/raw/game_stats_2023_24.csv")


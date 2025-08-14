import requests
import pandas as pd
import os
from datetime import datetime, timedelta
import html
import json
from bs4 import BeautifulSoup
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
NBA_API_KEY = os.getenv("NBA_API_KEY")
NBA_API_BASE = 'https://www.balldontlie.io/api/v1/players?per_page=100'

HEADERS = {
    "Authorization": f"Bearer {NBA_API_KEY}"
}

def get_all_players():
    print("[DEBUG] Starting player fetch...")
    players = []
    page = 1

    while True:
        print(f"[DEBUG] Fetching player page {page}...")
        try:
            resp = requests.get(f"{NBA_API_BASE}/players", params={"per_page": 100, "page": page}, timeout=10)
            if resp.status_code != 200:
                print(f"[ERROR] Request failed: {resp.status_code}")
                print(resp.text)
                break

            try:
                data = resp.json()
            except json.JSONDecodeError:
                soup = BeautifulSoup(resp.text, "html.parser")
                cleaned_text = soup.get_text()
                data = json.loads(cleaned_text)

        except requests.RequestException as e:
            print(f"[ERROR] Request error: {e}")
            break

        batch = data.get('data', [])
        print(f"[DEBUG] Retrieved {len(batch)} players on this page.")
        if not batch:
            break

        players.extend(batch)
        if page >= data['meta']['total_pages']:
            break
        page += 1

    df = pd.json_normalize(players)
    df.columns = df.columns.str.lower().str.replace(" ", "_")
    df = df.drop_duplicates(subset=["id"])
    print(f"[DEBUG] Total players fetched: {len(df)}")
    return df

def get_player_game_stats(player_id, start_date, end_date):
    stats = []
    page = 1
    print(f"[DEBUG] Fetching stats for player {player_id} ({start_date} → {end_date})")

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
                print(f"[ERROR] Stats request failed for player {player_id}: {resp.status_code}")
                print(resp.text)
                break

            try:
                data = resp.json()
            except json.JSONDecodeError:
                soup = BeautifulSoup(resp.text, "html.parser")
                cleaned_text = soup.get_text()
                data = json.loads(cleaned_text)

        except requests.RequestException as e:
            print(f"[ERROR] Stats request error for player {player_id}: {e}")
            break

        batch = data.get('data', [])
        print(f"[DEBUG] Retrieved {len(batch)} stat records on page {page} for player {player_id}")
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

    print(f"[DEBUG] Fetching game stats for {len(df_players)} players...")
    for idx, player in df_players.iterrows():
        df_stats = get_player_game_stats(player['id'], start_date, end_date)
        if not df_stats.empty:
            all_stats.append(df_stats)
        print(f"[DEBUG] Progress: {idx+1}/{len(df_players)} players processed")

    if not all_stats:
        print("[WARNING] No stats fetched for any player.")
        return pd.DataFrame()

    combined = pd.concat(all_stats, ignore_index=True)
    print(f"[DEBUG] Total stat records combined: {len(combined)}")
    return combined

if __name__ == "__main__":
    print("[DEBUG] Starting NBA stats scrape...")

    raw_path = Path('Data/raw')
    if not raw_path.exists():
        raw_path.mkdir(parents=True, exist_ok=True)
    elif not raw_path.is_dir():
        raise Exception(f"Path {raw_path} exists but is not a directory.")

    # Step 1: Get players
    df_players = get_all_players()
    df_players.to_csv(raw_path / 'all_players.csv', index=False)
    print(f"[DEBUG] Saved all players to {raw_path / 'all_players.csv'}")

    # Step 2: Get game stats
    start_date = "2023-10-01"
    end_date = "2024-04-15"
    df_stats = get_all_game_stats(start_date, end_date)
    df_stats.to_csv(raw_path / 'game_stats_2023_24.csv', index=False)
    print(f"[DEBUG] Saved game stats to {raw_path / 'game_stats_2023_24.csv'}")

    print("[DEBUG] NBA stats scrape completed successfully.")

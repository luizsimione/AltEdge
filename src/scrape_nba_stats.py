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
    """Fetch all NBA players from the API with proper pagination and error handling."""
    players = []
    cursor = None

    while True:
        params = {'per_page': 100}
        if cursor:
            params['cursor'] = cursor

        try:
            response = requests.get(
                f"{NBA_API_BASE}/players",
                params=params,
                headers=Headers,
                timeout=10
            )
        except requests.RequestException as e:
            print(f"Request error: {e}")
            break

        # Log status for debugging
        print(f"Status Code: {response.status_code}")

        if response.status_code != 200:
            # Attempt to clean HTML error pages if present
            soup = BeautifulSoup(response.text, "html.parser")
            cleaned_text = soup.get_text()
            print(f"Request failed: {response.status_code}")
            print(f"Cleaned response:\n{cleaned_text}")
            break

        # Log partial raw response for debugging
        print(f"First 300 chars of response: {response.text[:300]}")

        try:
            data = response.json()
        except json.JSONDecodeError:
            # Fallback: attempt to clean and re-parse
            soup = BeautifulSoup(response.text, "html.parser")
            cleaned_text = soup.get_text()
            print("Could not parse JSON. Cleaned text:")
            print(cleaned_text)
            try:
                data = json.loads(cleaned_text)
            except json.JSONDecodeError:
                print("Still could not parse response. Stopping.")
                break

        # Log how many players we got in this batch
        batch_count = len(data.get('data', []))
        print(f"Retrieved {batch_count} players in this batch.")

        players.extend(data.get('data', []))

        cursor = data.get('meta', {}).get('next_cursor')
        if not cursor:
            break

    # Convert to DataFrame
    df = pd.json_normalize(players)

    if isinstance(df, pd.DataFrame) and not df.empty:
        df.columns = df.columns.map(str).str.lower().str.replace(" ", "_")
        df = df.drop_duplicates(subset=["id"])
        print(f"Final player count: {len(df)}")
    else:
        print("No player data returned")

    return df


def get_advanced_stats(start_season=2023, end_season=2024):
    """Fetch advanced NBA stats for a given season range."""
    all_stats = []
    cursor = None

    for season in range(start_season, end_season + 1):
        print(f"Fetching advanced stats for season {season}...")
        cursor = None

        while True:
            params = {
                'per_page': 100,
                'seasons[]': season
            }
            if cursor:
                params['cursor'] = cursor

            try:
                response = requests.get(
                    f"{NBA_API_BASE}/stats/advanced",
                    params=params,
                    headers=Headers,
                    timeout=10
                )
            except requests.RequestException as e:
                print(f"Request error: {e}")
                break

            if response.status_code != 200:
                soup = BeautifulSoup(response.text, "html.parser")
                print(f"Request failed: {response.status_code}")
                print(soup.get_text())
                break

            try:
                data = response.json()
            except json.JSONDecodeError:
                soup = BeautifulSoup(response.text, "html.parser")
                cleaned_text = soup.get_text()
                print("Could not parse JSON. Cleaned text:")
                print(cleaned_text)
                try:
                    data = json.loads(cleaned_text)
                except json.JSONDecodeError:
                    print("Still could not parse. Stopping.")
                    break

            all_stats.extend(data.get('data', []))

            cursor = data.get('meta', {}).get('next_cursor')
            if not cursor:
                break

    df_stats = pd.json_normalize(all_stats)
    df_stats.columns = df_stats.columns.str.lower().str.replace(" ", "_")
    return df_stats


if __name__ == "__main__":
    # Step 1 — Players
    df_players = get_all_players()
    print(f"Fetched {len(df_players)} unique players.")
    os.makedirs('Data/raw', exist_ok=True)
    df_players.to_csv('Data/raw/all_players.csv', index=False)

    # Step 2 — Advanced Stats
    df_stats = get_advanced_stats(start_season=2023, end_season=2024)
    print(f"Fetched {len(df_stats)} advanced stats records.")
    df_stats.to_csv('Data/raw/advanced_stats.csv', index=False)

    print("All data saved in Data/raw/")
 
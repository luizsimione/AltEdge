import os
import time
import re
import requests
import pandas as pd
import datetime
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("NBA_API_KEY")
BASE_URL = "https://api.balldontlie.io/v1"  # <- correct base

HEADERS = {"Authorization": API_KEY} if API_KEY else {}

def _request(url, params, max_retries=5):
    """Robust GET with basic retry/backoff, esp. for 429 on free tier."""
    attempt = 0
    while True:
        resp = requests.get(url, headers=HEADERS, params=params, timeout=30)
        if resp.status_code == 200:
            return resp.json()
        # free tier: 5 req/min → 429 likely
        if resp.status_code in (429, 503) and attempt < max_retries:
            wait = 12 if resp.status_code == 429 else 5
            print(f"[WARN] {resp.status_code} - backing off {wait}s (attempt {attempt+1}/{max_retries})")
            time.sleep(wait)
            attempt += 1
            continue
        # anything else: surface error and stop
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:500]}")

def get_all_players(per_page=100, max_pages=None):
    """
    Fetch ALL NBA players using cursor pagination.
    Returns a flattened pandas DataFrame.
    """
    if not API_KEY:
        raise RuntimeError("Missing NBA_API_KEY in environment. Put it in your .env as NBA_API_KEY=...")

    print("[DEBUG] Starting player fetch...")

    url = f"{BASE_URL}/players"
    cursor = None
    players = []
    pages = 0

    while True:
        params = {"per_page": per_page}
        if cursor is not None:
            params["cursor"] = cursor

        data = _request(url, params)

        batch = data.get("data", [])
        if not batch:
            print("[DEBUG] No more data found, stopping.")
            break

        players.extend(batch)
        pages += 1
        print(f"[DEBUG] Fetched {len(batch)} players (total: {len(players)})")

        # stop if the API does not return a next cursor
        cursor = data.get("meta", {}).get("next_cursor")
        if not cursor:
            print("[DEBUG] No next_cursor, completed pagination.")
            break

        if max_pages and pages >= max_pages:
            print(f"[DEBUG] Reached max_pages={max_pages}, stopping early.")
            break

    if not players:
        # Nothing came back; avoid column ops on empty DF
        return pd.DataFrame()

    # Flatten nested team fields (team.id -> team_id, etc.)
    df = pd.json_normalize(players, sep="_")

    # Clean column names to snake_case with underscores only
    def clean(name: str) -> str:
        name = str(name).lower()
        name = re.sub(r"[^0-9a-z]+", "_", name)
        return name.strip("_")

    df.columns = [clean(c) for c in df.columns]
    return df


if __name__ == "__main__":
    print("[DEBUG] Starting NBA stats scrape...")
    try:
        df_players = get_all_players(per_page=100)  # per_page max is 100
    except Exception as e:
        print(f"[ERROR] {e}")
        raise

    print("[DEBUG] Finished fetching players.")
    print(df_players.head(10))
    print(f"[DEBUG] Total players collected: {len(df_players)}")

    # Optional: save to disk for downstream steps
    out_path = "data/players.csv"
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    df_players.to_csv(out_path, index=False)
    print(f"[DEBUG] Saved players to {out_path}")


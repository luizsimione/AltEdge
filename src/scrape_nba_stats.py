import requests
import pandas as pd
import time

# List of 10 All-NBA players with their NBA.com player IDs
# These IDs must correspond to NBA.com’s internal player IDs
players = {
    "Luka Doncic": 1629029,
    "Joel Embiid": 203954,
    "Giannis Antetokounmpo": 203507,
    "Kevin Durant": 201142,
    "Jayson Tatum": 1628369,
    "Ja Morant": 1629630,
    "Anthony Edwards": 2734395,
    "Shai Gilgeous-Alexander": 1627783,
    "LeBron James": 2544,
    "Damian Lillard": 203081
}

season = "2024-25"
all_stats = []

headers = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://www.nba.com/"
}

for name, player_id in players.items():
    print(f"[INFO] Fetching stats for {name}")
    url = f"https://data.nba.com/data/v2015/json/mobile_teams/nba/{season}/players/player_{player_id}_gamelog.json"
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(f"[ERROR] Failed to fetch {name}: {response.status_code}")
            continue
        
        data = response.json()
        # Extract games — structure may vary slightly, adapt as needed
        games = data.get("g", [])
        for game in games:
            stats = {
                "player": name,
                "game_date": game.get("gd"),
                "opponent": game.get("opp"),
                "pts": game.get("pts"),
                "reb": game.get("reb"),
                "ast": game.get("ast"),
                "fgm": game.get("fgm"),
                "fga": game.get("fga"),
                "3pm": game.get("tpm"),
                "3pa": game.get("tpa"),
                "stl": game.get("stl"),
                "blk": game.get("blk"),
                "turnovers": game.get("to"),
                "minutes": game.get("min")
            }
            all_stats.append(stats)
        
        time.sleep(1)  # be polite to NBA.com
    except Exception as e:
        print(f"[ERROR] Exception fetching {name}: {e}")

# Save all stats to CSV
df = pd.DataFrame(all_stats)
df.to_csv("all_nba_10_players_stats.csv", index=False)
print("[INFO] Saved all stats to all_nba_10_players_stats.csv")

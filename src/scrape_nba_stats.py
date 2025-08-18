import requests
import csv
from datetime import datetime, timedelta
import os

# ===== CONFIG =====
 # put your key here
PLAYERS = {
    "Luka Doncic": 203507,
    "Joel Embiid": 203954,
    "Giannis Antetokounmpo": 201587,
    "Kevin Durant": 201142,
    "Jayson Tatum": 1628369,
    "Ja Morant": 1629630,
    "Anthony Edwards": 39073827,
    "Shai Gilgeous-Alexander": 1628963,
    "LeBron James": 2544,
    "Damian Lillard": 203081
}
START_DATE = "2025-01-01"  # adjust to season start
END_DATE = datetime.today().strftime("%Y-%m-%d")

OUTPUT_DIR = "Data/raw"
os.makedirs(OUTPUT_DIR, exist_ok=True)
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "all_nba_10_players_stats.csv")

# ===== HELPER =====
def daterange(start_date, end_date):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    for n in range((end - start).days + 1):
        yield start + timedelta(n)

# ===== MAIN =====
all_stats = []

for player_name, player_id in PLAYERS.items():
    print(f"[INFO] Fetching stats for {player_name}")
    for single_date in daterange(START_DATE, END_DATE):
        date_str = single_date.strftime("%Y-%m-%d")
        url = f"https://api.sportsdata.io/v3/nba/stats/json/PlayerGameStatsByPlayer/{date_str}/{player_id}?key={API_KEY}"
        try:
            response = requests.get(url)
            if response.status_code == 200:
                try:
                    data = response.json()
                    if data:
                        data["PlayerName"] = player_name
                        all_stats.append(data)
                except ValueError:
                    continue  # skip empty/invalid JSON
            elif response.status_code in [403, 404]:
                # skip forbidden or no-game days silently
                continue
            else:
                print(f"[ERROR] {player_name} {date_str} status: {response.status_code}")
        except requests.RequestException as e:
            print(f"[ERROR] Request failed for {player_name} on {date_str}: {e}")

if all_stats:
    # Save CSV
    keys = sorted(all_stats[0].keys())
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(all_stats)
    print(f"[INFO] Saved all stats to {OUTPUT_FILE}")
else:
    print("[WARNING] No stats to save!")

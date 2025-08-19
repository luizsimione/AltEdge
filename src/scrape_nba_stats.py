import requests
import csv
import os
from dotenv import load_dotenv

# ===== LOAD ENV =====
load_dotenv()
API_KEY = os.getenv("SPORTSDATA_API_KEY")
if not API_KEY:
    raise ValueError("API_KEY not found in .env file")

# ===== CONFIG =====
OUTPUT_DIR = "Data/raw"
os.makedirs(OUTPUT_DIR, exist_ok=True)

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

SEASON = 2024  # Change to the season you want

# ===== FETCH DATA =====
for player_name, player_id in PLAYERS.items():
    print(f"[INFO] Fetching stats for {player_name}")
    url = f"https://api.sportsdata.io/v3/nba/stats/json/PlayerSeasonStats/{SEASON}/{player_id}?key={API_KEY}"

    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            if not data:
                print(f"[WARNING] No data found for {player_name}")
                continue

            # Add player name to each game entry
            for game in data:
                game["PlayerName"] = player_name

            # Save CSV per player
            output_file = os.path.join(OUTPUT_DIR, f"{player_name.replace(' ', '_')}_stats.csv")
            keys = sorted(data[0].keys())
            with open(output_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=keys)
                writer.writeheader()
                writer.writerows(data)

            print(f"[INFO] Saved stats for {player_name} to {output_file}")
        else:
            print(f"[ERROR] {player_name} status: {response.status_code}")

    except requests.RequestException as e:
        print(f"[ERROR] Request failed for {player_name}: {e}")
#


# ===== CONFIG =====
PLAYER_NAME = "Luka Doncic"
OUTPUT_DIR = "data/raw"
os.makedirs(OUTPUT_DIR, exist_ok=True)
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "luka_doncic_one_game.csv")

# Luka Doncic 2023-24 game log
URL = "https://www.basketball-reference.com/players/d/doncilu01/gamelog/2024"

# ===== SCRAPE =====
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/116.0.0.0 Safari/537.36"
}

response = requests.get(URL, headers=headers)
if response.status_code != 200:
    raise Exception(f"Failed to fetch page: {response.status_code}")

soup = BeautifulSoup(response.content, "html.parser")

# Basketball-Reference sometimes puts tables inside comments
import re
comments = soup.find_all(string=lambda text: isinstance(text, str) and "<table" in text)
table_html = None

for c in comments:
    if "id=\"pgl_basic\"" in c:
        table_html = c
        break

if table_html is None:
    raise Exception("Could not find game log table.")

table_soup = BeautifulSoup(table_html, "html.parser")
table = table_soup.find("table", id="pgl_basic")

df = pd.read_html(str(table))[0]

# Drop repeated headers
df = df[df['G'] != 'G']

# Take first game only
first_game = df.iloc[0:1]

# Save CSV
first_game.to_csv(OUTPUT_FILE, index=False)
print(f"[INFO] Saved one game for {PLAYER_NAME} to {OUTPUT_FILE}")


import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
import os

# ===== PATHS =====
RAW_CSV = "data/raw/Shai Stats - Sheet1.csv"
PROCESSED_DIR = "data/processed"
os.makedirs(PROCESSED_DIR, exist_ok=True)
PROCESSED_CSV = os.path.join(PROCESSED_DIR, "SGA_features.csv")

# ===== LOAD DATA =====
df = pd.read_csv(RAW_CSV)
print("[INFO] Loaded CSV with shape:", df.shape)

# ===== BASIC FEATURE ENGINEERING =====
# Convert date to datetime if exists
if "Date" in df.columns:
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.sort_values("Date")

# Example features: lag stats (previous game performance)
for stat in ["PTS", "AST", "REB"]:
    if stat in df.columns:
        df[f"{stat}_prev"] = df[stat].shift(1)
        df[f"{stat}_prev"].fillna(df[stat].mean(), inplace=True)

# Target variable: Points scored in current game
y = df["PTS"] if "PTS" in df.columns else None
X = df[[col for col in df.columns if "_prev" in col]]

# ===== TRAIN/TEST SPLIT =====
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)

# ===== MODEL =====
model = RandomForestRegressor(n_estimators=100, random_state=42)
model.fit(X_train, y_train)
y_pred = model.predict(X_test)

# ===== EVALUATION =====
mse = mean_squared_error(y_test, y_pred)
print(f"[INFO] Test MSE: {mse:.2f}")

# ===== SAVE FEATURES (OPTIONAL) =====
df.to_csv(PROCESSED_CSV, index=False)
print(f"[INFO] Saved processed features to {PROCESSED_CSV}")

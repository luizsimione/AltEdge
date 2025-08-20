# src/model_build.py

import argparse
import os
import sys
import math
import warnings
from datetime import datetime

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error
from scipy.stats import norm

warnings.filterwarnings("ignore")


# ---------- Helpers ----------
def find_col(df, candidates):
    """Return the first existing column (case-insensitive) matching candidates."""
    lower_map = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in lower_map:
            return lower_map[cand.lower()]
    return None


def coerce_numeric(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def build_features(df, date_col, stat_cols):
    """
    Build lag/rolling features for each stat in stat_cols.
    Also adds 'days_rest' from date diffs and a simple recent form indicator.
    """
    df = df.sort_values(by=date_col).reset_index(drop=True)

    # Days rest
    df["_game_date_dt"] = pd.to_datetime(df[date_col], errors="coerce")
    df["days_rest"] = df["_game_date_dt"].diff().dt.days.fillna(2).clip(lower=0, upper=10)

    # Rolling features per stat
    for stat in stat_cols:
        s = df[stat]
        df[f"{stat}_lag1"] = s.shift(1)
        df[f"{stat}_lag2"] = s.shift(2)
        df[f"{stat}_roll3"] = s.rolling(3).mean().shift(1)
        df[f"{stat}_roll5"] = s.rolling(5).mean().shift(1)
        df[f"{stat}_roll10"] = s.rolling(10).mean().shift(1)
        df[f"{stat}_std5"] = s.rolling(5).std().shift(1)
        df[f"{stat}_std10"] = s.rolling(10).std().shift(1)

    # Recent form proxy (last 3 games total of points if available)
    if "Points" in stat_cols:
        df["form3"] = df["Points"].rolling(3).sum().shift(1)

    # Game number in season
    df["game_no"] = np.arange(1, len(df) + 1)

    # Drop where targets or essential features are missing
    df = df.dropna().reset_index(drop=True)
    df = df.drop(columns=["_game_date_dt"])
    return df


def train_and_eval(time_df, feature_cols, target_col):
    """
    Time-series split (last 20% as test). Train RandomForestRegressor and return
    model, predictions DF, residual std from train.
    """
    n = len(time_df)
    if n < 25:
        raise ValueError(f"Not enough rows after feature building for target '{target_col}' (need ≥ 25, have {n}).")

    split_idx = int(n * 0.8)
    train_df = time_df.iloc[:split_idx].copy()
    test_df = time_df.iloc[split_idx:].copy()

    X_train, y_train = train_df[feature_cols], train_df[target_col]
    X_test, y_test = test_df[feature_cols], test_df[target_col]

    model = RandomForestRegressor(
        n_estimators=400,
        max_depth=None,
        min_samples_leaf=2,
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_train, y_train)

    yhat_train = model.predict(X_train)
    yhat_test = model.predict(X_test)

    mae_train = mean_absolute_error(y_train, yhat_train)
    mae_test = mean_absolute_error(y_test, yhat_test)

    # Residual std on train (for probability modeling)
    resid_std = np.std(y_train - yhat_train)
    resid_std = max(resid_std, 1e-6)

    pred_df = test_df[[date_col_global]].copy()
    pred_df[f"{target_col}_actual"] = y_test.values
    pred_df[f"{target_col}_pred"] = yhat_test

    metrics = {
        "mae_train": mae_train,
        "mae_test": mae_test,
        "resid_std_train": resid_std,
        "n_train": len(train_df),
        "n_test": len(test_df),
    }
    return model, pred_df, metrics


def prob_over(pred, line, resid_std):
    """Assume Normal(pred, resid_std^2) and compute P(Y > line)."""
    if resid_std <= 0:
        return float(pred > line)
    z = (line - pred) / resid_std
    return float(1.0 - norm.cdf(z))


# ---------- Main ----------
def main(args):
    os.makedirs("Data/processed", exist_ok=True)
    
    # Load CSV
    if not os.path.exists(args.csv):
        print(f"[ERROR] CSV not found: {args.csv}")
        sys.exit(1)

    df_raw = pd.read_csv(args.csv)
    print(f"[INFO] Loaded CSV with shape: {df_raw.shape}")

    # Robust column discovery
    global date_col_global
    date_col_global = find_col(df_raw, ["GameDate", "Date", "GAME_DATE", "game_date"])
    pts_col = find_col(df_raw, ["Points", "PTS", "points", "Pts"])
    ast_col = find_col(df_raw, ["Assists", "AST", "assists"])
    reb_col = find_col(df_raw, ["Rebounds", "REB", "TotalRebounds", "total_rebounds"])

    missing = [name for name, col in [
        ("date", date_col_global), ("points", pts_col), ("assists", ast_col), ("rebounds", reb_col)
    ] if col is None]

    if missing:
        print(f"[ERROR] Could not find required column(s): {', '.join(missing)}")
        print("       Open your CSV and confirm the exact headers. Common ones: Date, Points, Assists, Rebounds")
        sys.exit(1)

    # Coerce numerics (lots of CSVs have 'Inactive' etc.)
    numeric_cols = [pts_col, ast_col, reb_col]
    df = df_raw.copy()
    df = coerce_numeric(df, numeric_cols)

    # Rename to canonical to simplify downstream
    df = df.rename(columns={
        date_col_global: "Date",
        pts_col: "Points",
        ast_col: "Assists",
        reb_col: "Rebounds",
    })
    date_col_global = "Date"

    # Build features
    df_feat = build_features(df, date_col_global, ["Points", "Assists", "Rebounds"])

    # Feature set (shared)
    base_feats = [
        "days_rest", "game_no",
        "Points_lag1", "Points_lag2", "Points_roll3", "Points_roll5", "Points_roll10", "Points_std5", "Points_std10",
        "Assists_lag1", "Assists_lag2", "Assists_roll3", "Assists_roll5", "Assists_roll10", "Assists_std5", "Assists_std10",
        "Rebounds_lag1", "Rebounds_lag2", "Rebounds_roll3", "Rebounds_roll5", "Rebounds_roll10", "Rebounds_std5", "Rebounds_std10",
        "form3",
    ]
    # Keep only columns that exist after dropna
    feature_cols = [c for c in base_feats if c in df_feat.columns]

    reports = []
    preds_join = df_feat[[date_col_global]].iloc[int(len(df_feat)*0.8):].copy()

    targets = [
        ("Points", args.pts_line),
        ("Assists", args.ast_line),
        ("Rebounds", args.reb_line),
    ]

    # Train and evaluate per target
    for target, line in targets:
        try:
            model, pred_df, metrics = train_and_eval(df_feat, feature_cols, target)
        except ValueError as e:
            print(f"[WARN] Skipping {target}: {e}")
            continue

        # Over probabilities on test set
        probs_over = []
        for _, row in pred_df.iterrows():
            p = prob_over(row[f"{target}_pred"], line, metrics["resid_std_train"])
            probs_over.append(p)
        pred_df[f"{target}_over_prob_at_{line}"] = probs_over
        pred_df[f"{target}_recommended_over_{line}"] = (pred_df[f"{target}_over_prob_at_{line}"] >= args.threshold).astype(int)

        # Collect for final CSV
        preds_join = preds_join.merge(pred_df, on=date_col_global, how="left")

        reports.append(
            f"=== {target} ===\n"
            f"Train MAE: {metrics['mae_train']:.3f}\n"
            f"Test  MAE: {metrics['mae_test']:.3f}\n"
            f"Train Residual STD: {metrics['resid_std_train']:.3f}\n"
            f"N train: {metrics['n_train']}, N test: {metrics['n_test']}\n"
            f"Line used: {line}\n"
        )

    # Save outputs
    player_slug = (args.player or "player").strip().lower().replace(" ", "_")
    out_csv = os.path.join("Data/processed", f"{player_slug}_test_predictions.csv")
    out_report = os.path.join("Data/processed", f"{player_slug}_model_report.txt")

    preds_join.to_csv(out_csv, index=False)

    with open(out_report, "w") as f:
        f.write(f"Player: {args.player}\n")
        f.write(f"CSV: {args.csv}\n")
        f.write(f"Rows used (post-feature): {len(df_feat)}\n")
        f.write(f"Features used ({len(feature_cols)}): {', '.join(feature_cols)}\n\n")
        for r in reports:
            f.write(r + "\n")

    print(f"[INFO] Saved predictions: {out_csv}")
    print(f"[INFO] Saved report: {out_report}")

    # Quick console recap
    last_row = preds_join.tail(1)
    if not last_row.empty:
        print("\n=== Latest Game Probabilities (test set last row) ===")
        print(last_row.to_string(index=False))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train simple O/U models for Points/Assists/Rebounds.")
    parser.add_argument("--csv", type=str, required=True, help="Path to player's game-by-game CSV (from Google Sheets export).")
    parser.add_argument("--player", type=str, default="Player", help="Player name for labeling outputs.")
    parser.add_argument("--pts_line", type=float, default=29.5, help="O/U line for points.")
    parser.add_argument("--ast_line", type=float, default=5.5, help="O/U line for assists.")
    parser.add_argument("--reb_line", type=float, default=5.5, help="O/U line for rebounds.")
    parser.add_argument("--threshold", type=float, default=0.55, help="Recommend OVER when prob >= threshold.")
    args = parser.parse_args()

    try:
        main(args)
    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(1)

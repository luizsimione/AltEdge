import pandas as pd
from texblob import TextBlob

def add_sentiment(df, text_col='tweet_text'):
    """
    Adds sentiment polarity score to each row based on a text column (e.g. tweets).
    """
    df['sentiment'] = df[text_col].apply(lambda x: TextBlob(x).sentiment.polarity if isinstance(x, str) else 0)
    return df


def add_rolling_stats(df, player_col='player_id', date_col='game_date', stat_cols=['points', 'rebounds', 'assists'], window=5):
    """
    Adds rolling average stats per player over a specified window of games.
    Assumes df sorted by date ascending.
    """
    df = df.sort_values([player_col, date_col])
    for stat in stat_cols:
        df[f'{stat}_rolling_avg'] = df.groupby(player_col)[stat].transform(lambda x: x.rolling(window, min_periods=1).mean())
    return df

def add_travel_rest_features(df):
    """
    Example placeholder for travel and rest features.
    Assumes df has boolean columns: 'back_to_back', 'crossed_timezones'.
    """
    df['fatigue_score'] = df.apply(
        lambda row: 1 if row.get('back_to_back', False) and row.get('crossed_timezones', False) else 0,
        axis=1
    )
    return df

def add_odds_features(df):
    """
    Placeholder: add features based on odds data.
    Example: difference between opening and current line.
    """
    if 'opening_line' in df.columns and 'current_line' in df.columns:
        df['line_movement'] = df['current_line'] - df['opening_line']
    else:
        df['line_movement'] = 0
    return df

df = add_rolling_stats(df)
df = add_travel_rest_features(df)
#df = add_sentiment(df, text_col='tweet_text')  # only once tweet data is merged
df = add_odds_features(df)

import pandas as pd

def create_over_under_label(df, actual_col, line_col):
    """
    Create a binary label: 1 if actual stat exceeds betting line, else 0.
    """
    df['over_under_label'] = df.apply(
        lambda row: 1 if row[actual_col] > row[line_col] else 0,
        axis=1
    )
    return df

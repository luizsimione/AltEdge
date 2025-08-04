import pandas as pd

def create_over_under_label(df, actual_col, line_col):
    """
    Create a binary label: 1 if actual stat exceeds betting line, else 0.
    """
    # Here goes your logic
    df['over_under_label'] = df.apply(lambda row: 1 if row[actual_col] > row[line_col] else 0, axis=1)
    return df

def create_over_under_label(df, actual_col, line_col):
    df['over_under_label'] = df.apply(
        lambda row: 1 if row[actual_col] > row[line_col] else 0,
        axis=1
    )
    return df

df_labeled = create_over_under_label(df, 'points', 'points_line')
print(df_labeled)



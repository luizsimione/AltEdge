import pandas as pd
from label_data import create_over_under_label  # Make sure this file exists and has your function

data = {
    'player': ['Player A', 'Player B', 'Player C', 'Player D'],
    'points': [25, 18, 30, 22],
    'points_line': [24.5, 20, 29, 22]
}

df = pd.DataFrame(data)
df_labeled = create_over_under_label(df, 'points', 'points_line')
print(df_labeled)

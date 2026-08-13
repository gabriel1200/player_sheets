import pandas as pd

df = pd.read_csv('2018_avg.csv')
df['PLAYER_ID'] = df['PLAYER_ID'].astype(str)
sub = df[df.PLAYER_ID == '201147']

print('rows:', len(sub))

nunique_per_col = sub.nunique()
varying = nunique_per_col[nunique_per_col > 1]

print(f'columns that vary across these rows: {len(varying)}')
print(varying)
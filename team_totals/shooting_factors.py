import pandas as pd

# Example columns

df = pd.read_csv('https://raw.githubusercontent.com/gabriel1200/player_sheets/refs/heads/master/team_totals/2026_team_totals.csv')

# Extract all columns ending with FGA or FGM
fga_cols = [c for c in df.columns if c.endswith('FGA')]
fgm_cols = [c for c in df.columns if c.endswith('FGM')]

# Find common prefixes
pairs = []
for fga in fga_cols:
    prefix = fga[:-3]  # remove 'FGA'
    if prefix[0:3].lower()!='opp':
        match = f"{prefix}FGM"
        if match in fgm_cols:

            pairs.append((fga, match))

print(pairs)
print(len(pairs))

import pandas as pd

df = pd.read_csv('year_files/2026_games.csv')

# Find all dates with missing POTENTIAL_AST
missing_dates = df[df['POTENTIAL_AST'].isna()]['date'].unique()

print("Missing dates:", missing_dates)

# Drop all rows with those dates
df_cleaned = df[~df['date'].isin(missing_dates)]

# Save back to the same file (overwrite)
df_cleaned.to_csv('year_files/2026_games.csv', index=False)

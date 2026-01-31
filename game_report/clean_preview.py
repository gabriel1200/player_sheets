import pandas as pd

df = pd.read_csv('year_files/2026_games.csv')

# Find all dates with missing POTENTIAL_AST
missing_dates = df[df['POTENTIAL_AST'].isna()]['date'].unique()

print("Missing dates:", missing_dates)


print(len(missing_dates))
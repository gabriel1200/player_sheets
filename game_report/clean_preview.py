import pandas as pd
file = 'year_files/2026ps_games.csv'
df = pd.read_csv(file)

# Find all dates with missing POTENTIAL_AST
missing_dates = df[df['POTENTIAL_AST'].isna()]['date'].unique()

print("Missing dates:", missing_dates)


print(len(missing_dates))
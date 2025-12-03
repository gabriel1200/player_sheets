import pandas as pd

df = pd.read_csv('year_files/2026_games.csv')

# Get all rows where POTENTIAL_AST is NA
na_rows = df[df['POTENTIAL_AST'].isna()]

# Show team abbreviation + potential_ast (will be NA)
result = na_rows[['PLAYER_NAME','TEAM_ABBREVIATION', 'POTENTIAL_AST', 'date']]


print(result)

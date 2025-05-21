import pandas as pd


def analyze_stat_win_rate(stat_column, year=2025, playoffs=False,by_team=False):
    # Build URL
    suffix = 'ps' if playoffs else ''
    url = f'https://raw.githubusercontent.com/gabriel1200/player_sheets/refs/heads/master/teamgame_report/year_files/all_games_{year}{suffix}.csv'

    # Load dataset
    print(f"Loading data from: {url}")
    df = pd.read_csv(url)
    df.drop_duplicates(subset=['GameId', 'TEAM_ID'], inplace=True)
    print(f"Games loaded: {len(df)} rows")

    # Make sure stat column exists
    if stat_column not in df.columns:
        print(f"Error: Column '{stat_column}' not found.")
        return

    # Pivot games: each row = one game, columns for each team
    game_pairs = df.sort_values('TEAM_ID').groupby('GameId')

    results = []

    for game_id, group in game_pairs:
        if group.shape[0] != 2:
            continue  # skip incomplete games

        team1, team2 = group.iloc[0], group.iloc[1]

        # Compare stat
        if pd.isna(team1[stat_column]) or pd.isna(team2[stat_column]):
            continue

        if team1[stat_column] == team2[stat_column]:
            continue  # skip ties

        # Determine which team had higher stat
        higher_team = team1 if team1[stat_column] > team2[stat_column] else team2
        lower_team = team2 if higher_team is team1 else team1

        results.append({
            'TEAM_NAME': higher_team['TEAM_NAME'],
            'TEAM_ID': higher_team['TEAM_ID'],
            'won': higher_team['W'],
            stat_column: higher_team[stat_column]
        })

    results_df = pd.DataFrame(results)

    if results_df.empty:
        print("No valid games found.")
        return



    # Summary
    avg_stat = results_df[stat_column].mean()
    win_rate = results_df['won'].mean() * 100

    print(f"\n--- Stat: {stat_column} ---")
    print(f"Games where team had higher {stat_column}: {len(results_df)}")
    print(f"Average {stat_column} in those games: {avg_stat:.2f}")
    print(f"Win rate when team had higher {stat_column}: {win_rate:.2f}%")

    if by_team==True:
        print("\n--- Team Breakdown ---")
        summary = results_df.groupby('TEAM_NAME').agg(
            games_with_higher_stat=('won', 'count'),
            wins=('won', 'sum'),
            avg_stat=(stat_column, 'mean')
        )
        summary['win_rate'] = summary['wins'] / summary['games_with_higher_stat'] * 100
        summary = summary.sort_values(by='win_rate', ascending=False)
        print(summary.to_string(formatters={'win_rate': '{:.2f}%'.format, 'avg_stat': '{:.2f}'.format}))

analyze_stat_win_rate('turnover_factor', year=2025, playoffs=False,by_team=True)

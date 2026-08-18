import os
import pandas as pd
from nba_api.stats.static import teams

def assess_missing_data(year_file='year_files/2026_games.csv', year=2026, ps=False):
    if not os.path.exists(year_file):
        print(f"File not found: {year_file}")
        return

    print(f"--- Assessing {year_file} ---")
    
    # 1. Read only sentinel tracking columns to keep memory light
    sentinel_map = {
        'Base Stats': 'PTS',
        'Advanced': 'OFF_RATING',
        'Passing Tracking': 'POTENTIAL_AST',
        'Drives Tracking': 'DRIVES',
        'Possessions Tracking': 'TOUCHES',
        'Rebounding Tracking': 'REB_CHANCES',
        'Shot Splits (Open)': 'open_FGA',
        'Pullups': 'pullup_PULL_UP_FGA',
        'Overall Defense': 'overall_def_D_FGA',
        'Hustle Stats': 'hustle_CONTESTED_SHOTS',
        'Post Touches': 'post_touch_POST_TOUCHES',
        'Speed & Dist': 'DIST_FEET',
        'Team Possessions': 'team_poss'
    }

    header = pd.read_csv(year_file, nrows=0).columns.tolist()
    available_sentinels = {label: col for label, col in sentinel_map.items() if col in header}
    
    df = pd.read_csv(year_file, usecols=['date', 'PLAYER_ID'] + list(available_sentinels.values()), low_memory=True)
    df['date'] = df['date'].astype(int)
    scraped_dates = set(df['date'].unique())

    # 2. Check for missing dates vs GitHub Schedule
    trail = 'ps' if ps else ''
    expected_dates = set()
    for team in teams.get_teams():
        path = f"https://raw.githubusercontent.com/gabriel1200/shot_data/refs/heads/master/team/{year}{trail}/{team['id']}.csv"
        try:
            team_df = pd.read_csv(path, usecols=['GAME_DATE'])
            expected_dates.update(team_df['GAME_DATE'].unique().tolist())
        except Exception:
            continue

    missing_dates = sorted(list(expected_dates - scraped_dates))
    print(f"\n1. Schedule Coverage:")
    print(f"   Expected Dates: {len(expected_dates)}")
    print(f"   Scraped Dates:  {len(scraped_dates)}")
    print(f"   Missing Dates ({len(missing_dates)}): {missing_dates}")

    # 3. Check for partial endpoint dropouts across scraped dates
    print(f"\n2. Endpoint Health Check across {len(scraped_dates)} dates:")
    for label, col in available_sentinels.items():
        # Identify dates where this sentinel is completely null
        date_nulls = df.groupby('date')[col].apply(lambda s: s.isna().all())
        bad_dates = date_nulls[date_nulls].index.tolist()
        if bad_dates:
            print(f"   [!] {label} ('{col}') missing entirely on {len(bad_dates)} dates: {bad_dates}")
        else:
            print(f"   [✓] {label} ('{col}') 100% complete")

if __name__ == '__main__':
    # Run for Regular Season or Playoffs
    assess_missing_data('year_files/2025_games.csv', year=2025, ps=False)
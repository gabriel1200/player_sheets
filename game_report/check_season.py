import os
import pandas as pd

file_path = 'year_files/2026ps_games.csv'

# Sentinel columns representing each endpoint group
SENTINELS = {
    'base_stats': 'PTS',
    'advanced': 'OFF_RATING',
    'passing_tracking': 'POTENTIAL_AST',
    'drives_tracking': 'DRIVES',
    'possessions_tracking': 'TOUCHES',
    'rebounding_tracking': 'REB_CHANCES',
    'shot_splits': 'wide_open_FGA',
    'pullups': 'pullup_FGA',
    'defense_overall': 'overall_def_D_FGA',
    'hustle': 'hustle_CONTESTED_SHOTS',
    'post_touches': 'post_touch_POST_TOUCHES',
    'speed_dist': 'DIST_FEET',
    'team_possessions': 'team_poss'
}

def audit_year():
    if not os.path.exists(file_path):
        print(f"File {file_path} not found.")
        return []

    header = pd.read_csv(file_path, nrows=0).columns.tolist()
    read_cols = ['date', 'PLAYER_ID'] + [col for col in SENTINELS.values() if col in header]
    df = pd.read_csv(file_path, usecols=read_cols, low_memory=True)
    df['date'] = df['date'].astype(int)

    dates_to_rescrape = set()

    for col_desc, sentinel_col in SENTINELS.items():
        if sentinel_col not in df.columns:
            print(f"[!] Warning: '{sentinel_col}' ({col_desc}) is completely missing from CSV header.")
            continue

        # Find dates where the sentinel column is entirely NaN
        date_nullity = df.groupby('date')[sentinel_col].apply(lambda s: s.isna().all())
        failed_dates = date_nullity[date_nullity].index.tolist()
        if failed_dates:
            print(f"[x] Failed '{col_desc}' on {len(failed_dates)} dates: {failed_dates}")
            dates_to_rescrape.update(failed_dates)

    bad_dates = sorted(list(dates_to_rescrape))
    print(f"\n---> Total bad dates found: {len(bad_dates)}")
    print(f"Dates to target: {bad_dates}")
    return bad_dates

if __name__ == '__main__':
    audit_year()
import os
import re
import sys
import pandas as pd
from nba_api.stats.static import teams

SENTINEL_MAP = {
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

def resolve_expected_dates(year, ps=False):
    """Fetches unique game dates from the team shot data repository."""
    trail = 'ps' if ps else ''
    expected_dates = set()
    for team in teams.get_teams():
        path = f"https://raw.githubusercontent.com/gabriel1200/shot_data/refs/heads/master/team/{year}{trail}/{team['id']}.csv"
        try:
            team_df = pd.read_csv(path, usecols=['GAME_DATE'])
            expected_dates.update(team_df['GAME_DATE'].unique().tolist())
        except Exception:
            continue
    return expected_dates

def assess_file(year_file, year=None, ps=None):
    if not os.path.exists(year_file):
        print(f"[!] File not found: {year_file}")
        return

    # Parse metadata dynamically from filename if not explicitly provided
    file_stem = os.path.splitext(os.path.basename(year_file))[0]
    if ps is None:
        ps = 'ps' in file_stem.lower()
    if year is None:
        year_match = re.search(r'\d{4}', file_stem)
        if year_match:
            year = int(year_match.group(0))
        else:
            print(f"[!] Could not extract 4-digit year from: {year_file}")
            return

    season_type_label = "Playoffs" if ps else "Regular Season"
    print(f"\n=======================================================")
    print(f" Assessing: {year_file}")
    print(f" Target:    Year {year} | {season_type_label}")
    print(f"=======================================================")

    # 1. Read header and available sentinels
    header = pd.read_csv(year_file, nrows=0).columns.tolist()
    available_sentinels = {label: col for label, col in SENTINEL_MAP.items() if col in header}
    missing_sentinel_cols = {label: col for label, col in SENTINEL_MAP.items() if col not in header}

    use_cols = ['date', 'PLAYER_ID'] + list(available_sentinels.values())
    df = pd.read_csv(year_file, usecols=use_cols, low_memory=True)
    df['date'] = df['date'].astype(int)
    scraped_dates = set(df['date'].unique())

    # 2. Check Schedule Coverage
    expected_dates = resolve_expected_dates(year, ps=ps)
    missing_schedule_dates = sorted(list(expected_dates - scraped_dates)) if expected_dates else []

    print(f"\n1. Schedule Coverage:")
    print(f"   Expected Dates: {len(expected_dates) if expected_dates else 'N/A (GitHub schedule unavailable)'}")
    print(f"   Scraped Dates:  {len(scraped_dates)}")
    if missing_schedule_dates:
        print(f"   [!] Missing Dates ({len(missing_schedule_dates)}): {missing_schedule_dates}")
    else:
        print(f"   [✓] 100% Scheduled Dates Present")

    # 3. Check Endpoint Health
    print(f"\n2. Endpoint Health Check ({len(scraped_dates)} dates scraped):")
    
    # Missing columns warning
    for label, col in missing_sentinel_cols.items():
        print(f"   [x] Column '{col}' ({label}) is COMPLETELY MISSING from CSV header")

    # Null rate checks for existing columns
    for label, col in available_sentinels.items():
        date_nulls = df.groupby('date')[col].apply(lambda s: s.isna().all())
        bad_dates = date_nulls[date_nulls].index.tolist()
        if bad_dates:
            print(f"   [!] {label} ('{col}') missing on {len(bad_dates)} dates: {bad_dates}")
        else:
            print(f"   [✓] {label} ('{col}') 100% complete")

def main():
    args = sys.argv[1:]

    # Mode 1: No arguments passed -> scan all files in year_files/
    if not args:
        year_dir = 'year_files'
        if not os.path.exists(year_dir):
            print(f"[!] Directory '{year_dir}' not found.")
            return
        
        target_files = sorted([
            os.path.join(year_dir, f) for f in os.listdir(year_dir) 
            if f.endswith('_games.csv') and not f.startswith('patch_cache_')
        ])

        if not target_files:
            print(f"[!] No valid *_games.csv files found in {year_dir}.")
            return

        print(f"Discovered {len(target_files)} game file(s) in {year_dir}/...")
        for f in target_files:
            assess_file(f)
        return

    # Mode 2: Specific year passed via CLI (e.g. `python assess_season.py 2025` or `python assess_season.py 2025 ps`)
    year = int(args[0])
    ps = len(args) > 1 and 'ps' in args[1].lower()
    trail = 'ps' if ps else ''
    target_file = f'year_files/{year}{trail}_games.csv'
    assess_file(target_file, year=year, ps=ps)

if __name__ == '__main__':
    main()
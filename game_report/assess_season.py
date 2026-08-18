import os
import re
import sys
import pandas as pd
from nba_api.stats.static import teams

# Standardized sentinel map
SENTINEL_MAP = {
    'Base Stats': 'PTS',
    'Advanced': 'OFF_RATING',
    'Passing Tracking': 'POTENTIAL_AST',
    'Drives Tracking': 'DRIVES',
    'Possessions Tracking': 'TOUCHES',
    'Rebounding Tracking': 'REB_CHANCES',
    'Shot Splits (Open)': 'open_FGA',
    'Pullups': ['PULL_UP_FGA', 'pullup_PULL_UP_FGA', 'pullup_FGA'],  # Flexible matching
    'Overall Defense': ['overall_def_D_FGA', 'overall_def_FREQ'],
    'Hustle Stats': 'hustle_CONTESTED_SHOTS',
    'Post Touches': ['post_touch_POST_TOUCHES', 'POST_TOUCHES'],
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

    # 1. Read header and map sentinels dynamically
    header = pd.read_csv(year_file, nrows=0).columns.tolist()
    
    resolved_sentinels = {}
    missing_sentinels = {}

    for label, candidates in SENTINEL_MAP.items():
        if isinstance(candidates, list):
            found_col = next((col for col in candidates if col in header), None)
            if found_col:
                resolved_sentinels[label] = found_col
            else:
                missing_sentinels[label] = candidates[0]
        else:
            if candidates in header:
                resolved_sentinels[label] = candidates
            else:
                missing_sentinels[label] = candidates

    use_cols = ['date', 'PLAYER_ID'] + list(resolved_sentinels.values())
    df = pd.read_csv(year_file, usecols=use_cols, low_memory=False)
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
    for label, col in missing_sentinels.items():
        print(f"   [x] Metric '{label}' (target: '{col}') is COMPLETELY MISSING from CSV header")

    # Null rate checks for detected columns
    for label, col in resolved_sentinels.items():
        date_nulls = df.groupby('date')[col].apply(lambda s: s.isna().all())
        bad_dates = date_nulls[date_nulls].index.tolist()
        if bad_dates:
            print(f"   [!] {label} ('{col}') missing entirely on {len(bad_dates)} dates: {bad_dates}")
        else:
            print(f"   [✓] {label} ('{col}') 100% complete")

    # 4. Check for Corrupted Rebounding Fields or Redundant Metadata
    corrupted_defense = [c for c in header if c.startswith('more_15ft_def_') and any(k in c for k in ['REB', 'DIST'])]
    if corrupted_defense:
        print(f"\n[!] Warning: Found {len(corrupted_defense)} corrupted more_15ft_def_ rebounding columns in header (run normalize_year_files.py to drop).")

    redundant_meta = [c for c in header if c.endswith('_TEAM_ID') and c != 'TEAM_ID']
    if redundant_meta:
        print(f"[!] Notice: Found {len(redundant_meta)} redundant *_TEAM_ID merge suffix columns.")

def main():
    args = sys.argv[1:]

    # Mode 1: No arguments -> scan all valid year files
    if not args:
        year_dir = 'year_files'
        if not os.path.exists(year_dir):
            print(f"[!] Directory '{year_dir}' not found.")
            return
        
        target_files = sorted([
            os.path.join(year_dir, f) for f in os.listdir(year_dir) 
            if f.endswith('_games.csv') and not f.startswith('patch_cache_') and 'backup' not in f and 'broken' not in f
        ])

        if not target_files:
            print(f"[!] No valid *_games.csv files found in {year_dir}.")
            return

        print(f"Discovered {len(target_files)} game file(s) in {year_dir}/...")
        for f in target_files:
            assess_file(f)
        return

    # Mode 2: Specific year / ps via CLI
    year = int(args[0])
    ps = len(args) > 1 and 'ps' in args[1].lower()
    trail = 'ps' if ps else ''
    target_file = f'year_files/{year}{trail}_games.csv'
    assess_file(target_file, year=year, ps=ps)

if __name__ == '__main__':
    main()
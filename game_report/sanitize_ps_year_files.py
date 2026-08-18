from pathlib import Path
import pandas as pd

YEAR_FILES_DIR = Path("year_files")

# Columns added downstream by build_all_games.py that must not exist in year_files/
SCHEDULE_METADATA_COLS = [
    'GAME_ID', 'HTM', 'VTM', 'opp_team', 'team', 'opp_id', 'series_key', 
    'season', 'playoffs', 'TEAM_ABBR'
]

def sanitize_file(file_path: Path):
    df = pd.read_csv(file_path, low_memory=False)
    initial_cols = len(df.columns)

    # 1. Strip pre-merged schedule metadata columns
    drop_meta = [c for c in SCHEDULE_METADATA_COLS if c in df.columns]
    df = df.drop(columns=drop_meta, errors='ignore')

    # 2. Drop duplicate speed_distance_* prefix columns if canonical DIST_FEET exists
    if 'DIST_FEET' in df.columns:
        speed_prefix_cols = [c for c in df.columns if c.startswith('speed_distance_')]
        df = df.drop(columns=speed_prefix_cols, errors='ignore')

    # 3. Drop redundant merge suffixes
    redundant_meta = [
        c for c in df.columns 
        if any(c.endswith(s) for s in ['_TEAM_ID', '_TEAM_ABBREVIATION', '_PLAYER_ID', '_PLAYER_NAME'])
        and c not in ['TEAM_ID', 'TEAM_ABBREVIATION', 'PLAYER_ID', 'PLAYER_NAME']
    ]
    df = df.drop(columns=redundant_meta, errors='ignore')

    df.to_csv(file_path, index=False)
    print(f"  [✓] {file_path.name}: Removed {initial_cols - len(df.columns)} redundant/metadata columns. (Final: {len(df.columns)} cols)")

def run():
    print("--- Sanitizing Reverse-Generated Playoff Files in year_files/ ---")
    ps_files = sorted(YEAR_FILES_DIR.glob("*ps_games.csv"))

    if not ps_files:
        print(f"[!] No playoff files (*ps_games.csv) found in {YEAR_FILES_DIR}.")
        return

    for f in ps_files:
        sanitize_file(f)

    print("\n[✓] All playoff year files sanitized and ready for build_all_games.py.")

if __name__ == '__main__':
    run()
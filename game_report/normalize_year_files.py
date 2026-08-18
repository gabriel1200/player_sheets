import os
import glob
from pathlib import Path
import pandas as pd

YEAR_FILES_DIR = Path("year_files")

def normalize_dataframe(df, file_name=""):
    initial_cols = len(df.columns)

    # -------------------------------------------------------------
    # 1. Canonical Pullup Renaming (pullup_PULL_UP_* -> PULL_UP_*)
    # -------------------------------------------------------------
    pullup_renames = {}
    for col in df.columns:
        if col.startswith('pullup_PULL_UP_'):
            pullup_renames[col] = col.replace('pullup_PULL_UP_', 'PULL_UP_')
        elif col.startswith('pullup_'):
            suffix = col.replace('pullup_', '')
            pullup_renames[col] = f"PULL_UP_{suffix}" if not suffix.startswith('PULL_UP_') else suffix

    if pullup_renames:
        df = df.rename(columns=pullup_renames)

    # -------------------------------------------------------------
    # 2. Corrupted 2025 Rebounding Defense Columns
    # -------------------------------------------------------------
    corrupted_defense = [
        c for c in df.columns 
        if c.startswith('more_15ft_def_') and any(k in c for k in ['REB', 'DIST'])
    ]

    # -------------------------------------------------------------
    # 3. Redundant Metadata & Leaked Merge Suffixes
    # -------------------------------------------------------------
    # Protected base columns that should never be dropped
    protected_cols = {
        'TEAM_ID', 'TEAM_ABBREVIATION', 'PLAYER_ID', 'PLAYER_NAME', 
        'GP', 'MIN', 'AGE', 'W', 'L', 'G', 'MIN1',
        'sp_work_DEF_RATING', 'sp_work_OFF_RATING', 'sp_work_NET_RATING', 'sp_work_PACE',
        'sp_work_DEF_RATING_RANK', 'sp_work_OFF_RATING_RANK', 'sp_work_NET_RATING_RANK', 'sp_work_PACE_RANK'
    }

    drop_suffixes = [
        '_TEAM_ID', '_TEAM_ABBREVIATION', '_PLAYER_ID', '_PLAYER_NAME',
        '_GP', '_MIN', '_AGE', '_W', '_L', '_G', 
        '_PLAYER_POSITION', '_PLAYER_LAST_TEAM_ID', '_PLAYER_LAST_TEAM_ABBREVIATION'
    ]

    redundant_meta = [
        c for c in df.columns 
        if any(c.endswith(s) for s in drop_suffixes) and c not in protected_cols
    ]

    explicit_drops = [
        'PLAYER_LAST_TEAM_ID', 
        'PLAYER_LAST_TEAM_ABBREVIATION', 
        'lt6ft_totals_PLAYER_ID'
    ]

    # Combine and drop all identified bad columns
    all_drops = set(corrupted_defense + redundant_meta + explicit_drops)
    existing_drops = [c for c in all_drops if c in df.columns]

    if existing_drops:
        df = df.drop(columns=existing_drops)

    final_cols = len(df.columns)
    print(f"  [{file_name}] Initial cols: {initial_cols} | Renamed Pullups: {len(pullup_renames)} | Dropped cols: {len(existing_drops)} | Final cols: {final_cols}")
    return df

def run_normalization():
    if not YEAR_FILES_DIR.exists():
        print(f"[!] Directory {YEAR_FILES_DIR} not found.")
        return

    # Find all main year files (excluding backups and cache files)
    target_files = sorted([
        f for f in YEAR_FILES_DIR.glob("*_games.csv") 
        if not f.name.startswith("patch_cache_") and "backup" not in f.name and "broken" not in f.name
    ])

    print(f"Discovered {len(target_files)} year_files to normalize...")

    for file_path in target_files:
        print(f"\nProcessing: {file_path.name}")
        df = pd.read_csv(file_path, low_memory=False)
        
        # Normalize in memory
        df_normalized = normalize_dataframe(df, file_name=file_path.name)
        
        # Overwrite in-place
        df_normalized.to_csv(file_path, index=False)
        print(f"  [✓] Overwritten: {file_path.name}")

    print("\n[✓] Schema normalization complete across all year_files.")

if __name__ == '__main__':
    run_normalization()
import pandas as pd
import glob
import os
from pathlib import Path

# 1. Setup paths
source_dir = Path(".").resolve()
# Target: /home/gaber/basketball/daily_tracking/careerlogs
target_dir = source_dir.parents[2] / "daily_tracking" / "careerlogs"
target_dir.mkdir(parents=True, exist_ok=True)

# 2. Load and Normalize the Master Game Index
INDEX_URL = "https://raw.githubusercontent.com/gabriel1200/shot_data/refs/heads/master/game_dates.csv"
print("Downloading and normalizing game index mapping...")
mapping_df = pd.read_csv(INDEX_URL)

# Normalize Index Keys: Strips leading '00's by converting to numeric
mapping_df['GAME_ID'] = pd.to_numeric(mapping_df['GAME_ID'], errors='coerce').fillna(0).astype(int)
mapping_df['TEAM_ID'] = pd.to_numeric(mapping_df['TEAM_ID'], errors='coerce').fillna(0).astype(int)

# Identify all year files
csv_files = sorted(glob.glob(str(source_dir / "all_*.csv")))

for file_path in csv_files:
    file_name = os.path.basename(file_path)
    if "sample" in file_name or "master" in file_name:
        continue
        
    print(f"Processing: {file_name}")
    
    # Load Year Data
    df = pd.read_csv(file_path, low_memory=False)
    
    # Normalize Year Keys: This handles cases where the CSV has "0021400001" vs "21400001"
    df['GAME_ID'] = pd.to_numeric(df['GAME_ID'], errors='coerce').fillna(0).astype(int)
    df['TEAM_ID'] = pd.to_numeric(df['TEAM_ID'], errors='coerce').fillna(0).astype(int)
    
    # 3. Clean and Merge
    # Drop existing metadata columns to avoid duplicates or suffix columns (_x, _y)
    cols_to_fill = ['HTM', 'VTM', 'opp_team', 'team', 'date', 'season', 'playoffs']
    df = df.drop(columns=[c for c in cols_to_fill if c in df.columns])
    
    # Merge using the normalized integer IDs
    df = df.merge(mapping_df, on=['GAME_ID', 'TEAM_ID'], how='left')
    
    # 4. Final Data Integrity Checks
    # Ensure Playoff tag is set (Priority: Mapping File -> Filename)
    if 'playoffs' not in df.columns:
        df['playoffs'] = "ps.csv" in file_name.lower()
    else:
        df['playoffs'] = df['playoffs'].fillna("ps.csv" in file_name.lower())

    # Normalize Player ID for filename consistency
    df['PLAYER_ID'] = pd.to_numeric(df['PLAYER_ID'], errors='coerce').fillna(0).astype(int)
    df = df[df['PLAYER_ID'] != 0]

    # 5. Schema-Aware Save (Fixes the 422 vs 432 field error)
    for p_id, p_group in df.groupby('PLAYER_ID'):
        player_file = target_dir / f"{int(p_id)}.csv"
        
        if player_file.exists():
            # Load existing file to align columns
            existing_df = pd.read_csv(player_file, low_memory=False)
            # concat(sort=False) aligns existing columns and appends new ones as NaNs
            combined_df = pd.concat([existing_df, p_group], ignore_index=True, sort=False)
            combined_df.to_csv(player_file, index=False)
        else:
            p_group.to_csv(player_file, index=False)

print(f"\nTask Complete. Data successfully normalized and merged.")
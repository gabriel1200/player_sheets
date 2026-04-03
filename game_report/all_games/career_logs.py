import pandas as pd
import glob
import os
from pathlib import Path

source_dir = Path(".").resolve()
target_dir = source_dir.parents[2] / "daily_tracking" / "careerlogs"
target_dir.mkdir(parents=True, exist_ok=True)

csv_files = sorted(glob.glob(str(source_dir / "all_*.csv"))) # Sorted to keep chronology

for file_path in csv_files:
    file_name = os.path.basename(file_path)
    if "sample" in file_name or "master" in file_name: continue
        
    print(f"Processing: {file_name}")
    df = pd.read_csv(file_path, low_memory=False)
    
    # Tag Playoffs
    df['playoffs'] = "ps.csv" in file_name.lower()
    
    # Clean ID
    df['PLAYER_ID'] = pd.to_numeric(df['PLAYER_ID'], errors='coerce').fillna(0).astype(int)
    df = df[df['PLAYER_ID'] != 0]

    for p_id, p_group in df.groupby('PLAYER_ID'):
        player_file = target_dir / f"{p_id}.csv"
        
        if player_file.exists():
            # Robust Merge: Load existing, concat, and save
            existing_df = pd.read_csv(player_file, low_memory=False)
            # sort=False keeps the original column order as much as possible
            combined_df = pd.concat([existing_df, p_group], ignore_index=True, sort=False)
            combined_df.to_csv(player_file, index=False)
        else:
            p_group.to_csv(player_file, index=False)
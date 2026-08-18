import os
from pathlib import Path
import pandas as pd

all_games_dir = Path("all_games")
year_files_dir = Path("year_files")
year_files_dir.mkdir(parents=True, exist_ok=True)

# Find all historical playoff parquet files (2014 to 2024)
ps_parquet_files = sorted(all_games_dir.glob("all_*ps.parquet"))

for pq_file in ps_parquet_files:
    stem = pq_file.stem  # e.g. "all_2024ps"
    year_str = stem.replace("all_", "").replace("ps", "")
    target_csv = year_files_dir / f"{year_str}ps_games.csv"

    if not target_csv.exists():
        print(f"Restoring {target_csv.name} from {pq_file.name}...")
        df = pd.read_parquet(pq_file)
        
        # Ensure date is integer and sorted
        if 'date' in df.columns:
            df['date'] = df['date'].astype(int)
            df.sort_values(by=['date', 'PLAYER_ID'], inplace=True)
            
        df.to_csv(target_csv, index=False)
        print(f"  [✓] Created {target_csv.name} ({len(df)} rows)")
    else:
        print(f"[-] Already exists: {target_csv.name}")

print("\n[✓] All missing playoff year_files restored.")
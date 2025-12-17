import pandas as pd
from pathlib import Path

TARGET_DIR = Path("games/2026")
COLUMN_NAME = "wide_open_FG3A"

for file in TARGET_DIR.glob("*.csv"):
    try:
        df = pd.read_csv(file)

        # Skip if column doesn't exist
        if COLUMN_NAME not in df.columns:
            print(f"Skipping {file.name} (column not found)")
            continue

        # Identify non-blank values
        non_blank = (
            df[COLUMN_NAME].notna() &
            (df[COLUMN_NAME].astype(str).str.strip() != "")
        )

        # If no valid entries exist, delete the file
        if not non_blank.any():

            #file.unlink()
            print('hirror')
            print(f"Deleted {file.name} (no valid {COLUMN_NAME} entries)")
        else:
            print(f"Kept {file.name}")

    except Exception as e:
        print(f"Error processing {file.name}: {e}")

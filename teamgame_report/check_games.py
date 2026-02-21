import pandas as pd
from pathlib import Path

TARGET_DIR = Path("games/2026")
COLUMN_NAME = "wide_open_FG3A"

problem_files = []
skipped_files = []
kept_files = []

for file in TARGET_DIR.glob("*.csv"):
    try:
        df = pd.read_csv(file)

        # Column missing
        if COLUMN_NAME not in df.columns:
            skipped_files.append(file.name)
            continue

        # Identify non-blank values
        non_blank = (
            df[COLUMN_NAME].notna() &
            (df[COLUMN_NAME].astype(str).str.strip() != "")
        )

        # No valid entries → problem file
        if not non_blank.any():
            problem_files.append(file.name)
        else:
            kept_files.append(file.name)

    except Exception as e:
        problem_files.append(f"{file.name} (error: {e})")

# ---- Reporting ----
print("\n=== SUMMARY ===")
print(f"Kept files: {len(kept_files)}")
print(f"Problem files (no valid {COLUMN_NAME}): {len(problem_files)}")
print(f"Skipped files (column missing): {len(skipped_files)}")

if problem_files:
    print("\n=== PROBLEM FILES ===")
    for f in problem_files:
        print(f)

if skipped_files:
    print("\n=== SKIPPED FILES (COLUMN MISSING) ===")
    for f in skipped_files:
        print(f)
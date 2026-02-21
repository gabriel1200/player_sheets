import pandas as pd

FILE_PATH = "year_files/2026_teamgames.csv"
COLUMN_NAME = "wide_open_FG3A"
DATE_COL = "date"

df = pd.read_csv(FILE_PATH)

# Rows with missing target column
missing_mask = df[COLUMN_NAME].isna()

# Dates that would be removed
missing_dates = df.loc[missing_mask, DATE_COL].unique()

print("\n=== SUMMARY ===")
print(f"Total rows: {len(df)}")
print(f"Rows with missing {COLUMN_NAME}: {missing_mask.sum()}")
print(f"Dates affected: {len(missing_dates)}")

# Detailed breakdown per date
if len(missing_dates) > 0:
    print("\n=== AFFECTED DATES ===")
    date_counts = (
        df.loc[missing_mask, DATE_COL]
        .value_counts()
        .sort_index()
    )

    for date, count in date_counts.items():
        print(f"{date}: {count} rows")

# Optional: show sample rows for inspection
print("\n=== SAMPLE PROBLEM ROWS ===")
print(
    df.loc[missing_mask]
      .sort_values(DATE_COL)
      .head(10)
)

# If you want the list programmatically
problem_dates = list(missing_dates)
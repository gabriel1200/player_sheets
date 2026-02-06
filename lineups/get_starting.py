import os
import pandas as pd

input_base = "../player_sheets/lineups/data"
output_base = "data/main_lineups"

os.makedirs(output_base, exist_ok=True)

keep_cols = [
    "EntityId",
    "TeamId",
    "TeamAbbreviation",
    "SecondsPlayed",
]

for year in range(2014, 2027):
    year_dir = os.path.join(input_base, str(year))
    if not os.path.isdir(year_dir):
        continue

    dfs = []

    for fname in os.listdir(year_dir):
        if not fname.endswith(".csv"):
            continue
        if "vs" in fname.lower():
            continue

        path = os.path.join(year_dir, fname)
        df = pd.read_csv(path)

        if "SecondsPlayed" not in df.columns:
            continue

        df["SecondsPlayed"] = pd.to_numeric(df["SecondsPlayed"], errors="coerce")

        top3 = (
            df[keep_cols]
            .dropna(subset=["SecondsPlayed"])
            .nlargest(3, "SecondsPlayed")
        )

        dfs.append(top3)

    if not dfs:
        continue

    out_df = pd.concat(dfs, ignore_index=True)

    out_path = os.path.join(
        output_base, f"main_lineups{year}.csv"
    )
    out_df.to_csv(out_path, index=False)

    print(f"Saved {out_path} ({len(out_df)} rows)")

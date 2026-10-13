"""Refresh season aggregate outputs from their saved NBA and PBP Stats inputs.

Regular seasons: fill only players whose PBP Stats row was omitted by the
500-player league response. Playoffs: refresh all base and PBP fields when the
saved aggregate inputs are newer than the year_totals file. External salary,
LEBRON and position columns are preserved as stored.
"""

from __future__ import annotations

import argparse
import ast
import os
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


HERE = Path(__file__).resolve().parent
OUTPUT = HERE.parent / "year_totals"


def pbp_columns() -> list[str]:
    tree = ast.parse((HERE / "gen_totals2.py").read_text())
    node = next(
        item.value for item in tree.body
        if isinstance(item, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "pbp_columns" for target in item.targets)
    )
    return list(ast.literal_eval(node))


def numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    return pd.to_numeric(frame[column], errors="coerce")


def recompute(frame: pd.DataFrame, mask: pd.Series) -> None:
    """Apply the derived-column formulas in gen_totals2.py to refreshed rows."""
    if not mask.any():
        return
    x = frame.loc[mask].copy()
    ft_rate = 100 * numeric(x, "FTA") / numeric(x, "FGA")
    x["FTM"] = numeric(x, "FtPoints")
    x["RimFGA"] = numeric(x, "AtRimFGA")
    x["RimFGM"] = numeric(x, "AtRimFGM")
    x["FG2_miss"] = numeric(x, "FG2A") - numeric(x, "FG2M")
    x["FG3_miss"] = numeric(x, "FG3A") - numeric(x, "FG3M")
    x["FG2_points"] = numeric(x, "FG2M") * 2
    x["FG3_points"] = numeric(x, "FG3M") * 3
    x["FGA"] = numeric(x, "FG2A") + numeric(x, "FG3A")
    x["RimPoints"] = x["RimFGM"] * 2
    x["RimMiss"] = x["RimFGA"] - x["RimFGM"]
    x["NonRim2FGA"] = numeric(x, "FG2A") - x["RimFGA"]
    x["NonRim2FGM"] = numeric(x, "FG2M") - x["RimFGM"]
    x["NonRimPoints"] = x["NonRim2FGM"] * 2
    ortg = numeric(x, "avg_ortg")
    orb = numeric(x, "avg_orb%")
    x["2shooting_factor"] = x["FG2_points"] - numeric(x, "FG2M") * ortg - (1 - orb) * x["FG2_miss"] * ortg
    x["3shooting_factor"] = x["FG3_points"] - numeric(x, "FG3M") * ortg - (1 - orb) * x["FG3_miss"] * ortg
    x["rimfactor"] = x["RimPoints"] - x["RimFGM"] * ortg - (1 - orb) * x["RimMiss"] * ortg
    x["nonrim2factor"] = x["NonRimPoints"] - x["NonRim2FGM"] * ortg - (1 - orb) * (x["NonRim2FGA"] - x["NonRim2FGM"]) * ortg
    x["morey_factor"] = x["RimPoints"] + x["FG3_points"] - (x["RimFGM"] + numeric(x, "FG3M")) * ortg - (1 - orb) * (x["RimMiss"] + x["FG3_miss"]) * ortg
    x["turnover_factor"] = -numeric(x, "Turnovers") * ortg
    x["ft_factor"] = x["FTM"] - 0.4 * numeric(x, "FTA") * ortg + 0.06 * (numeric(x, "FTA") - x["FTM"]) * ortg
    x["shooting_factor"] = x["3shooting_factor"] + x["2shooting_factor"]
    x["scoring_factor"] = x["ft_factor"] + x["shooting_factor"]
    x["FT_RATE"] = ft_rate
    x["on-ball-time%"] = 200 * numeric(x, "TIME_OF_POSS") / numeric(x, "MIN")
    x["ON_BALL_TIME_PCT"] = x["on-ball-time%"]
    x["Stops"] = sum(numeric(x, key).fillna(0) for key in (
        "Charge_Fouls_Drawn", "Offensive_Fouls_Drawn", "Steals", "RecoveredBlocks"
    ))
    for key in (
        "FG2A", "FG3A", "FTA", "2pt_And_1_Free_Throw_Trips", "3pt_And_1_Free_Throw_Trips",
        "Turnovers", "BadPassTurnovers", "Points", "SelfOReb",
    ):
        x[key] = numeric(x, key).fillna(0)
    x["improved_tsa"] = numeric(x, "FG2A") + numeric(x, "FG3A") + 0.5 * (
        numeric(x, "FTA") - numeric(x, "2pt_And_1_Free_Throw_Trips")
        - numeric(x, "3pt_And_1_Free_Throw_Trips")
    )
    x["NonPassTurnover"] = numeric(x, "Turnovers") - numeric(x, "BadPassTurnovers")
    denominator = x["improved_tsa"] - numeric(x, "SelfOReb") + x["NonPassTurnover"]
    x["adjusted_trueshooting_pct"] = numeric(x, "PTS") / denominator / 2
    for column in x.columns.intersection(frame.columns):
        frame.loc[mask, column] = x[column].to_numpy()

    total_possessions = (
        numeric(frame, "improved_tsa").sum()
        - numeric(frame, "SelfOReb").sum()
        + numeric(frame, "NonPassTurnover").sum()
    )
    league_avg = numeric(frame, "PTS").sum() / total_possessions / 2
    frame["relative_adjusted_ts_pct"] = numeric(frame, "adjusted_trueshooting_pct") - league_avg


def refresh(year: int, playoffs: bool, write: bool) -> None:
    suffix = "_ps" if playoffs else ""
    csv_path = OUTPUT / f"{year}{suffix}.csv"
    avg_path = HERE / f"{year}{suffix}_avg.csv"
    pbp_path = HERE / f"{year}{suffix}_pbp.csv"
    if not all(path.exists() for path in (csv_path, avg_path, pbp_path)):
        return
    output = pd.read_csv(csv_path, low_memory=False)
    avg = pd.read_csv(avg_path, low_memory=False).set_index("PLAYER_ID")
    pbp = pd.read_csv(pbp_path, low_memory=False)
    pbp["EntityId"] = pbp["EntityId"].astype(int)
    if pbp["EntityId"].duplicated().any():
        raise ValueError(f"Duplicate PBP Stats player in {pbp_path}")
    pbp = pbp.set_index("EntityId")
    ids = output["PLAYER_ID"]
    if set(ids) != set(avg.index) or not set(ids).issubset(pbp.index):
        raise ValueError(f"Player IDs do not match saved inputs for {year}{suffix}")
    before_gp = int(output["GP"].sum())
    mask = pd.Series(True, index=output.index) if playoffs else output["Minutes"].isna()
    if not mask.any() and not playoffs:
        return
    before_null = int(output.loc[mask].isna().sum().sum())

    if playoffs:
        for column in avg.columns.intersection(output.columns):
            output[column] = avg[column].reindex(ids).to_numpy()

    avg_lower = {column.lower() for column in avg.columns}
    for original in pbp_columns():
        column = original.replace(" ", "_")
        if column == "3SecondViolations":
            column = "ThreeSecondViolations"
        if column not in output or column.lower() in avg_lower:
            continue
        values = pbp[original].reindex(ids[mask]).to_numpy() if original in pbp else np.full(mask.sum(), np.nan)
        output.loc[mask, column] = values

    recompute(output, mask)
    if int(output["GP"].sum()) != (int(avg["GP"].sum()) if playoffs else before_gp):
        raise ValueError(f"Games played check failed for {year}{suffix}")
    after_null = int(output.loc[mask].isna().sum().sum())
    print(f"{year}{suffix}: refreshed {int(mask.sum())} players; "
          f"null cells {before_null} -> {after_null}; total GP {before_gp} -> {int(output.GP.sum())}", flush=True)
    if write:
        tmp_csv = csv_path.with_suffix(".tmp.csv")
        tmp_parquet = csv_path.with_suffix(".tmp.parquet")
        output.to_csv(tmp_csv, index=False)
        old_schema = pq.read_schema(csv_path.with_suffix(".parquet"))
        table = pa.Table.from_pandas(output, preserve_index=False).cast(old_schema, safe=True)
        pq.write_table(table, tmp_parquet)
        os.replace(tmp_csv, csv_path)
        os.replace(tmp_parquet, csv_path.with_suffix(".parquet"))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, default=2026)
    parser.add_argument("--playoffs", action="store_true")
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    if args.all:
        for year in range(2001, args.year + 1):
            refresh(year, False, args.write)
        refresh(args.year, True, args.write)
    else:
        refresh(args.year, args.playoffs, args.write)


if __name__ == "__main__":
    main()

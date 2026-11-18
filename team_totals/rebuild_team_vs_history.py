"""One-off repair (2026-11-18): rebuild the per-team opponent files {team_id}vs{carry}.csv from the season files.

From 2026-01-24 to 2026-11-18, scrape_teams_vs (team_average_scrape.ipynb) took each per-team opponent file's past seasons
from the team's own file, so every season but the current one was the team's own row. The season files
{year}vs{carry}.csv hold pbpstats' Opponent rows for every team and are correct (checked: their Points equal the
team rows' OpponentPoints, within pbpstats' own 4-point disagreement in 2024-25).

Each file keeps its columns, column order and set of seasons; only the rows change. Run from team_totals/:
    python rebuild_team_vs_history.py          # dry run: what would change
    python rebuild_team_vs_history.py --write
"""
import glob
import re
import sys

import pandas as pd

WRITE = "--write" in sys.argv

for carry in ("", "ps"):
    season = {}
    for f in glob.glob(f"[12][0-9][0-9][0-9]vs{carry}.csv"):
        m = re.fullmatch(rf"(\d{{4}})vs{carry}\.csv", f)
        if m:
            d = pd.read_csv(f)
            d["TeamId"] = d.TeamId.astype(int)
            season[int(m.group(1))] = d
    changed = 0
    for f in sorted(glob.glob(f"1610612???vs{carry}.csv")):
        tid = int(f[:10])
        cur = pd.read_csv(f)
        team = pd.read_csv(f"{tid}{carry}.csv")
        rows = []
        for yr in cur.year.tolist():
            s = season.get(int(yr))
            r = s[s.TeamId == tid] if s is not None else None
            if r is None or len(r) != 1:
                raise SystemExit(f"{f}: no single {yr}vs{carry}.csv row for {tid}; nothing written")
            rows.append(r)
        new = pd.concat(rows).reindex(columns=cur.columns).reset_index(drop=True)  # older seasons lack the take-foul columns
        # The latest season was written correctly by the daily run: it must agree with the season file.
        last = int(cur.year.max())
        a = cur[cur.year == last].reset_index(drop=True)
        b = new[new.year == last].reset_index(drop=True)
        if not (a.Points.astype(float).values == b.Points.astype(float).values).all():
            print(f"{f}: note, {last} differs from {last}vs{carry}.csv (pulled at different times): {a.Points.tolist()} vs {b.Points.tolist()}")
        # After the repair no past season may equal the team's own row.
        j = new[["year", "Points"]].merge(team[["year", "Points", "OpponentPoints"]], on="year", suffixes=("", "_team"))
        same_as_team = int((j.Points == j.Points_team).sum())
        opp_mismatch = int((j.Points - j.OpponentPoints).abs().gt(10).sum())
        n_diff = int((cur.Points.values != new.Points.values).sum())
        changed += n_diff > 0
        print(f"{f}: {len(new)} seasons, {n_diff} rows replaced; rows still equal to the team's own Points: {same_as_team}; "
              f"seasons where vs Points and the team's OpponentPoints differ by >10: {opp_mismatch}")
        if WRITE:
            new.to_csv(f, index=False)
    print(f"carry={carry!r}: {changed} files {'rewritten' if WRITE else 'would change'}")

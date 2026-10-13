"""Complete a season's PBP Stats player totals beyond the 500-row league response.

The public get-totals endpoint has no pagination parameter, but TeamId filters
return all players on a team. Existing league rows are kept intact; only players
missing from the league response are added. For players appearing on multiple
teams, get-all-season-stats supplies the combined season row.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import time
from pathlib import Path

import pandas as pd
import requests


API = "https://api.pbpstats.com"
HERE = Path(__file__).resolve().parent


def get_json(session: requests.Session, path: str, params: dict) -> dict:
    for attempt in range(3):
        try:
            response = session.get(f"{API}{path}", params=params, timeout=(10, 60))
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError):
            if attempt == 2:
                raise
            time.sleep(2 * (attempt + 1))
    raise AssertionError("unreachable")


def cached_get(session: requests.Session, cache: Path, name: str, path: str, params: dict) -> dict:
    cache.mkdir(parents=True, exist_ok=True)
    file = cache / f"{name}.json"
    if file.exists():
        return json.loads(file.read_text())
    data = get_json(session, path, params)
    tmp = file.with_suffix(".tmp")
    tmp.write_text(json.dumps(data))
    os.replace(tmp, file)
    return data


def combine_team_rows(rows: list[dict]) -> dict:
    """Preserve additive counts if the API's combined player route fails.

    Ratios and averages have different denominators, so leave those empty
    rather than silently summing or averaging them incorrectly.
    """
    result = {}
    nonadditive = (
        "pct", "frequency", "accuracy", "rating", "avg", "usage", "onoff",
        "shotquality", "fg3apct", "fg2apct", "efg", "tspct", "fg3pct", "fg2pct",
    )
    for key in set().union(*(row.keys() for row in rows)):
        if key in {"EntityId", "Name", "ShortName", "TeamId", "TeamAbbreviation", "RowId"}:
            continue
        values = [row.get(key) for row in rows]
        if all(value is None or isinstance(value, (int, float)) for value in values):
            if any(part in key.lower() for part in nonadditive):
                continue
            result[key] = sum(value for value in values if value is not None)
    return result


def complete(
    year: int, playoffs: bool, cache: Path, write: bool,
    source_path: Path | None = None, destination_path: Path | None = None,
) -> None:
    suffix = "_ps" if playoffs else ""
    season = f"{year - 1}-{str(year)[-2:]}"
    season_type = "Playoffs" if playoffs else "Regular Season"
    avg = pd.read_csv(HERE / f"{year}{suffix}_avg.csv", low_memory=False)
    existing_path = source_path or HERE / f"{year}{suffix}_pbp.csv"
    destination_path = destination_path or existing_path
    existing = pd.read_csv(existing_path, low_memory=False)
    existing["EntityId"] = existing["EntityId"].astype(str)
    target = set(avg["PLAYER_ID"].astype(str))
    missing = target - set(existing["EntityId"])
    print(f"{season_type} {season}: {len(target)} aggregate players, "
          f"{len(existing)} PBP rows, {len(missing)} missing", flush=True)
    if not missing:
        if write and destination_path != existing_path:
            tmp = destination_path.with_suffix(".tmp.csv")
            shutil.copyfile(existing_path, tmp)
            os.replace(tmp, destination_path)
        return

    # Fetch all teams, since a traded player's final TEAM_ID only identifies
    # one of the teams on which they played.
    teams = sorted(int(team) for team in avg["TEAM_ID"].dropna().unique() if int(team) > 0)
    if len(teams) not in (29, 30):
        raise ValueError(f"Expected 29 or 30 NBA teams, got {len(teams)}")
    by_player: dict[str, list[dict]] = {player: [] for player in missing}
    session = requests.Session()
    for team in teams:
        response = cached_get(
            session, cache, f"{season_type.replace(' ', '_')}_{team}",
            "/get-totals/nba",
            {"Season": season, "SeasonType": season_type, "Type": "Player", "TeamId": str(team)},
        )
        rows = response.get("multi_row_table_data", [])
        if not rows:
            raise ValueError(f"No PBP Stats players returned for team {team}")
        for row in rows:
            player = str(row.get("EntityId", ""))
            if player in by_player:
                by_player[player].append(row)
        print(f"team {team}: {len(rows)} players", flush=True)

    absent = sorted(player for player, rows in by_player.items() if not rows)
    if absent:
        raise ValueError(f"PBP Stats team requests did not return player IDs {absent}")

    avg_by_id = avg.set_index(avg["PLAYER_ID"].astype(str))
    additions = []
    combined = []
    for player in sorted(missing):
        rows = by_player[player]
        expected_gp = int(avg_by_id.loc[player, "GP"])
        if len(rows) == 1 and abs(int(rows[0].get("GamesPlayed", -1)) - expected_gp) <= 1:
            row = rows[0].copy()
        else:
            try:
                response = cached_get(
                    session, cache, f"{season_type.replace(' ', '_')}_player_{player}",
                    "/get-all-season-stats/nba", {"EntityType": "Player", "EntityId": player},
                )
                seasons = response.get("results", {}).get(season_type, [])
                matches = [item for item in seasons if item.get("Season") == season]
                if not matches:
                    raise ValueError(f"No combined {season} row for {player}")
                # This route returns each team stint followed by the combined
                # season row when a player changed teams.
                row = matches[-1].copy()
            except (requests.RequestException, ValueError) as exc:
                if len(rows) == 1:
                    raise
                row = combine_team_rows(rows)
                print(f"player {player}: combined route failed ({exc}); "
                      "summed counts, left ratios blank", flush=True)
            row["EntityId"] = player
            row["Name"] = avg_by_id.loc[player, "PLAYER_NAME"]
            row["TeamId"] = avg_by_id.loc[player, "TEAM_ID"]
            combined.append(player)
        row["year"] = f"{year}{suffix}" if playoffs else year
        additions.append(row)

    # Keep the original source schema and rows byte-for-byte. The combined
    # route has extra display fields such as Season and Team which are not
    # part of the get-totals result or the year_totals pipeline.
    added = pd.DataFrame(additions).reindex(columns=existing.columns)
    full = pd.concat([existing, added], ignore_index=True)
    if full["EntityId"].duplicated().any() or set(full["EntityId"]) != target:
        raise ValueError("Completed PBP Stats file does not have one row per aggregate player")
    added_gp = added.set_index("EntityId")["GamesPlayed"].astype(int)
    gp_diff = (added_gp - avg_by_id["GP"].reindex(added_gp.index).astype(int)).abs()
    if (gp_diff > 1).any():
        raise ValueError(f"Recovered PBP Stats games played disagree by >1: {gp_diff[gp_diff > 1].to_dict()}")
    print(f"completed: {len(full)} rows; {len(combined)} combined players: {combined}; "
          f"recovered GP off by one: {int((gp_diff == 1).sum())}", flush=True)
    if write:
        tmp = destination_path.with_suffix(".tmp.csv")
        shutil.copyfile(existing_path, tmp)
        if not tmp.read_bytes().endswith(b"\n"):
            with tmp.open("ab") as file:
                file.write(b"\n")
        added.to_csv(tmp, mode="a", header=False, index=False)
        os.replace(tmp, destination_path)
        print(f"wrote {destination_path}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, default=2026)
    parser.add_argument("--playoffs", action="store_true")
    parser.add_argument("--all", action="store_true", help="Audit and complete every 2001–2026 season")
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--cache", type=Path, default=Path("/tmp/pbpstats-complete"))
    args = parser.parse_args()
    if args.all:
        for year in range(2001, args.year + 1):
            for playoffs in (False, True):
                suffix = "_ps" if playoffs else ""
                if (HERE / f"{year}{suffix}_avg.csv").exists() and (HERE / f"{year}{suffix}_pbp.csv").exists():
                    complete(year, playoffs, args.cache / str(year), args.write)
    else:
        complete(args.year, args.playoffs, args.cache / str(args.year), args.write)


if __name__ == "__main__":
    main()

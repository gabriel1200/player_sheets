#!/usr/bin/env python
# Generated from pbp_gamelogs.ipynb by nb2py.py; edit the notebook.


import glob
import os, datetime as _dt
_today = _dt.date.today()
SEASON_YEAR = int(os.environ.get("NBA_SEASON_YEAR") or _today.year + (_today.month >= 10))  # end year: 2027 = 2026-27 (see ~/basketball/season.sh)
PLAYOFFS = os.environ.get("NBA_PLAYOFFS") == "1"
#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#!/usr/bin/env python
# coding: utf-8

import requests
import pandas as pd
from datetime import datetime
import time
from typing import List, Dict, Tuple
import logging
import os

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 Edg/115.0.1901.183',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'DNT': '1',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Cache-Control': 'max-age=0',
}

class PBPStatsAPI:
    def __init__(self, start_year: int = 2013, end_year: int = 2024):
        self.base_url = "https://api.pbpstats.com/get-game-logs/nba?"
        self.season_types = ["Regular Season"]
        self.start_year = start_year
        self.end_year = end_year    

    def get_season_years(self) -> List[str]:
        """Generate season strings from start_year to end_year."""
        return [f"{year-1}-{str(year)[2:]}" for year in range(self.start_year, self.end_year + 1)]

    def get_team_game_logs(self, team_id: str, entity_type: str = "Team") -> pd.DataFrame:
        """Fetch all game logs for a specific team within the specified year range."""
        all_games = []
        seasons = self.get_season_years()

        for season in seasons:
            for season_type in self.season_types:
                params = {
                    "Season": season,
                    "SeasonType": season_type,
                    "EntityId": team_id,
                    "EntityType": entity_type
                }

                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        print(self.base_url)
                        print(params)
                        response = requests.get(self.base_url, params=params, timeout=10,headers=headers)
                        response.raise_for_status()
                        data_response = response.json()

                        raw_data = data_response.get("multi_row_table_data", [])
                        if not raw_data:
                            logging.info(f"No data for {team_id} in {season}")
                            break

                        games_data = pd.DataFrame(raw_data)
                        games_data["Season"] = season
                        games_data["year"] = int(season.split("-")[0]) + 1
                        games_data["SeasonType"] = season_type
                        all_games.append(games_data)

                        time.sleep(1.5) 
                        break

                    except (requests.exceptions.RequestException, ValueError) as e:
                        wait_time = (attempt + 1) * 2
                        logging.warning(
                            f"Attempt {attempt + 1}/{max_retries} failed for team {team_id} "
                            f"in {season} {season_type}: {str(e)}. Retrying in {wait_time}s..."
                        )
                        time.sleep(wait_time)
                else:
                    logging.error(f"All retries failed for team {team_id} in {season} {season_type}.")

        return pd.concat(all_games, ignore_index=True) if all_games else pd.DataFrame()

def keep_other_season_types(file_path: str, new_df: pd.DataFrame) -> pd.DataFrame:
    """Merge new logs over an existing team file without dropping other season types.

    self.season_types is toggled by hand (e.g. only "Playoffs" in May), and
    writing the fetch result straight to disk used to wipe the team's
    regular-season rows. Rows for the season types just fetched are replaced;
    everything else in the file is kept.
    """
    if not os.path.exists(file_path) or 'SeasonType' not in new_df.columns:
        return new_df
    try:
        old = pd.read_csv(file_path)
    except Exception as e:
        logging.warning(f"Could not read {file_path} to merge season types: {e}")
        return new_df
    if 'SeasonType' not in old.columns:
        return new_df
    kept = old[~old['SeasonType'].isin(new_df['SeasonType'].unique())]
    if kept.empty:
        return new_df
    return pd.concat([kept, new_df], ignore_index=True)

def get_team_abbreviations():
    """Returns a dictionary mapping team IDs to team abbreviations."""
    return {
        '1610612755': 'PHI', '1610612744': 'GSW', '1610612752': 'NYK', '1610612737': 'ATL', 
        '1610612758': 'SAC', '1610612738': 'BOS', '1610612765': 'DET', '1610612747': 'LAL', 
        '1610612741': 'CHI', '1610612764': 'WAS', '1610612745': 'HOU', '1610612760': 'OKC', 
        '1610612749': 'MIL', '1610612756': 'PHX', '1610612757': 'POR', '1610612739': 'CLE', 
        '1610612761': 'TOR', '1610612762': 'UTA', '1610612754': 'IND', '1610612751': 'BRK', 
        '1610612743': 'DEN', '1610612759': 'SAS', '1610612746': 'LAC', '1610612742': 'DAL', 
        '1610612766': 'CHO', '1610612748': 'MIA', '1610612753': 'ORL', '1610612750': 'MIN', 
        '1610612763': 'MEM', '1610612740': 'NOP'
    }

def derive_opponent_logs(year_dir: str, team_dict: Dict[str, str], reference_path: str, season: str
                         ) -> Tuple[Dict[str, pd.DataFrame], List[str]]:
    """Opponent logs built from the other teams' own logs, with no requests (2026-11-18).

    A team's opponent row for a game is the other team's own row for that game. Checked on every row of 2025-26 and
    2026-27 (3,050) against pbpstats' Opponent request: every column equal except Minutes, which pbpstats rounds per
    side (the two differ by 1 s on 12-17% of rows, never more; this carries the other team's own Minutes). Relabeled:
    team_id is this team, Opponent the other team as this team's own row names it, team this team (team_dict).

    Returns ({team_id: opponent logs}, [team_ids to fetch instead]): a team goes to the second list when its own file
    lacks a game the reference lists for it, or another team's row for one of its games isn't saved.
    """
    key = ["GameId", "SeasonType"]
    own = {}
    for f in glob.glob(os.path.join(year_dir, "16106127??.csv")):
        try:
            own[os.path.basename(f)[:10]] = pd.read_csv(f, low_memory=False)
        except Exception as e:
            logging.warning(f"Could not read {f} to build opponent logs: {e}")
    if not own:
        return {}, []
    try:
        ref = pd.read_csv(reference_path)
        ref = ref[ref["season"] == season]
        sched = ref.groupby(ref["TEAM_ID"].astype(str))["GAME_ID"].apply(lambda s: set(s.astype(int))).to_dict()
    except Exception as e:
        logging.warning(f"Could not read {reference_path} for the schedule check: {e}")
        sched = {}
    rows = pd.concat([d.assign(_src=t) for t, d in own.items()], ignore_index=True)
    derived, missing = {}, []
    for tid, mine in own.items():
        games = mine[key + ["Opponent"]].drop_duplicates(key)
        if sched.get(tid, set()) - set(games["GameId"].astype(int)):
            missing.append(tid)  # its own logs are behind (a failed Team fetch): fetch its Opponent logs as before
            continue
        theirs = rows[rows["_src"] != tid].merge(games[key], on=key)
        if len(theirs.drop_duplicates(key)) < len(games):
            missing.append(tid)
            continue
        vs = theirs.drop(columns=["_src", "Opponent"]).merge(games, on=key, how="left")
        vs = vs.drop(columns=[c for c in vs.columns if vs[c].isna().all()])  # columns only other teams' files carry
        vs["team_id"] = tid
        if tid in team_dict:
            vs["team"] = team_dict[tid]
        if "Date" in vs.columns:
            vs = vs.sort_values(["Date", "GameId"], kind="stable")
        derived[tid] = vs.reset_index(drop=True)
    return derived, missing


def fetch_all_teams_game_logs(
    team_ids: List[str],
    start_year: int,
    end_year: int,
    entity_type: str = "Team"
) -> Tuple[Dict[str, pd.DataFrame], List[str]]:
    """
    Fetches game logs for all given teams.

    Returns:
        team_games: Dict mapping team_id -> DataFrame of game logs (empty DF on failure)
        failed_ids: List of team_ids whose logs came back empty after all per-request retries
    """
    api = PBPStatsAPI(start_year=start_year, end_year=end_year)
    team_games = {}
    failed_ids = []
    total_teams = len(team_ids)

    print(f"Starting scrape for {total_teams} teams ({start_year}-{end_year}) [{entity_type}]...")

    for idx, team_id in enumerate(team_ids):
        t_id_str = str(team_id)
        logging.info(f"[{idx+1}/{total_teams}] Fetching {entity_type} logs for {t_id_str}...")
        result = api.get_team_game_logs(t_id_str, entity_type)
        team_games[t_id_str] = result

        if result.empty:
            failed_ids.append(t_id_str)
            logging.warning(f"[{entity_type}] Empty result for team {t_id_str} — flagged for team-level retry.")

    return team_games, failed_ids


def retry_failed_teams(
    failed_ids: List[str],
    start_year: int,
    end_year: int,
    entity_type: str,
    wait_between: int = 5
) -> Tuple[Dict[str, pd.DataFrame], List[str]]:
    """
    One additional team-level retry pass for any teams that came back empty.
    Waits `wait_between` seconds before each retry to give the API a breather.

    Returns:
        recovered: Dict of team_id -> DataFrame for teams that now have data
        still_failed: List of team_ids that are still empty after the retry
    """
    if not failed_ids:
        return {}, []

    logging.info(f"[{entity_type}] Retrying {len(failed_ids)} failed team(s): {failed_ids}")
    api = PBPStatsAPI(start_year=start_year, end_year=end_year)
    recovered = {}
    still_failed = []

    for team_id in failed_ids:
        logging.info(f"[{entity_type}] Team-level retry for {team_id}...")
        time.sleep(wait_between)
        result = api.get_team_game_logs(team_id, entity_type)

        if result.empty:
            still_failed.append(team_id)
            logging.error(f"[{entity_type}] Team {team_id} still empty after team-level retry.")
        else:
            recovered[team_id] = result
            logging.info(f"[{entity_type}] Team {team_id} recovered successfully.")

    return recovered, still_failed


def log_final_failures(entity_type: str, still_failed: List[str]) -> None:
    """Logs a clear summary of teams that could not be fetched after all retries."""
    if not still_failed:
        logging.info(f"[{entity_type}] All teams fetched successfully.")
        return

    logging.error(
        f"\n{'='*60}\n"
        f"[{entity_type}] FINAL FAILURES — {len(still_failed)} team(s) could not be fetched:\n"
        f"  {still_failed}\n"
        f"These teams will NOT have updated {'VS' if entity_type == 'Opponent' else ''} files.\n"
        f"Re-run the script or investigate the API for these IDs.\n"
        f"{'='*60}"
    )

def _parse_dates_from_df(df: pd.DataFrame, label: str) -> Dict[int, datetime]:
    """
    Shared helper: extracts {team_id (int): max Date} from a DataFrame.
    Handles float/string team_id coercion.
    """
    if df.empty or 'Date' not in df.columns:
        return {}

    df = df.copy()
    df['Date'] = pd.to_datetime(df['Date'])

    try:
        df['team_id'] = df['team_id'].astype(int)
    except Exception as e:
        logging.warning(f"[{label}] Error converting team_id to int: {e}")
        df['team_id'] = pd.to_numeric(df['team_id'], errors='coerce')
        df = df.dropna(subset=['team_id'])
        df['team_id'] = df['team_id'].astype(int)

    return df.groupby('team_id')['Date'].max().to_dict()


def get_latest_local_dates(year_dir: str) -> Dict[int, datetime]:
    """
    Reads local Team CSVs to find the max date played for each team.
    Returns: Dict {team_id (int): last_game_date (datetime)}
    """
    all_logs_path = os.path.join(year_dir, 'all_logs.csv')
    df = pd.DataFrame()

    if os.path.exists(all_logs_path):
        try:
            df = pd.read_csv(all_logs_path)
            logging.info(f"Loaded existing local data from {all_logs_path}")
        except Exception as e:
            logging.warning(f"Could not read {all_logs_path}, scanning individual files. Error: {e}")

    if df.empty and os.path.exists(year_dir):
        logging.info("Scanning individual team files for dates...")
        files = [f for f in os.listdir(year_dir) if f.endswith('.csv') and 'vs' not in f and 'all_logs' not in f]
        frames = []
        for f in files:
            try:
                temp_df = pd.read_csv(os.path.join(year_dir, f))
                if 'team_id' not in temp_df.columns:
                    try:
                        temp_df['team_id'] = int(f.split('.')[0])
                    except ValueError:
                        logging.warning(f"Could not parse team_id from filename {f}, skipping.")
                        continue
                frames.append(temp_df)
            except:
                continue
        if frames:
            df = pd.concat(frames)

    return _parse_dates_from_df(df, label="Team")


def get_latest_local_dates_vs(year_dir: str) -> Dict[int, datetime]:
    """
    Reads local Opponent (VS) CSVs to find the max date played for each team.
    Returns: Dict {team_id (int): last_game_date (datetime)}
    """
    vs_all_logs_path = os.path.join(year_dir, 'vs_all_logs.csv')
    df = pd.DataFrame()

    if os.path.exists(vs_all_logs_path):
        try:
            df = pd.read_csv(vs_all_logs_path)
            logging.info(f"Loaded existing local VS data from {vs_all_logs_path}")
        except Exception as e:
            logging.warning(f"Could not read {vs_all_logs_path}, scanning individual VS files. Error: {e}")

    if df.empty and os.path.exists(year_dir):
        logging.info("Scanning individual VS team files for dates...")
        files = [f for f in os.listdir(year_dir) if f.endswith('.csv') and 'vs' in f and 'all_logs' not in f]
        frames = []
        for f in files:
            try:
                temp_df = pd.read_csv(os.path.join(year_dir, f))
                if 'team_id' not in temp_df.columns:
                    try:
                        # filenames are like 1610612746vs.csv
                        temp_df['team_id'] = int(f.split('vs')[0])
                    except ValueError:
                        logging.warning(f"Could not parse team_id from VS filename {f}, skipping.")
                        continue
                frames.append(temp_df)
            except:
                continue
        if frames:
            df = pd.concat(frames)

    return _parse_dates_from_df(df, label="Opponent")

def _needs_update(team_id: int, local_dates: Dict[int, datetime], ref_max_dates: Dict[int, datetime]) -> Tuple[bool, str]:
    """Returns (should_update, reason) for a single team/entity-type combination."""
    ref_date = ref_max_dates.get(team_id)
    local_date = local_dates.get(team_id)

    if team_id not in local_dates:
        return True, "No Local Data"
    if team_id not in ref_max_dates or pd.isna(ref_date):
        return True, "No Ref Data"
    if ref_date > local_date:
        return True, "Ref > Local"
    return False, "Up to Date"


def get_teams_needing_update(
    reference_file: str,
    local_dates: Dict[int, datetime],
    local_dates_vs: Dict[int, datetime],
    all_team_ids: List[int],
    debug_mode: bool = True
) -> Tuple[List[int], List[int]]:
    """
    Compares reference dates vs local Team and Opponent dates independently.
    Returns two lists: (teams_needing_team_update, teams_needing_opp_update)
    """
    try:
        logging.info(f"Reading reference dates from {reference_file}...")
        ref_df = pd.read_csv(reference_file)
        ref_df.columns = [c.lower() for c in ref_df.columns]

        date_col = next((col for col in ref_df.columns if 'date' in col), None)
        team_col = next((col for col in ref_df.columns if 'team_id' in col or 'team' in col), None)

        if not date_col or not team_col:
            logging.warning("Could not identify Date or Team columns in reference CSV. Scraping ALL teams.")
            return all_team_ids, all_team_ids

        if pd.api.types.is_numeric_dtype(ref_df[date_col]):
            ref_df[date_col] = pd.to_datetime(ref_df[date_col].astype(str), format='%Y%m%d', errors='coerce')
        else:
            ref_df[date_col] = pd.to_datetime(ref_df[date_col], errors='coerce')

        ref_df[team_col] = ref_df[team_col].astype(int)
        ref_max_dates = ref_df.groupby(team_col)[date_col].max().to_dict()

        teams_team = []
        teams_opp = []

        if debug_mode:
            print(f"\n{'='*95}")
            print(f"{'TEAM ID':<12} | {'REF DATE':<12} | {'TEAM LOCAL':<12} | {'TEAM ACTION':<20} | {'OPP LOCAL':<12} | {'OPP ACTION'}")
            print(f"{'-'*95}")

        for team_id in all_team_ids:
            ref_date = ref_max_dates.get(team_id)
            r_str = ref_date.strftime('%Y-%m-%d') if (ref_date and not pd.isna(ref_date)) else "None"

            team_update, team_reason = _needs_update(team_id, local_dates, ref_max_dates)
            opp_update, opp_reason = _needs_update(team_id, local_dates_vs, ref_max_dates)

            if team_update:
                teams_team.append(team_id)
            if opp_update:
                teams_opp.append(team_id)

            if debug_mode:
                local_date = local_dates.get(team_id)
                local_date_vs = local_dates_vs.get(team_id)
                tl_str = local_date.strftime('%Y-%m-%d') if local_date else "None"
                ol_str = local_date_vs.strftime('%Y-%m-%d') if local_date_vs else "None"
                t_action = f"UPDATE ({team_reason})" if team_update else "SKIP"
                o_action = f"UPDATE ({opp_reason})" if opp_update else "SKIP"
                print(f"{team_id:<12} | {r_str:<12} | {tl_str:<12} | {t_action:<20} | {ol_str:<12} | {o_action}")

        if debug_mode:
            print(f"{'='*95}\n")

        return teams_team, teams_opp

    except Exception as e:
        logging.error(f"Failed to fetch or parse reference dates: {e}")
        logging.warning("Defaulting to scraping ALL teams.")
        return all_team_ids, all_team_ids

if __name__ == "__main__":
    start_year = SEASON_YEAR
    end_year = start_year

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

    year_dir = f"team/{start_year}"
    os.makedirs(year_dir, exist_ok=True)

    try:
        df_index = pd.read_csv('https://raw.githubusercontent.com/gabriel1200/site_Data/refs/heads/master/index_master.csv')
        # index_master gets the new season's rows only after it starts; last season lists the same teams.
        df_index = df_index[df_index.year >= start_year - 1]
        df_index = df_index[df_index.team != 'TOT']
        # Ensure correct type for the list
        all_team_ids = df_index['team_id'].astype(int).unique().tolist()
    except Exception as e:
        logging.error(f"Failed to load master index: {e}")
        exit()

    local_ref_file = "../../shot_data/game_dates.csv"
    if os.path.exists(local_ref_file):
        reference_path = local_ref_file
        logging.info(f"Using local reference file: {local_ref_file}")
    else:
        reference_path = "https://raw.githubusercontent.com/gabriel1200/shot_data/refs/heads/master/game_dates.csv"
        logging.info(f"Using remote reference file: {reference_path}")

    local_dates_map = get_latest_local_dates(year_dir)
    local_dates_vs_map = get_latest_local_dates_vs(year_dir)

    teams_needing_team, teams_needing_opp = get_teams_needing_update(
        reference_path, local_dates_map, local_dates_vs_map, all_team_ids, debug_mode=True
    )

    logging.info(f"Total Teams: {len(all_team_ids)}")
    logging.info(f"Teams requiring Team update: {len(teams_needing_team)}")
    logging.info(f"Teams requiring Opponent update: {len(teams_needing_opp)}")

    if teams_needing_team:
        teams_to_scrape_str = [str(t) for t in teams_needing_team]

        # --- TEAM LOGS ---
        team_game_logs, team_failed = fetch_all_teams_game_logs(
            teams_to_scrape_str, start_year, end_year, entity_type="Team"
        )

        team_recovered, team_still_failed = retry_failed_teams(
            team_failed, start_year, end_year, entity_type="Team"
        )
        team_game_logs.update(team_recovered)
        log_final_failures("Team", team_still_failed)

        for team_id_str, games_df in team_game_logs.items():
            if not games_df.empty:
                os.makedirs(year_dir, exist_ok=True)
                for year in range(start_year, end_year + 1):
                    team_df = games_df[games_df.year == year].copy()
                    if not team_df.empty:
                        team_df['team_id'] = team_id_str
                        file_path = f"{year_dir}/{team_id_str}.csv"
                        team_df = keep_other_season_types(file_path, team_df)
                        team_df.to_csv(file_path, index=False)
                        logging.info(f"Updated file: {file_path}")
    else:
        logging.info("No teams need a Team log update.")

    # --- OPPONENT LOGS ---
    # Built from the team files just written (derive_opponent_logs; the check is in its docstring). pbpstats' Opponent
    # request is now only the fallback, for a team whose own logs or an opponent's are missing a game.
    team_dict = get_team_abbreviations()
    season_str = f"{start_year - 1}-{str(start_year)[-2:]}"
    derived_opp, opp_missing = derive_opponent_logs(year_dir, team_dict, reference_path, season_str)
    for team_id_str, team_df in derived_opp.items():
        file_path = f"{year_dir}/{team_id_str}vs.csv"
        team_df = keep_other_season_types(file_path, team_df)
        team_df.to_csv(file_path, index=False)
    logging.info(f"Opponent logs built from the other teams' own logs: {len(derived_opp)} teams")

    if opp_missing:
        logging.info(f"Fetching Opponent logs for teams with a missing game: {opp_missing}")
        opp_teams_str = [str(t) for t in opp_missing]
        opp_game_logs, opp_failed = fetch_all_teams_game_logs(
            opp_teams_str, start_year, end_year, entity_type="Opponent"
        )

        opp_recovered, opp_still_failed = retry_failed_teams(
            opp_failed, start_year, end_year, entity_type="Opponent"
        )
        opp_game_logs.update(opp_recovered)
        log_final_failures("Opponent", opp_still_failed)

        for team_id_str, games_df in opp_game_logs.items():
            if not games_df.empty:
                for year in range(start_year, end_year + 1):
                    team_df = games_df[games_df.year == year].copy()
                    if not team_df.empty:
                        team_df['team_id'] = team_id_str
                        if str(team_id_str) in team_dict:
                            team_df['team'] = team_dict[str(team_id_str)]
                        file_path = f"{year_dir}/{team_id_str}vs.csv"
                        team_df = keep_other_season_types(file_path, team_df)
                        team_df.to_csv(file_path, index=False)
                        logging.info(f"Updated VS file: {file_path}")

    logging.info("Re-aggregating all files...")

    files = [f for f in os.listdir(year_dir) if f.endswith('.csv') and 'vs' not in f and 'all_logs' not in f]
    if files:
        totals = []
        for file in files:
            try:
                df = pd.read_csv(os.path.join(year_dir, file))
                df['TeamId'] = file.split('.')[0] 
                totals.append(df)
            except Exception as e:
                logging.warning(f"Skipping corrupt file {file}: {e}")

        if totals:
            master = pd.concat(totals, ignore_index=True)
            master.to_csv(os.path.join(year_dir, 'all_logs.csv'), index=False)
            logging.info(f"Saved {year_dir}/all_logs.csv")

    files_vs = [f for f in os.listdir(year_dir) if f.endswith('.csv') and 'vs' in f and 'all_logs' not in f]
    if files_vs:
        totals_vs = []
        for file in files_vs:
            try:
                df = pd.read_csv(os.path.join(year_dir, file))
                split1 = file.split('.')[0]
                split2 = split1.split('vs')[0]
                df['TeamId'] = split2
                totals_vs.append(df)
            except Exception as e:
                logging.warning(f"Skipping corrupt vs file {file}: {e}")

        if totals_vs:
            master_vs = pd.concat(totals_vs, ignore_index=True)
            master_vs.to_csv(os.path.join(year_dir, 'vs_all_logs.csv'), index=False)
            logging.info(f"Saved {year_dir}/vs_all_logs.csv")

    logging.info("Done.")

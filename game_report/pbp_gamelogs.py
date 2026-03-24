#!/usr/bin/env python
# coding: utf-8

# In[ ]:


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
                        response = requests.get(self.base_url, params=params, timeout=10)
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
    start_year = 2026
    end_year = start_year

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

    year_dir = f"team/{start_year}"
    os.makedirs(year_dir, exist_ok=True)

    try:
        df_index = pd.read_csv('https://raw.githubusercontent.com/gabriel1200/site_Data/refs/heads/master/index_master.csv')
        df_index = df_index[df_index.year >= start_year]
        df_index = df_index[df_index.team != 'TOT']
        # Ensure correct type for the list
        all_team_ids = df_index['team_id'].astype(int).unique().tolist()
    except Exception as e:
        logging.error(f"Failed to load master index: {e}")
        exit()

    local_ref_file = "game_dates.csv"
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
                        team_df.to_csv(file_path, index=False)
                        logging.info(f"Updated file: {file_path}")
    else:
        logging.info("No teams need a Team log update.")

    if teams_needing_opp:
        # --- OPPONENT LOGS ---
        logging.info("Fetching Opponent logs for updated teams...")
        team_dict = get_team_abbreviations()

        opp_teams_str = [str(t) for t in teams_needing_opp]
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
                        team_df.to_csv(file_path, index=False)
                        logging.info(f"Updated VS file: {file_path}")
    else:
        logging.info("No teams need an Opponent log update.")

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


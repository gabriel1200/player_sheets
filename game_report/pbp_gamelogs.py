#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#!/usr/bin/env python
# coding: utf-8

import requests
import pandas as pd
from datetime import datetime
import time
from typing import List, Dict
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

def fetch_all_teams_game_logs(team_ids: List[str], start_year: int, end_year: int, entity_type:str ="Team") -> Dict[str, pd.DataFrame]:
    api = PBPStatsAPI(start_year=start_year, end_year=end_year)
    team_games = {}
    total_teams = len(team_ids)

    print(f"Starting scrape for {total_teams} teams ({start_year}-{end_year})...")

    for idx, team_id in enumerate(team_ids):
        t_id_str = str(team_id)
        logging.info(f"[{idx+1}/{total_teams}] Fetching logs for {t_id_str} ({entity_type})...")
        team_games[t_id_str] = api.get_team_game_logs(t_id_str, entity_type)

    return team_games

def get_latest_local_dates(year_dir: str) -> Dict[int, datetime]:
    """
    Reads local CSVs to find the max date played for each team.
    Returns: Dict {team_id (int): last_game_date (datetime)}
    """
    local_dates = {}
    all_logs_path = os.path.join(year_dir, 'all_logs.csv')
    df = pd.DataFrame()

    # 1. Try reading the master file
    if os.path.exists(all_logs_path):
        try:
            df = pd.read_csv(all_logs_path)
            logging.info(f"Loaded existing local data from {all_logs_path}")
        except Exception as e:
            logging.warning(f"Could not read {all_logs_path}, scanning individual files. Error: {e}")

    # 2. If master file is empty or missing, scan individual files
    if df.empty and os.path.exists(year_dir):
        logging.info("Scanning individual team files for dates...")
        files = [f for f in os.listdir(year_dir) if f.endswith('.csv') and 'vs' not in f and 'all_logs' not in f]
        frames = []
        for f in files:
            try:
                temp_df = pd.read_csv(os.path.join(year_dir, f))
                # If team_id is missing, grab from filename and force INT
                if 'team_id' not in temp_df.columns:
                     # Ensure we convert the filename part to int immediately
                     try:
                         tid = int(f.split('.')[0])
                         temp_df['team_id'] = tid
                     except ValueError:
                         logging.warning(f"Could not parse team_id from filename {f}, skipping.")
                         continue
                frames.append(temp_df)
            except:
                continue
        if frames:
            df = pd.concat(frames)

    if not df.empty:
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'])

            # --- FORCE INT TYPE FOR TEAM_ID ---
            # This handles cases where it might be loaded as float (e.g. 1610612744.0) or string
            try:
                df['team_id'] = df['team_id'].astype(int)
            except Exception as e:
                logging.warning(f"Error converting team_id to int: {e}")
                # Fallback: try to coerce non-numeric to NaN then drop, then int
                df['team_id'] = pd.to_numeric(df['team_id'], errors='coerce')
                df = df.dropna(subset=['team_id'])
                df['team_id'] = df['team_id'].astype(int)

            max_dates = df.groupby('team_id')['Date'].max()
            local_dates = max_dates.to_dict() # Dictionary keys are now Integers

    return local_dates

def get_teams_needing_update(reference_file: str, local_dates: Dict[int, datetime], all_team_ids: List[int], debug_mode: bool = True) -> List[int]:
    """
    Compares reference dates (GitHub) vs Local dates.
    Uses INTEGERS for team_ids to match correctly.
    """
    teams_to_scrape = []

    try:
        logging.info(f"Reading reference dates from {reference_file}...")

        ref_df = pd.read_csv(reference_file)

        ref_df.columns = [c.lower() for c in ref_df.columns]

        date_col = next((col for col in ref_df.columns if 'date' in col), None)
        team_col = next((col for col in ref_df.columns if 'team_id' in col or 'team' in col), None)

        if not date_col or not team_col:
            logging.warning("Could not identify Date or Team columns in reference CSV. Scraping ALL teams.")
            return all_team_ids

        # --- FIX: Convert integer YYYYMMDD to datetime ---
        if pd.api.types.is_numeric_dtype(ref_df[date_col]):
             ref_df[date_col] = pd.to_datetime(ref_df[date_col].astype(str), format='%Y%m%d', errors='coerce')
        else:
             ref_df[date_col] = pd.to_datetime(ref_df[date_col], errors='coerce')

        # --- FIX: Ensure Reference Team IDs are INTs ---
        ref_df[team_col] = ref_df[team_col].astype(int)

        # Get max date per team (Key is INT)
        ref_max_dates = ref_df.groupby(team_col)[date_col].max().to_dict()

        if debug_mode:
            print(f"\n{'='*65}")
            print(f"{'TEAM ID':<12} | {'LOCAL DATE':<12} | {'REF DATE':<12} | {'ACTION'}")
            print(f"{'-'*65}")

        for team_id in all_team_ids:
            # team_id is already an INT from the master index list

            should_update = False
            reason = ""

            ref_date = ref_max_dates.get(team_id)
            local_date = local_dates.get(team_id)

            # Case 1: No local data at all -> UPDATE
            if team_id not in local_dates:
                should_update = True
                reason = "No Local Data"

            # Case 2: No reference data -> Safety Update
            elif team_id not in ref_max_dates or pd.isna(ref_date):
                should_update = True
                reason = "No Ref Data"

            # Case 3: Reference date is newer than local date -> UPDATE
            elif ref_date > local_date:
                should_update = True
                reason = "Ref > Local"

            # Case 4: Up to date -> SKIP
            else:
                should_update = False
                reason = "Up to Date"

            if should_update:
                teams_to_scrape.append(team_id)

            if debug_mode:
                l_str = local_date.strftime('%Y-%m-%d') if local_date else "None"
                r_str = ref_date.strftime('%Y-%m-%d') if (ref_date and not pd.isna(ref_date)) else "None"
                action = "UPDATE" if should_update else "SKIP"
                print(f"{team_id:<12} | {l_str:<12} | {r_str:<12} | {action} ({reason})")

        if debug_mode:
            print(f"{'='*65}\n")

        return teams_to_scrape

    except Exception as e:
        logging.error(f"Failed to fetch or parse reference dates: {e}")
        logging.warning("Defaulting to scraping ALL teams.")
        return all_team_ids

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

    teams_to_scrape = get_teams_needing_update(reference_path, local_dates_map, all_team_ids, debug_mode=True)

    logging.info(f"Total Teams: {len(all_team_ids)}")
    logging.info(f"Teams requiring update: {len(teams_to_scrape)}")

    if teams_to_scrape:
        # Convert IDs back to string for the API Calls
        teams_to_scrape_str = [str(t) for t in teams_to_scrape]

        team_game_logs = fetch_all_teams_game_logs(teams_to_scrape_str, start_year, end_year, entity_type="Team")

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

        logging.info("Fetching Opponent logs for updated teams...")
        team_dict = get_team_abbreviations()
        opp_game_logs = fetch_all_teams_game_logs(teams_to_scrape_str, start_year, end_year, entity_type="Opponent")

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
        logging.info("No teams need updating.")

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


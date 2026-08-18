import os
import time
import pandas as pd
import requests
from datetime import datetime
from requests.exceptions import RequestException

TARGET_FILE = 'year_files/2024_games.csv'
TARGET_DATE = 20240309

def format_date_to_url(date):
    date_obj = datetime.strptime(str(date), '%Y%m%d')
    return date_obj.strftime('%m%%2F%d%%2F%Y')

def pull_data(url, max_retries=3, delay_seconds=4):
    headers = {
        "Host": "stats.nba.com",
        "Connection": "keep-alive",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
        "Dnt": "1",
        "Sec-Ch-Ua": '"Not=A?Brand";v="99", "Google Chrome";v="151", "Chromium";v="151"',
        "Sec-Ch-Ua-Mobile": "?1",
        "Sec-Ch-Ua-Platform": '"Android"',
        "User-Agent": "Mozilla/5.0 (Linux; Android 15; Pixel 9) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Mobile Safari/537.36",
        "Accept": "*/*",
        "Origin": "https://www.nba.com",
        "Sec-Fetch-Site": "same-site",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
        "Referer": "https://www.nba.com/",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "en-US,en;q=0.9",
    }

    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            json_data = response.json()

            if len(json_data.get("resultSets", [])) == 1:
                data = json_data["resultSets"][0]["rowSet"]
                columns = json_data["resultSets"][0]["headers"]
                df = pd.DataFrame.from_records(data, columns=columns)
            else:
                data = json_data["resultSets"]["rowSet"]
                columns = json_data["resultSets"]["headers"][1]["columnNames"]
                df = pd.DataFrame.from_records(data, columns=columns)

            time.sleep(0.7)
            return df
        except (RequestException, ValueError, KeyError) as e:
            print(f"    [Attempt {attempt + 1}] API Error: {e}")
            if attempt < max_retries - 1:
                time.sleep(delay_seconds)
            else:
                print(f"    [!] Max retries reached for: {url}")
                return pd.DataFrame()

    return pd.DataFrame()

def patch_single_date():
    if not os.path.exists(TARGET_FILE):
        print(f"[!] File not found: {TARGET_FILE}")
        return

    print(f"--- Patching single dropout date {TARGET_DATE} in {TARGET_FILE} ---")
    df_main = pd.read_csv(TARGET_FILE, low_memory=False)
    df_main['date'] = df_main['date'].astype(int)

    date_str = format_date_to_url(TARGET_DATE)
    season = "2023-24"
    stype = "Regular%20Season"

    day_patch = df_main[df_main['date'] == TARGET_DATE][['PLAYER_ID', 'date']].copy()

    # 1. Pull Tracking Measures (Passing, Drives, Possessions, Rebounding, PostTouch, SpeedDistance)
    for ptype in ['Passing', 'Drives', 'Possessions', 'Rebounding', 'PostTouch', 'SpeedDistance']:
        print(f"Fetching PtMeasureType={ptype}...")
        url = f"https://stats.nba.com/stats/leaguedashptstats?College=&Conference=&Country=&DateFrom={date_str}&DateTo={date_str}&Division=&DraftPick=&DraftYear=&GameScope=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PerMode=Totals&PlayerExperience=&PlayerOrTeam=Player&PlayerPosition=&PtMeasureType={ptype}&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight="
        df_pt = pull_data(url)
        if not df_pt.empty and 'PLAYER_ID' in df_pt.columns:
            ignore = {'PLAYER_NAME', 'TEAM_ID', 'TEAM_ABBREVIATION', 'GP', 'W', 'L', 'MIN', 'AGE'}
            cols = [c for c in df_pt.columns if c not in ignore and c != 'PLAYER_ID']
            if ptype == 'PostTouch':
                df_pt.rename(columns={c: f'post_touch_{c}' for c in cols}, inplace=True)
                cols = [f'post_touch_{c}' for c in cols]
            day_patch = day_patch.merge(df_pt[['PLAYER_ID'] + cols], on='PLAYER_ID', how='left')

    # 2. Pull Open Shots Split
    print("Fetching Open Shots...")
    shotcols = ['FGA_FREQUENCY', 'FGM', 'FGA', 'FG_PCT', 'EFG_PCT', 'FG2A_FREQUENCY', 'FG2M', 'FG2A', 'FG2_PCT', 'FG3A_FREQUENCY', 'FG3M', 'FG3A', 'FG3_PCT']
    url_open = f"https://stats.nba.com/stats/leaguedashplayerptshot?CloseDefDistRange=4-6%20Feet%20-%20Open&College=&Conference=&Country=&DateFrom={date_str}&DateTo={date_str}&Division=&DraftPick=&DraftYear=&DribbleRange=&GameScope=&GameSegment=&GeneralRange=&Height=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&Season={season}&SeasonSegment=&SeasonType={stype}&ShotClockRange=&ShotDistRange=&StarterBench=&TeamID=0&TouchTimeRange=&VsConference=&VsDivision=&Weight="
    df_open = pull_data(url_open)
    if not df_open.empty and 'PLAYER_ID' in df_open.columns:
        df_open.rename(columns={col: f'open_{col}' for col in shotcols if col in df_open.columns}, inplace=True)
        open_cols = [f'open_{col}' for col in shotcols if f'open_{col}' in df_open.columns]
        day_patch = day_patch.merge(df_open[['PLAYER_ID'] + open_cols], on='PLAYER_ID', how='left')

    # 3. In-place merge update for target date rows
    print("\nApplying updates to master DataFrame...")
    df_main.set_index(['PLAYER_ID', 'date'], inplace=True)
    day_patch.set_index(['PLAYER_ID', 'date'], inplace=True)

    common_cols = [c for c in day_patch.columns if c in df_main.columns]
    for col in common_cols:
        df_main[col].update(day_patch[col])

    new_cols = [c for c in day_patch.columns if c not in df_main.columns]
    for col in new_cols:
        df_main[col] = day_patch[col]

    df_main.reset_index(inplace=True)
    df_main.to_csv(TARGET_FILE, index=False)
    print(f"[✓] Successfully patched {TARGET_DATE} into {TARGET_FILE}!")

if __name__ == '__main__':
    patch_single_date()
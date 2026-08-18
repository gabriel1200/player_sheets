import os
import time
import pandas as pd
import requests
from datetime import datetime
from requests.exceptions import RequestException

def format_date_to_url(date):
    date_obj = datetime.strptime(str(date), '%Y%m%d')
    return date_obj.strftime('%m%%2F%d%%2F%Y')

def pull_data(url, max_retries=3, delay_seconds=5):
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
                return pd.DataFrame.from_records(data, columns=columns)
            else:
                data = json_data["resultSets"]["rowSet"]
                columns = json_data["resultSets"]["headers"][1]["columnNames"]
                return pd.DataFrame.from_records(data, columns=columns)
        except (RequestException, ValueError, KeyError) as e:
            print(f"  [Attempt {attempt + 1}] Error: {e}")
            if attempt < max_retries - 1:
                time.sleep(delay_seconds)
    return pd.DataFrame()

def patch_rebounding_dates(target_dates=[20260113, 20260305], file_path='year_files/2026_games.csv', year=2026):
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return

    print(f"Loading {file_path}...")
    df_main = pd.read_csv(file_path)
    df_main['date'] = df_main['date'].astype(int)

    season = f"{year - 1}-{str(year)[-2:]}"
    stype = 'Regular%20Season'
    
    reb_frames = []

    for date_num in target_dates:
        date_str = format_date_to_url(date_num)
        print(f"Fetching Rebounding Tracking for {date_num}...")

        url_reb = (
            f"https://stats.nba.com/stats/leaguedashptstats?"
            f"College=&Conference=&Country=&DateFrom={date_str}&DateTo={date_str}&Division=&DraftPick=&DraftYear="
            f"&GameScope=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome="
            f"&PORound=0&PerMode=Totals&PlayerExperience=&PlayerOrTeam=Player&PlayerPosition="
            f"&PtMeasureType=Rebounding&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0"
            f"&VsConference=&VsDivision=&Weight="
        )
        df_reb = pull_data(url_reb)
        if not df_reb.empty and 'PLAYER_ID' in df_reb.columns:
            # Drop redundant metadata columns
            ignore_cols = {'PLAYER_NAME', 'TEAM_ID', 'TEAM_ABBREVIATION', 'GP', 'W', 'L', 'MIN', 'AGE'}
            keep_cols = [c for c in df_reb.columns if c not in ignore_cols]
            df_reb = df_reb[keep_cols].copy()
            df_reb['date'] = date_num
            reb_frames.append(df_reb)
        else:
            print(f"  [!] Failed to pull rebounding for date {date_num}")
        
        time.sleep(1.0)

    if not reb_frames:
        print("No rebounding data fetched. Exiting.")
        return

    patch_df = pd.concat(reb_frames, ignore_index=True)
    patch_df.drop_duplicates(subset=['PLAYER_ID', 'date'], inplace=True)
    reb_stat_cols = [c for c in patch_df.columns if c not in ['PLAYER_ID', 'date']]

    print(f"\nPatching {len(reb_stat_cols)} rebounding columns for {len(target_dates)} dates...")

    # Update df_main in-place using combine_first / update on the target index
    df_main.set_index(['PLAYER_ID', 'date'], inplace=True)
    patch_df.set_index(['PLAYER_ID', 'date'], inplace=True)

    for col in reb_stat_cols:
        if col in df_main.columns:
            df_main[col].update(patch_df[col])
        else:
            df_main[col] = patch_df[col]

    df_main.reset_index(inplace=True)
    df_main.to_csv(file_path, index=False)
    print(f"[✓] Successfully patched {file_path} in-place!")

if __name__ == '__main__':
    patch_rebounding_dates()
import os
import glob
import time
import pandas as pd
import requests
from datetime import datetime
from requests.exceptions import RequestException

TARGET_FILE = 'year_files/2025ps_games.csv'
CACHE_DIR = 'year_files/cache_2025ps'

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

def run_repair_2025ps(file_path=TARGET_FILE, year=2025, ps=True):
    if not os.path.exists(file_path):
        print(f"[!] File not found: {file_path}")
        return

    os.makedirs(CACHE_DIR, exist_ok=True)
    print(f"--- Launching Isolated Checkpoint Repair for {file_path} ---")
    df_main = pd.read_csv(file_path, low_memory=False)
    df_main['date'] = df_main['date'].astype(int)

    # 1. Purge stale / bugged column names from main DataFrame
    drop_candidates = [c for c in df_main.columns if 
                       c.startswith('overall_def_') or 
                       c.startswith('pullup_') or 
                       c.startswith('PULL_UP_') or 
                       c.startswith('lt6ft_') or
                       c.startswith('D_FGM') or c.startswith('D_FGA') or c.startswith('D_FG_PCT')]
    df_main.drop(columns=drop_candidates, inplace=True, errors='ignore')

    stype = 'Playoffs'
    season = f"{year - 1}-{str(year)[-2:]}"
    all_dates = sorted(df_main['date'].unique().tolist())

    # 2. Check Directory Cache
    cached_files = glob.glob(os.path.join(CACHE_DIR, "*.csv"))
    completed_dates = set()
    for f in cached_files:
        try:
            d_str = os.path.splitext(os.path.basename(f))[0]
            completed_dates.add(int(d_str))
        except ValueError:
            continue

    remaining_dates = [d for d in all_dates if d not in completed_dates]
    print(f"Total dates: {len(all_dates)} | Cached: {len(completed_dates)} | Remaining: {len(remaining_dates)}\n")

    # 3. Pull Only the 3 Missing Endpoints
    for idx, date_num in enumerate(remaining_dates, 1):
        date_str = format_date_to_url(date_num)
        print(f"[{idx}/{len(remaining_dates)}] Fetching Date {date_num}...")

        # 1. Pullups (df11)
        url_pullup = (
            f"https://stats.nba.com/stats/leaguedashptstats?"
            f"College=&Conference=&Country=&DateFrom={date_str}&DateTo={date_str}&Division=&DraftPick=&DraftYear="
            f"&GameScope=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome="
            f"&PORound=0&PerMode=Totals&PlayerExperience=&PlayerOrTeam=Player&PlayerPosition="
            f"&PtMeasureType=PullUpShot&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0"
            f"&VsConference=&VsDivision=&Weight="
        )
        df_pullup = pull_data(url_pullup)
        if not df_pullup.empty and 'PLAYER_ID' in df_pullup.columns:
            ignore = {'PLAYER_ID', 'PLAYER_NAME', 'TEAM_ID', 'TEAM_ABBREVIATION', 'GP', 'W', 'L', 'MIN'}
            cols = [c for c in df_pullup.columns if c not in ignore]
            df_pullup.rename(columns={c: f'pullup_{c}' for c in cols}, inplace=True)
            df_pullup = df_pullup[['PLAYER_ID'] + [f'pullup_{c}' for c in cols]]
        else:
            df_pullup = pd.DataFrame(columns=['PLAYER_ID'])

        # 2. Overall Defense (df18)
        url_def_overall = (
            f"https://stats.nba.com/stats/leaguedashptdefend?"
            f"College=&Conference=&Country=&DateFrom={date_str}&DateTo={date_str}&DefenseCategory=Overall&Division="
            f"&DraftPick=&DraftYear=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0"
            f"&OpponentTeamID=0&Outcome=&PORound=0&PerMode=PerGame&Period=0&PlayerExperience=&PlayerPosition="
            f"&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight="
        )
        df_def = pull_data(url_def_overall)
        if not df_def.empty and 'CLOSE_DEF_PERSON_ID' in df_def.columns:
            df_def.rename(columns={'CLOSE_DEF_PERSON_ID': 'PLAYER_ID'}, inplace=True)
            ignore = {'PLAYER_ID', 'PLAYER_NAME', 'PLAYER_LAST_TEAM_ID', 'PLAYER_LAST_TEAM_ABBREVIATION', 'PLAYER_POSITION', 'AGE', 'GP', 'G', 'FREQ'}
            cols = [c for c in df_def.columns if c not in ignore]
            df_def.rename(columns={c: f'overall_def_{c}' for c in cols}, inplace=True)
            df_def = df_def[['PLAYER_ID'] + [f'overall_def_{c}' for c in cols]]
        else:
            df_def = pd.DataFrame(columns=['PLAYER_ID'])

        # 3. Less Than 6Ft Defense (df14)
        url_def_6ft = (
            f"https://stats.nba.com/stats/leaguedashptdefend?"
            f"College=&Conference=&Country=&DateFrom={date_str}&DateTo={date_str}&DefenseCategory=Less%20Than%206Ft&Division="
            f"&DraftPick=&DraftYear=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0"
            f"&OpponentTeamID=0&Outcome=&PORound=0&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition="
            f"&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight="
        )
        df_6ft = pull_data(url_def_6ft)
        if not df_6ft.empty and 'CLOSE_DEF_PERSON_ID' in df_6ft.columns:
            df_6ft.rename(columns={'CLOSE_DEF_PERSON_ID': 'PLAYER_ID'}, inplace=True)
            ignore = {'PLAYER_ID', 'PLAYER_NAME', 'PLAYER_LAST_TEAM_ID', 'PLAYER_LAST_TEAM_ABBREVIATION', 'PLAYER_POSITION', 'AGE', 'GP', 'G', 'FREQ'}
            cols = [c for c in df_6ft.columns if c not in ignore]
            df_6ft.rename(columns={c: f'lt6ft_totals_{c}' for c in cols}, inplace=True)
            df_6ft = df_6ft[['PLAYER_ID'] + [f'lt6ft_totals_{c}' for c in cols]]
        else:
            df_6ft = pd.DataFrame(columns=['PLAYER_ID'])

        # Combine current day patch
        day_patch = df_main[df_main['date'] == date_num][['PLAYER_ID', 'date']].copy()
        day_patch = day_patch.merge(df_pullup, on='PLAYER_ID', how='left')
        day_patch = day_patch.merge(df_def, on='PLAYER_ID', how='left')
        day_patch = day_patch.merge(df_6ft, on='PLAYER_ID', how='left')

        # Save to isolated file
        date_cache_file = os.path.join(CACHE_DIR, f"{date_num}.csv")
        day_patch.to_csv(date_cache_file, index=False)
        print(f"  [✓] Checkpointed {date_num} ({len(day_patch)} rows)")

    # 4. Final Assembly & Overwrite
    print(f"\nAll 47 playoff dates processed! Combining cached files from {CACHE_DIR}...")
    all_cache_files = glob.glob(os.path.join(CACHE_DIR, "*.csv"))
    cached_df_list = [pd.read_csv(f, low_memory=False) for f in all_cache_files]
    full_cache_df = pd.concat(cached_df_list, ignore_index=True, sort=False)
    full_cache_df.drop_duplicates(subset=['PLAYER_ID', 'date'], inplace=True)

    print(f"Merging combined cache ({full_cache_df.shape}) into {file_path}...")
    final_df = df_main.merge(full_cache_df, on=['PLAYER_ID', 'date'], how='left')
    final_df.to_csv(file_path, index=False)
    print(f"[✓] Successfully repaired {file_path}! Final shape: {final_df.shape}")

if __name__ == '__main__':
    run_repair_2025ps()
import os
import glob
import time
import pandas as pd
import requests
from datetime import datetime
from requests.exceptions import RequestException

TARGET_FILE = 'year_files/2024ps_games.csv'
CACHE_DIR = 'year_files/cache_2024ps'

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

def run_repair_2024ps(file_path=TARGET_FILE, year=2024, ps=True):
    if not os.path.exists(file_path):
        print(f"[!] File not found: {file_path}")
        return

    os.makedirs(CACHE_DIR, exist_ok=True)
    print(f"--- Launching Isolated Checkpoint Repair for {file_path} ---")
    df_main = pd.read_csv(file_path, low_memory=False)
    df_main['date'] = df_main['date'].astype(int)

    # 1. Clean out stale/broken column names
    drop_candidates = [c for c in df_main.columns if 
                       c.startswith('pullup_') or 
                       c.startswith('PULL_UP_') or 
                       c == 'DIST_FEET' or
                       c == 'team_poss']
    df_main.drop(columns=drop_candidates, inplace=True, errors='ignore')

    stype = 'Playoffs' if ps else 'Regular%20Season'
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

        # 1. Pullups
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

        # 2. Speed & Distance
        url_speed = (
            f"https://stats.nba.com/stats/leaguedashptstats?"
            f"College=&Conference=&Country=&DateFrom={date_str}&DateTo={date_str}&Division=&DraftPick=&DraftYear="
            f"&GameScope=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome="
            f"&PORound=0&PerMode=PerGame&PlayerExperience=&PlayerOrTeam=Player&PlayerPosition="
            f"&PtMeasureType=SpeedDistance&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0"
            f"&VsConference=&VsDivision=&Weight="
        )
        df_speed = pull_data(url_speed)
        if not df_speed.empty and 'PLAYER_ID' in df_speed.columns:
            ignore = {'PLAYER_ID', 'PLAYER_NAME', 'TEAM_ID', 'TEAM_ABBREVIATION', 'GP', 'W', 'L', 'MIN'}
            cols = [c for c in df_speed.columns if c not in ignore]
            df_speed = df_speed[['PLAYER_ID'] + cols]
        else:
            df_speed = pd.DataFrame(columns=['PLAYER_ID'])

        # 3. Team Possessions
        url_poss = (
            f"https://stats.nba.com/stats/leaguedashteamstats?"
            f"College=&Conference=&Country=&DateFrom={date_str}&DateTo={date_str}&Division=&DraftPick=&DraftYear="
            f"&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&MeasureType=Advanced&Month=0"
            f"&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition="
            f"&PlusMinus=N&Rank=N&Season={season}&SeasonSegment=&SeasonType={stype}&ShotClockRange=&StarterBench=&TeamID=0"
            f"&VsConference=&VsDivision=&Weight="
        )
        df_poss = pull_data(url_poss)

        day_patch = df_main[df_main['date'] == date_num][['PLAYER_ID', 'TEAM_ID', 'date']].copy()
        if not df_poss.empty and 'POSS' in df_poss.columns:
            poss_map = dict(zip(df_poss['TEAM_ID'], df_poss['POSS']))
            day_patch['team_poss'] = day_patch['TEAM_ID'].map(poss_map)
        else:
            day_patch['team_poss'] = None

        day_patch = day_patch.merge(df_pullup, on='PLAYER_ID', how='left')
        day_patch = day_patch.merge(df_speed, on='PLAYER_ID', how='left')
        day_patch.drop(columns=['TEAM_ID'], inplace=True, errors='ignore')

        # Checkpoint single date
        date_cache_file = os.path.join(CACHE_DIR, f"{date_num}.csv")
        day_patch.to_csv(date_cache_file, index=False)
        print(f"  [✓] Isolated save for {date_num} ({len(day_patch)} rows)")

    # 4. Final Assemble & Overwrite
    print(f"\nAll dates processed! Combining cached files from {CACHE_DIR}...")
    all_cache_files = glob.glob(os.path.join(CACHE_DIR, "*.csv"))
    cached_df_list = [pd.read_csv(f, low_memory=False) for f in all_cache_files]
    full_cache_df = pd.concat(cached_df_list, ignore_index=True, sort=False)
    full_cache_df.drop_duplicates(subset=['PLAYER_ID', 'date'], inplace=True)

    print(f"Merging combined cache ({full_cache_df.shape}) into {file_path}...")
    final_df = df_main.merge(full_cache_df, on=['PLAYER_ID', 'date'], how='left')
    final_df.to_csv(file_path, index=False)
    print(f"[✓] Successfully repaired {file_path}! Final shape: {final_df.shape}")

if __name__ == '__main__':
    run_repair_2024ps()
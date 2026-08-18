import os
import glob
import time
import pandas as pd
import requests
from datetime import datetime
from requests.exceptions import RequestException

TARGET_FILE = 'year_files/2025_games.csv'
CACHE_DIR = 'year_files/cache_2025'

CATASTROPHIC_DATES = {20250111, 20250308}
DATES_MISSING_HUSTLE_TRACKING_END = 20250320

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

def run_repair_2025(file_path=TARGET_FILE, year=2025, ps=False):
    if not os.path.exists(file_path):
        print(f"[!] File not found: {file_path}")
        return

    os.makedirs(CACHE_DIR, exist_ok=True)
    print(f"--- Launching Isolated Checkpoint Repair for {file_path} ---")
    df_main = pd.read_csv(file_path, low_memory=False)
    df_main['date'] = df_main['date'].astype(int)

    # 1. Clean out stale/broken column names
    drop_candidates = [c for c in df_main.columns if 
                       c.startswith('overall_def_') or 
                       c.startswith('pullup_') or 
                       c.startswith('PULL_UP_') or 
                       c.startswith('lt6ft_') or
                       c.startswith('D_FGM') or c.startswith('D_FGA') or c.startswith('D_FG_PCT') or
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

    # 3. Targeted Scrape Loop
    for idx, date_num in enumerate(remaining_dates, 1):
        date_str = format_date_to_url(date_num)
        is_catastrophic = date_num in CATASTROPHIC_DATES
        needs_hustle_speed = date_num <= DATES_MISSING_HUSTLE_TRACKING_END or is_catastrophic

        print(f"[{idx}/{len(remaining_dates)}] Fetching Date {date_num} (Catastrophic={is_catastrophic}, Hustle/Speed={needs_hustle_speed})...")

        # --- A. UNIVERSAL ENDPOINTS ---
        # 1. Pullups
        url_pullup = f"https://stats.nba.com/stats/leaguedashptstats?College=&Conference=&Country=&DateFrom={date_str}&DateTo={date_str}&Division=&DraftPick=&DraftYear=&GameScope=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PerMode=Totals&PlayerExperience=&PlayerOrTeam=Player&PlayerPosition=&PtMeasureType=PullUpShot&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight="
        df_pullup = pull_data(url_pullup)
        if not df_pullup.empty and 'PLAYER_ID' in df_pullup.columns:
            ignore = {'PLAYER_ID', 'PLAYER_NAME', 'TEAM_ID', 'TEAM_ABBREVIATION', 'GP', 'W', 'L', 'MIN'}
            cols = [c for c in df_pullup.columns if c not in ignore]
            df_pullup.rename(columns={c: f'pullup_{c}' for c in cols}, inplace=True)
            df_pullup = df_pullup[['PLAYER_ID'] + [f'pullup_{c}' for c in cols]]
        else:
            df_pullup = pd.DataFrame(columns=['PLAYER_ID'])

        # 2. Overall Defense
        url_def_overall = f"https://stats.nba.com/stats/leaguedashptdefend?College=&Conference=&Country=&DateFrom={date_str}&DateTo={date_str}&DefenseCategory=Overall&Division=&DraftPick=&DraftYear=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PerMode=PerGame&Period=0&PlayerExperience=&PlayerPosition=&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight="
        df_def = pull_data(url_def_overall)
        if not df_def.empty and 'CLOSE_DEF_PERSON_ID' in df_def.columns:
            df_def.rename(columns={'CLOSE_DEF_PERSON_ID': 'PLAYER_ID'}, inplace=True)
            ignore = {'PLAYER_ID', 'PLAYER_NAME', 'PLAYER_LAST_TEAM_ID', 'PLAYER_LAST_TEAM_ABBREVIATION', 'PLAYER_POSITION', 'AGE', 'GP', 'G', 'FREQ'}
            cols = [c for c in df_def.columns if c not in ignore]
            df_def.rename(columns={c: f'overall_def_{c}' for c in cols}, inplace=True)
            df_def = df_def[['PLAYER_ID'] + [f'overall_def_{c}' for c in cols]]
        else:
            df_def = pd.DataFrame(columns=['PLAYER_ID'])

        # 3. Less Than 6Ft Defense
        url_def_6ft = f"https://stats.nba.com/stats/leaguedashptdefend?College=&Conference=&Country=&DateFrom={date_str}&DateTo={date_str}&DefenseCategory=Less%20Than%206Ft&Division=&DraftPick=&DraftYear=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight="
        df_6ft = pull_data(url_def_6ft)
        if not df_6ft.empty and 'CLOSE_DEF_PERSON_ID' in df_6ft.columns:
            df_6ft.rename(columns={'CLOSE_DEF_PERSON_ID': 'PLAYER_ID'}, inplace=True)
            ignore = {'PLAYER_ID', 'PLAYER_NAME', 'PLAYER_LAST_TEAM_ID', 'PLAYER_LAST_TEAM_ABBREVIATION', 'PLAYER_POSITION', 'AGE', 'GP', 'G', 'FREQ'}
            cols = [c for c in df_6ft.columns if c not in ignore]
            df_6ft.rename(columns={c: f'lt6ft_totals_{c}' for c in cols}, inplace=True)
            df_6ft = df_6ft[['PLAYER_ID'] + [f'lt6ft_totals_{c}' for c in cols]]
        else:
            df_6ft = pd.DataFrame(columns=['PLAYER_ID'])

        # 4. Team Possessions
        url_poss = f"https://stats.nba.com/stats/leaguedashteamstats?College=&Conference=&Country=&DateFrom={date_str}&DateTo={date_str}&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&MeasureType=Advanced&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={season}&SeasonSegment=&SeasonType={stype}&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight="
        df_poss = pull_data(url_poss)

        day_patch = df_main[df_main['date'] == date_num][['PLAYER_ID', 'TEAM_ID', 'date']].copy()
        if not df_poss.empty and 'POSS' in df_poss.columns:
            poss_map = dict(zip(df_poss['TEAM_ID'], df_poss['POSS']))
            day_patch['team_poss'] = day_patch['TEAM_ID'].map(poss_map)
        else:
            day_patch['team_poss'] = None

        day_patch = day_patch.merge(df_pullup, on='PLAYER_ID', how='left')
        day_patch = day_patch.merge(df_def, on='PLAYER_ID', how='left')
        day_patch = day_patch.merge(df_6ft, on='PLAYER_ID', how='left')

        # --- B. HUSTLE / SPEED / POST TOUCHES ---
        if needs_hustle_speed:
            url_hustle = f"https://stats.nba.com/stats/leaguehustlestatsplayer?College=&Conference=&Country=&DateFrom={date_str}&DateTo={date_str}&Division=&DraftPick=&DraftYear=&GameScope=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=PerGame&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={season}&SeasonSegment=&SeasonType={stype}&TeamID=0&VsConference=&VsDivision=&Weight="
            df_hustle = pull_data(url_hustle)
            if not df_hustle.empty and 'PLAYER_ID' in df_hustle.columns:
                ignore = {'PLAYER_ID', 'PLAYER_NAME', 'TEAM_ID', 'TEAM_ABBREVIATION', 'AGE', 'G'}
                cols = [c for c in df_hustle.columns if c not in ignore]
                df_hustle.rename(columns={c: f'hustle_{c}' for c in cols}, inplace=True)
                day_patch = day_patch.merge(df_hustle[['PLAYER_ID'] + [f'hustle_{c}' for c in cols]], on='PLAYER_ID', how='left')

            url_post = f"https://stats.nba.com/stats/leaguedashptstats?College=&Conference=&Country=&DateFrom={date_str}&DateTo={date_str}&Division=&DraftPick=&DraftYear=&GameScope=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PerMode=PerGame&PlayerExperience=&PlayerOrTeam=Player&PlayerPosition=&PtMeasureType=PostTouch&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight="
            df_post = pull_data(url_post)
            if not df_post.empty and 'PLAYER_ID' in df_post.columns:
                ignore = {'PLAYER_ID', 'PLAYER_NAME', 'TEAM_ID', 'TEAM_ABBREVIATION', 'GP', 'W', 'L', 'MIN'}
                cols = [c for c in df_post.columns if c not in ignore]
                df_post.rename(columns={c: f'post_touch_{c}' for c in cols}, inplace=True)
                day_patch = day_patch.merge(df_post[['PLAYER_ID'] + [f'post_touch_{c}' for c in cols]], on='PLAYER_ID', how='left')

            url_speed = f"https://stats.nba.com/stats/leaguedashptstats?College=&Conference=&Country=&DateFrom={date_str}&DateTo={date_str}&Division=&DraftPick=&DraftYear=&GameScope=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PerMode=PerGame&PlayerExperience=&PlayerOrTeam=Player&PlayerPosition=&PtMeasureType=SpeedDistance&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight="
            df_speed = pull_data(url_speed)
            if not df_speed.empty and 'PLAYER_ID' in df_speed.columns:
                ignore = {'PLAYER_ID', 'PLAYER_NAME', 'TEAM_ID', 'TEAM_ABBREVIATION', 'GP', 'W', 'L', 'MIN'}
                cols = [c for c in df_speed.columns if c not in ignore]
                day_patch = day_patch.merge(df_speed[['PLAYER_ID'] + cols], on='PLAYER_ID', how='left')

        # --- C. CATASTROPHIC DATES FULL RE-PULL ---
        if is_catastrophic:
            for ptype in ['Passing', 'Drives', 'Possessions', 'Rebounding']:
                url_pt = f"https://stats.nba.com/stats/leaguedashptstats?College=&Conference=&Country=&DateFrom={date_str}&DateTo={date_str}&Division=&DraftPick=&DraftYear=&GameScope=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PerMode=Totals&PlayerExperience=&PlayerOrTeam=Player&PlayerPosition=&PtMeasureType={ptype}&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight="
                df_pt = pull_data(url_pt)
                if not df_pt.empty and 'PLAYER_ID' in df_pt.columns:
                    ignore = {'PLAYER_NAME', 'TEAM_ID', 'TEAM_ABBREVIATION', 'GP', 'W', 'L', 'MIN'}
                    cols = [c for c in df_pt.columns if c not in ignore and c != 'PLAYER_ID']
                    day_patch = day_patch.merge(df_pt[['PLAYER_ID'] + cols], on='PLAYER_ID', how='left')

            shotcols = ['FGA_FREQUENCY', 'FGM', 'FGA', 'FG_PCT', 'EFG_PCT', 'FG2A_FREQUENCY', 'FG2M', 'FG2A', 'FG2_PCT', 'FG3A_FREQUENCY', 'FG3M', 'FG3A', 'FG3_PCT']
            url_open = f"https://stats.nba.com/stats/leaguedashplayerptshot?CloseDefDistRange=4-6%20Feet%20-%20Open&College=&Conference=&Country=&DateFrom={date_str}&DateTo={date_str}&Division=&DraftPick=&DraftYear=&DribbleRange=&GameScope=&GameSegment=&GeneralRange=&Height=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&Season={season}&SeasonSegment=&SeasonType={stype}&ShotClockRange=&ShotDistRange=&StarterBench=&TeamID=0&TouchTimeRange=&VsConference=&VsDivision=&Weight="
            df_open = pull_data(url_open)
            if not df_open.empty and 'PLAYER_ID' in df_open.columns:
                df_open.rename(columns={col: f'open_{col}' for col in shotcols if col in df_open.columns}, inplace=True)
                open_cols = [f'open_{col}' for col in shotcols if f'open_{col}' in df_open.columns]
                day_patch = day_patch.merge(df_open[['PLAYER_ID'] + open_cols], on='PLAYER_ID', how='left')

        day_patch.drop(columns=['TEAM_ID'], inplace=True, errors='ignore')

        # Save date to its own isolated file
        date_cache_file = os.path.join(CACHE_DIR, f"{date_num}.csv")
        day_patch.to_csv(date_cache_file, index=False)
        print(f"  [✓] Isolated save for {date_num} ({len(day_patch)} rows)")

    # 4. Assemble All Cached Files
    print(f"\nAll dates processed! Reading all cached date files from {CACHE_DIR}...")
    all_cache_files = glob.glob(os.path.join(CACHE_DIR, "*.csv"))
    cached_df_list = [pd.read_csv(f, low_memory=False) for f in all_cache_files]
    full_cache_df = pd.concat(cached_df_list, ignore_index=True, sort=False)
    full_cache_df.drop_duplicates(subset=['PLAYER_ID', 'date'], inplace=True)

    print(f"Merging combined cache ({full_cache_df.shape}) into {file_path}...")
    df_main.set_index(['PLAYER_ID', 'date'], inplace=True)
    full_cache_df.set_index(['PLAYER_ID', 'date'], inplace=True)

    # In-place update existing columns (like catastrophic dates)
    common_cols = [c for c in full_cache_df.columns if c in df_main.columns]
    for col in common_cols:
        df_main[col].update(full_cache_df[col])

    # Append brand new patched columns
    new_cols = [c for c in full_cache_df.columns if c not in df_main.columns]
    for col in new_cols:
        df_main[col] = full_cache_df[col]

    df_main.reset_index(inplace=True)
    df_main.to_csv(file_path, index=False)
    print(f"[✓] Successfully repaired {file_path}! Final shape: {df_main.shape}")

if __name__ == '__main__':
    run_repair_2025()
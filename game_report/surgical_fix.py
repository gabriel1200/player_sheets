import os
import re
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
                df = pd.DataFrame.from_records(data, columns=columns)
            else:
                data = json_data["resultSets"]["rowSet"]
                columns = json_data["resultSets"]["headers"][1]["columnNames"]
                df = pd.DataFrame.from_records(data, columns=columns)

            time.sleep(0.8)
            return df

        except (RequestException, ValueError, KeyError) as e:
            print(f"  [Attempt {attempt + 1}] Error: {e}")
            if attempt < max_retries - 1:
                time.sleep(delay_seconds)
            else:
                print(f"  [!] Max retries reached for: {url}")
                return pd.DataFrame()

    return pd.DataFrame()

def repair_specific_endpoints(file_path='year_files/2026_games.csv', year=None, ps=None):
    if not os.path.exists(file_path):
        print(f"[!] File not found: {file_path}")
        return

    # Derive dynamic cache file name based on input target file
    dir_name, base_name = os.path.split(file_path)
    file_stem, _ = os.path.splitext(base_name)
    cache_file = os.path.join(dir_name, f"patch_cache_{file_stem}.csv")

    # Automatically parse season type and year if not explicitly passed
    if ps is None:
        ps = 'ps' in file_stem.lower()
    
    if year is None:
        year_match = re.search(r'\d{4}', file_stem)
        if year_match:
            year = int(year_match.group(0))
        else:
            raise ValueError(f"Could not parse 4-digit year from file path: {file_path}")

    stype = 'Playoffs' if ps else 'Regular%20Season'
    season = f"{year - 1}-{str(year)[-2:]}"

    print(f"Target file: {file_path}")
    print(f"Cache file:  {cache_file}")
    print(f"Detected:    Year={year}, Season={season}, SeasonType={stype}")

    print(f"\nLoading base file: {file_path}...")
    df_main = pd.read_csv(file_path)
    df_main['date'] = df_main['date'].astype(int)

    # 1. Purge stale/broken columns from the main frame
    drop_candidates = [c for c in df_main.columns if 
                       c.startswith('overall_def_') or 
                       c.startswith('pullup_') or 
                       c.startswith('PULL_UP_') or 
                       c.startswith('lt6ft_') or
                       c.startswith('D_FGM') or c.startswith('D_FGA') or c.startswith('D_FG_PCT') or
                       c == 'team_poss']
    
    df_main.drop(columns=drop_candidates, inplace=True, errors='ignore')
    all_dates = sorted(df_main['date'].unique().tolist())

    # 2. Check for existing progress in this file's specific cache
    completed_dates = set()
    if os.path.exists(cache_file):
        try:
            cached_df = pd.read_csv(cache_file)
            completed_dates = set(cached_df['date'].astype(int).unique())
            print(f"[+] Found existing cache with {len(completed_dates)} completed dates.")
        except Exception as e:
            print(f"[!] Warning reading cache: {e}. Starting fresh.")

    remaining_dates = [d for d in all_dates if d not in completed_dates]
    print(f"Total dates: {len(all_dates)} | Completed: {len(completed_dates)} | Remaining: {len(remaining_dates)}")

    # 3. Pull missing dates and append to dynamic cache incrementally
    for idx, date_num in enumerate(remaining_dates, 1):
        date_str = format_date_to_url(date_num)
        print(f"[{idx}/{len(remaining_dates)}] Fetching date {date_num}...")

        # --- 1. Pullup Stats ---
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
            pullup_cols = [c for c in df_pullup.columns if c not in ignore]
            df_pullup.rename(columns={c: f'pullup_{c}' for c in pullup_cols}, inplace=True)
            df_pullup = df_pullup[['PLAYER_ID'] + [f'pullup_{c}' for c in pullup_cols]]
        else:
            df_pullup = pd.DataFrame(columns=['PLAYER_ID'])

        # --- 2. Overall Defense ---
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
            def_cols = [c for c in df_def.columns if c not in ignore]
            df_def.rename(columns={c: f'overall_def_{c}' for c in def_cols}, inplace=True)
            df_def = df_def[['PLAYER_ID'] + [f'overall_def_{c}' for c in def_cols]]
        else:
            df_def = pd.DataFrame(columns=['PLAYER_ID'])

        # --- 3. Less Than 6Ft Defense ---
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
            cols_6ft = [c for c in df_6ft.columns if c not in ignore]
            df_6ft.rename(columns={c: f'lt6ft_totals_{c}' for c in cols_6ft}, inplace=True)
            df_6ft = df_6ft[['PLAYER_ID'] + [f'lt6ft_totals_{c}' for c in cols_6ft]]
        else:
            df_6ft = pd.DataFrame(columns=['PLAYER_ID'])

        # --- 4. Team Possessions ---
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
        day_patch = day_patch.merge(df_def, on='PLAYER_ID', how='left')
        day_patch = day_patch.merge(df_6ft, on='PLAYER_ID', how='left')
        day_patch.drop(columns=['TEAM_ID'], inplace=True)

        # Incremental Save to file-specific cache
        write_header = not os.path.exists(cache_file)
        day_patch.to_csv(cache_file, mode='a', header=write_header, index=False)
        print(f"  [✓] Checkpointed date {date_num} ({len(day_patch)} rows)")

    # 4. Final Assemble & Merge
    print(f"\nMerging complete cache ({cache_file}) into master file...")
    full_cache_df = pd.read_csv(cache_file)
    full_cache_df.drop_duplicates(subset=['PLAYER_ID', 'date'], inplace=True)

    final_df = df_main.merge(full_cache_df, on=['PLAYER_ID', 'date'], how='left')
    final_df.to_csv(file_path, index=False)
    print(f"[✓] Successfully repaired and overwritten: {file_path}")
    print(f"Final shape: {final_df.shape}")

if __name__ == '__main__':
    # You can pass any year file path here (e.g. 'year_files/2026_games.csv')
    repair_specific_endpoints('year_files/2026_games.csv')
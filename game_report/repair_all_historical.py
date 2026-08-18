import os
import sys
import glob
import time
import pandas as pd
import requests
from datetime import datetime
from requests.exceptions import RequestException

# Known dates where Second Spectrum cameras were down upstream on stats.nba.com
KNOWN_TRACKING_OUTAGES = {
    2024: [20240309],
    2022: [20220106]
}

def format_date_to_url(date):
    date_obj = datetime.strptime(str(date), '%Y%m%d')
    return date_obj.strftime('%m%%2F%d%%2F%Y')

def pull_data(url, max_retries=3, delay_seconds=3):
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

            time.sleep(0.6)
            return df

        except (RequestException, ValueError, KeyError) as e:
            if attempt < max_retries - 1:
                time.sleep(delay_seconds)
            else:
                return pd.DataFrame()

    return pd.DataFrame()

def repair_season(year: int, ps: bool = False):
    trail = 'ps' if ps else ''
    file_path = f"year_files/{year}{trail}_games.csv"
    cache_dir = f"year_files/cache_{year}{trail}"

    if not os.path.exists(file_path):
        print(f"[-] Skipping {file_path} (file not found)")
        return

    os.makedirs(cache_dir, exist_ok=True)
    stype = 'Playoffs' if ps else 'Regular%20Season'
    season = f"{year - 1}-{str(year)[-2:]}"

    print(f"\n=======================================================")
    print(f" Starting Batch Repair: Year {year} | {'Playoffs' if ps else 'Regular Season'}")
    print(f" File: {file_path}")
    print(f"=======================================================")

    df_main = pd.read_csv(file_path, low_memory=False)
    df_main['date'] = df_main['date'].astype(int)

    # 1. Purge stale / broken column names
    drop_candidates = [c for c in df_main.columns if 
                       c.startswith('overall_def_') or 
                       c.startswith('pullup_') or 
                       c.startswith('lt6ft_') or
                       c.startswith('hustle_') or
                       c.startswith('speed_distance_') or
                       c in ['DIST_FEET', 'DIST_MILES', 'DIST_MILES_OFF', 'DIST_MILES_DEF', 'AVG_SPEED', 'AVG_SPEED_OFF', 'AVG_SPEED_DEF'] or
                       c == 'team_poss']
    df_main.drop(columns=drop_candidates, inplace=True, errors='ignore')

    all_dates = sorted(df_main['date'].unique().tolist())
    cached_files = glob.glob(os.path.join(cache_dir, "*.csv"))
    completed_dates = {int(os.path.splitext(os.path.basename(f))[0]) for f in cached_files if os.path.splitext(os.path.basename(f))[0].isdigit()}

    remaining_dates = [d for d in all_dates if d not in completed_dates]
    print(f"Total dates: {len(all_dates)} | Cached: {len(completed_dates)} | Remaining: {len(remaining_dates)}")

    # 2. Targeted Scrape Loop
    for idx, date_num in enumerate(remaining_dates, 1):
        date_str = format_date_to_url(date_num)
        print(f"  [{idx}/{len(remaining_dates)}] Fetching Date {date_num}...")

        # 1. Overall Defense (df18)
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

        # 2. Team Possessions (df17)
        url_poss = f"https://stats.nba.com/stats/leaguedashteamstats?College=&Conference=&Country=&DateFrom={date_str}&DateTo={date_str}&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&MeasureType=Advanced&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={season}&SeasonSegment=&SeasonType={stype}&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight="
        df_poss = pull_data(url_poss)

        day_patch = df_main[df_main['date'] == date_num][['PLAYER_ID', 'TEAM_ID', 'date']].copy()
        if not df_poss.empty and 'POSS' in df_poss.columns:
            poss_map = dict(zip(df_poss['TEAM_ID'], df_poss['POSS']))
            day_patch['team_poss'] = day_patch['TEAM_ID'].map(poss_map)
        else:
            day_patch['team_poss'] = None

        day_patch = day_patch.merge(df_def, on='PLAYER_ID', how='left')

        # 3. Hustle Stats (Available starting from 2016)
        if year >= 2016:
            url_hustle = f"https://stats.nba.com/stats/leaguehustlestatsplayer?College=&Conference=&Country=&DateFrom={date_str}&DateTo={date_str}&Division=&DraftPick=&DraftYear=&GameScope=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=PerGame&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={season}&SeasonSegment=&SeasonType={stype}&TeamID=0&VsConference=&VsDivision=&Weight="
            df_hustle = pull_data(url_hustle)
            if not df_hustle.empty and 'PLAYER_ID' in df_hustle.columns:
                ignore = {'PLAYER_ID', 'PLAYER_NAME', 'TEAM_ID', 'TEAM_ABBREVIATION', 'AGE', 'G'}
                cols = [c for c in df_hustle.columns if c not in ignore]
                df_hustle.rename(columns={c: f'hustle_{c}' for c in cols}, inplace=True)
                day_patch = day_patch.merge(df_hustle[['PLAYER_ID'] + [f'hustle_{c}' for c in cols]], on='PLAYER_ID', how='left')

        # 4. Speed & Distance (df26)
        url_speed = f"https://stats.nba.com/stats/leaguedashptstats?College=&Conference=&Country=&DateFrom={date_str}&DateTo={date_str}&Division=&DraftPick=&DraftYear=&GameScope=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PerMode=PerGame&PlayerExperience=&PlayerOrTeam=Player&PlayerPosition=&PtMeasureType=SpeedDistance&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight="
        df_speed = pull_data(url_speed)
        if not df_speed.empty and 'PLAYER_ID' in df_speed.columns:
            ignore = {'PLAYER_ID', 'PLAYER_NAME', 'TEAM_ID', 'TEAM_ABBREVIATION', 'GP', 'W', 'L', 'MIN', 'AGE'}
            cols = [c for c in df_speed.columns if c not in ignore]
            day_patch = day_patch.merge(df_speed[['PLAYER_ID'] + cols], on='PLAYER_ID', how='left')

        day_patch.drop(columns=['TEAM_ID'], inplace=True, errors='ignore')

        # Save to date cache
        date_cache_file = os.path.join(cache_dir, f"{date_num}.csv")
        day_patch.to_csv(date_cache_file, index=False)

    # 3. Assemble and Overwrite Master File
    print(f"Combining cached files from {cache_dir}...")
    all_cache_files = glob.glob(os.path.join(cache_dir, "*.csv"))
    cached_df_list = [pd.read_csv(f, low_memory=False) for f in all_cache_files]
    full_cache_df = pd.concat(cached_df_list, ignore_index=True, sort=False)
    full_cache_df.drop_duplicates(subset=['PLAYER_ID', 'date'], inplace=True)

    print(f"Merging cache into {file_path}...")
    df_main.set_index(['PLAYER_ID', 'date'], inplace=True)
    full_cache_df.set_index(['PLAYER_ID', 'date'], inplace=True)

    common_cols = [c for c in full_cache_df.columns if c in df_main.columns]
    for col in common_cols:
        df_main[col].update(full_cache_df[col])

    new_cols = [c for c in full_cache_df.columns if c not in df_main.columns]
    for col in new_cols:
        df_main[col] = full_cache_df[col]

    df_main.reset_index(inplace=True)
    df_main.to_csv(file_path, index=False)
    print(f"[✓] Completed {file_path}! Shape: {df_main.shape}")

def main():
    args = sys.argv[1:]

    # Default batch: Run 2022 down through 2020 (both Regular Season and Playoffs)
    if not args:
        target_years = [2022, 2021, 2020]
    else:
        target_years = [int(y) for y in args if y.isdigit()]

    print(f"Queued Seasons for Repair: {target_years}")

    for year in target_years:
        # 1. Regular Season
        repair_season(year, ps=False)
        
        # 2. Playoffs
        repair_season(year, ps=True)

    print("\n[✓] All queued historical seasons repaired successfully!")

if __name__ == '__main__':
    main()
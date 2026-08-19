#!/usr/bin/env python3
import os
import sys
import time
import glob
import re
import pandas as pd
import requests
from datetime import datetime
from requests.exceptions import RequestException
from nba_api.stats.static import teams

HEADERS = {
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

def format_date_to_url(date):
    date_obj = datetime.strptime(str(date), '%Y%m%d')
    return date_obj.strftime('%m%%2F%d%%2F%Y')

def pull_data(url, max_retries=3, delay_seconds=2.0):
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=HEADERS, timeout=15)
            response.raise_for_status()
            json_data = response.json()

            if len(json_data.get("resultSets", [])) == 1:
                data = json_data["resultSets"][0]["rowSet"]
                columns = json_data["resultSets"][0]["headers"]
                df = pd.DataFrame.from_records(data, columns=columns)
            elif isinstance(json_data.get("resultSets"), dict):
                data = json_data["resultSets"]["rowSet"]
                columns = json_data["resultSets"]["headers"][1]["columnNames"]
                df = pd.DataFrame.from_records(data, columns=columns)
            else:
                data = json_data["resultSets"][0]["rowSet"]
                columns = json_data["resultSets"][0]["headers"]
                df = pd.DataFrame.from_records(data, columns=columns)

            time.sleep(0.5)
            return df
        except (RequestException, ValueError, KeyError, IndexError):
            if attempt < max_retries - 1:
                time.sleep(delay_seconds * (attempt + 1))
            else:
                return pd.DataFrame()
    return pd.DataFrame()

def get_scheduled_dates(year, ps=False):
    trail = 'ps' if ps else ''
    dates = set()
    for team in teams.get_teams():
        team_id = team['id']
        path = f"https://raw.githubusercontent.com/gabriel1200/shot_data/refs/heads/master/team/{year}{trail}/{team_id}.csv"
        try:
            team_df = pd.read_csv(path, usecols=['GAME_DATE'])
            dates.update(team_df['GAME_DATE'].dropna().unique().tolist())
        except Exception:
            continue
    return sorted([int(d) for d in dates])

def rescrape_date(date_num, year, season, stype):
    date = format_date_to_url(date_num)
    unit = 'Player'
    shotcolumns = [
        'FGA_FREQUENCY', 'FGM', 'FGA', 'FG_PCT', 'EFG_PCT', 
        'FG2A_FREQUENCY', 'FG2M', 'FG2A', 'FG2_PCT', 
        'FG3A_FREQUENCY', 'FG3M', 'FG3A', 'FG3_PCT'
    ]

    # Full parameter strings required by stats.nba.com
    base_p = f"College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=&PaceAdjust=N&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={season}&SeasonSegment=&SeasonType={stype}&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight="
    pt_p = f"College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&GameScope=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PerMode=Totals&PlayerExperience=&PlayerOrTeam={unit}&PlayerPosition=&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight="
    shot_p = f"College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&DribbleRange=&GameScope=&GameSegment=&GeneralRange=&Height=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&Season={season}&SeasonSegment=&SeasonType={stype}&ShotClockRange=&ShotDistRange=&StarterBench=&TeamID=0&TouchTimeRange=&VsConference=&VsDivision=&Weight="
    loc_p = f"College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={season}&SeasonSegment=&SeasonType={stype}&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight="
    def_p = f"College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PerMode=PerGame&Period=0&PlayerExperience=&PlayerPosition=&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight="

    # 1. Base Stats
    url1 = f"https://stats.nba.com/stats/leaguedashplayerstats?{base_p}&MeasureType=Base"
    df = pull_data(url1)
    if df.empty or 'PLAYER_ID' not in df.columns:
        return pd.DataFrame()

    # 2. Advanced Stats
    url2 = f"https://stats.nba.com/stats/leaguedashplayerstats?{base_p}&MeasureType=Advanced"
    df2 = pull_data(url2)

    # 3. Passing Tracking
    url3 = f"https://stats.nba.com/stats/leaguedashptstats?{pt_p}&PtMeasureType=Passing"
    df3 = pull_data(url3)

    # 4. Drives Tracking
    url4 = f"https://stats.nba.com/stats/leaguedashptstats?{pt_p}&PtMeasureType=Drives"
    df4 = pull_data(url4)

    # 5. Possessions Tracking
    url5 = f"https://stats.nba.com/stats/leaguedashptstats?{pt_p}&PtMeasureType=Possessions"
    df5 = pull_data(url5)

    # 6. Rebounding Tracking
    url6 = f"https://stats.nba.com/stats/leaguedashptstats?{pt_p}&PtMeasureType=Rebounding"
    df6 = pull_data(url6)

    # 7-10. Contested Shot Splits
    url7 = f"https://stats.nba.com/stats/leaguedashplayerptshot?CloseDefDistRange=0-2%20Feet%20-%20Very%20Tight&{shot_p}"
    df7 = pull_data(url7)
    if not df7.empty:
        df7.rename(columns={c: f"very_tight_{c}" for c in shotcolumns if c in df7.columns}, inplace=True)

    url8 = f"https://stats.nba.com/stats/leaguedashplayerptshot?CloseDefDistRange=2-4%20Feet%20-%20Tight&{shot_p}"
    df8 = pull_data(url8)
    if not df8.empty:
        df8.rename(columns={c: f"tight_{c}" for c in shotcolumns if c in df8.columns}, inplace=True)

    url9 = f"https://stats.nba.com/stats/leaguedashplayerptshot?CloseDefDistRange=4-6%20Feet%20-%20Open&{shot_p}"
    df9 = pull_data(url9)
    if not df9.empty:
        df9.rename(columns={c: f"open_{c}" for c in shotcolumns if c in df9.columns}, inplace=True)

    url10 = f"https://stats.nba.com/stats/leaguedashplayerptshot?CloseDefDistRange=6%2B%20Feet%20-%20Wide%20Open&{shot_p}"
    df10 = pull_data(url10)
    if not df10.empty:
        df10.rename(columns={c: f"wide_open_{c}" for c in shotcolumns if c in df10.columns}, inplace=True)

    # 11. Pullups (Canonical naming)
    url11 = f"https://stats.nba.com/stats/leaguedashptstats?{pt_p}&PtMeasureType=PullUpShot"
    df11 = pull_data(url11)

    # 12. Efficiency Tracking
    url12 = f"https://stats.nba.com/stats/leaguedashptstats?{pt_p}&PtMeasureType=Efficiency"
    df12 = pull_data(url12)

    # 13. Zone Shot Locations
    url13 = f"https://stats.nba.com/stats/leaguedashplayershotlocations?DistanceRange=By%20Zone&{loc_p}"
    df13 = pull_data(url13)
    if not df13.empty:
        zone_columns = [
            'PLAYER_ID', 'PLAYER_NAME', 'TEAM_ID', 'TEAM_ABBREVIATION', 'AGE', 'NICKNAME',
            'RA_FGM', 'RA_FGA', 'RA_FG_PCT',
            'ITP_FGM', 'ITP_FGA', 'ITP_FG_PCT',
            'MID_FGM', 'MID_FGA', 'MID_FG_PCT',
            'LEFT_CORNER_3_FGM', 'LEFT_CORNER_3_FGA', 'LEFT_CORNER_3_FG_PCT',
            'RIGHT_CORNER_3_FGM', 'RIGHT_CORNER_3_FGA', 'RIGHT_CORNER_3_FG_PCT',
            'ABOVE_BREAK_3_FGM', 'ABOVE_BREAK_3_FGA', 'ABOVE_BREAK_3_FG_PCT',
            'BACKCOURT_FGM', 'BACKCOURT_FGA', 'BACKCOURT_FG_PCT',
            'CORNER_3_FGM', 'CORNER_3_FGA', 'CORNER_3_FG_PCT'
        ]
        if len(df13.columns) == len(zone_columns):
            df13.columns = zone_columns

    # 14. 5ft Range Shot Locations
    url15 = f"https://stats.nba.com/stats/leaguedashplayershotlocations?DistanceRange=5ft%20Range&{loc_p}"
    df15 = pull_data(url15)
    if not df15.empty:
        range_columns = [
            'PLAYER_ID', 'PLAYER_NAME', 'TEAM_ID', 'TEAM_ABBR', 'AGE', 'NICKNAME',
            'FGM_LT_5', 'FGA_LT_5', 'FGP_LT_5',
            'FGM_5_9', 'FGA_5_9', 'FGP_5_9',
            'FGM_10_14', 'FGA_10_14', 'FGP_10_14',
            'FGM_15_19', 'FGA_15_19', 'FGP_15_19',
            'FGM_20_24', 'FGA_20_24', 'FGP_20_24',
            'FGM_25_29', 'FGA_25_29', 'FGP_25_29',
            'FGM_30_34', 'FGA_30_34', 'FGP_30_34',
            'FGM_35_39', 'FGA_35_39', 'FGP_35_39',
            'FGM_40_PLUS', 'FGA_40_PLUS', 'FGP_40_PLUS'
        ]
        if len(df15.columns) == len(range_columns):
            df15.columns = range_columns

    # 15. Catch & Shoot
    url16 = f"https://stats.nba.com/stats/leaguedashptstats?{pt_p}&PtMeasureType=CatchShoot"
    df16 = pull_data(url16)

    # 16. Team Possessions
    url17 = f"https://stats.nba.com/stats/leaguedashteamstats?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&MeasureType=Advanced&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={season}&SeasonSegment=&SeasonType={stype}&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight="
    df17 = pull_data(url17)
    if not df17.empty and 'POSS' in df17.columns and 'TEAM_ID' in df17.columns:
        poss_map = dict(zip(df17['TEAM_ID'], df17['POSS']))
        df['team_poss'] = df['TEAM_ID'].map(poss_map)
    else:
        df['team_poss'] = None

    # Defensive Tracking Helper
    def parse_def(cat, prefix):
        u = f"https://stats.nba.com/stats/leaguedashptdefend?DefenseCategory={cat}&{def_p}"
        d = pull_data(u)
        if not d.empty and 'CLOSE_DEF_PERSON_ID' in d.columns:
            d.rename(columns={'CLOSE_DEF_PERSON_ID': 'PLAYER_ID'}, inplace=True)
            ignore = {'PLAYER_ID', 'PLAYER_NAME', 'PLAYER_LAST_TEAM_ID', 'PLAYER_LAST_TEAM_ABBREVIATION', 'PLAYER_POSITION', 'AGE', 'GP', 'G', 'FREQ'}
            cols = [c for c in d.columns if c not in ignore]
            d.rename(columns={c: f"{prefix}_{c}" for c in cols}, inplace=True)
            return d[['PLAYER_ID'] + [f"{prefix}_{c}" for c in cols]]
        return pd.DataFrame(columns=['PLAYER_ID'])

    df18 = parse_def('Overall', 'overall_def')
    df19 = parse_def('3%20Pointers', 'three_pt_def')
    df20 = parse_def('2%20Pointers', 'two_pt_def')
    df21 = parse_def('Less%20Than%206Ft', 'less_6ft_def')
    df22 = parse_def('Less%20Than%2010Ft', 'less_10ft_def')
    df23 = parse_def('Greater%20Than%2015Ft', 'more_15ft_def')

    # Hustle Stats (>= 2017)
    df24 = pd.DataFrame(columns=['PLAYER_ID'])
    if year >= 2017:
        url24 = f"https://stats.nba.com/stats/leaguehustlestatsplayer?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&GameScope=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=PerGame&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={season}&SeasonSegment=&SeasonType={stype}&TeamID=0&VsConference=&VsDivision=&Weight="
        d24 = pull_data(url24)
        if not d24.empty and 'PLAYER_ID' in d24.columns:
            ignore = {'PLAYER_ID', 'PLAYER_NAME', 'TEAM_ID', 'TEAM_ABBREVIATION', 'AGE', 'G'}
            cols = [c for c in d24.columns if c not in ignore]
            d24.rename(columns={c: f"hustle_{c}" for c in cols}, inplace=True)
            df24 = d24[['PLAYER_ID'] + [f"hustle_{c}" for c in cols]]

    # Post Touch (>= 2018)
    df25 = pd.DataFrame(columns=['PLAYER_ID'])
    if year >= 2018:
        url25 = f"https://stats.nba.com/stats/leaguedashptstats?{pt_p}&PtMeasureType=PostTouch"
        d25 = pull_data(url25)
        if not d25.empty and 'PLAYER_ID' in d25.columns:
            ignore = {'PLAYER_ID', 'PLAYER_NAME', 'TEAM_ID', 'TEAM_ABBREVIATION', 'GP', 'W', 'L', 'MIN', 'AGE'}
            cols = [c for c in d25.columns if c not in ignore]
            df25 = d25[['PLAYER_ID'] + cols]

    # Speed & Distance
    url26 = f"https://stats.nba.com/stats/leaguedashptstats?{pt_p}&PtMeasureType=SpeedDistance"
    df26 = pull_data(url26)
    if not df26.empty and 'PLAYER_ID' in df26.columns:
        ignore = {'PLAYER_ID', 'PLAYER_NAME', 'TEAM_ID', 'TEAM_ABBREVIATION', 'GP', 'W', 'L', 'MIN', 'AGE'}
        cols = [c for c in df26.columns if c not in ignore]
        df26 = df26[['PLAYER_ID'] + cols]
    else:
        df26 = pd.DataFrame(columns=['PLAYER_ID'])

    frames = [
        df2, df3, df4, df5, df6, df7, df8, df9, df10,
        df11, df12, df13, df15, df16,
        df18, df19, df20, df21, df22, df23,
        df24, df25, df26
    ]

    for frame in frames:
        if frame.empty or 'PLAYER_ID' not in frame.columns:
            continue
        joined_columns = [c for c in frame.columns if c not in df.columns or c == 'PLAYER_ID']
        if len(joined_columns) > 1:
            df = df.merge(frame[joined_columns], on='PLAYER_ID', how='left').reset_index(drop=True)

    df['year'] = year
    df['date'] = int(date_num)
    df['playoffs'] = (stype == 'Playoffs')

    extra_columns = [
        '_PLAYER_NAME', '_PLAYER_LAST_TEAM_ID', '_GP', '_PLAYER_POSITION',
        '_PLAYER_LAST_TEAM_ABBREVIATION', '_PLAYER_ID', '_MIN',
        '_TEAM_ABBREVIATION', '_G', '_W', '_L', '_AGE', '_TEAM_ID'
    ]
    cols_to_drop = [c for c in df.columns if any(c.endswith(ex) for ex in extra_columns)]
    df.drop(columns=cols_to_drop, inplace=True, errors='ignore')
    df.drop_duplicates(subset=['PLAYER_ID', 'date'], inplace=True)

    return df

def process_season(year, ps=False):
    trail = 'ps' if ps else ''
    stype = 'Playoffs' if ps else 'Regular%20Season'
    season = f"{year - 1}-{str(year)[-2:]}"
    out_file = f"year_files/{year}{trail}_games.csv"
    cache_dir = f"year_files/cache_{year}{trail}"

    os.makedirs("year_files", exist_ok=True)
    os.makedirs(cache_dir, exist_ok=True)

    print(f"\n=======================================================")
    print(f" EMERGENCY RESCRAPE: Year {year} | {'Playoffs' if ps else 'Regular Season'}")
    print(f" Target File: {out_file}")
    print(f"=======================================================")

    all_dates = get_scheduled_dates(year, ps=ps)
    if not all_dates:
        print(f"[!] Warning: No dates resolved for {year}{trail}. Skipping.")
        return

    cached_files = glob.glob(os.path.join(cache_dir, "*.csv"))
    completed_dates = {
        int(os.path.splitext(os.path.basename(f))[0])
        for f in cached_files
        if os.path.splitext(os.path.basename(f))[0].isdigit() and os.path.getsize(f) > 0
    }

    remaining_dates = [d for d in all_dates if d not in completed_dates]
    print(f"Total Dates: {len(all_dates)} | Cached: {len(completed_dates)} | Remaining: {len(remaining_dates)}")

    for idx, date_num in enumerate(remaining_dates, 1):
        print(f"  [{idx}/{len(remaining_dates)}] Scraping Date {date_num}...", end="", flush=True)
        try:
            day_df = rescrape_date(date_num, year, season, stype)
            if not day_df.empty:
                cache_path = os.path.join(cache_dir, f"{date_num}.csv")
                day_df.to_csv(cache_path, index=False)
                print(f" [✓] Done ({len(day_df)} rows)")
            else:
                print(f" [!] Empty/Offline on stats.nba.com")
        except Exception as e:
            print(f" [x] Failed: {e}")

    # Combine cache
    print(f"Assembling master file from {cache_dir}...")
    valid_cache_files = [f for f in glob.glob(os.path.join(cache_dir, "*.csv")) if os.path.getsize(f) > 0]
    if not valid_cache_files:
        print(f"[!] No valid cached files to combine for {year}{trail}.")
        return

    df_list = [pd.read_csv(f, low_memory=False) for f in valid_cache_files]
    master_df = pd.concat(df_list, ignore_index=True, sort=False)
    master_df.drop_duplicates(subset=['PLAYER_ID', 'date'], inplace=True)
    master_df.sort_values(by=['date', 'PLAYER_ID'], inplace=True)
    master_df.to_csv(out_file, index=False)
    print(f"[✓] Saved {out_file} successfully! Final Shape: {master_df.shape}")

def main():
    args = sys.argv[1:]
    target_years = [int(a) for a in args if a.isdigit()] if args else [2017, 2018, 2019]
    print(f"Queued Seasons for Emergency Rescrape: {target_years}")

    for year in target_years:
        process_season(year, ps=False)
        process_season(year, ps=True)

    print("\n[✓] All requested seasons have been fully regenerated!")

if __name__ == '__main__':
    main()
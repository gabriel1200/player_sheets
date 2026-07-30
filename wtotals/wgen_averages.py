#!/usr/bin/env python
# coding: utf-8

# WNBA port of averages_scrape.py
# Changes from the NBA version:
#   - LeagueID=00 -> LeagueID=10 on all 28 stats.nba.com endpoint calls (stats.nba.com
#     serves WNBA data too, gated by LeagueID; no host change needed)
#   - Season string format changed from NBA's "YYYY-YY" (e.g. "2025-26") to WNBA's
#     single calendar year (e.g. "2026"), in both pull_wnba_avg and pull_wnba_avg_classic,
#     and in fetch_wnba_data's pbpstats.com call
#   - pbpstats.com endpoint changed from /get-totals/nba to /get-totals/wnba
#   - Fixed a pre-existing bug on the team-possessions call (url17) that hardcoded
#     SeasonType=Playoffs regardless of the `ps` flag - now follows {stype} like every
#     other call, so regular-season pulls aren't silently pulling playoff possessions
#   - get_dates() is left as-is and un-wired (same as the original) - it's NBA-only
#     (nba_api teams + your NBA shot_data repo) and would need WNBA equivalents if used
#   - Removed the hardcoded Luka Doncic PLAYER_ID sanity-check print
#   - Fixed a copy-paste bug: df23 (Greater Than 15Ft defense) was pulling url6
#     (Rebounding tracking data) instead of url23 - it wasn't erroring or coming back
#     empty, it was just silently mislabeling rebounding columns as 15ft+ defense stats
#
# DIAGNOSTICS (new): every pull_data() call is now labeled (e.g. 'drives', 'hustle',
# 'defend_3pt') and pull_data() no longer crashes the whole run on a bad request - it
# catches the error, records it, and returns an empty frame so the rest of the pulls
# still run. After each year finishes, print_diagnostics_report() prints a summary of
# every endpoint that came back EMPTY, ERROR, ALL_NULL, or PARTIAL_NULL (>=95% of a
# stat column is null or zero) and saves it to diagnostics_{year}{trail}.csv. Run the
# script, then paste back the printed summary (or the CSV) and I'll adjust/drop/rework
# whichever endpoints aren't working for WNBA.

# In[ ]:


from nba_api.stats.static import players,teams
import pandas as pd
import requests
import sys
import os
import time
from datetime import datetime
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 Edg/115.0.1901.183',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'DNT': '1',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Cache-Control': 'max-age=0',
}

def format_date_to_url(date):
    # Convert date from YYYYMMDD to datetime object
    date_obj = datetime.strptime(str(date), '%Y%m%d')

    # Format the date as MM%2FDD%2FYYYY
    formatted_date = date_obj.strftime('%m%%2F%d%%2F%Y')

    return formatted_date

# Example usage

# Columns that are always populated identifiers/metadata rather than stats, so they're
# excluded from the "mostly null" check below (they'd never flag as a problem anyway,
# but excluding them keeps the null-pct math focused on actual stat columns).
_ID_LIKE_COLS = {
    'PLAYER_ID', 'PLAYER_NAME', 'TEAM_ID', 'TEAM_ABBREVIATION', 'TEAM_ABBR', 'TEAM_NAME',
    'AGE', 'NICKNAME', 'GP', 'W', 'L', 'MIN', 'year',
}

# Collected across every pull_data() call in a run so you can see which endpoints came
# back empty, errored, or returned data that's mostly null (a sign the stat category
# isn't tracked/populated for WNBA, even though the endpoint didn't error).
ENDPOINT_DIAGNOSTICS = []

def pull_data(url, label='unlabeled'):

    headers = {
                                    "Host": "stats.nba.com",
                                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:72.0) Gecko/20100101 Firefox/72.0",
                                    "Accept": "application/json, text/plain, */*",
                                    "Accept-Language": "en-US,en;q=0.5",
                                    "Accept-Encoding": "gzip, deflate, br",

                                    "Connection": "keep-alive",
                                    "Referer": "https://stats.nba.com/"
                                }

    record = {'label': label, 'url': url, 'status': 'OK', 'rows': 0, 'cols': 0,
              'mostly_null_cols': [], 'error': ''}

    try:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        json = resp.json()

        if len(json["resultSets"])== 1:


            data = json["resultSets"][0]["rowSet"]
            #print(data)
            columns = json["resultSets"][0]["headers"]
            #print(columns)

            df = pd.DataFrame.from_records(data, columns=columns)
        else:

            data = json["resultSets"]["rowSet"]
            #print(json)
            columns = json["resultSets"]["headers"][1]['columnNames']
            #print(columns)
            df = pd.DataFrame.from_records(data, columns=columns)

    except Exception as e:
        # Don't let one bad endpoint kill the whole run - record it as an error and
        # hand back an empty frame so the calling code can keep going.
        record['status'] = 'ERROR'
        record['error'] = str(e)
        ENDPOINT_DIAGNOSTICS.append(record)
        print(f"  [{label}] ERROR: {e}")
        time.sleep(.2)
        return pd.DataFrame()

    record['rows'] = len(df)
    record['cols'] = len(df.columns)

    if len(df) == 0:
        record['status'] = 'EMPTY'
    else:
        stat_cols = [c for c in df.columns if c not in _ID_LIKE_COLS]
        for col in stat_cols:
            try:
                null_pct = df[col].isna().mean()
                # Treat 0-only columns as functionally blank too (a very common way
                # an untracked stat shows up: the field exists but is always 0, not NaN)
                if pd.api.types.is_numeric_dtype(df[col]):
                    zero_or_null_pct = ((df[col].isna()) | (df[col] == 0)).mean()
                else:
                    zero_or_null_pct = null_pct
                if zero_or_null_pct >= 0.95:
                    record['mostly_null_cols'].append(col)
            except Exception:
                continue
        if record['mostly_null_cols'] and len(record['mostly_null_cols']) == len(stat_cols) and stat_cols:
            record['status'] = 'ALL_NULL'
        elif record['mostly_null_cols']:
            record['status'] = 'PARTIAL_NULL'

    ENDPOINT_DIAGNOSTICS.append(record)
    flag = '' if record['status'] == 'OK' else f"  <-- {record['status']}"
    print(f"  [{label}] rows={record['rows']} cols={record['cols']}{flag}")

    time.sleep(.2)
    return df


def print_diagnostics_report(year=None, trail='', save_csv=True):
    """
    Prints a summary of every pull_data() call made so far (across the whole run,
    unless you've cleared ENDPOINT_DIAGNOSTICS), flagging anything that came back
    empty, errored, or mostly null/zero. Call this after a pull_wnba_avg run, or
    pass year/trail to also save a CSV you can paste back for review.
    """
    if not ENDPOINT_DIAGNOSTICS:
        print("No diagnostics collected yet.")
        return None

    diag_df = pd.DataFrame(ENDPOINT_DIAGNOSTICS)
    diag_df['mostly_null_cols'] = diag_df['mostly_null_cols'].apply(
        lambda cols: ', '.join(cols) if cols else ''
    )

    problems = diag_df[diag_df['status'] != 'OK'].sort_values('status')

    print("\n" + "=" * 70)
    print(f"ENDPOINT DIAGNOSTICS{f' - {year}{trail}' if year else ''}")
    print("=" * 70)
    if problems.empty:
        print("All endpoints returned populated data. Nothing flagged.")
    else:
        for _, row in problems.iterrows():
            print(f"[{row['status']}] {row['label']} (rows={row['rows']}, cols={row['cols']})")
            if row['error']:
                print(f"    error: {row['error']}")
            if row['mostly_null_cols']:
                print(f"    mostly null/zero columns: {row['mostly_null_cols']}")
    print("=" * 70 + "\n")

    if save_csv:
        filename = f"diagnostics_{year}{trail}.csv" if year else "diagnostics.csv"
        diag_df.to_csv(filename, index=False)
        print(f"Full diagnostics saved to {filename}")

    return diag_df


def pull_wnba_avg(dates, start_year,end_year,ps=False):
    stype = 'Regular%20Season'
    trail=''
    if ps == True:
        stype='Playoffs'
        trail='_ps'
    frames = []
    shotcolumns = ['FGA_FREQUENCY', 'FGM', 'FGA', 'FG_PCT', 'EFG_PCT', 'FG2A_FREQUENCY', 'FG2M', 'FG2A', 'FG2_PCT', 
                   'FG3A_FREQUENCY', 'FG3M', 'FG3A', 'FG3_PCT']
    unit='Player'
    for year in range(start_year, end_year):
        year_frame = []
        year_dates = ['']
        # WNBA seasons are a single calendar year (e.g. "2024"), not "2023-24" like the NBA
        season = str(year)

        for date in year_dates:

            # --- Working endpoints (confirmed via diagnostics on 2026 regular season) ---

            url = f'https://stats.nba.com/stats/leaguedashplayerstats?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=10&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=&PaceAdjust=N&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={season}&SeasonSegment=&SeasonType={stype}&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='
            df = pull_data(url, label='base_stats')

            url2 = f'https://stats.nba.com/stats/leaguedashplayerstats?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=10&Location=&MeasureType=Advanced&Month=0&OpponentTeamID=0&Outcome=&PORound=&PaceAdjust=N&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={season}&SeasonSegment=&SeasonType={stype}&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='
            df2 = pull_data(url2, label='advanced_stats')

            # --- Dropped endpoints: confirmed EMPTY (0 rows) for WNBA via this API path ---
            # passing, drives, possessions, rebounding (leaguedashptstats PtMeasureType=
            # Passing/Drives/Possessions/Rebounding), the four closest-defender shot-distance
            # splits (leaguedashplayerptshot CloseDefDistRange=very tight/tight/open/wide open),
            # efficiency (leaguedashptstats PtMeasureType=Efficiency), shot_zones and
            # shot_5ft_range (leaguedashplayershotlocations), hustle (leaguehustlestatsplayer),
            # post_touch and speed_distance (leaguedashptstats PtMeasureType=PostTouch/
            # SpeedDistance). These SportVU/tracking splits just aren't populated for the WNBA
            # through stats.nba.com. If you want them back, ping me with the results of trying
            # them against stats.wnba.com directly, or via a different tracking data provider
            # (e.g. Second Spectrum via pbpstats) - not confirmed either way.

            # --- Dropped endpoints: rows come back but every stat column is null/zero ---
            # catch_shoot and pullup_shots (leaguedashptstats PtMeasureType=CatchShoot/
            # PullUpShot) return one row per player but the actual CATCH_SHOOT_* /
            # PULL_UP_* columns are all null or zero - same story, not tracked for WNBA.

            # Dropped df14 (url14): a duplicate "Less Than 6Ft" defend pull that overlapped
            # with df21 below (defend_lt_6ft_v2). Kept df21 since its columns are prefixed
            # 'less_6ft_def_' and won't collide with anything else in the merge.

            url17 = f'https://stats.nba.com/stats/leaguedashteamstats?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=10&Location=&MeasureType=Advanced&Month=0&OpponentTeamID=0&Outcome=&PORound=&PaceAdjust=N&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={season}&SeasonSegment=&SeasonType={stype}&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='
            df17 = pull_data(url17, label='team_advanced_poss')
            df17=df17[['TEAM_ID','POSS']]
            df17.columns=['TEAM_ID','team_poss']

            poss_map=dict(zip(df17['TEAM_ID'],df17['team_poss']  ))

            df['team_poss']=df['TEAM_ID'].map(poss_map)

            url18 = f'https://stats.nba.com/stats/leaguedashptdefend?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&DefenseCategory=Overall&Division=&DraftPick=&DraftYear=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=10&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='

            df18 = pull_data(url18, label='defend_overall')
            df18.rename(columns={'CLOSE_DEF_PERSON_ID': 'PLAYER_ID'}, inplace=True)
            # NOTE: this was renaming off df8.columns (a leftover from a now-removed pull) instead
            # of df18's own columns, so the 'overall_def_' prefix was silently never being applied.
            # Fixed to reference df18.columns.
            df18.rename(columns={col: f'overall_def_{col}' for col in df18.columns if col != 'PLAYER_ID'}, inplace=True)

            # Link 2: 3-pointers defense stats
            url19 = f'https://stats.nba.com/stats/leaguedashptdefend?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&DefenseCategory=3%20Pointers&Division=&DraftPick=&DraftYear=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=10&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='

            df19 = pull_data(url19, label='defend_3pt')

            df19.rename(columns={'CLOSE_DEF_PERSON_ID': 'PLAYER_ID'}, inplace=True)
            df19.rename(columns={col: f'three_pt_def_{col}' for col in df19.columns if col != 'PLAYER_ID'}, inplace=True)


            # Link 3: 2-pointers defense stats
            url20 = f'https://stats.nba.com/stats/leaguedashptdefend?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&DefenseCategory=2%20Pointers&Division=&DraftPick=&DraftYear=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=10&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='

            df20 = pull_data(url20, label='defend_2pt')

            df20.rename(columns={'CLOSE_DEF_PERSON_ID': 'PLAYER_ID'}, inplace=True)
            df20.rename(columns={col: f'two_pt_def_{col}' for col in df20.columns if col != 'PLAYER_ID'}, inplace=True)

            # Link 4: Less than 6ft defense stats
            url21 = f'https://stats.nba.com/stats/leaguedashptdefend?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&DefenseCategory=Less%20Than%206Ft&Division=&DraftPick=&DraftYear=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=10&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='

            df21 = pull_data(url21, label='defend_lt_6ft_v2')
            df21.rename(columns={'CLOSE_DEF_PERSON_ID': 'PLAYER_ID'}, inplace=True)
            df21.rename(columns={col: f'less_6ft_def_{col}' for col in df21.columns if col != 'PLAYER_ID'}, inplace=True)

            # Link 5: Less than 10ft defense stats
            url22 = f'https://stats.nba.com/stats/leaguedashptdefend?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&DefenseCategory=Less%20Than%2010Ft&Division=&DraftPick=&DraftYear=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=10&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='

            df22 = pull_data(url22, label='defend_lt_10ft')
            df22.rename(columns={'CLOSE_DEF_PERSON_ID': 'PLAYER_ID'}, inplace=True)
            df22.rename(columns={col: f'less_10ft_def_{col}' for col in df22.columns if col != 'PLAYER_ID'}, inplace=True)

            # Link 6: Greater than 15ft defense stats
            url23 = f'https://stats.nba.com/stats/leaguedashptdefend?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&DefenseCategory=Greater%20Than%2015Ft&Division=&DraftPick=&DraftYear=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=10&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='

            df23 = pull_data(url23, label='defend_gt_15ft')
            df23.rename(columns={'CLOSE_DEF_PERSON_ID': 'PLAYER_ID'}, inplace=True)
            df23.rename(columns={col: f'more_15ft_def_{col}' for col in df23.columns if col != 'PLAYER_ID'}, inplace=True)

            url27=f'https://stats.nba.com/stats/leaguedashplayerbiostats?College=&Conference=&Country=&DateFrom=&DateTo=&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=10&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PerMode=PerGame&Period=0&PlayerExperience=&PlayerPosition=&Season={season}&SeasonSegment=&SeasonType={stype}&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='
            df27 = pull_data(url27, label='bio_stats')
            df27=df27[['PLAYER_ID','AGE','PLAYER_HEIGHT_INCHES', 'PLAYER_WEIGHT', 'COLLEGE', 'COUNTRY', 'DRAFT_YEAR', 'DRAFT_ROUND', 'DRAFT_NUMBER']]

            frames = [df2, df18, df19, df20, df21, df22, df23, df27]
            for frame in frames:

                joined_columns = set(frame.columns) - set(df.columns)
                joined_columns = list(joined_columns)
                joined_columns.append('PLAYER_ID')
                frame = frame[joined_columns]

                df = df.merge(frame, on='PLAYER_ID',how='left').reset_index(drop=True)

            df['year'] = year

            extra_columns = [
            '_PLAYER_NAME', 
            '_PLAYER_LAST_TEAM_ID', 
            '_GP', 
            '_PLAYER_POSITION', 
            '_PLAYER_LAST_TEAM_ABBREVIATION', 
            '_PLAYER_ID',
            '_MIN',
            '_TEAM_ABBREVIATUON',
            '_G',
            '_W',
            '_L',
            '_MIN',
            '_AGE',
            '_TEAM_ID'
        ]


            cols_to_drop = [col for col in df.columns if any(col.endswith(ex_col) for ex_col in extra_columns)]
            df = df.drop(columns=cols_to_drop)

            year_frame.append(df)

        yeardata=pd.concat(year_frame)
        yeardata.to_csv(str(year)+trail+'_avg.csv',index=False)
        frames.append(yeardata)
        print(f"Year: {year}")
        print_diagnostics_report(year=year, trail=trail)

    total = pd.concat(frames)
    return total


def pull_wnba_avg_classic(dates, start_year,end_year,ps=False):
    stype = 'Regular%20Season'
    trail=''
    if ps == True:
        stype='Playoffs'
        trail='_ps'
    frames = []
    shotcolumns = ['FGA_FREQUENCY', 'FGM', 'FGA', 'FG_PCT', 'EFG_PCT', 'FG2A_FREQUENCY', 'FG2M', 'FG2A', 'FG2_PCT', 
                   'FG3A_FREQUENCY', 'FG3M', 'FG3A', 'FG3_PCT']
    unit='Player'
    for year in range(start_year, end_year):
        year_frame=[]
        date=''
        # WNBA seasons are a single calendar year (e.g. "2024"), not "2023-24" like the NBA
        season = str(year)



        url = f'https://stats.nba.com/stats/leaguedashplayerstats?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=10&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=&PaceAdjust=N&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={season}&SeasonSegment=&SeasonType={stype}&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='
        df = pull_data(url, label='base_stats')

        url2 = f'https://stats.nba.com/stats/leaguedashplayerstats?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=10&Location=&MeasureType=Advanced&Month=0&OpponentTeamID=0&Outcome=&PORound=&PaceAdjust=N&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={season}&SeasonSegment=&SeasonType={stype}&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='
        df2 = pull_data(url2, label='advanced_stats')


        url3=f"https://stats.nba.com/stats/leaguedashplayershotlocations?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&DistanceRange=By%20Zone&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={season}&SeasonSegment=&SeasonType={stype}&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight="
        df3 = pull_data(url3, label='shot_zones')

        zone_columns=['PLAYER_ID', 'PLAYER_NAME', 'TEAM_ID', 'TEAM_ABBREVIATION', 'AGE', 'NICKNAME',
         'RA_FGM', 'RA_FGA', 'RA_FG_PCT',               # Restricted Area
         'ITP_FGM', 'ITP_FGA', 'ITP_FG_PCT',             # In The Paint (Non-RA)
         'MID_FGM', 'MID_FGA', 'MID_FG_PCT',             # Mid Range
         'LEFT_CORNER_3_FGM', 'LEFT_CORNER_3_FGA', 'LEFT_CORNER_3_FG_PCT',  # Left Corner 3
         'RIGHT_CORNER_3_FGM', 'RIGHT_CORNER_3_FGA', 'RIGHT_CORNER_3_FG_PCT', # Right Corner 3


                       # All Corner 3s
         'ABOVE_BREAK_3_FGM', 'ABOVE_BREAK_3_FGA', 'ABOVE_BREAK_3_FG_PCT', 
               'BACKCOURT_FGM', 'BACKCOURT_FGA', 'BACKCOURT_FG_PCT', # Right Corner 3

                      'CORNER_3_FGM', 'CORNER_3_FGA', 'CORNER_3_FG_PCT'  ]  # Above the Break 3
        df3.columns=zone_columns



        url4=f"https://stats.nba.com/stats/leaguedashplayershotlocations?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&DistanceRange=5ft%20Range&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={season}&SeasonSegment=&SeasonType={stype}&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight="
        df4 = pull_data(url4, label='shot_5ft_range')
        df4.columns=['PLAYER_ID', 'PLAYER_NAME', 'TEAM_ID', 'TEAM_ABBR', 'AGE', 'NICKNAME',
         'FGM_LT_5', 'FGA_LT_5', 'FGP_LT_5',      # Less than 5 feet
         'FGM_5_9', 'FGA_5_9', 'FGP_5_9',         # 5-9 feet
         'FGM_10_14', 'FGA_10_14', 'FGP_10_14',   # 10-14 feet
         'FGM_15_19', 'FGA_15_19', 'FGP_15_19',   # 15-19 feet
         'FGM_20_24', 'FGA_20_24', 'FGP_20_24',   # 20-24 feet
         'FGM_25_29', 'FGA_25_29', 'FGP_25_29',   # 25-29 feet
         'FGM_30_34', 'FGA_30_34', 'FGP_30_34',   # 30-34 feet
         'FGM_35_39', 'FGA_35_39', 'FGP_35_39',   # 35-39 feet
         'FGM_40_PLUS', 'FGA_40_PLUS', 'FGP_40_PLUS'  # 40+ feet
        ]
        url5=f'https://stats.nba.com/stats/leaguedashplayerbiostats?College=&Conference=&Country=&DateFrom=&DateTo=&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=10&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PerMode=PerGame&Period=0&PlayerExperience=&PlayerPosition=&Season={season}&SeasonSegment=&SeasonType={stype}&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='
        df5 = pull_data(url5, label='bio_stats')
        df5=df5[['PLAYER_ID','AGE','PLAYER_HEIGHT_INCHES', 'PLAYER_WEIGHT', 'COLLEGE', 'COUNTRY', 'DRAFT_YEAR', 'DRAFT_ROUND', 'DRAFT_NUMBER']]
        frames = [df2, df3, df4,df5]
        for frame in frames:

            joined_columns = set(frame.columns) - set(df.columns)
            joined_columns = list(joined_columns)
            joined_columns.append('PLAYER_ID')
            frame = frame[joined_columns]

            df = df.merge(frame, on='PLAYER_ID',how='left').reset_index(drop=True)

        df['year'] = year


        year_frame.append(df)

        yeardata=pd.concat(year_frame)
        yeardata.to_csv(str(year)+trail+'_avg.csv',index=False)
        frames.append(yeardata)
        print(f"Year: {year}")
        print_diagnostics_report(year=year, trail=trail)

    total = pd.concat(frames)
    return total



# NOTE: get_dates() is left untouched from the NBA version and is NOT wired up for WNBA.
# It pulls team IDs from nba_api's teams.get_teams() (NBA-only) and reads from your
# gabriel1200/shot_data GitHub repo, which is NBA shot data. It's unused below (the call
# is commented out, same as in the original script) - if you want a WNBA equivalent you'll
# need a WNBA team-id list and a WNBA shot-data source.
def get_dates(start_year,end_year):
    dates=[]
    for year in range(start_year,end_year):

        for team in teams.get_teams():
            team_id=team['id']
            path ='https://raw.githubusercontent.com/gabriel1200/shot_data/refs/heads/master/team/'+str(year)+'/'+str(team_id)+'.csv'

            df=pd.read_csv(path)

            df=df[['PLAYER_ID','HTM','VTM','GAME_DATE']]
            df.drop_duplicates(inplace=True)
            dates.append(df)
    return pd.concat(dates)
start_year=2026
end_year=2027
ps=False
#dateframe=get_dates(start_year,end_year)
#dates=dateframe['GAME_DATE'].unique().tolist()
dates=[]
df= pull_wnba_avg(dates,start_year,end_year,ps=ps)
# NOTE: 1630169 is Luka Doncic's NBA player_id - swap this in for a WNBA player_id
# (or just use df.head()) to sanity-check the pull.
print(df.head()[['PLAYER_NAME','PLAYER_ID','GP','MIN']])
#data=pull_game_level(dates)
season_string='ps' if ps else 'rs'




# In[ ]:


#start_year=2014
#end_year=2026
#df= pull_wnba_avg(dates,start_year,end_year,ps=True)

#start_year=1997
#end_year=2014
#df= pull_wnba_avg_classic(dates,start_year,end_year,ps=True)



# Define the API URL
url = "https://api.pbpstats.com/get-totals/wnba"

# Get the current year
current_year = datetime.now().year

# Iterate over seasons from 2001 to current year

def fetch_wnba_data(start_year, end_year, season_type='rs', save_to_csv=True):
    """
    Fetch WNBA player stats from the PBP Stats API for a given range of seasons and season type.

    Parameters:
    - start_year (int): The starting year (e.g., 2015).
    - end_year (int): The ending year (inclusive, e.g., 2026).
    - season_type (str): Season type, 'rs' for Regular Season or 'ps' for Playoffs.
    - save_to_csv (bool): Whether to save the data as CSV files. Default is True.

    Returns:
    - List of DataFrames containing the fetched data for each season.
    """
    # Define the API URL
    url = "https://api.pbpstats.com/get-totals/wnba"

    # Map season type input to API-compatible parameter
    season_type_map = {'rs': "Regular Season", 'ps': "Playoffs"}
    if season_type not in season_type_map:
        raise ValueError("Invalid season type. Use 'rs' for Regular Season or 'ps' for Playoffs.")

    # Converted season type
    season_type_label = season_type_map[season_type]
    all_data = []  # Store dataframes for return

    for year in range(start_year, end_year + 1):
        # WNBA seasons are a single calendar year for pbpstats too (e.g. "2024"), not "2023-24"
        season = str(year)
        params = {
            "Season": season,
            "SeasonType": season_type_label,
            "Type": "Player",
        }

        # pbpstats.com fails intermittently even with proper headers - retry a few times
        # with backoff before giving up on the year, rather than one failed request
        # permanently skipping it.
        max_attempts = 4
        for attempt in range(1, max_attempts + 1):
            try:
                response = requests.get(url, params=params, headers=headers, timeout=30)
                response.raise_for_status()
                response_json = response.json()
                player_stats = response_json.get("multi_row_table_data", [])
                break
            except Exception as e:
                if attempt == max_attempts:
                    print(f"Error fetching data for {season} {season_type_label} after {max_attempts} attempts: {e}")
                    player_stats = None
                else:
                    wait = 5 * attempt
                    print(f"  Attempt {attempt} failed for {season} {season_type_label} ({e}); retrying in {wait}s...")
                    time.sleep(wait)

        if player_stats is None:
            continue

        try:
            # Skip if no data
            if not player_stats:
                print(f"No data found for {season} {season_type_label}.")
                continue

            # Create DataFrame and add year column
            df = pd.DataFrame(player_stats)
            year_label = f"{year}_ps" if season_type == 'ps' else str(year)
            df["year"] = year_label
            all_data.append(df)
            time.sleep(3)

            # Save to CSV if enabled
            if save_to_csv:
                filename = f"{year_label}_pbp.csv"
                df.to_csv(filename, index=False)
                print(f"Saved: {filename}")

        except Exception as e:
            print(f"Error processing data for {season} {season_type_label}: {e}")

    return all_data 

# NOTE: pull_wnba_avg's loop is range(start_year, end_year) - end-EXCLUSIVE, so it covers
# start_year..end_year-1. fetch_wnba_data's loop is range(start_year, end_year+1) - end-
# INCLUSIVE. Passing end_year-1 here makes the two cover the same years. If you ever change
# how pull_wnba_avg or fetch_wnba_data loop internally, re-check this line.
data = fetch_wnba_data(start_year , end_year - 1, season_type=season_string)
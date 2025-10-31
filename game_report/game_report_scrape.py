#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from nba_api.stats.static import players,teams
import pandas as pd
import requests
import sys
import os
import time
from datetime import datetime
from requests.exceptions import RequestException # Import for better error handling
def format_date_to_url(date):
    # Convert date from YYYYMMDD to datetime object
    date_obj = datetime.strptime(str(date), '%Y%m%d')

    # Format the date as MM%2FDD%2FYYYY
    formatted_date = date_obj.strftime('%m%%2F%d%%2F%Y')

    return formatted_date

# Example usage

def pull_data(url, max_retries=3, delay_seconds=5):
    """
    Pulls data from a URL with retry logic for handling transient errors.

    Args:
        url (str): The API endpoint URL.
        max_retries (int): The maximum number of times to retry the request.
        delay_seconds (int): The time in seconds to wait between retries.

    Returns:
        pandas.DataFrame or None: The parsed data as a DataFrame, or None if all retries fail.
    """
    headers = {
        "Host": "stats.nba.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Referer": "https://stats.nba.com/",
        "Origin": "https://stats.nba.com",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
    }

    for attempt in range(max_retries):
        try:
            # --- API Request Attempt ---
            print(f"Attempt {attempt + 1} of {max_retries} to pull data from {url}")
            response = requests.get(url, headers=headers)
            response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)
            json_data = response.json()

            # --- Data Parsing Logic (from your original code) ---
            if len(json_data.get("resultSets", [])) == 1:
                data = json_data["resultSets"][0]["rowSet"]
                columns = json_data["resultSets"][0]["headers"]
                df = pd.DataFrame.from_records(data, columns=columns)
            else:
                # Assuming this else block is for a different structure
                data = json_data["resultSets"]["rowSet"]
                # Assuming 'headers' is a list of dicts and the second element has 'columnNames'
                columns = json_data["resultSets"]["headers"][1]['columnNames']
                df = pd.DataFrame.from_records(data, columns=columns)

            time.sleep(1.2)
            print('pulled successfully')
            return df

        except (RequestException, ValueError, KeyError) as e:
            # Handle request errors (e.g., network issues, timeouts) or JSON/Key errors
            print(f"Error on attempt {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                print(f"Waiting {delay_seconds} seconds before retrying...")
                time.sleep(delay_seconds)
            else:
                print(f"Max retries ({max_retries}) reached. Failed to pull data.")
                # You can log the error or return a specific value here
                time.sleep(1.2) # Maintain the original delay even on final failure before returning
                return None # Return None or raise an exception if preferred

    return None # Should not be reached, but good practice


def pull_game_level(dateframe, start_year,end_year,ps=False):
    stype = 'Regular%20Season'
    trail=''
    if ps == True:
        stype='Playoffs'
        trail='ps'
    dframes = []
    shotcolumns = ['FGA_FREQUENCY', 'FGM', 'FGA', 'FG_PCT', 'EFG_PCT', 'FG2A_FREQUENCY', 'FG2M', 'FG2A', 'FG2_PCT', 
                   'FG3A_FREQUENCY', 'FG3M', 'FG3A', 'FG3_PCT']

    unit='Player'
    for year in range(start_year, end_year):
        count=0
        countframe=dateframe[dateframe.year==year].reset_index()
        year_frame=[]
        test = False
        game_date = 20250125
        if test != False:
            countframe=countframe[countframe.GAME_DATE<game_date]

        year_dates = countframe['GAME_DATE'].unique().tolist()
        if os.path.exists('year_files/'+str(year)+trail+'_games.csv'):
            df= pd.read_csv('year_files/'+str(year)+trail+'_games.csv')
            df['date']=df['date'].astype(int)
            df.sort_values(by='date',ascending=False)
            df.drop_duplicates(subset=['date','PLAYER_ID','TEAM_ID'],inplace=True)
            if test != False:
                df=df[df.date<game_date]

            year_frame.append(df)

            year_dates=[int(date) for date in year_dates if date not in df['date'].unique().tolist()]
            year_dates=year_dates[::-1]


        season = str(year - 1) + '-' + str(year)[-2:]
        print(year_dates)
        for date in year_dates:
            try:
                date_num = int(date)
                date = format_date_to_url(date)


                url = f'https://stats.nba.com/stats/leaguedashplayerstats?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=&PaceAdjust=N&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={season}&SeasonSegment=&SeasonType={stype}&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='

                df = pull_data(url)

                url2 = f'https://stats.nba.com/stats/leaguedashplayerstats?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&MeasureType=Advanced&Month=0&OpponentTeamID=0&Outcome=&PORound=&PaceAdjust=N&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={season}&SeasonSegment=&SeasonType={stype}&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='
                df2 = pull_data(url2)

                url3 = f'https://stats.nba.com/stats/leaguedashptstats?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&GameScope=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=&PerMode=Totals&PlayerExperience=&PlayerOrTeam={unit}&PlayerPosition=&PtMeasureType=Passing&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='
                df3 = pull_data(url3)

                url4 = f'https://stats.nba.com/stats/leaguedashptstats?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&GameScope=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=&PerMode=Totals&PlayerExperience=&PlayerOrTeam={unit}&PlayerPosition=&PtMeasureType=Drives&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='
                df4 = pull_data(url4)

                url5 = f'https://stats.nba.com/stats/leaguedashptstats?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&GameScope=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=&PerMode=Totals&PlayerExperience=&PlayerOrTeam={unit}&PlayerPosition=&PtMeasureType=Possessions&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='
                df5 = pull_data(url5)

                url6 = f'https://stats.nba.com/stats/leaguedashptstats?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&GameScope=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=&PerMode=Totals&PlayerExperience=&PlayerOrTeam={unit}&PlayerPosition=&PtMeasureType=Rebounding&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='
                df6 = pull_data(url6)

                url7 = f'https://stats.nba.com/stats/leaguedashplayerptshot?CloseDefDistRange=0-2%20Feet%20-%20Very%20Tight&College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&DribbleRange=&GameScope=&GameSegment=&GeneralRange=&Height=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&Season={season}&SeasonSegment=&SeasonType={stype}&ShotClockRange=&ShotDistRange=&StarterBench=&TeamID=0&TouchTimeRange=&VsConference=&VsDivision=&Weight='
                df7 = pull_data(url7)

                term = 'very_tight_'
                df7.rename(columns={col: term + col for col in shotcolumns}, inplace=True)

                url8 = 'https://stats.nba.com/stats/leaguedashplayerptshot?CloseDefDistRange=2-4%20Feet%20-%20Tight&College=&Conference=&Country=&DateFrom=' + date + '&DateTo=' + date + '&Division=&DraftPick=&DraftYear=&DribbleRange=&GameScope=&GameSegment=&GeneralRange=&Height=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&Season=' + season + '&SeasonSegment=&SeasonType='+stype+'&ShotClockRange=&ShotDistRange=&StarterBench=&TeamID=0&TouchTimeRange=&VsConference=&VsDivision=&Weight='
                df8 = pull_data(url8)
                term = 'tight_'
                df8.rename(columns={col: term + col for col in shotcolumns},inplace=True)

                url9 = 'https://stats.nba.com/stats/leaguedashplayerptshot?CloseDefDistRange=4-6%20Feet%20-%20Open&College=&Conference=&Country=&DateFrom=' + date + '&DateTo=' + date + '&Division=&DraftPick=&DraftYear=&DribbleRange=&GameScope=&GameSegment=&GeneralRange=&Height=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&Season=' + season + '&SeasonSegment=&SeasonType='+stype+'&ShotClockRange=&ShotDistRange=&StarterBench=&TeamID=0&TouchTimeRange=&VsConference=&VsDivision=&Weight='
                df9 = pull_data(url9)
                term = 'open_'
                df9.rename(columns={col: term + col for col in shotcolumns},inplace=True)

                url10 = 'https://stats.nba.com/stats/leaguedashplayerptshot?CloseDefDistRange=6%2B%20Feet%20-%20Wide%20Open&College=&Conference=&Country=&DateFrom=' + date + '&DateTo=' + date + '&Division=&DraftPick=&DraftYear=&DribbleRange=&GameScope=&GameSegment=&GeneralRange=&Height=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&Season=' + season + '&SeasonSegment=&SeasonType='+stype+'&ShotClockRange=&ShotDistRange=&StarterBench=&TeamID=0&TouchTimeRange=&VsConference=&VsDivision=&Weight='
                df10 = pull_data(url10)
                term = 'wide_open_'
                df10.rename(columns={col: term + col for col in shotcolumns},inplace=True)
                url11 = 'https://stats.nba.com/stats/leaguedashptstats?College=&Conference=&Country=&DateFrom=' + date + '&DateTo=' + date + '&Division=&DraftPick=&DraftYear=&GameScope=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=&PerMode=Totals&PlayerExperience=&PlayerOrTeam=Player&PlayerPosition=&PtMeasureType=PullUpShot&Season=' + season + '&SeasonSegment=&SeasonType='+stype+'&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='
                df11 = pull_data(url11) 
                shotcolumns2=shotcolumns+['EFG%']
                term='pullup_'
                df11.rename(columns={col: term + col for col in shotcolumns2},inplace=True)

                url12 = 'https://stats.nba.com/stats/leaguedashptstats?College=&Conference=&Country=&DateFrom=' + date + '&DateTo=' + date + '&Division=&DraftPick=&DraftYear=&GameScope=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=&PerMode=Totals&PlayerExperience=&PlayerOrTeam=Player&PlayerPosition=&PtMeasureType=Efficiency&Season=' + season + '&SeasonSegment=&SeasonType='+stype+'&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='


                df12 = pull_data(url12) 
                url13=f"https://stats.nba.com/stats/leaguedashplayershotlocations?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&DistanceRange=By%20Zone&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={season}&SeasonSegment=&SeasonType={stype}&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight="

                df13=pull_data(url13)

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

                df13.columns=zone_columns
                url14=f"https://stats.nba.com/stats/leaguedashptdefend?College=&Conference=&Country=&DateFrom{date}=&DateTo={date}&DefenseCategory=Less%20Than%206Ft&Division=&DraftPick=&DraftYear=&GameSegment=&Height=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight="
                df14=pull_data(url14)
                df14.rename(columns={'CLOSE_DEF_PERSON_ID':'PLAYER_ID'},inplace=True)

                url15=f"https://stats.nba.com/stats/leaguedashplayershotlocations?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&DistanceRange=5ft%20Range&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={season}&SeasonSegment=&SeasonType={stype}&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight="
                df15=pull_data(url15)
                df15.columns=['PLAYER_ID', 'PLAYER_NAME', 'TEAM_ID', 'TEAM_ABBR', 'AGE', 'NICKNAME',
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
                url16 = f'https://stats.nba.com/stats/leaguedashptstats?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&GameScope=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=&PerMode=Totals&PlayerExperience=&PlayerOrTeam={unit}&PlayerPosition=&PtMeasureType=CatchShoot&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='
                df16=pull_data(url16)


                url17 = f'https://stats.nba.com/stats/leaguedashteamstats?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&MeasureType=Advanced&Month=0&OpponentTeamID=0&Outcome=&PORound=&PaceAdjust=N&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={season}&SeasonSegment=&SeasonType=Playoffs&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='
                df17 = pull_data(url17)
                df17=df17[['TEAM_ID','POSS']]
                df17.columns=['TEAM_ID','team_poss']

                poss_map=dict(zip(df17['TEAM_ID'],df17['team_poss']  ))

                df['team_poss']=df['TEAM_ID'].map(poss_map)

                url18 = f'https://stats.nba.com/stats/leaguedashptdefend?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&DefenseCategory=Overall&Division=&DraftPick=&DraftYear=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PerMode=PerGame&Period=0&PlayerExperience=&PlayerPosition=&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='

                df18 = pull_data(url18)
                df18.rename(columns={'CLOSE_DEF_PERSON_ID': 'PLAYER_ID'}, inplace=True)
                df18.rename(columns={col: f'overall_def_{col}' for col in df8.columns if col != 'PLAYER_ID'}, inplace=True)

                # Link 2: 3-pointers defense stats
                url19 = f'https://stats.nba.com/stats/leaguedashptdefend?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&DefenseCategory=3%20Pointers&Division=&DraftPick=&DraftYear=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PerMode=PerGame&Period=0&PlayerExperience=&PlayerPosition=&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='

                df19 = pull_data(url19)

                df19.rename(columns={'CLOSE_DEF_PERSON_ID': 'PLAYER_ID'}, inplace=True)
                df19.rename(columns={col: f'three_pt_def_{col}' for col in df19.columns if col != 'PLAYER_ID'}, inplace=True)


                # Link 3: 2-pointers defense stats
                url20 = f'https://stats.nba.com/stats/leaguedashptdefend?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&DefenseCategory=2%20Pointers&Division=&DraftPick=&DraftYear=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PerMode=PerGame&Period=0&PlayerExperience=&PlayerPosition=&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='

                df20 = pull_data(url20)

                df20.rename(columns={'CLOSE_DEF_PERSON_ID': 'PLAYER_ID'}, inplace=True)
                df20.rename(columns={col: f'two_pt_def_{col}' for col in df20.columns if col != 'PLAYER_ID'}, inplace=True)


                # Link 4: Less than 6ft defense stats
                url21 = f'https://stats.nba.com/stats/leaguedashptdefend?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&DefenseCategory=Less%20Than%206Ft&Division=&DraftPick=&DraftYear=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PerMode=PerGame&Period=0&PlayerExperience=&PlayerPosition=&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='

                df21 = pull_data(url21)
                df21.rename(columns={'CLOSE_DEF_PERSON_ID': 'PLAYER_ID'}, inplace=True)
                df21.rename(columns={col: f'less_6ft_def_{col}' for col in df21.columns if col != 'PLAYER_ID'}, inplace=True)

                # Link 5: Less than 10ft defense stats
                url22 = f'https://stats.nba.com/stats/leaguedashptdefend?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&DefenseCategory=Less%20Than%2010Ft&Division=&DraftPick=&DraftYear=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PerMode=PerGame&Period=0&PlayerExperience=&PlayerPosition=&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='

                df22 = pull_data(url22)
                df22.rename(columns={'CLOSE_DEF_PERSON_ID': 'PLAYER_ID'}, inplace=True)
                df22.rename(columns={col: f'less_10ft_def_{col}' for col in df22.columns if col != 'PLAYER_ID'}, inplace=True)


                # Link 6: Less than 15ft defense stats
                url23 = f'https://stats.nba.com/stats/leaguedashptdefend?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&DefenseCategory=Greater%20Than%2015Ft&Division=&DraftPick=&DraftYear=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PerMode=PerGame&Period=0&PlayerExperience=&PlayerPosition=&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='

                df23 = pull_data(url6)
                df23.rename(columns={'CLOSE_DEF_PERSON_ID': 'PLAYER_ID'}, inplace=True)
                df23.rename(columns={col: f'more_15ft_def_{col}' for col in df23.columns if col != 'PLAYER_ID'}, inplace=True)



                url24 = f'https://stats.nba.com/stats/leaguehustlestatsplayer?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&GameScope=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=PerGame&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={season}&SeasonSegment=&SeasonType={stype}&TeamID=0&VsConference=&VsDivision=&Weight='

                df24 = pull_data(url24)
                df24.rename(columns={col: f'hustle_{col}' for col in df24.columns if col != 'PLAYER_ID'}, inplace=True)

                # Link 8: Post touch stats
                url25 = f'https://stats.nba.com/stats/leaguedashptstats?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&GameScope=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PerMode=PerGame&PlayerExperience=&PlayerOrTeam=Player&PlayerPosition=&PtMeasureType=PostTouch&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='

                df25 = pull_data(url25)
                df25.rename(columns={col: f'post_touch_{col}' for col in df25.columns if col != 'PLAYER_ID'}, inplace=True)


                # Link 9: Speed distance stats
                url26 = f'https://stats.nba.com/stats/leaguedashptstats?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&GameScope=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PerMode=PerGame&PlayerExperience=&PlayerOrTeam=Player&PlayerPosition=&PtMeasureType=SpeedDistance&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='

                df26 = pull_data(url26)
                df26 = pull_data(url26)
                frames = [df2, df3, df4, df5, df6, df7, df8, df9, df10,df11,df12,df13,df14,df15,df16,df18,df19,df20,df21,df22,df23,df24,df25,df26]
                for frame in frames:

                    joined_columns = set(frame.columns) - set(df.columns)
                    joined_columns = list(joined_columns)
                    joined_columns.append('PLAYER_ID')
                    frame = frame[joined_columns]

                    df = df.merge(frame, on='PLAYER_ID',how='left').reset_index(drop=True)

                df['year'] = year
                df['date']=date_num
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
                count+=1
                print(date_num)
                if count %10==0:

                    yeardata=pd.concat(year_frame)
                    print(len(yeardata))
                    yeardata['playoffs']=ps
                    yeardata.to_csv(str(year)+trail+'_games.csv',index=False)
            except Exception as e:
                print(str(e))
                print(str(date_num))
                time.sleep(1)


        yeardata=pd.concat(year_frame)
        print(len(yeardata))
        yeardata['playoffs']=ps
        print('year_files/'+str(year)+trail+'_games.csv')
        yeardata.to_csv('year_files/'+str(year)+trail+'_games.csv',index=False)
        dframes.append(yeardata)
        print(f"Year: {year}")

    total = pd.concat(dframes)
    return total

start_year=2026
end_year=2027




def get_dates(start_year, end_year, ps=False):
    trail = 'ps' if ps else ''
    dates = []

    for year in range(start_year, end_year):
        for team in teams.get_teams():
            team_id = team['id']
            base = 'https://raw.githubusercontent.com/gabriel1200/shot_data/refs/heads/master/team/'
            path = f"{base}{year}{trail}/{team_id}.csv"
            print(path)

            try:
                df = pd.read_csv(path)
                df = df[['PLAYER_ID', 'TEAM_ID', 'HTM', 'VTM', 'GAME_DATE', 'GAME_ID']]
                df.sort_values(by='GAME_DATE', inplace=True)
                df.drop_duplicates(inplace=True)
                df['year'] = year
                dates.append(df)
            except Exception as e:
                print(f"Skipping {path}: {e}")

    return pd.concat(dates, ignore_index=True) if dates else pd.DataFrame()
ps=False
dateframe=get_dates(start_year,end_year,ps=ps)

dates=dateframe['GAME_DATE'].unique().tolist()
dates.sort()

df= pull_game_level(dateframe,start_year,end_year,ps=ps)
#data=pull_game_level(dates)
df
df.drop_duplicates(subset=['PLAYER_ID','TEAM_ID','date'])


# In[ ]:


dates.sort()
dates


# In[ ]:


'''
dateframe = pd.read_csv('../../shot_data/game_dates.csv')
dateframe=dateframe[dateframe.season=='2024-25']
dateframe.sort_values(by='date',inplace=True,ascending=False)

print(dateframe.head(30))
print(dateframe.columns)
dateframe=dateframe[['date','GAME_ID','TEAM_ID']]
dateframe.date.max()

print(len(test))
merge=test.merge(dateframe,how='left')
print(len(merge))
merge


merge[merge.GAME_ID.isna()]
'''


# In[ ]:


if ps==False:
    trail=''
else:
    trail='ps'
print(trail)
frames= []
count=0
index_master=pd.read_csv('index_master.csv')
index_master=index_master[index_master.team!='TOT']
index_master['team_id']=index_master['team_id'].astype(int)
index_master['nba_id']=index_master['nba_id'].astype(int)



for year in range(start_year,end_year):
    # Load the game data for the specific year.
    games_collected=[]
    df = pd.read_csv(f'year_files/{year}{trail}_games.csv')

    team_map=dict(zip(df['TEAM_ID'],df['TEAM_ABBREVIATION']))

    # Filter index_master for the current year.
    year_index = index_master[index_master['year'] == year].reset_index()

    # Process each unique date in the dataset.
    for date in df['date'].unique().tolist():
        datedf=df[df.date==date].reset_index(drop=True)
        datedf=datedf.drop_duplicates(subset=['PLAYER_ID','date'])
        # Filter game data by date.

        gameframe = dateframe[dateframe['GAME_DATE'] == date].reset_index()
        gameframe.rename(columns={'GAME_DATE':'date'},inplace=True)
        # Get the unique team and game data for the specific date from gameframe.

        to_merge = gameframe[['TEAM_ID', 'GAME_ID', 'date', 'year']].drop_duplicates().reset_index(drop=True)


        save_frame=datedf.merge(to_merge,on=['TEAM_ID','date','year'],how='left')


        save_frame.drop_duplicates(inplace=True)


        # Merge game data with index_master to ensure correct team alignment.
        # Match on 'player' and 'team' columns from index_master and 'TEAM_ID' from the game data.

        # Identify rows where the merge may have issues.
        if save_frame['GAME_ID'].isna().any():

            missing=save_frame[save_frame['GAME_ID'].isna()].reset_index(drop=True)
            print(gameframe)
            print(missing)
            save_frame.dropna(subset='GAME_ID',inplace=True)
            missing.drop(columns=['GAME_ID','TEAM_ID','TEAM_ABBREVIATION'],inplace=True)

            missing=missing.merge(gameframe,on=['PLAYER_ID','year','date'],how='left')
            missing['TEAM_ABBREVIATION']=missing['TEAM_ID'].map(team_map)

            save_frame=pd.concat([save_frame,missing])

        if save_frame['GAME_ID'].isna().any():

            missing=save_frame[save_frame['GAME_ID'].isna()].reset_index(drop=True)
            missing.drop(columns='GAME_ID',inplace=True)
            save_frame.dropna(subset='GAME_ID',inplace=True)
            missed=[]
            print(missing['PLAYER_ID'])

            for missed_player in missing['PLAYER_ID'].unique().tolist():
                missing_frame=missing[missing.PLAYER_ID==missed_player].reset_index(drop=True)
                print(missing_frame)
                temp_index=year_index[year_index.nba_id==missed_player].reset_index(drop=True)
                team_id=temp_index.iloc[0]['team_id']
                team=temp_index.iloc[0]['team']
                missing_frame['TEAM_ID']=int(team_id)
                missing_frame['TEAM_ABBREVIATION']=team
                missing_frame= missing_frame.merge(to_merge,on=['TEAM_ID','date','year'],how='left')
                missed.append(missing_frame)

            missing=pd.concat(missed)
            save_frame=pd.concat([save_frame,missing])
        if save_frame['GAME_ID'].isna().any():

            missing=save_frame[save_frame['GAME_ID'].isna()].reset_index(drop=True)
            missing.drop(columns='GAME_ID',inplace=True)
            save_frame.dropna(subset='GAME_ID',inplace=True)
            missed=[]

            for missed_player in missing['PLAYER_ID'].unique().tolist():
                missing_frame=missing[missing.PLAYER_ID==missed_player].reset_index(drop=True)
                temp_index=year_index[year_index.nba_id==missed_player].reset_index(drop=True)
                team_id=temp_index.iloc[1]['team_id']
                team=temp_index.iloc[1]['team']
                missing_frame['TEAM_ID']=int(team_id)
                missing_frame['TEAM_ABBREVIATION']=team
                missing_frame= missing_frame.merge(to_merge,on=['TEAM_ID','date','year'],how='left')
                missed.append(missing_frame)

            missing=pd.concat(missed)
            save_frame=pd.concat([save_frame,missing])

        if save_frame['GAME_ID'].isna().any():

            missing=save_frame[save_frame['GAME_ID'].isna()].reset_index(drop=True)
            missing.drop(columns='GAME_ID',inplace=True)
            save_frame.dropna(subset='GAME_ID',inplace=True)
            missed=[]

            for missed_player in missing['PLAYER_ID'].unique().tolist():
                missing_frame=missing[missing.PLAYER_ID==missed_player].reset_index(drop=True)
                temp_index=year_index[year_index.nba_id==missed_player].reset_index(drop=True)
                team_id=temp_index.iloc[2]['team_id']
                team=temp_index.iloc[2]['team']
                missing_frame['TEAM_ID']=int(team_id)
                missing_frame['TEAM_ABBREVIATION']=team
                missing_frame= missing_frame.merge(to_merge,on=['TEAM_ID','date','year'],how='left')
                missed.append(missing_frame)

            missing=pd.concat(missed)
            save_frame=pd.concat([save_frame,missing])

        if save_frame['GAME_ID'].isna().any():
            missing=save_frame[save_frame['GAME_ID'].isna()]
            print('test point')
            print(missing)

        # Remove any duplicate entries after the merge.
        save_frame.drop_duplicates(inplace=True)
        save_frame['GAME_ID']=save_frame['GAME_ID'].astype(int)
        # Save each game by unique GAME_ID.
        for game_id in save_frame['GAME_ID'].unique():
            gameid_frame = save_frame[save_frame['GAME_ID'] == game_id].reset_index(drop=True)
            gameid_frame.to_csv(f'{year}/{game_id}.csv', index=False)
            games_collected.append(gameid_frame)
            count += 1
    all_games=pd.concat(games_collected)




    all_games.to_csv('all_games/all_'+str(year)+trail+'.csv',index=False)
    all_games.to_parquet('all_games/all_'+str(year)+trail+'.parquet', index=False)
    #all_games.head(1).to_csv('all_games/sample.csv')



            # Exit early for testing if more than 8 files are saved.



# In[ ]:





# In[ ]:





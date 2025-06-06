#!/usr/bin/env python
# coding: utf-8

# In[1]:


from nba_api.stats.static import players,teams
import pandas as pd
import requests
import sys
import os
import time
from datetime import datetime

def format_date_to_url(date):
    # Convert date from YYYYMMDD to datetime object
    date_obj = datetime.strptime(str(date), '%Y%m%d')

    # Format the date as MM%2FDD%2FYYYY
    formatted_date = date_obj.strftime('%m%%2F%d%%2F%Y')

    return formatted_date

# Example usage

def pull_data(url):


    headers = {
                                    "Host": "stats.nba.com",
                                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:72.0) Gecko/20100101 Firefox/72.0",
                                    "Accept": "application/json, text/plain, */*",
                                    "Accept-Language": "en-US,en;q=0.5",
                                    "Accept-Encoding": "gzip, deflate, br",

                                    "Connection": "keep-alive",
                                    "Referer": "https://stats.nba.com/"
                                }
    json = requests.get(url,headers = headers).json()

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

    time.sleep(.2)
    return df


def pull_avg(dates, start_year,end_year,ps=False):
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
        season = str(year - 1) + '-' + str(year)[-2:]

        for date in year_dates:


            url = f'https://stats.nba.com/stats/leaguedashplayerstats?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=&PaceAdjust=N&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={season}&SeasonSegment=&SeasonType={stype}&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='
            df = pull_data(url)

            url2 = f'https://stats.nba.com/stats/leaguedashplayerstats?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&MeasureType=Advanced&Month=0&OpponentTeamID=0&Outcome=&PORound=&PaceAdjust=N&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={season}&SeasonSegment=&SeasonType={stype}&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='
            df2 = pull_data(url2)

            url3 = f'https://stats.nba.com/stats/leaguedashptstats?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&GameScope=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=&PerMode=Totals&PlayerExperience=&PlayerOrTeam={unit}&PlayerPosition=&PtMeasureType=Passing&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='
            df3 = pull_data(url3)
            print(df3.columns)

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

            url18 = f'https://stats.nba.com/stats/leaguedashptdefend?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&DefenseCategory=Overall&Division=&DraftPick=&DraftYear=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='

            df18 = pull_data(url18)
            df18.rename(columns={'CLOSE_DEF_PERSON_ID': 'PLAYER_ID'}, inplace=True)
            df18.rename(columns={col: f'overall_def_{col}' for col in df8.columns if col != 'PLAYER_ID'}, inplace=True)

            # Link 2: 3-pointers defense stats
            url19 = f'https://stats.nba.com/stats/leaguedashptdefend?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&DefenseCategory=3%20Pointers&Division=&DraftPick=&DraftYear=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='

            df19 = pull_data(url19)

            df19.rename(columns={'CLOSE_DEF_PERSON_ID': 'PLAYER_ID'}, inplace=True)
            df19.rename(columns={col: f'three_pt_def_{col}' for col in df19.columns if col != 'PLAYER_ID'}, inplace=True)


            # Link 3: 2-pointers defense stats
            url20 = f'https://stats.nba.com/stats/leaguedashptdefend?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&DefenseCategory=2%20Pointers&Division=&DraftPick=&DraftYear=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='

            df20 = pull_data(url20)

            df20.rename(columns={'CLOSE_DEF_PERSON_ID': 'PLAYER_ID'}, inplace=True)
            df20.rename(columns={col: f'two_pt_def_{col}' for col in df20.columns if col != 'PLAYER_ID'}, inplace=True)


            # Link 4: Less than 6ft defense stats
            url21 = f'https://stats.nba.com/stats/leaguedashptdefend?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&DefenseCategory=Less%20Than%206Ft&Division=&DraftPick=&DraftYear=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='

            df21 = pull_data(url21)
            df21.rename(columns={'CLOSE_DEF_PERSON_ID': 'PLAYER_ID'}, inplace=True)
            df21.rename(columns={col: f'less_6ft_def_{col}' for col in df21.columns if col != 'PLAYER_ID'}, inplace=True)

            # Link 5: Less than 10ft defense stats
            url22 = f'https://stats.nba.com/stats/leaguedashptdefend?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&DefenseCategory=Less%20Than%2010Ft&Division=&DraftPick=&DraftYear=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='

            df22 = pull_data(url22)
            df22.rename(columns={'CLOSE_DEF_PERSON_ID': 'PLAYER_ID'}, inplace=True)
            df22.rename(columns={col: f'less_10ft_def_{col}' for col in df22.columns if col != 'PLAYER_ID'}, inplace=True)


            # Link 6: Less than 15ft defense stats
            url23 = f'https://stats.nba.com/stats/leaguedashptdefend?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&DefenseCategory=Greater%20Than%2015Ft&Division=&DraftPick=&DraftYear=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='

            df23 = pull_data(url6)
            df23.rename(columns={'CLOSE_DEF_PERSON_ID': 'PLAYER_ID'}, inplace=True)
            df23.rename(columns={col: f'more_15ft_def_{col}' for col in df23.columns if col != 'PLAYER_ID'}, inplace=True)



            url24 = f'https://stats.nba.com/stats/leaguehustlestatsplayer?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&GameScope=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=Totals&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={season}&SeasonSegment=&SeasonType={stype}&TeamID=0&VsConference=&VsDivision=&Weight='

            df24 = pull_data(url24)
            df24.rename(columns={col: f'hustle_{col}' for col in df24.columns if col != 'PLAYER_ID'}, inplace=True)

            # Link 8: Post touch stats
            url25 = f'https://stats.nba.com/stats/leaguedashptstats?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&GameScope=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PerMode=Totals&PlayerExperience=&PlayerOrTeam=Player&PlayerPosition=&PtMeasureType=PostTouch&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='

            df25 = pull_data(url25)
            df25.rename(columns={col: f'post_touch_{col}' for col in df25.columns if col != 'PLAYER_ID'}, inplace=True)


            # Link 9: Speed distance stats
            url26 = f'https://stats.nba.com/stats/leaguedashptstats?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&GameScope=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PerMode=Totals&PlayerExperience=&PlayerOrTeam=Player&PlayerPosition=&PtMeasureType=SpeedDistance&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='

            df26 = pull_data(url26)
            frames = [df2, df3, df4, df5, df6, df7, df8, df9, df10,df11,df12,df13,df14,df15,df16,df18,df19,df20,df21,df22,df23,df24,df25,df26]
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

    total = pd.concat(frames)
    return total


def pull_avg_classic(dates, start_year,end_year,ps=False):
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
        season = str(year - 1) + '-' + str(year)[-2:]



        url = f'https://stats.nba.com/stats/leaguedashplayerstats?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=&PaceAdjust=N&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={season}&SeasonSegment=&SeasonType={stype}&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='
        df = pull_data(url)

        url2 = f'https://stats.nba.com/stats/leaguedashplayerstats?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&MeasureType=Advanced&Month=0&OpponentTeamID=0&Outcome=&PORound=&PaceAdjust=N&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={season}&SeasonSegment=&SeasonType={stype}&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='
        df2 = pull_data(url2)


        url3=f"https://stats.nba.com/stats/leaguedashplayershotlocations?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&DistanceRange=By%20Zone&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={season}&SeasonSegment=&SeasonType={stype}&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight="
        df3=pull_data(url3)

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
        df4=pull_data(url4)
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

        frames = [df2, df3, df4]
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

    total = pd.concat(frames)
    return total



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
start_year=2025
end_year=2026
ps=True
#dateframe=get_dates(start_year,end_year)
#dates=dateframe['GAME_DATE'].unique().tolist()
dates=[]
df= pull_avg(dates,start_year,end_year,ps=ps)
print(df[df.PLAYER_ID==1630169][['PLAYER_NAME','PLAYER_ID','PULL_UP_EFG_PCT','GP']])
#data=pull_game_level(dates)
season_string='ps' if ps else 'rs'




# In[2]:


start_year=2014
end_year=2026
#df= pull_avg(dates,start_year,end_year,ps=True)

start_year=1997
end_year=2014
#df= pull_avg_classic(dates,start_year,end_year,ps=True)



# Define the API URL
url = "https://api.pbpstats.com/get-totals/nba"

# Get the current year
current_year = datetime.now().year

# Iterate over seasons from 2001 to current year

def fetch_nba_data(start_year, end_year, season_type='rs', save_to_csv=True):
    """
    Fetch NBA player stats from the PBP Stats API for a given range of seasons and season type.

    Parameters:
    - start_year (int): The starting year (e.g., 2001).
    - end_year (int): The ending year (inclusive, e.g., 2024).
    - season_type (str): Season type, 'rs' for Regular Season or 'ps' for Playoffs.
    - save_to_csv (bool): Whether to save the data as CSV files. Default is True.

    Returns:
    - List of DataFrames containing the fetched data for each season.
    """
    # Define the API URL
    url = "https://api.pbpstats.com/get-totals/nba"

    # Map season type input to API-compatible parameter
    season_type_map = {'rs': "Regular Season", 'ps': "Playoffs"}
    if season_type not in season_type_map:
        raise ValueError("Invalid season type. Use 'rs' for Regular Season or 'ps' for Playoffs.")

    # Converted season type
    season_type_label = season_type_map[season_type]
    all_data = []  # Store dataframes for return

    for year in range(start_year, end_year + 1):
        # Format the season for API (e.g., "2024-25")
        season = f"{year-1}-{str(year)[-2:]}"
        params = {
            "Season": season,
            "SeasonType": season_type_label,
            "Type": "Player",
        }

        try:
            # Fetch data from the API
            response = requests.get(url, params=params)
            response.raise_for_status()  # Raise exception for HTTP errors
            response_json = response.json()
            player_stats = response_json.get("multi_row_table_data", [])

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
            print(f"Error fetching data for {season} {season_type_label}: {e}")

    return all_data
start_year=2025
data = fetch_nba_data(start_year, 2025, season_type=season_string)


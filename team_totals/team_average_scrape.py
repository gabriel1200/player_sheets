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

def format_date_to_url(date):
    # Convert date from YYYYMMDD to datetime object
    date_obj = datetime.strptime(str(date), '%Y%m%d')
    
    # Format the date as MM%2FDD%2FYYYY
    formatted_date = date_obj.strftime('%m%%2F%d%%2F%Y')
    
    return formatted_date

# Example usage
from fourfactors import four_factors_data

def pull_data(url):
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

    json = requests.get(url,headers = headers).json()

    if len(json["resultSets"])== 1:

        
        data = json["resultSets"][0]["rowSet"]
        ##print(data)
        columns = json["resultSets"][0]["headers"]
        ##print(columns)
        
        df = pd.DataFrame.from_records(data, columns=columns)
    else:

        data = json["resultSets"]["rowSet"]
        ##print(json)
        columns = json["resultSets"]["headers"][1]['columnNames']
        ##print(columns)
        df = pd.DataFrame.from_records(data, columns=columns)

    time.sleep(1.2)
    return df


def pull_game_avg( start_year,end_year,ps=False,unit='Player'):
    stype = 'Regular%20Season'
    trail=''
    if ps == True:
        stype='Playoffs'
        trail='ps'
    if unit.lower()=='team':
        trail+='_team'
    year_frames = []
    shotcolumns = ['FGA_FREQUENCY', 'FGM', 'FGA', 'FG_PCT', 'EFG_PCT', 'FG2A_FREQUENCY', 'FG2M', 'FG2A', 'FG2_PCT', 
                   'FG3A_FREQUENCY', 'FG3M', 'FG3A', 'FG3_PCT']
    
 
    for year in range(start_year, end_year):

            

        season = str(year - 1) + '-' + str(year)[-2:]


        try:

            date = ''


            url = f'https://stats.nba.com/stats/leaguedashteamstats?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=&PaceAdjust=N&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={season}&SeasonSegment=&SeasonType={stype}&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='
        
            df = pull_data(url)
            #print(df)
            #print('frame1')

            url2 = f'https://stats.nba.com/stats/leaguedashteamstats?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&MeasureType=Advanced&Month=0&OpponentTeamID=0&Outcome=&PORound=&PaceAdjust=N&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={season}&SeasonSegment=&SeasonType={stype}&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='
            df2 = pull_data(url2)
            #print(df2)
            #print('frame2')

                 
            url3 = f'https://stats.nba.com/stats/leaguedashptstats?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&GameScope=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=&PerMode=Totals&PlayerExperience=&PlayerOrTeam={unit}&PlayerPosition=&PtMeasureType=Passing&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='
            df3 = pull_data(url3)

            #print(df3)
            #print('frame3')


            url4 = f'https://stats.nba.com/stats/leaguedashptstats?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&GameScope=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=&PerMode=Totals&PlayerExperience=&PlayerOrTeam={unit}&PlayerPosition=&PtMeasureType=Drives&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='
            df4 = pull_data(url4)

            #print(df4)
            #print('frame4')


            url5 = f'https://stats.nba.com/stats/leaguedashptstats?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&GameScope=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=&PerMode=Totals&PlayerExperience=&PlayerOrTeam={unit}&PlayerPosition=&PtMeasureType=Possessions&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='
            df5 = pull_data(url5)

            #print(df5)
            #print('frame5')


            url6 = f'https://stats.nba.com/stats/leaguedashptstats?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&GameScope=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=&PerMode=Totals&PlayerExperience=&PlayerOrTeam={unit}&PlayerPosition=&PtMeasureType=Rebounding&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='
            df6 = pull_data(url6)

            #print(df6)
            #print('frame6')
            shots = ["0-2%20Feet%20-%20Very%20Tight","2-4%20Feet%20-%20Tight","4-6%20Feet%20-%20Open","6%2B%20Feet%20-%20Wide%20Open"]
            shot=shots[0]
            url7 = (
                f'https://stats.nba.com/stats/leaguedashteamptshot?CloseDefDistRange={shot}'
                f'&College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick='
                f'&DraftYear=&DribbleRange=&GameScope=&GameSegment=&GeneralRange='
                f'&Height=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0'
                f'&Outcome=&PORound=0&PerMode=Totals&Period=0&PlayerExperience='
                f'&PlayerPosition=&Season={season}'
                f'&SeasonSegment=&SeasonType={stype}&ShotClockRange='
                f'&ShotDistRange=&StarterBench=&TeamID=0&TouchTimeRange='
                f'&VsConference=&VsDivision=&Weight='
            )
            df7 = pull_data(url7)


    
            term = 'very_tight_'
            df7.rename(columns={col: term + col for col in shotcolumns}, inplace=True)
            
            shot=shots[1]
            url8 = (
                f'https://stats.nba.com/stats/leaguedashteamptshot?CloseDefDistRange={shot}'
                f'&College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick='
                f'&DraftYear=&DribbleRange=&GameScope=&GameSegment=&GeneralRange='
                f'&Height=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0'
                f'&Outcome=&PORound=0&PerMode=Totals&Period=0&PlayerExperience='
                f'&PlayerPosition=&Season={season}'
                f'&SeasonSegment=&SeasonType={stype}&ShotClockRange='
                f'&ShotDistRange=&StarterBench=&TeamID=0&TouchTimeRange='
                f'&VsConference=&VsDivision=&Weight='
            )
            df8 = pull_data(url8)

            term = 'tight_'
            df8.rename(columns={col: term + col for col in shotcolumns},inplace=True)

            #print(df8)
            #print('frame8')
            shot=shots[2]
            url9 = (
                f'https://stats.nba.com/stats/leaguedashteamptshot?CloseDefDistRange={shot}'
                f'&College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick='
                f'&DraftYear=&DribbleRange=&GameScope=&GameSegment=&GeneralRange='
                f'&Height=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0'
                f'&Outcome=&PORound=0&PerMode=Totals&Period=0&PlayerExperience='
                f'&PlayerPosition=&Season={season}'
                f'&SeasonSegment=&SeasonType={stype}&ShotClockRange='
                f'&ShotDistRange=&StarterBench=&TeamID=0&TouchTimeRange='
                f'&VsConference=&VsDivision=&Weight='
            )
            df9 = pull_data(url9)

            term = 'open_'
            df9.rename(columns={col: term + col for col in shotcolumns},inplace=True)

            #print(df9)
            #print('frame9')
            shot=shots[3]
            url10 = (
                f'https://stats.nba.com/stats/leaguedashteamptshot?CloseDefDistRange={shot}'
                f'&College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick='
                f'&DraftYear=&DribbleRange=&GameScope=&GameSegment=&GeneralRange='
                f'&Height=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0'
                f'&Outcome=&PORound=0&PerMode=Totals&Period=0&PlayerExperience='
                f'&PlayerPosition=&Season={season}'
                f'&SeasonSegment=&SeasonType={stype}&ShotClockRange='
                f'&ShotDistRange=&StarterBench=&TeamID=0&TouchTimeRange='
                f'&VsConference=&VsDivision=&Weight='
            )
            df10 = pull_data(url10)

            term = 'wide_open_'
            df10.rename(columns={col: term + col for col in shotcolumns},inplace=True)

            shot=shots[0]
            url18 = (
                f'https://stats.nba.com/stats/leaguedashoppptshot?CloseDefDistRange={shot}'
                f'&College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick='
                f'&DraftYear=&DribbleRange=&GameScope=&GameSegment=&GeneralRange='
                f'&Height=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0'
                f'&Outcome=&PORound=0&PerMode=Totals&Period=0&PlayerExperience='
                f'&PlayerPosition=&Season={season}'
                f'&SeasonSegment=&SeasonType={stype}&ShotClockRange='
                f'&ShotDistRange=&StarterBench=&TeamID=0&TouchTimeRange='
                f'&VsConference=&VsDivision=&Weight='
            )
            df18 = pull_data(url18)


    
            term = 'opp_very_tight_'
            df18.rename(columns={col: term + col for col in shotcolumns}, inplace=True)
            
            shot=shots[1]
            url19 = (
                f'https://stats.nba.com/stats/leaguedashoppptshot?CloseDefDistRange={shot}'
                f'&College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick='
                f'&DraftYear=&DribbleRange=&GameScope=&GameSegment=&GeneralRange='
                f'&Height=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0'
                f'&Outcome=&PORound=0&PerMode=Totals&Period=0&PlayerExperience='
                f'&PlayerPosition=&Season={season}'
                f'&SeasonSegment=&SeasonType={stype}&ShotClockRange='
                f'&ShotDistRange=&StarterBench=&TeamID=0&TouchTimeRange='
                f'&VsConference=&VsDivision=&Weight='
            )
            df19 = pull_data(url19)

            term = 'opp_tight_'
            df19.rename(columns={col: term + col for col in shotcolumns},inplace=True)

            #print(df8)
            #print('frame8')
            shot=shots[2]
            url20 = (
                f'https://stats.nba.com/stats/leaguedashoppptshot?CloseDefDistRange={shot}'
                f'&College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick='
                f'&DraftYear=&DribbleRange=&GameScope=&GameSegment=&GeneralRange='
                f'&Height=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0'
                f'&Outcome=&PORound=0&PerMode=Totals&Period=0&PlayerExperience='
                f'&PlayerPosition=&Season={season}'
                f'&SeasonSegment=&SeasonType={stype}&ShotClockRange='
                f'&ShotDistRange=&StarterBench=&TeamID=0&TouchTimeRange='
                f'&VsConference=&VsDivision=&Weight='
            )
            df20 = pull_data(url20)

            term = 'opp_open_'
            df20.rename(columns={col: term + col for col in shotcolumns},inplace=True)

            #print(df9)
            #print('frame9')
            shot=shots[3]
            url21 = (
                f'https://stats.nba.com/stats/leaguedashoppptshot?CloseDefDistRange={shot}'
                f'&College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick='
                f'&DraftYear=&DribbleRange=&GameScope=&GameSegment=&GeneralRange='
                f'&Height=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0'
                f'&Outcome=&PORound=0&PerMode=Totals&Period=0&PlayerExperience='
                f'&PlayerPosition=&Season={season}'
                f'&SeasonSegment=&SeasonType={stype}&ShotClockRange='
                f'&ShotDistRange=&StarterBench=&TeamID=0&TouchTimeRange='
                f'&VsConference=&VsDivision=&Weight='
            )
            df21 = pull_data(url21)

            term = 'opp_wide_open_'
            df21.rename(columns={col: term + col for col in shotcolumns},inplace=True)



            #print(df10)
            #print('frame10')
            
            url11 = 'https://stats.nba.com/stats/leaguedashptstats?College=&Conference=&Country=&DateFrom=' + date + '&DateTo=' + date + '&Division=&DraftPick=&DraftYear=&GameScope=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=&PerMode=Totals&PlayerExperience=&PlayerOrTeam=Team&PlayerPosition=&PtMeasureType=PullUpShot&Season=' + season + '&SeasonSegment=&SeasonType='+stype+'&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='
            df11 = pull_data(url11) 
            shotcolumns2=shotcolumns+['EFG%']
            term='pullup_'
            df11.rename(columns={col: term + col for col in shotcolumns2},inplace=True)

            #print(df11)
            #print('frame11')
            
            url12 = 'https://stats.nba.com/stats/leaguedashptstats?College=&Conference=&Country=&DateFrom=' + date + '&DateTo=' + date + '&Division=&DraftPick=&DraftYear=&GameScope=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=&PerMode=Totals&PlayerExperience=&PlayerOrTeam=Team&PlayerPosition=&PtMeasureType=Efficiency&Season=' + season + '&SeasonSegment=&SeasonType='+stype+'&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='


            df12 = pull_data(url12) 


            #print(df12)
            #print('frame12')
            
            url13=f"https://stats.nba.com/stats/leaguedashteamshotlocations?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&DistanceRange=By%20Zone&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={season}&SeasonSegment=&SeasonType={stype}&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight="
            
            df13=pull_data(url13)

            zone_columns=[ 'TEAM_ID', 'TEAM_ABBREVIATION',
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


            #print(df13)
            #print('frame13')
            
            url14=f"https://stats.nba.com/stats/leaguedashptteamdefend?College=&Conference=&Country=&DateFrom{date}=&DateTo={date}&DefenseCategory=Less%20Than%206Ft&Division=&DraftPick=&DraftYear=&GameSegment=&Height=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight="
            df14=pull_data(url14)
         
            #print('frame14')
            
            url15=f"https://stats.nba.com/stats/leaguedashteamshotlocations?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&DistanceRange=5ft%20Range&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={season}&SeasonSegment=&SeasonType={stype}&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight="
            df15=pull_data(url15)
            #print(df15.columns)
            df15.columns=['TEAM_ID', 'TEAM_NAME', 
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
            #print(df15)
            #print('frame15')
            url16 = f'https://stats.nba.com/stats/leaguedashptstats?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&GameScope=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=&PerMode=Totals&PlayerExperience=&PlayerOrTeam={unit}&PlayerPosition=&PtMeasureType=CatchShoot&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='
            df16=pull_data(url16)


            #print(df16)
            #print('frame16')
            url17 = f'https://stats.nba.com/stats/leaguedashteamstats?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&MeasureType=Advanced&Month=0&OpponentTeamID=0&Outcome=&PORound=&PaceAdjust=N&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={season}&SeasonSegment=&SeasonType={stype}&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='
            df17 = pull_data(url17)
            df17=df17[['TEAM_ID','POSS']]
            df17.columns=['TEAM_ID','team_poss']



            
            


            url22 = f'https://stats.nba.com/stats/leaguedashteamstats?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&MeasureType=Misc&Month=0&OpponentTeamID=0&Outcome=&PORound=&PaceAdjust=N&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={season}&SeasonSegment=&SeasonType={stype}&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='
            df22=pull_data(url22)
            poss_map=dict(zip(df17['TEAM_ID'],df17['team_poss']  ))
            df['team_poss']=df['TEAM_ID'].map(poss_map)
            df['team_poss']=df['TEAM_ID'].map(poss_map)
           
            frames = [df2, df3, df4, df5, df6, df7, df8, df9, df10,df11,df12,df13,df14,df15,df16,df18,df19,df20,df21,df22]
            i=1
            for frame in frames:
                
                joined_columns = set(frame.columns) - set(df.columns)
                joined_columns = list(joined_columns)
                print(joined_columns)
                joined_columns.append('TEAM_ID')
                frame = frame[joined_columns]

                df = df.merge(frame, on='TEAM_ID',how='left').reset_index(drop=True)
                i+=1
                if i ==17:
                    i+=1
                print(i)

            df['year'] = year
            print(year)
            

            year_frames.append(df)

        

            df['playoffs']=ps
            #print(df)
            df.to_csv(str(year)+trail+'_games.csv',index=False)
        except Exception as e:
            #print(str(e))
            
            #print(str(date_num))
            time.sleep(1)
            sys.exit()
            
    

    yeardata=pd.concat(year_frames)

    

    return yeardata


start_year=2025
end_year=2026

ps=False

df= pull_game_avg(start_year,end_year,unit='Team',ps=ps)
#df= pull_game_avg(start_year,end_year,ps=True,unit='Team')

#data=pull_game_level(dates)
df


# In[2]:


import requests
import pandas as pd
import time

# Define the base URL for the SWAR API
def scrape_teams(ps=False):
    base_url = "https://api.pbpstats.com/get-totals/nba"
    stype="Regular Season"
    carry=""
    if ps == True:
        stype="Playoffs"
        carry="ps"
    
    # Set up parameters
    params = {
        "SeasonType": stype,
        "Season": "",  # This will be dynamically filled for each season
        "Type": "Team",
    }
    
    # Define the range of seasons you want data for
    start_season = 2024  # Example start season
    end_season = 2024  # Example end season
    if ps == True:
        end_season=2023
    # Empty list to collect all data
    all_data = []
    
    # Loop through each season to fetch possessions data
    for season in range(start_season, end_season + 1):
        params["Season"] = f"{season}-{str(season + 1)[-2:]}"
        
        
        # Make a request to the API
        response = requests.get(base_url, params=params)
        data = response.json()
        
        # Process each team in the season
        df=pd.DataFrame(data['multi_row_table_data'])
        df['season']=season
        df['year']=season+1
        year =season+1
        df.to_csv(str(year)+carry+'.csv',index=False)
        print(season+1)
        time.sleep(2.5)
        all_data.append(df)
    
    # Convert collected data into a DataFrame
    df = pd.concat(all_data)

    for team_id in df['TeamId'].unique().tolist():
        teamdf=df[df['TeamId']==team_id]

        olddf=pd.read_csv(team_id+carry+'.csv')

        new_years = teamdf['year'].unique().tolist()
        olddf=olddf[~olddf.year.isin(new_years)]
        teamdf=pd.concat([olddf,teamdf])
        teamdf.drop_duplicates(inplace=False)

        teamdf.to_csv(team_id+carry+".csv",index=False)
    return df


def scrape_teams_vs(ps=False):
    base_url = "https://api.pbpstats.com/get-totals/nba"
    stype="Regular Season"
    carry=""
    if ps == True:
        stype="Playoffs"
        carry="ps"
    
    # Set up parameters
    params = {
        "SeasonType": stype,
        "Season": "",  # This will be dynamically filled for each season
        "Type": "Opponent",
    }
    
    # Define the range of seasons you want data for
    start_season = 2024  # Example start season
    
    end_season = 2024  # Example end season
    if ps == True:
        end_season=2023
    
    # Empty list to collect all data
    all_data = []
    
    # Loop through each season to fetch possessions data
    for season in range(start_season, end_season + 1):
        params["Season"] = f"{season}-{str(season + 1)[-2:]}"
        
        
        # Make a request to the API
        response = requests.get(base_url, params=params)
        data = response.json()
        
        # Process each team in the season
        df=pd.DataFrame(data['multi_row_table_data'])
        df['season']=season
        df['year']=season+1
        year =season+1
        print(season+1)
        time.sleep(2.5)
        df.to_csv(str(year)+'vs'+carry+'.csv',index=False)
        all_data.append(df)
    
    # Convert collected data into a DataFrame
    df = pd.concat(all_data)
    for team_id in df['TeamId'].unique().tolist():
        teamdf=df[df['TeamId']==team_id]
    
        olddf=pd.read_csv(team_id+carry+'.csv')

        new_years = teamdf['year'].unique().tolist()
        olddf=olddf[~olddf.year.isin(new_years)]
        teamdf=pd.concat([olddf,teamdf])
        teamdf.drop_duplicates(inplace=False)
    
        teamdf.to_csv(team_id+'vs'+carry+".csv",index=False)
    return df


# Display a sample of the data
scrape_teams(ps=False)
scrape_teams_vs(ps=False)


# In[3]:


trail = ''

ortg = []
orebperc = []
total_points = []
total_fg_miss = []
total_oreb = []
total_off_poss = []

total_opp_dreb = []

total_ftoreb=[]
total_fgoreb=[]

total_opp_fgdreb=[]
total_opp_ftdreb=[]
# Loop through each year for regular season
for year in range(2001, 2026):
    # Read the CSV file for the year
    df = pd.read_csv(f"{year}{trail}.csv")
    opp = pd.read_csv(f"{year}vs{trail}.csv")
    opp_def_rebounds=opp['DefRebounds'].sum()
    opp_def_rebounds=opp['DefRebounds'].sum()

    opp_def_rebounds_fg=opp['DefThreePtRebounds'].sum() + opp['DefTwoPtRebounds'].sum() 
    opp_def_ftrebounds=opp['FTDefRebounds'].sum()

    total_opp_fgdreb.append(opp_def_rebounds_fg)
   
    total_opp_dreb.append(opp_def_rebounds)
    total_opp_ftdreb.append(opp_def_ftrebounds)
    # Create and calculate required columns
    
    df['OREB'] = df['OffRebounds']
    df['FGA'] = df['FG2A'] + df['FG3A']
    df['FGM'] = df['FG2M'] + df['FG3M']
    df['FG2_miss'] = df['FG2A'] - df['FG2M']
    df['FG3_miss'] = df['FG3A'] - df['FG3M']
    df['FG_miss'] = df['FG3_miss'] + df['FG2_miss']
    df['fg_OffRebounds'] = df['OffThreePtRebounds'] + df['OffTwoPtRebounds']

    
    
    # Append aggregated metrics for the year
    ortg.append(df['Points'].sum() / df['OffPoss'].sum())
    orebperc.append(df['OREB'].sum() / (opp_def_rebounds+df['OREB'].sum()) )
    total_points.append(df['Points'].sum())
    total_fg_miss.append(df['FG_miss'].sum())
    total_oreb.append(df['OREB'].sum())
    total_off_poss.append(df['OffPoss'].sum())
    total_fgoreb.append(df['fg_OffRebounds'].sum())
    total_ftoreb.append(df['FTOffRebounds'].sum())

# Create a DataFrame with the results for regular season
years = list(range(2001, 2026))

seasons=[str(year-1)+'-'+str(year)[-2:] for year in years]
testdf = pd.DataFrame({
    'year': years,
     'season':seasons,
    'ortg': ortg,
    'oreb%': orebperc,
    'total_points': total_points,
    'total_fg_miss': total_fg_miss,
    'total_oreb': total_oreb,
      'total_opp_dreb':total_opp_dreb,
    'total_off_poss': total_off_poss,
    'total_opp_ft_dreb':total_opp_ftdreb,
    'total_ftoreb': total_ftoreb,
    'total_fgoreb':total_fgoreb,

    'total_opp_fgdreb':total_opp_fgdreb
  
})

# Save the DataFrame to CSV
testdf.to_csv('team_averages.csv', index=False)

# Playoff section
trail = 'ps'

# Reset lists for playoff metrics
ortg = []
orebperc = []
total_points = []
total_fg_miss = []
total_oreb = []
total_off_poss = []

total_opp_dreb = []

total_ftoreb=[]
total_fgoreb=[]

total_opp_fgdreb=[]
total_opp_ftdreb=[]

# Loop through each year for playoffs
for year in range(2001, 2025):
    # Read the CSV file for the year
    # Read the CSV file for the year
    df = pd.read_csv(f"{year}{trail}.csv")
    opp = pd.read_csv(f"{year}vs{trail}.csv")
    opp_def_rebounds=opp['DefRebounds'].sum()
    opp_def_rebounds=opp['DefRebounds'].sum()

    opp_def_rebounds_fg=opp['DefThreePtRebounds'].sum() + opp['DefTwoPtRebounds'].sum() 
    opp_def_ftrebounds=opp['FTDefRebounds'].sum()

    total_opp_fgdreb.append(opp_def_rebounds_fg)
   
    total_opp_dreb.append(opp_def_rebounds)
    total_opp_ftdreb.append(opp_def_ftrebounds)
    # Create and calculate required columns
    # Create and calculate required columns
    
    df['OREB'] = df['OffRebounds']
    df['FGA'] = df['FG2A'] + df['FG3A']
    df['FGM'] = df['FG2M'] + df['FG3M']
    df['FG2_miss'] = df['FG2A'] - df['FG2M']
    df['FG3_miss'] = df['FG3A'] - df['FG3M']
    df['FG_miss'] = df['FG3_miss'] + df['FG2_miss']
    df['fg_OffRebounds'] = df['OffThreePtRebounds'] + df['OffTwoPtRebounds'] 

    
    
    # Append aggregated metrics for the year
    ortg.append(df['Points'].sum() / df['OffPoss'].sum())
    orebperc.append(df['OREB'].sum() / (opp_def_rebounds+df['OREB'].sum()) )
    total_points.append(df['Points'].sum())
    total_fg_miss.append(df['FG_miss'].sum())
    total_oreb.append(df['OREB'].sum())
    total_off_poss.append(df['OffPoss'].sum())
    total_fgoreb.append(df['fg_OffRebounds'].sum())
    total_ftoreb.append(df['FTOffRebounds'].sum())
# Create a DataFrame with the results for playoffs

years = list(range(2001, 2025))
seasons=[str(year-1)+'-'+str(year)[-2:] for year in years]
testdf = pd.DataFrame({
    'year': years,
     'season':seasons,
    'ortg': ortg,
    'oreb%': orebperc,
    'total_points': total_points,
    'total_fg_miss': total_fg_miss,
    'total_oreb': total_oreb,
      'total_opp_dreb':total_opp_dreb,
    'total_off_poss': total_off_poss,
    'total_opp_ft_dreb':total_opp_ftdreb,
    'total_ftoreb': total_ftoreb,
    'total_fgoreb':total_fgoreb,

    'total_opp_fgdreb':total_opp_fgdreb
  
})

# Save the DataFrame to CSV
testdf.to_csv('team_averages_ps.csv', index=False)



# In[4]:


for year in range(2001,2026):    
    trail='ps'
    trail=''
    ps = True if trail =='ps' else False
    pbp=pd.read_csv(str(year)+trail+('.csv'))
    pbpvs=pd.read_csv(str(year)+'vs'+trail+('.csv'))
 

    pbp=four_factors_data(pbp,pbpvs,year,ps=ps)

    pbpvs=four_factors_data(pbpvs,pbp,year,ps=ps)
    columns = ['ft_factor', 'oreb_factor', 'turnover_factor', '2shooting_factor', '3shooting_factor']
    oppcolumns ={}
    for c in columns:
        oppcolumns[c]='opp_'+c
    columns.append('TeamId')
   

    vsframe=pbpvs[columns].reset_index(drop=True)
    vsframe.rename(columns=oppcolumns,inplace=True)


    pbp=pbp.merge(vsframe,on='TeamId')
    pbp['OffMinutes'] = (pbp['SecondsPerPossOff'] * pbp['OffPoss']) / 60
    pbp['DefMinutes'] = (pbp['SecondsPerPossDef'] * pbp['DefPoss']) / 60
    pbp['OPace'] = 48 * ((pbp['OffPoss']) / (2 * (pbp['OffMinutes'])))
    pbp['DPace'] = 48 * ((pbp['DefPoss']) / (2 * (pbp['DefMinutes'])))

    pbp['o_rating']=100* pbp['Points']/pbp['OffPoss']
    pbp['d_rating']=100* pbp['OpponentPoints']/pbp['DefPoss']
    pbp['3p_rate']=100* pbp['FG3A']/pbp['FGA']

    pbp['TEAM_ID']=pbp['TeamId']

    if year>=2014:

    
        nba = pd.read_csv(str(year)+trail+'_team_games.csv')


    
    
    
        keepcol = [col for col in nba.columns if col not in pbp.columns]
        nokeep = [col for col in nba.columns if col in pbp.columns]
  
        
        keepcol.append('TEAM_ID')
        nba=nba[keepcol]
       

        total=pbp.merge(nba,on='TEAM_ID')

    else:
        total=pbp.reset_index()
    total.rename({'3SecondViolations':'ThreeSecondViolations'},inplace=True)
    total.to_csv(str(year)+trail+'_team_totals.csv',index=False)
    


# In[5]:


trail=''
frames=[]
for year in range(2001,2026):
    df= pd.read_csv(str(year)+trail+'_team_totals.csv')
    df['year']=year

    frames.append(df)
master=pd.concat(frames)
master.to_csv('all_teamyears.csv',index=False)


trail='ps'
frames=[]
for year in range(2001,2025):
    df= pd.read_csv(str(year)+trail+'_team_totals.csv')
    df['year']=year

    frames.append(df)
master=pd.concat(frames)
master.to_csv('all_teamyears_ps.csv',index=False)


# In[ ]:





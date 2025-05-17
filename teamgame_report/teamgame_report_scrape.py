#!/usr/bin/env python
# coding: utf-8

# In[11]:


from nba_api.stats.static import players,teams
import pandas as pd
import requests
import sys
import os
import time
from datetime import datetime
from fourfactors import four_factors_data
import numpy as np
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

    time.sleep(.1)
    return df


def pull_game_level_team(dateframe, start_year,end_year,ps=False):
    stype = 'Regular%20Season'
    trail=''
    if ps == True:
        stype='Playoffs'
        trail='ps'
    dframes = []
    shotcolumns = ['FGA_FREQUENCY', 'FGM', 'FGA', 'FG_PCT', 'EFG_PCT', 'FG2A_FREQUENCY', 'FG2M', 'FG2A', 'FG2_PCT', 
                   'FG3A_FREQUENCY', 'FG3M', 'FG3A', 'FG3_PCT']
    
    unit='Team'
    for year in range(start_year, end_year):
        count=0
        countframe=dateframe[dateframe.year==year].reset_index()
        year_frame=[]
        test = False
        game_date = 20250125
        if test != False:
            countframe=countframe[countframe.GAME_DATE<game_date]

        year_dates = countframe['GAME_DATE'].unique().tolist()
        if os.path.exists('year_files/'+str(year)+trail+'_teamgames.csv'):
            print('file exists')
            df= pd.read_csv('year_files/'+str(year)+trail+'_teamgames.csv')
            df['date']=df['date'].astype(int)
            df.sort_values(by='date',ascending=False)
            df.drop_duplicates(subset=['date','TEAM_ID'],inplace=True)
            print(len(df))
            if test != False:
                df=df[df.date<game_date]
            
            year_frame.append(df)

            year_dates=[int(date) for date in year_dates if date not in df['date'].unique().tolist()]
            year_dates=year_dates[::-1]
            print(len(year_frame))
            

        season = str(year - 1) + '-' + str(year)[-2:]
        year_dates.sort()
        print(year_dates)
        for date in year_dates:
            try:
                date_num = int(date)
                date = format_date_to_url(date)
    
    
                url = f'https://stats.nba.com/stats/leaguedashteamstats?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=&PaceAdjust=N&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={season}&SeasonSegment=&SeasonType={stype}&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='
            
                df = pull_data(url)
    
                url2 = f'https://stats.nba.com/stats/leaguedashteamstats?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&MeasureType=Advanced&Month=0&OpponentTeamID=0&Outcome=&PORound=&PaceAdjust=N&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={season}&SeasonSegment=&SeasonType={stype}&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='
                df2 = pull_data(url2)
    
                url3 = f'https://stats.nba.com/stats/leaguedashptstats?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&GameScope=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=&PerMode=Totals&PlayerExperience=&PlayerOrTeam={unit}&PlayerPosition=&PtMeasureType=Passing&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='
                df3 = pull_data(url3)
    
                url4 = f'https://stats.nba.com/stats/leaguedashptstats?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&GameScope=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=&PerMode=Totals&PlayerExperience=&PlayerOrTeam={unit}&PlayerPosition=&PtMeasureType=Drives&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='
                df4 = pull_data(url4)
    
                url5 = f'https://stats.nba.com/stats/leaguedashptstats?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&GameScope=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=&PerMode=Totals&PlayerExperience=&PlayerOrTeam={unit}&PlayerPosition=&PtMeasureType=Possessions&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='
                df5 = pull_data(url5)
    
                url6 = f'https://stats.nba.com/stats/leaguedashptstats?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&GameScope=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=&PerMode=Totals&PlayerExperience=&PlayerOrTeam={unit}&PlayerPosition=&PtMeasureType=Rebounding&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='
                df6 = pull_data(url6)
    
                url7 = f'https://stats.nba.com/stats/leaguedashteamptshot?CloseDefDistRange=0-2%20Feet%20-%20Very%20Tight&College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&DribbleRange=&GameScope=&GameSegment=&GeneralRange=&Height=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&Season={season}&SeasonSegment=&SeasonType={stype}&ShotClockRange=&ShotDistRange=&StarterBench=&TeamID=0&TouchTimeRange=&VsConference=&VsDivision=&Weight='
                df7 = pull_data(url7)
    
                term = 'very_tight_'
                df7.rename(columns={col: term + col for col in shotcolumns}, inplace=True)
                
                url8 = 'https://stats.nba.com/stats/leaguedashteamptshot?CloseDefDistRange=2-4%20Feet%20-%20Tight&College=&Conference=&Country=&DateFrom=' + date + '&DateTo=' + date + '&Division=&DraftPick=&DraftYear=&DribbleRange=&GameScope=&GameSegment=&GeneralRange=&Height=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&Season=' + season + '&SeasonSegment=&SeasonType='+stype+'&ShotClockRange=&ShotDistRange=&StarterBench=&TeamID=0&TouchTimeRange=&VsConference=&VsDivision=&Weight='
                df8 = pull_data(url8)
                term = 'tight_'
                df8.rename(columns={col: term + col for col in shotcolumns},inplace=True)
    
                url9 = 'https://stats.nba.com/stats/leaguedashteamptshot?CloseDefDistRange=4-6%20Feet%20-%20Open&College=&Conference=&Country=&DateFrom=' + date + '&DateTo=' + date + '&Division=&DraftPick=&DraftYear=&DribbleRange=&GameScope=&GameSegment=&GeneralRange=&Height=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&Season=' + season + '&SeasonSegment=&SeasonType='+stype+'&ShotClockRange=&ShotDistRange=&StarterBench=&TeamID=0&TouchTimeRange=&VsConference=&VsDivision=&Weight='
                df9 = pull_data(url9)
                term = 'open_'
                df9.rename(columns={col: term + col for col in shotcolumns},inplace=True)
    
                url10 = 'https://stats.nba.com/stats/leaguedashteamptshot?CloseDefDistRange=6%2B%20Feet%20-%20Wide%20Open&College=&Conference=&Country=&DateFrom=' + date + '&DateTo=' + date + '&Division=&DraftPick=&DraftYear=&DribbleRange=&GameScope=&GameSegment=&GeneralRange=&Height=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&Season=' + season + '&SeasonSegment=&SeasonType='+stype+'&ShotClockRange=&ShotDistRange=&StarterBench=&TeamID=0&TouchTimeRange=&VsConference=&VsDivision=&Weight='
                df10 = pull_data(url10)
                term = 'wide_open_'
                df10.rename(columns={col: term + col for col in shotcolumns},inplace=True)
                url11 = 'https://stats.nba.com/stats/leaguedashptstats?College=&Conference=&Country=&DateFrom=' + date + '&DateTo=' + date + '&Division=&DraftPick=&DraftYear=&GameScope=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=&PerMode=Totals&PlayerExperience=&PlayerOrTeam='+unit+'&PlayerPosition=&PtMeasureType=PullUpShot&Season=' + season + '&SeasonSegment=&SeasonType='+stype+'&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='
                df11 = pull_data(url11) 
                shotcolumns2=shotcolumns+['EFG%']
                term='pullup_'
                df11.rename(columns={col: term + col for col in shotcolumns2},inplace=True)
    
                url12 = 'https://stats.nba.com/stats/leaguedashptstats?College=&Conference=&Country=&DateFrom=' + date + '&DateTo=' + date + '&Division=&DraftPick=&DraftYear=&GameScope=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=&PerMode=Totals&PlayerExperience=&PlayerOrTeam=Team&PlayerPosition=&PtMeasureType=Efficiency&Season=' + season + '&SeasonSegment=&SeasonType='+stype+'&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='


                df12 = pull_data(url12) 
                url13=f"https://stats.nba.com/stats/leaguedashteamshotlocations?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&DistanceRange=By%20Zone&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={season}&SeasonSegment=&SeasonType={stype}&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight="
                
                df13=pull_data(url13)
                #print(df13.columns)
    
                zone_columns=['TEAM_ID', 'TEAM_NAME',
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
                url14=f"https://stats.nba.com/stats/leaguedashptteamdefend?College=&Conference=&Country=&DateFrom{date}=&DateTo={date}&DefenseCategory=Less%20Than%206Ft&Division=&DraftPick=&DraftYear=&GameSegment=&Height=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight="
                df14=pull_data(url14)
                #print(df14.columns)
                df14.rename(columns={'CLOSE_DEF_PERSON_ID':'PLAYER_ID'},inplace=True)
    
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
                url16 = f'https://stats.nba.com/stats/leaguedashptstats?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&GameScope=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=&PerMode=Totals&PlayerExperience=&PlayerOrTeam={unit}&PlayerPosition=&PtMeasureType=CatchShoot&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='
                df16=pull_data(url16)
    
                
                url17 = f'https://stats.nba.com/stats/leaguedashteamstats?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&MeasureType=Advanced&Month=0&OpponentTeamID=0&Outcome=&PORound=&PaceAdjust=N&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={season}&SeasonSegment=&SeasonType=Playoffs&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='
                df17 = pull_data(url17)
                df17=df17[['TEAM_ID','POSS']]
                df17.columns=['TEAM_ID','team_poss']
    
                poss_map=dict(zip(df17['TEAM_ID'],df17['team_poss']  ))
    
                df['team_poss']=df['TEAM_ID'].map(poss_map)
                url_misc = f'https://stats.nba.com/stats/leaguedashteamstats?College=&Conference=&Country=&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&MeasureType=Misc&Month=0&OpponentTeamID=0&Outcome=&PORound=&PaceAdjust=N&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={season}&SeasonSegment=&SeasonType={stype}&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='
                
                df18 = pull_data(url_misc)
               
                frames = [df2, df3, df4, df5, df6, df7, df8, df9, df10,df11,df12,df13,df14,df15,df16,df18]
           
                framecount=1
                for frame in frames:
                    framecount+=1
                    
                    joined_columns = set(frame.columns) - set(df.columns)
                    joined_columns = list(joined_columns)
            

                    joined_columns.append('TEAM_ID')
                    frame = frame[joined_columns]
    
                    df = df.merge(frame, on='TEAM_ID',how='left').reset_index(drop=True)
                    
    
                df['year'] = year
                df['date']=date_num
      
                year_frame.append(df)
                count+=1
                print(date_num)
                if count %5==0:
            
                    yeardata=pd.concat(year_frame)
                    print(len(yeardata))
                    yeardata['playoffs']=ps
                    yeardata.to_csv('year_files/'+str(year)+trail+'_teamgames.csv',index=False)
            except Exception as e:
                print(str(e))
                print(str(date_num))
                time.sleep(1)
    

        yeardata=pd.concat(year_frame)
        print(len(yeardata))
        yeardata['playoffs']=ps
        yeardata.to_csv('year_files/'+str(year)+trail+'_teamgames.csv',index=False)
        dframes.append(yeardata)
        print(f"Year: {year}")

    total = pd.concat(dframes)
    return total

start_year=2025
end_year=2026



def get_dates(start_year,end_year,ps=False):
    trail='ps'
    if ps == False:
        trail=''
    dates=[]
    for year in range(start_year,end_year):
    
        for team in teams.get_teams():
            team_id=team['id']
            path = '../../shot_data/team/'+str(year)+trail+'/'+str(team_id)+'.csv'
            if os.path.exists(path):
             

                df=pd.read_csv(path)
    
                df=df[['TEAM_ID','GAME_DATE','GAME_ID']]
                df.sort_values(by='GAME_DATE',inplace=True)
                df.drop_duplicates(inplace=True)
                df['year']=year
                dates.append(df)
    return pd.concat(dates)
ps=True
dateframe=get_dates(start_year,end_year,ps=ps)

dates=dateframe['GAME_DATE'].unique().tolist()

dates.sort()
df= pull_game_level_team(dateframe,start_year,end_year,ps=ps)
#data=pull_game_level(dates)
inverted_team_dict = {
    '1610612760': 'OKC',
    '1610612749': 'MIL',
    '1610612758': 'SAC',
    '1610612747': 'LAL',
    '1610612738': 'BOS',
    '1610612743': 'DEN',
    '1610612750': 'MIN',
    '1610612752': 'NYK',
    '1610612756': 'PHX',
    '1610612753': 'ORL',
    '1610612766': 'CHA',
    '1610612739': 'CLE',
    '1610612746': 'LAC',
    '1610612737': 'ATL',
    '1610612748': 'MIA',
    '1610612742': 'DAL',
    '1610612765': 'DET',
    '1610612763': 'MEM',
    '1610612761': 'TOR',
    '1610612741': 'CHI',
    '1610612754': 'IND',
    '1610612759': 'SAS',
    '1610612745': 'HOU',
    '1610612751': 'BKN',
    '1610612764': 'WAS',
    '1610612744': 'GSW',
    '1610612755': 'PHI',
    '1610612762': 'UTA',
    '1610612757': 'POR',
    '1610612740': 'NOP'
}


# In[12]:


for year in range(2026,2026):    
    trail='ps'
    trail=''
    ps = True if trail =='ps' else False
    pbp=pd.read_csv(str(year)+trail+('.csv'))
    pbpvs=pd.read_csv(str(year)+'vs'+trail+('.csv'))
 

    pbp=four_factors_data(pbp,pbpvs,year,ps=ps)

    pbpvs=four_factors_data(pbpvs,pbp,year,ps=ps)
    columns = ['ft_factor', 'oreb_factor', 'turnover_factor', '2shooting_factor', '3shooting_factor','rimfactor','nonrim2factor']

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


# In[13]:


import pandas as pd
import sys
all_pbp = []
all_pbp_vs = []
all_nba = []
ps=True
trail='ps' if ps else ''
start_year=2014
end_year=2026

for year in range(start_year, end_year):
    # Read CSV files for each year
    pbp = pd.read_csv(f"../game_report/team/{year}all_logs.csv")
    pbp_vs = pd.read_csv(f"../game_report/team/{year}vs_all_logs.csv")
    if ps ==False:
        pbp=pbp[pbp.SeasonType!='Playoffs']

    
        pbp_vs=pbp_vs[pbp_vs.SeasonType!='Playoffs']
        
    else:
        pbp=pbp[pbp.SeasonType=='Playoffs']

        print(pbp.SeasonType.unique())
        
        pbp_vs=pbp_vs[pbp_vs.SeasonType=='Playoffs']

        pbp.dropna(subset='GameId',inplace=True)
        pbp_vs.dropna(subset='GameId',inplace=True)
        
        pbp.dropna(subset='TeamId',inplace=True)
        pbp_vs.dropna(subset='TeamId',inplace=True)
        # For pbp DataFrame
        non_infinite_pbp = pbp.select_dtypes(include=['float64', 'int64']).apply(lambda x: x[~np.isinf(x)])

        # For pbp_vs DataFrame
        non_infinite_pbp_vs = pbp_vs.select_dtypes(include=['float64', 'int64']).apply(lambda x: x[~np.isinf(x)])

        # Print the results




    nba = pd.read_csv(f"year_files/{year}{trail}_teamgames.csv")
    nba['TEAM_ABBREVIATION'] = nba.apply(lambda row: inverted_team_dict.get(str(row['TEAM_ID'])) if pd.isnull(row['TEAM_ABBREVIATION']) else row['TEAM_ABBREVIATION'], axis=1)

    missing=nba[nba.TEAM_ABBREVIATION.isna()]
    missing.to_csv(f"{year}_missing.csv",index=False)
 
    pbp.dropna(subset='GameId',inplace=True)
    pbp_vs.dropna(subset='GameId',inplace=True)

    pbp['date'] = pbp['Date'].str.replace('-', '', regex=False)
    pbp_vs['date'] = pbp_vs['Date'].str.replace('-', '', regex=False)

    # Apply four_factors_data function

  
    pbp = four_factors_data(pbp, pbp_vs, year, ps=ps)
    pbp_vs = four_factors_data(pbp_vs, pbp, year, ps=ps)

    


    columns = ['ft_factor', 'oreb_factor', 'morey_factor','turnover_factor', '2shooting_factor', '3shooting_factor','rimfactor','nonrim2factor', 'TeamId','date','GameId']
    oppcolumns = {c: 'opp_' + c for c in columns if c != 'TeamId' and c != 'date' and c != 'GameId'}
    columns.append('Points')
    
    vsframe = pbp_vs[columns].reset_index(drop=True)
    
    vsframe.rename(columns=oppcolumns, inplace=True)
    vsframe.rename(columns={'Points':'OpponentPoints'},inplace=True)
    
    # Merge pbp and vsframe
    pbp = pbp.merge(vsframe, on=['TeamId','date','GameId'])
    pbp.rename(columns={'TeamId': 'TEAM_ID'},inplace=True)
    # Calculate additional metrics
    pbp['OffMinutes'] = (pbp['SecondsPerPossOff'] * pbp['OffPoss']) / 60
    pbp['DefMinutes'] = (pbp['SecondsPerPossDef'] * pbp['DefPoss']) / 60
    pbp['OPace'] = 48 * ((pbp['OffPoss']) / (2 * (pbp['OffMinutes'])))
    pbp['DPace'] = 48 * ((pbp['DefPoss']) / (2 * (pbp['DefMinutes'])))
    pbp['o_rating'] = 100 * pbp['Points'] / pbp['OffPoss']
    pbp['d_rating'] = 100 * pbp['OpponentPoints'] / pbp['DefPoss']
    pbp['3p_rate'] = 100 * pbp['FG3A'] / pbp['FGA']

    
    # Format date
    
    nba['date'] = nba['date'].astype(str)
    



    to_keep=[col for col in pbp.columns if col not in nba.columns]

    id_col=['date','TEAM_ID']
    to_keep = to_keep +id_col
    pbp=pbp[to_keep]
    # Merge pbp and nba data
    merged_data = pd.merge(nba, pbp, on=['TEAM_ID', 'date'], how='left')
    merged_data['AtRimFG3A']=merged_data['AtRimFGA'] +merged_data['FG3A']
    merged_data['FGA']=merged_data['FG2A']+merged_data['FG3A']
    merged_data['FGM']=merged_data['FG2M']+merged_data['FG3M']
    merged_data['FG_missed']=merged_data['FGA']-merged_data['FGM']
    merged_data['GameId']=merged_data['GameId'].astype(int)
    merged_data['date']=merged_data['date'].astype(int)
    merged_data.sort_values(by=['date','GameId'],inplace=True)

    # Create a deep copy of merged_data
    copied_data = merged_data.copy(deep=True)
    copied_data.sort_values(by=['date','GameId'],inplace=True)



# Swap the column names
    copied_data.rename(columns={'Opponent': 'TEAM_ABBREVIATION', 'TEAM_ABBREVIATION': 'Opponent'},inplace=True)
    opp_dict = {
        "very_tight_FG3A_FREQUENCY": "opp_very_tight_FG3A_FREQUENCY",
        "very_tight_FG3A": "opp_very_tight_FG3A",
        "very_tight_FG3M": "opp_very_tight_FG3M",
        "very_tight_FG3_PCT": "opp_very_tight_FG3_PCT",
        "tight_FG3M": "opp_tight_FG3M",
        "tight_FG3A": "opp_tight_FG3A",
        "tight_FG3A_FREQUENCY": "opp_tight_FG3A_FREQUENCY",
        "tight_FG3_PCT": "opp_tight_FG3_PCT",
        "open_FG3A_FREQUENCY": "opp_open_FG3A_FREQUENCY",
        "open_FG3A": "opp_open_FG3A",
        "open_FG3_PCT": "opp_open_FG3_PCT",
        "open_FG3M": "opp_open_FG3M",
        "wide_open_FG3_PCT": "opp_wide_open_FG3_PCT",
        "wide_open_FG3M": "opp_wide_open_FG3M",
        "wide_open_FG3A": "opp_wide_open_FG3A",
        "wide_open_FG3A_FREQUENCY": "opp_wide_open_FG3A_FREQUENCY",
        "FG_missed":"opp_FG_missed",
        "OREB":"opp_OREB",
        "DREB":"opp_DREB"
    }
    copied_data.rename(columns=opp_dict,inplace=True)
    
    col_list = [
        'opp_very_tight_FG3A_FREQUENCY',
        'opp_very_tight_FG3A',
        'opp_very_tight_FG3M',
        'opp_very_tight_FG3_PCT',
        'opp_tight_FG3M',
        'opp_tight_FG3A',
        'opp_tight_FG3A_FREQUENCY',
        'opp_tight_FG3_PCT',
        'opp_open_FG3A_FREQUENCY',
        'opp_open_FG3A',
        'opp_open_FG3_PCT',
        'opp_open_FG3M',
        'opp_wide_open_FG3_PCT',
        'opp_wide_open_FG3M',
        'opp_wide_open_FG3A',
        'opp_wide_open_FG3A_FREQUENCY',
        'opp_FG_missed',
        'TEAM_ABBREVIATION',
        "opp_OREB",
        "opp_DREB",
    
        'GameId'
    ]

    copied_data=copied_data[col_list].reset_index()



    merged_data=merged_data.merge(copied_data,how='inner',on=['TEAM_ABBREVIATION','GameId'])
    merged_data["team_oreb_chances"]=merged_data["OREB"]+merged_data["opp_DREB"]
    merged_data["team_dreb_chances"]=merged_data["DREB"]+merged_data["opp_OREB"]

    ranked=[col for col in merged_data.columns if 'rank' in col.lower()]
    print(len(merged_data.columns))
    merged_data.drop(columns=ranked,inplace=True)
    print(len(merged_data.columns))
    merged_data.to_csv(f"year_files/all_games_{year}{trail}.csv",index=False)
    merged_data.to_parquet(f"year_files/all_games_{year}{trail}.parquet",index=False)
    # Create folder for the year if it doesn't exist
    year_folder = f"games/{year}"
    os.makedirs(year_folder, exist_ok=True)

    # Save each game's data to its own file
    for game_id in merged_data['GameId'].unique():
        game_df = merged_data[merged_data['GameId'] == game_id]
        game_df.to_csv(f"{year_folder}/{game_id}.csv", index=False)


  




# In[ ]:





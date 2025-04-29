#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import requests
import plotly.graph_objects as go
import math
from scipy import stats
import string
import numpy as np
import time
from scipy.stats import zscore
import sys
import os
import glob



start_time = time.time()
directory = "data/2025"
ps = True

# Use glob to find all CSV files in the directory
csv_files = glob.glob(os.path.join(directory, "*.csv"))

# Loop through the list of files and delete based on `ps`
for file in csv_files:
    filename = os.path.basename(file)
    if (ps and '_ps' in filename) or (not ps and '_ps' not in filename):
        try:
            os.remove(file)
            print(f"Deleted: {file}")
        except Exception as e:
            print(f"Error deleting {file}: {e}")

print("Relevant CSV files deleted.")
time.sleep(1)

time.sleep(1)


# In[ ]:


def lineuppull(team_id, season, opp=False, ps=False):
    term = "Opponent" if opp else "Team"
    s_type = "Playoffs" if ps else "Regular Season"
    
    wowy_url = "https://api.pbpstats.com/get-wowy-stats/nba"
    print(team_id)
    wowy_params = {
        "TeamId": team_id,
        "Season": season,
        "SeasonType": s_type,
        "Type": term
    }
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
    
    wowy_response = requests.get(wowy_url, params=wowy_params, headers=headers)
    wowy = wowy_response.json()
    combos = wowy["multi_row_table_data"]
    frame_length = len(combos)
    df = pd.DataFrame(combos, index=[0]*frame_length)
    return df

def get_filename(team_id, year, opp=False, ps=False):
    """Generate filename based on parameters"""
    filename = f"{team_id}"
    if opp:
        filename += "_vs"
    if ps:
        filename += "_ps"
    filename += ".csv"
    return filename

def pull_onoff(years, opp=False, ps=False):
    count = 0
    if ps == False:
        player_index = pd.read_csv('index_master.csv')
    else:
         player_index = pd.read_csv('index_master_ps.csv')
    player_index = player_index[player_index.team != 'TOT']
    player_index = player_index[player_index.year > 2000]
    player_index = player_index.drop_duplicates()
    player_index['team_id'] =player_index['team_id'].astype(int)
    all_frames = []
    
    for year in years:
        # Create year directory if it doesn't exist
        year_dir = f"data/{year}"
        os.makedirs(year_dir, exist_ok=True)
        
        season_index = player_index[player_index.year == year].reset_index(drop=True)
        season = f"{year-1}-{str(year)[-2:]}"
        
        frames = []
        fail_list = []

        season_index.dropna(subset='team_id',inplace=True)
        
        for team_id in season_index.team_id.unique():
            # Generate filename for this team/year combination
            filename = get_filename(int(team_id), year, opp, ps)
            filepath = os.path.join(year_dir, filename)
            
            # Check if file already exists
            if os.path.exists(filepath):
                print(f"File already exists for team {team_id} in {year}, skipping...")
                # Optionally read existing file and add to frames
                existing_df = pd.read_csv(filepath)
                frames.append(existing_df)
                continue
            
            try:
                df = lineuppull(team_id, season, opp=opp, ps=ps)
                df = df.reset_index(drop=True)
                df['team_id'] = team_id
                df['year'] = year
                df['season'] = season
                df['team_vs'] = opp
                
                # Save individual team file
                df.to_csv(filepath, index=False)
                time.sleep(4)
                print(f"Saved data for team {team_id} in {year}")
                
                frames.append(df)
                count += 1
                
            except Exception as e:
                print(f"Error processing team {team_id} in {year}: {str(e)}")
                fail_list.append((team_id, year))
        
        if frames:
            year_frame = pd.concat(frames)
            all_frames.append(year_frame)
            print(f'Year {year} Completed')
        
    if fail_list:
        print("\nFailed to process the following team/year combinations:")
        for team, year in fail_list:
            print(f"Team: {team}, Year: {year}")
    
    return pd.concat(all_frames) if all_frames else pd.DataFrame()
        
#pull_onoff(years,opp=True,ps=True) 
#pull_onoff(years,opp=False,ps=True) 
years=[i for i in range(2025,2026)]
df = pull_onoff(years,opp=False,ps=ps) 
df = pull_onoff(years,opp=True,ps=ps) 
time.sleep(5)

df = pull_onoff(years,opp=False,ps=ps) 
df = pull_onoff(years,opp=True,ps=ps) 
end_time = time.time()
elapsed_time = end_time - start_time
print(f"Time taken: {elapsed_time} seconds")
years=[i for i in range(2001,2025)]
#df = pull_onoff(years,opp=False,ps=True) 
#df = pull_onoff(years,opp=True,ps=True) 




def calculate_basketball_percentages(df):
    """
    Calculate basketball percentage statistics from raw totals.
    
    Args:
        df (pandas.DataFrame): DataFrame containing the raw totals columns
        
    Returns:
        pandas.DataFrame: DataFrame with added percentage columns
    """
    # Make a copy to avoid modifying the original
    result = df.copy()
    
    # Basic shooting percentages
    result['Fg3Pct'] = (result['FG3M'] / result['FG3A'] * 1).fillna(0)
    result['Fg2Pct'] = (result['FG2M'] / result['FG2A'] * 1).fillna(0)
    result['FGA'] = (result['FG2A'] + result['FG3A'])
    result['PenaltyFGA'] = (result['PenaltyFG2A'] + result['PenaltyFG3A'])
    result['SecondChanceFGA'] = (result['SecondChanceFG2A'] + result['SecondChanceFG3A'])

    result['FGA']= result['FG2A']+result['FG3A']
    result['FGM']= result['FG2M']+result['FG3M']
    result['NonHeaveFg3Pct'] = (result['FG3M'] / (result['FG3A']-result['HeaveAttempts']) * 1).fillna(0)
    
    # Advanced shooting percentages
    result['EfgPct'] = ((result['FG2M'] + 1.5 * result['FG3M']) / (result['FG2A'] + result['FG3A']) * 1).fillna(0)
    # Assuming you have a DataFrame named result with the necessary columns
    
    # Step 1: Extract relevant columns for easier calculations
    points = result['Points']
    fga = result['FGA']
    fta = result['FTA']
    and1_2pt = result['2pt And 1 Free Throw Trips']
    and1_3pt = result['3pt And 1 Free Throw Trips']
    
    # Step 2: Calculate the adjusted free throw weighting factor
    w = (and1_2pt + 1.5 * and1_3pt + 0.44 * (fta - and1_2pt - and1_3pt)) / fta
    
    # Step 3: Calculate True Shooting Percentage (TS%) and add to result DataFrame
    result['TsPct'] = points / (2 * (fga + w * fta))
    
    # The 'TS%' column in the result DataFrame now contains the calculated TS% values

    #result['TsPct'] = (result['Points'] / (2 * (result['FG2A'] + result['FG3A'] + 0.44 * result['FTA'])) * 1).fillna(0)
    
    # Second chance percentages
    result['SecondChanceFg3Pct'] = (result['SecondChanceFG3M'] / result['SecondChanceFG3A'] * 1).fillna(0)
    result['SecondChanceFg2Pct'] = (result['SecondChanceFG2M'] / result['SecondChanceFG2A'] * 1).fillna(0)
    result['SecondChanceEfgPct'] = ((result['SecondChanceFG2M'] + 1.5 * result['SecondChanceFG3M']) / 
                                   (result['SecondChanceFG2A'] + result['SecondChanceFG3A']) * 1).fillna(0)
    result['SecondChanceTsPct'] = (result['SecondChancePoints'] / 
                                  (2 * (result['SecondChanceFG2A'] + result['SecondChanceFG3A'])) * 1).fillna(0)

    
    result['SecondChancePointsPct'] = (result['SecondChancePoints'] / result['Points'] * 1).fillna(0)
    
    # Shot distribution
    result['FG3APct'] = (result['FG3A'] / (result['FG2A'] + result['FG3A']) * 1).fillna(0)
    
    result['FG2APctBlocked'] = (result['Fg2aBlocked'] / result['FG2A'] * 1).fillna(0)
    result['AtRimPctBlocked'] = (result['opp_BlockedAtRim'] / result['AtRimFGA'] * 1).fillna(0)
    result['LongMidRangePctBlocked'] = (result['opp_BlockedLongMidRange'] / result['LongMidRangeFGA'] * 1).fillna(0)
    result['ShortMidRangePctBlocked'] = (result['opp_BlockedShortMidRange'] / result['ShortMidRangeFGA'] * 1).fillna(0)
    result['FG3APctBlocked'] = (result['Fg3aBlocked'] / result['FG3A'] * 1).fillna(0)
    result['Corner3PctBlocked'] = (result['Blocked3s'] / result['Corner3FGA'] * 1).fillna(0)
    result['Arc3PctBlocked'] = (result['Blocked3s'] / result['Arc3FGA'] * 1).fillna(0)
    
    # Rebound percentages - Field Goals (corrected to be relative to missed shots)
    result['DefFGReboundPct'] = (result['DefRebounds'] / (result['opp_FG2A'] - result['opp_FG2M'] + result['opp_FG3A'] - result['opp_FG3M']) * 1).fillna(0)
    result['OffFGReboundPct'] = (result['OffRebounds'] / (result['FG2A'] - result['FG2M'] + result['FG3A'] - result['FG3M']) * 1).fillna(0)
    
    # Rebound percentages by shot location (corrected to be relative to missed shots of that type)
    result['OffLongMidRangeReboundPct'] = (result['OffTwoPtRebounds'] / (result['LongMidRangeFGA'] - result['LongMidRangeFGM']) * 1).fillna(0)
    result['DefLongMidRangeReboundPct'] = (result['DefTwoPtRebounds'] / (result['LongMidRangeFGA'] - result['LongMidRangeFGM']) * 1).fillna(0)
    result['DefArc3ReboundPct'] = (result['DefThreePtRebounds'] / (result['Arc3FGA'] - result['Arc3FGM']) * 1).fillna(0)
    result['OffArc3ReboundPct'] = (result['OffThreePtRebounds'] / (result['Arc3FGA'] - result['Arc3FGM']) * 1).fillna(0)
    result['DefAtRimReboundPct'] = (result['DefTwoPtRebounds'] / (result['AtRimFGA'] - result['AtRimFGM']) * 1).fillna(0)
    result['OffAtRimReboundPct'] = (result['OffTwoPtRebounds'] / (result['AtRimFGA'] - result['AtRimFGM']) * 1).fillna(0)
    result['DefShortMidRangeReboundPct'] = (result['DefTwoPtRebounds'] / (result['ShortMidRangeFGA'] - result['ShortMidRangeFGM']) * 1).fillna(0)
    result['OffShortMidRangeReboundPct'] = (result['OffTwoPtRebounds'] / (result['ShortMidRangeFGA'] - result['ShortMidRangeFGM']) * 1).fillna(0)
    result['DefCorner3ReboundPct'] = (result['DefThreePtRebounds'] / (result['Corner3FGA'] - result['Corner3FGM']) * 1).fillna(0)
    result['OffCorner3ReboundPct'] = (result['OffThreePtRebounds'] / (result['Corner3FGA'] - result['Corner3FGM']) * 1).fillna(0)
    
    # Free throw rebound percentages (corrected to be relative to FT misses)

    # Assist percentages
    result['Assisted2sPct'] = (result['PtsAssisted2s'] / (2 * result['FG2M']) * 1).fillna(0)
    result['Assisted3sPct'] = (result['PtsAssisted3s'] / (3 * result['FG3M']) * 1).fillna(0)
    result['NonPutbacksAssisted2sPct'] = (result['PtsAssisted2s'] / (2 * (result['FG2M'] - result['PtsPutbacks']/2)) * 1).fillna(0)
    result['Corner3PctAssisted'] = (result['Corner3Assists'] / result['Corner3FGM'] * 1).fillna(0)
    result['Arc3PctAssisted'] = (result['Arc3Assists'] / result['Arc3FGM'] * 1).fillna(0)
    result['SecondChanceCorner3PctAssisted'] = (result['Corner3Assists'] / result['SecondChanceCorner3FGM'] * 1).fillna(0)
    result['SecondChanceArc3PctAssisted'] = (result['Arc3Assists'] / result['SecondChanceArc3FGM'] * 1).fillna(0)
    result['SecondChanceAtRimPctAssisted'] = (result['AtRimAssists'] / result['SecondChanceAtRimFGM'] * 1).fillna(0)
    result['AtRimPctAssisted'] = (result['AtRimAssists'] / result['AtRimFGM'] * 1).fillna(0)
    result['ShortMidRangePctAssisted'] = (result['ShortMidRangeAssists'] / result['ShortMidRangeFGM'] * 1).fillna(0)
    result['LongMidRangePctAssisted'] = (result['LongMidRangeAssists'] / result['LongMidRangeFGM'] * 1).fillna(0)
    
    # Penalty percentages
    result['PenaltyPointsPct'] = (result['PenaltyPoints'] / result['Points'] * 1).fillna(0)
    result['PenaltyOffPossPct'] = (result['PenaltyOffPoss'] / result['OffPoss'] * 1).fillna(0)
    result['PenaltyFg2Pct'] = (result['PenaltyFG2M'] / result['PenaltyFG2A'] * 1).fillna(0)
    result['PenaltyFg3Pct'] = (result['PenaltyFG3M'] / result['PenaltyFG3A'] * 1).fillna(0)
    result['PenaltyEfgPct'] = ((result['PenaltyFG2M'] + 1.5 * result['PenaltyFG3M']) / 
                              (result['PenaltyFG2A'] + result['PenaltyFG3A']) * 1).fillna(0)
    result['PenaltyTsPct'] = (result['PenaltyPoints'] / 
                             (2 * (result['PenaltyFG2A'] + result['PenaltyFG3A'] + 0.44 * result['FTA'])) * 1).fillna(0)
    
    # Miscellaneous percentages
    result['BlocksRecoveredPct'] = (result['RecoveredBlocks'] / result['Blocks'] * 1).fillna(0)
    result['LiveBallTurnoverPct'] = (result['LiveBallTurnovers'] / result['Turnovers'] * 1).fillna(0)
    result['SelfORebPct'] = (result['SelfOReb'] /(result['FGA']- result['FGM']) * 1).fillna(0)
    
    # Fouls percentages
    total_shooting_fouls = result['TwoPtShootingFoulsDrawn'] + result['ThreePtShootingFoulsDrawn']
    result['ShootingFoulsDrawnPct'] = (result['ShootingFouls'] / (result['FG2A']+result['FG3A']) * 1).fillna(0)
    result['TwoPtShootingFoulsDrawnPct'] = ((result['TwoPtShootingFoulsDrawn'])/ (result['FG2A']+result['2pt And 1 Free Throw Trips'])* 1).fillna(0)
    result['ThreePtShootingFoulsDrawnPct'] = (result['ThreePtShootingFoulsDrawn'] / result['FG3A'] * 1).fillna(0)
    total_def_rebounds = result['DefTwoPtRebounds'] + result['DefThreePtRebounds']
    total_off_rebounds = result['OffTwoPtRebounds'] + result['OffThreePtRebounds']
    result['ThreePtShootingFoulsDrawnPct'] = result['ThreePtShootingFoulsDrawn'] / result['FG3A'] * 1
    
    result['DefTwoPtReboundPct'] = (result['DefTwoPtRebounds'] / total_def_rebounds * 1).fillna(0)
    result['DefThreePtReboundPct'] = (result['DefThreePtRebounds'] / total_def_rebounds * 1).fillna(0)
    result['OffTwoPtReboundPct'] = (result['OffTwoPtRebounds'] /(result['FG2A']-result['FG2M']) * 1).fillna(0)
    result['OffThreePtReboundPct'] = (result['OffThreePtRebounds'] / total_off_rebounds * 1).fillna(0)
    
    result['OffFTReboundPct']=(result['FTOffRebounds']/(result['opp_FTDefRebounds']+result['FTOffRebounds']))

    result['DefFTReboundPct']=(result['FTDefRebounds']/(result['opp_FTOffRebounds']+result['FTDefRebounds']))
    result['AtRimFrequency'] = result['AtRimFGA'] / result['FGA']
    result['ShortMidRangeFrequency'] = result['ShortMidRangeFGA'] / result['FGA']
    result['LongMidRangeFrequency'] = result['LongMidRangeFGA'] / result['FGA']
    result['Corner3Frequency'] = result['Corner3FGA'] / result['FGA']
    result['Arc3Frequency'] = result['Arc3FGA'] / result['FGA']
    result['SecondChanceArc3Frequency'] = result['SecondChanceArc3FGA'] / result['SecondChanceFGA']
    result['AtRimFG3AFrequency'] = (result['AtRimFGA'] + result['FG3A']) / result['FGA']
    result['SecondChanceAtRimFrequency'] = result['SecondChanceAtRimFGA'] / result['SecondChanceFGA']
    result['SecondChanceCorner3Frequency'] = result['SecondChanceCorner3FGA'] / result['SecondChanceFGA']
    result['PenaltyAtRimFrequency'] = result['PenaltyAtRimFGA'] / result['PenaltyFGA']
    result['PenaltyArc3Frequency'] = result['PenaltyArc3FGA'] / result['PenaltyFGA']
    result['PenaltyCorner3Frequency'] = result['PenaltyCorner3FGA'] / result['PenaltyFGA']
    # Calculating accuracy metrics
    result['AtRimAccuracy'] = result['AtRimFGM'] / result['AtRimFGA']
    result['UnblockedAtRimAccuracy'] = (result['AtRimFGM'] - result['Fg2aBlocked']) / result['AtRimFGA']
    
    result['ShortMidRangeAccuracy'] = result['ShortMidRangeFGM'] / result['ShortMidRangeFGA']
    result['UnblockedShortMidRangeAccuracy'] = (result['ShortMidRangeFGM'] - result['Fg2aBlocked']) / result['ShortMidRangeFGA']
    
    result['LongMidRangeAccuracy'] = result['LongMidRangeFGM'] / result['LongMidRangeFGA']
    result['UnblockedLongMidRangeAccuracy'] = (result['LongMidRangeFGM'] - result['Fg2aBlocked']) / result['LongMidRangeFGA']
    
    result['Corner3Accuracy'] = result['Corner3FGM'] / result['Corner3FGA']
    result['UnblockedCorner3Accuracy'] = (result['Corner3FGM'] - result['Fg3aBlocked']) / result['Corner3FGA']
    
    result['Arc3Accuracy'] = result['Arc3FGM'] / result['Arc3FGA']
    result['UnblockedArc3Accuracy'] = (result['Arc3FGM'] - result['Fg3aBlocked']) / result['Arc3FGA']
    
    # Second-chance accuracy metrics
    result['SecondChanceAtRimAccuracy'] = result['SecondChanceAtRimFGM'] / result['SecondChanceAtRimFGA']
    result['SecondChanceCorner3Accuracy'] = result['SecondChanceCorner3FGM'] / result['SecondChanceCorner3FGA']
    result['SecondChanceArc3Accuracy'] = result['SecondChanceArc3FGM'] / result['SecondChanceArc3FGA']
    
    # Penalty accuracy metrics
    result['PenaltyAtRimAccuracy'] = result['PenaltyAtRimFGM'] / result['PenaltyAtRimFGA']
    result['PenaltyCorner3Accuracy'] = result['PenaltyCorner3FGM'] / result['PenaltyCorner3FGA']
    result['PenaltyArc3Accuracy'] = result['PenaltyArc3FGM'] / result['PenaltyArc3FGA']
    
    # Non-heave accuracy metric
    result['NonHeaveArc3Accuracy'] = result['NonHeaveArc3FGM'] / result['NonHeaveArc3FGA']


    return result

def calculate_weighted_average(df, value_col, weight_col, group_by=None):
    """
    Calculate weighted average of a value column based on a weight column.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        Input dataframe
    value_col : str
        Name of the column containing the values to average
    weight_col : str
        Name of the column containing the weights
    group_by : str or list, optional
        Column(s) to group by before calculating weighted average
        
    Returns:
    --------
    If group_by is None: returns a float (weighted average)
    If group_by is specified: returns a Series with weighted averages per group
    
    Examples:
    --------
    # Single weighted average
    df = pd.DataFrame({
        'value': [10, 20, 30],
        'weight': [1, 2, 3]
    })
    result = calculate_weighted_average(df, 'value', 'weight')
    
    # Grouped weighted averages
    df = pd.DataFrame({
        'category': ['A', 'A', 'B', 'B'],
        'value': [10, 20, 30, 40],
        'weight': [1, 2, 3, 4]
    })
    result = calculate_weighted_average(df, 'value', 'weight', 'category')
    """
    
    # Input validation
    if value_col not in df.columns:
        raise ValueError(f"Value column '{value_col}' not found in dataframe")
    if weight_col not in df.columns:
        raise ValueError(f"Weight column '{weight_col}' not found in dataframe")
    
    # Handle negative weights
    if (df[weight_col] < 0).any():
        raise ValueError("Negative weights found. Please ensure all weights are non-negative")
    
    # Remove rows where either value or weight is null
    df = df.dropna(subset=[value_col, weight_col])
    
    # If all weights are zero, return nan
    if (df[weight_col] == 0).all():
        print('hit zero condition')
        return np.nan
    
    if group_by is None:
        # Calculate single weighted average
        weighted_sum = (df[value_col] * df[weight_col]).sum()
        weight_sum = df[weight_col].sum()
        return weighted_sum / weight_sum if weight_sum != 0 else np.nan
    else:
        # Calculate grouped weighted averages
        grouped = df.groupby(group_by)
        weighted_sums = grouped.apply(lambda x: (x[value_col] * x[weight_col]).sum())
        weight_sums = grouped[weight_col].sum()
        weight_sum = df[weight_col].sum()
        return weighted_sums / weight_sum


def player_rows(year,player_id,team_id,vs=False,on=True,ps=False):
    pstring = "_ps" if ps else ""
    if vs == False:
        df1 = pd.read_csv(f"data/{year}/{team_id}{pstring}.csv")
        df2 = pd.read_csv(f"data/{year}/{team_id}_vs{pstring}.csv")
    else:
        df2 = pd.read_csv(f"data/{year}/{team_id}{pstring}.csv")
        df1 = pd.read_csv(f"data/{year}/{team_id}_vs{pstring}.csv")
    notfound=set(df2.columns)-set(df1.columns)

    df2.drop(columns='team_vs',inplace=True)


    
    id_col=['EntityId']
    oppnames=[]
    for col in df2.columns:
        newcol = 'opp_'+col if col not in id_col else col
        oppnames.append(newcol)
    df2.columns= oppnames
    df=df1.merge(df2,on=id_col)

    for col in notfound:
        df[col]=0
    if on:
        df = df[df['EntityId'].apply(lambda x: player_id in x.split('-'))]
    else:
        df = df[~df['EntityId'].apply(lambda x: player_id in x.split('-'))]
        
    df.fillna(0,inplace=True)
    
    
    id_col=['EntityId',
     'TeamId',
     'Name',
     'ShortName',
     'RowId',
     'TeamAbbreviation',
    'team_id',
     'year',
     'season',
     'team_vs']
    df['FGA']= df['FG2A']+df['FG3A']
    df['FGM']= df['FG2M']+df['FG3M']
    df['opp_FGA']= df['opp_FG2A']+df['opp_FG3A']
    df['opp_FGM']= df['opp_FG2M']+df['opp_FG3M']


    missing=['3pt And 1 Free Throw Trips','opp_BlockedLongMidRange','opp_FTOffRebounds']
    for col in missing:
        if col not in df.columns:
            df[col]=0
    
    df.drop(columns=['opp_Name', 'opp_ShortName', 'opp_RowId', 'opp_TeamAbbreviation', 'opp_season'],inplace=True)
    df['two_point_misses']= df['FG2A'] - df['FG2M']
    df['opp_two_point_misses']= df['opp_FG2A'] - df['opp_FG2M']
    
    # Specific location misses
    df['at_rim_misses']= df['AtRimFGA'] - df['AtRimFGM']
    df['opp_at_rim_misses']= df['opp_AtRimFGA'] - df['opp_AtRimFGM']
    
    df['short_midrange_misses']= df['ShortMidRangeFGA'] - df['ShortMidRangeFGM']
    
    df['opp_short_midrange_misses']= df['opp_ShortMidRangeFGA'] - df['opp_ShortMidRangeFGM']
    
    df['long_midrange_misses']= df['LongMidRangeFGA'] - df['LongMidRangeFGM']
    df['opp_long_midrange_misses']= df['opp_LongMidRangeFGA'] - df['opp_LongMidRangeFGM']
    
    # Three point misses by location
    df['corner3_misses']= df['Corner3FGA'] - df['Corner3FGM']
    df['opp_corner3_misses']= df['opp_Corner3FGA'] - df['opp_Corner3FGM']
    
    df['arc3_misses']= df['Arc3FGA'] - df['Arc3FGM']
    df['opp_arc3_misses']= df['opp_Arc3FGA'] - df['opp_Arc3FGM']
    
    # Free throw misses
    df['ft_misses']= df['FTA'] - df['FtPoints']
    
    df['opp_ft_misses']= df['opp_FTA'] - df['opp_FtPoints']
    
    
    # Total misses
    df['fg_misses']= (df['FGA'] - df['FGM'])
    df['opp_fg_misses']= (df['opp_FGA'] - df['opp_FGM'])
    
    weight_mapping = {
        'DefTwoPtReboundPct': 'opp_two_point_misses',
        'OffTwoPtReboundPct': 'two_point_misses',
        'DefThreePtReboundPct':'opp_FG3A',
        'DefFGReboundPct': 'opp_fg_misses',
        'OffFGReboundPct': 'fg_misses',
        'OffLongMidRangeReboundPct': 'long_midrange_misses',
        'DefLongMidRangeReboundPct': 'opp_long_midrange_misses',
        'OffThreePtReboundPct': 'opp_FG3A',
        'OffArc3ReboundPct': 'arc3_misses',
        'DefArc3ReboundPct': 'opp_arc3_misses',
        'DefAtRimReboundPct': 'opp_at_rim_misses',
        'DefShortMidRangeReboundPct': 'opp_short_midrange_misses',
        'DefCorner3ReboundPct': 'opp_corner3_misses',
        'OffAtRimReboundPct': 'at_rim_misses',
        'SelfORebPct': 'fg_misses',
        'OffShortMidRangeReboundPct': 'short_midrange_misses',
        #'DefFTReboundPct': 'FTDefRebounds',
        #'OffFTReboundPct':'opp_FTDefRebounds',
        'OffCorner3ReboundPct': 'corner3_misses',
        'SecondChanceTsPct':'SecondChanceOffPoss',
        'SecondChanceCorner3PctAssisted':'SecondChanceCorner3FGM',
    
    
        'SecondChanceArc3PctAssisted':'SecondChanceArc3FGM',
        'SecondChanceAtRimPctAssisted':'SecondChanceAtRimFGM'
    }
    
    values=[]
    for key in weight_mapping.keys():
        if df[weight_mapping[key]].sum()==0:
            val=0
        else:
            val = calculate_weighted_average(df, key, weight_mapping[key], 'team_id').iloc[0]

    
        values.append(val)
    
        
    weight_list=list(weight_mapping.keys())
    pct= [col for col in df.columns if 'pct' in col.lower()]
    sum = [col for col in df.columns if col not in id_col and col not in pct]
    sum
    sums= df.groupby('TeamId').sum(numeric_only=True)[sum].reset_index(drop=True)
    rebounds=[
        "OffAtRimReboundPct",
        "OffShortMidRangeReboundPct",
        "OffLongMidRangeReboundPct",
        "OffArc3ReboundPct",
        "DefAtRimReboundPct",
        "DefShortMidRangeReboundPct",
        "DefLongMidRangeReboundPct",
        "DefArc3ReboundPct",
        "DefCorner3ReboundPct"
    ]
    
    exclude = [
    
        "PenaltyTsPct"
    ]
    pct = [col for col in pct if col not in exclude]
    
    pct = [col for col in pct if 'opp_' not in col.lower()]
    newframe=calculate_basketball_percentages(sums)
    
    newframe[weight_list]=values
    
    to_drop=[col for col in newframe if 'opp_' in col.lower()]
    new_pct=[col for col in newframe.columns if 'pct' in col.lower() and 'opp' not in col.lower()]
    '''
    for col in new_pct:
    
        print(col)
        print('Generated Value')
        print(newframe[col])
        print('Comp Value')
        print(comp[col])
        print('')
    '''
    to_drop=[col for col in newframe if 'opp_' in col.lower()]
    newframe['GamesPlayed']=df['GamesPlayed'].max()
    
    newframe.drop(columns=to_drop,inplace=True)
    newframe['player_id']=player_id
    newframe['player_on']=on
    newframe['player_vs']=vs
    year=int(year)
    newframe['season']=str(year-1)+'-'+str(year)[-2:]
    newframe['team_id']=team_id
    newframe.drop(columns='GamesPlayed',inplace=True)
    #newframe['GamesPlayed']=newframe['OffPoss']/75
    #newframe['GamesPlayed']=newframe['GamesPlayed'].round(0)

    
    return newframe

def get_year(year,ps=False,vs=False):
    if ps == False:
        index=pd.read_csv('index_master.csv')
    else:
        index=pd.read_csv('index_master_ps.csv')    
    index=index[index.year==year]
    index.dropna(subset='team_id',inplace=True)
    rows=[]
    count=0
    
    for player_id,team_id in zip(index['nba_id'],index['team_id']):
        player_id=int(player_id)
        team_id=int(team_id)

        row1=player_rows(str(year),str(player_id),str(team_id),vs=vs)
        row2=player_rows(str(year),str(player_id),str(team_id),on=False,vs=vs)
        rows.append(row1)
        rows.append(row2)
        count+=1
        if count %100==0:
            print(count)
    frame = pd.concat(rows)
    frame['year']=year
    return frame
'''
frame= get_year(2025)
frame_vs=get_year(2025,vs=True)
gp = pd.read_csv('games.csv')
print(gp.columns)
gp = gp[gp.year==2025]

gp=gp[['nba_id','G']]

gp.columns=['player_id','GamesPlayed']
gp['player_id']=gp['player_id'].astype(int)

gp['player_id']=gp['player_id'].astype(str)
frame = frame.merge(gp,on='player_id')
frame_vs = frame_vs.merge(gp,on='player_id')

frame.to_csv('2025.csv',index=False)
frame_vs.to_csv('2025vs.csv',index=False)
end_time = time.time()
elapsed_time = end_time - start_time
print(f"Time taken: {elapsed_time} seconds")
'''
#frame.to_csv('../../contract/nba_rapm/on-off/years/2025.csv',index=False)
#frame_vs.to_csv('../../contract/nba_rapm/on-off/years/2025vs.csv',index=False)




# In[3]:


df


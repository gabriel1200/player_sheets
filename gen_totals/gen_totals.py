#!/usr/bin/env python
# coding: utf-8

# In[20]:


import pandas as pd
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import glob
import os
import numpy as np

ps=False

SEASON_YEAR=2026
avg = pd.read_csv('../team_totals/team_averages.csv')

# Convert necessary columns to integers
avg['total_fgoreb'] = avg['total_fgoreb'].astype(int)
avg['total_opp_fgdreb'] = avg['total_opp_fgdreb'].astype(int)

# Calculate 'fgoreb%'
avg['fgoreb%'] = avg['total_fgoreb'] / (avg['total_fgoreb'] + avg['total_opp_fgdreb'])

# Rename columns
avg.rename(columns={'oreb%': 'ORB%', 'ortg': 'ORtg', 'season': 'Season'}, inplace=True)

# Add missing seasons with real historical values
missing_data = pd.DataFrame({
    'Season': [1997, 1998, 1999, 2000],
    'fgoreb%': [0.308, 0.314, 0.302, 0.289],
    'ORtg': [1.067, 1.050, 1.022, 1.041]
})

# Append missing data to the DataFrame
AVERAGE = pd.concat([avg, missing_data], ignore_index=True)




def collect_yeardata(ps=False):
    frames = []
    trail=''
    if ps ==True:
        trail='_ps'
    end_year = SEASON_YEAR
    for year in range(1997,end_year+1):
        file= str(year)+trail+'_avg.csv'

        df = pd.read_csv(file)
        df['PLAYER_ID']=df['PLAYER_ID'].astype(str)

        if year >2000:
            df2=pd.read_csv(str(year)+trail+'_pbp'+'.csv')

            df2.rename(columns={'EntityId':'PLAYER_ID'},inplace=True)

            curcol =[col.lower() for col in df.columns]
            keepcol =[col for col in df2.columns if col.lower() not in curcol]
            keepcol.append('PLAYER_ID')
            keepcol.append('year')
            df2=df2[keepcol]
            df2['PLAYER_ID']=df2['PLAYER_ID'].astype(str)
            if ps:
                df2['year']=df2['year'].str.split('_').str[0].astype(int)

            df=df.merge(df2,on=['PLAYER_ID','year'],how='left')
        df['year']=year
        frames.append(df)

    return pd.concat(frames)
sum_metrics = [
    "MIN", "FGM", "FGA", "FG3M", "FG3A", "FTM", "FTA", "OREB", "DREB", "REB",
    "AST", "TOV", "STL", "BLK", "BLKA", "PF", "PFD", "PTS", "PLUS_MINUS", 
    "DD2", "TD3", "FGM_PG", "sp_work_PACE", "sp_work_DEF_RATING", "DEF_RATING", 
    "sp_work_NET_RATING", "E_DEF_RATING", "OFF_RATING", "PACE_PER40", 
    "AST_RATIO", "sp_work_OFF_RATING", "E_PACE", "NET_RATING", "E_NET_RATING", 
    "PACE", "E_OFF_RATING", "FGA_PG", "AST_TO", "ITP_FGM", 
    "ABOVE_BREAK_3_FGA", "MID_FGM", "RIGHT_CORNER_3_FGM", "MID_FGA", 
    "RA_FGA", "LEFT_CORNER_3_FGM", "ITP_FGA", "LEFT_CORNER_3_FGA", 
    "BACKCOURT_FGA", "RA_FGM", "CORNER_3_FGM", "RIGHT_CORNER_3_FGA", 
    "BACKCOURT_FGM", "CORNER_3_FGA", "ABOVE_BREAK_3_FGM", "FGM_10_14", 
    "FGP_5_9", "FGP_35_39", "FGP_LT_5", "FGP_10_14", "FGA_15_19", 
    "FGP_15_19", "FGA_25_29", "FGM_40_PLUS", "FGM_LT_5", "FGM_15_19", 
    "FGA_40_PLUS", "FGM_20_24", "FGA_30_34", "FGM_5_9", "FGM_30_34", 
    "FGA_10_14", "FGP_30_34", "FGA_5_9", "FGP_25_29", "FGP_40_PLUS", 
    "FGM_25_29", "FGA_20_24", "FGP_20_24", "FGA_35_39", 
    "FGM_35_39", "FGA_LT_5", "POTENTIAL_AST", "AST_ADJ", "FT_AST", 
    "PASSES_RECEIVED", "PASSES_MADE", "SECONDARY_AST", "AST_PTS_CREATED", 
    "DRIVE_FTM", "DRIVE_TOV", "DRIVE_FGM", "DRIVE_PASSES", "DRIVES", 
    "DRIVE_PTS", "DRIVE_PF", "DRIVE_AST", "DRIVE_FTA", "DRIVE_FGA", 
    "TOUCHES", "AVG_SEC_PER_TOUCH", "POINTS", "TIME_OF_POSS", 
    "AVG_DRIB_PER_TOUCH", "PAINT_TOUCHES", "FRONT_CT_TOUCHES", 
    "ELBOW_TOUCHES", "POST_TOUCHES", "PTS_PER_ELBOW_TOUCH", 
    "PTS_PER_PAINT_TOUCH", "PTS_PER_TOUCH", "PTS_PER_POST_TOUCH", 
    "DREB_CHANCE_DEFER", "AVG_DREB_DIST", "REB_UNCONTEST", 
    "OREB_CHANCE_DEFER", "OREB_CHANCES", "AVG_OREB_DIST", "DREB_CONTEST", 
    "OREB_UNCONTEST", "AVG_REB_DIST", "REB_CONTEST", "REB_CHANCES", 
    "REB_CHANCE_DEFER", "DREB_UNCONTEST", "DREB_CHANCES", "OREB_CONTEST", 
    "very_tight_FG3M",  "very_tight_FGA_FREQUENCY", 
    "very_tight_FG3A", "very_tight_FG2M",
    "very_tight_FGM", "very_tight_FG2A_FREQUENCY", "very_tight_FGA", 
    "very_tight_FG2A", "very_tight_FG3A_FREQUENCY", "tight_FGM", 
    "tight_FGA_FREQUENCY", "tight_FG3A", "tight_FGA", "tight_FG2A_FREQUENCY", 
    "tight_FG2M", "tight_FG2A", "tight_FG3A_FREQUENCY", "tight_FG3M", 
    "open_FG3A_FREQUENCY", "open_FG3A", "open_FG2A", "open_FG3M", 
    "open_FG2A_FREQUENCY", "open_FG2M", "open_FGM", "open_FGA_FREQUENCY", 
    "open_FGA", "wide_open_FG2A", "wide_open_FG2M", "wide_open_FGM", 
    "wide_open_FG3M", "wide_open_FGA_FREQUENCY", "wide_open_FGA", 
    "wide_open_FG2A_FREQUENCY", "wide_open_FG3A", "wide_open_FG3A_FREQUENCY", 
    "PULL_UP_PTS", "PULL_UP_FG3A", "PULL_UP_FGA", "PULL_UP_FGM", 
    "PULL_UP_FG3M", "PAINT_TOUCH_PTS", "ELBOW_TOUCH_PTS", "POST_TOUCH_PTS", 
    "CATCH_SHOOT_PTS", "FGM_LT_06", "FGA_LT_06", "PLUSMINUS", "CATCH_SHOOT_FG3M", "CATCH_SHOOT_FG3A", 
    "CATCH_SHOOT_FGA", "CATCH_SHOOT_FGM", "NBA_FANTASY_PTS",

    'more_15ft_def_REB_CHANCE_DEFER', 'less_10ft_def_PLUSMINUS', 'two_pt_def_FG2A', 'hustle_LOOSE_BALLS_RECOVERED', 'DIST_FEET', 
    'less_6ft_def_PLUSMINUS', 'DIST_MILES_DEF', 'post_touch_POST_TOUCH_FOULS', 'two_pt_def_FG2M', 'three_pt_def_FG3M', 'post_touch_POST_TOUCH_FGM', 
    'hustle_BOX_OUTS', 'hustle_DEF_BOXOUTS', 'post_touch_POST_TOUCH_TOV', 'DIST_MILES', 'more_15ft_def_AVG_OREB_DIST', 'more_15ft_def_DREB_UNCONTEST', 
    'AVG_SPEED_DEF', 'more_15ft_def_AVG_DREB_DIST', 'more_15ft_def_AVG_REB_DIST', 'post_touch_POST_TOUCH_PASSES', 'hustle_OFF_BOXOUTS', 'hustle_SCREEN_ASSISTS', 
    'less_6ft_def_FGM_LT_06', 'more_15ft_def_OREB', 'hustle_CONTESTED_SHOTS_2PT', 'hustle_BOX_OUT_PLAYER_REBS', 'D_FGA', 'more_15ft_def_DREB', 'post_touch_POST_TOUCH_FGA', 
    'post_touch_TOUCHES', 'hustle_OFF_LOOSE_BALLS_RECOVERED', 'less_10ft_def_FGM_LT_10', 'more_15ft_def_DREB_CHANCES', 'more_15ft_def_OREB_CONTEST', 'AVG_SPEED', 'team_poss',
      'D_FGM', 'post_touch_POST_TOUCH_FTM', 'three_pt_def_PLUSMINUS', 'more_15ft_def_OREB_UNCONTEST', 'AVG_SPEED_OFF', 'more_15ft_def_REB', 'more_15ft_def_DREB_CONTEST', 
      'hustle_CHARGES_DRAWN', 'less_6ft_def_FGA_LT_06',  'more_15ft_def_OREB_CHANCES', 'hustle_BOX_OUT_PLAYER_TEAM_REBS', 'hustle_CONTESTED_SHOTS_3PT',
        'more_15ft_def_REB_CONTEST', 'post_touch_POST_TOUCHES', 'three_pt_def_FG3A',  'hustle_CONTESTED_SHOTS',
      'hustle_DEFLECTIONS', 'more_15ft_def_DREB_CHANCE_DEFER','post_touch_POST_TOUCH_AST', 
'hustle_SCREEN_AST_PTS', 'post_touch_POST_TOUCH_FTA', 'more_15ft_def_OREB_CHANCE_DEFER', 'less_10ft_def_FGA_LT_10',
 'post_touch_POST_TOUCH_PTS', 'hustle_DEF_LOOSE_BALLS_RECOVERED', 'DIST_MILES_OFF', 'more_15ft_def_REB_UNCONTEST', 'more_15ft_def_REB_CHANCES'
]
pct_metrics = [
    "W_PCT",                "FG_PCT",             "FG3_PCT",            "FT_PCT",
    "TM_TOV_PCT",          "REB_PCT",           "AST_PCT",           "DREB_PCT",
    "E_TOV_PCT",           "TS_PCT",            "EFG_PCT",           "E_USG_PCT",
    "OREB_PCT",            "USG_PCT",           "MID_FG_PCT",       "BACKCOURT_FG_PCT",
    "ABOVE_BREAK_3_FG_PCT", "CORNER_3_FG_PCT", "RA_FG_PCT",         "LEFT_CORNER_3_FG_PCT",
    "RIGHT_CORNER_3_FG_PCT", "ITP_FG_PCT",      "AST_TO_PASS_PCT_ADJ", "AST_TO_PASS_PCT",
    "DRIVE_PTS_PCT",       "DRIVE_FT_PCT",      "DRIVE_PASSES_PCT",  "DRIVE_TOV_PCT",
    "DRIVE_PF_PCT",        "DRIVE_FG_PCT",      "DRIVE_AST_PCT",     "REB_CHANCE_PCT",
    "OREB_CONTEST_PCT",    "OREB_CHANCE_PCT",   "DREB_CHANCE_PCT",   "DREB_CONTEST_PCT",
    "DREB_CHANCE_PCT_ADJ", "REB_CHANCE_PCT_ADJ", "OREB_CHANCE_PCT_ADJ", "REB_CONTEST_PCT",
    "very_tight_EFG_PCT",  "very_tight_FG2_PCT", "very_tight_FG3_PCT", "very_tight_FG_PCT",
    "tight_FG_PCT",        "tight_EFG_PCT",     "tight_FG2_PCT",     "tight_FG3_PCT",
    "open_FG2_PCT",        "open_EFG_PCT",      "open_FG3_PCT",      "open_FG_PCT",
    "wide_open_FG2_PCT",   "wide_open_FG_PCT",  "wide_open_FG3_PCT",  "wide_open_EFG_PCT",
    "PULL_UP_EFG_PCT",     "PULL_UP_FG3_PCT",   "PULL_UP_FG_PCT",    "ELBOW_TOUCH_FG_PCT",
    "CATCH_SHOOT_FG_PCT",  "PAINT_TOUCH_FG_PCT", "POST_TOUCH_FG_PCT",  "EFF_FG_PCT",
    "LT_06_PCT",           "NS_LT_06_PCT",      "CATCH_SHOOT_FG3_PCT","CATCH_SHOOT_EFG_PCT",
    'hustle_PCT_BOX_OUTS_TEAM_REB', 'post_touch_POST_TOUCH_AST_PCT', 'post_touch_POST_TOUCH_FG_PCT', 'D_FG_PCT',
      'post_touch_POST_TOUCH_PTS_PCT', 'more_15ft_def_OREB_CHANCE_PCT_ADJ', 'PCT_PLUSMINUS', 'post_touch_POST_TOUCH_TOV_PCT',
        'less_6ft_def_LT_06_PCT', 'more_15ft_def_REB_CHANCE_PCT_ADJ', 'hustle_PCT_LOOSE_BALLS_RECOVERED_OFF', 'post_touch_POST_TOUCH_FOULS_PCT', 
        'three_pt_def_FREQ', 'more_15ft_def_DREB_CHANCE_PCT_ADJ', 'less_6ft_def_NS_LT_06_PCT', 'three_pt_def_FG3_PCT', 'more_15ft_def_REB_CHANCE_PCT', 
        'post_touch_POST_TOUCH_PASSES_PCT', 'three_pt_def_NS_FG3_PCT', 'more_15ft_def_OREB_CONTEST_PCT', 'two_pt_def_FG2_PCT', 'less_10ft_def_FREQ',
          'less_10ft_def_NS_LT_10_PCT', 'NORMAL_FG_PCT', 'less_6ft_def_FREQ', 'more_15ft_def_DREB_CONTEST_PCT', 'more_15ft_def_REB_CONTEST_PCT', 
          'two_pt_def_FREQ', 'hustle_PCT_LOOSE_BALLS_RECOVERED_DEF', 'post_touch_POST_TOUCH_FT_PCT', 'hustle_PCT_BOX_OUTS_REB', 'hustle_PCT_BOX_OUTS_DEF',
            'more_15ft_def_OREB_CHANCE_PCT', 'hustle_PCT_BOX_OUTS_OFF', 'two_pt_def_NS_FG2_PCT', 'more_15ft_def_DREB_CHANCE_PCT', 'less_10ft_def_LT_10_PCT'
]
pbp_columns = [
    "TeamId", "Name", "ShortName", "RowId",  "SecondsPlayed",
    "GamesPlayed", "Minutes", "PlusMinus", "OffPoss", "DefPoss", "PenaltyOffPoss", "PenaltyDefPoss",
    "SecondChanceOffPoss", "TotalPoss", "AtRimFGM", "AtRimFGA", "SecondChanceAtRimFGM",
    "SecondChanceAtRimFGA", "PenaltyAtRimFGM", "PenaltyAtRimFGA", "ShortMidRangeFGM",
    "ShortMidRangeFGA", "LongMidRangeFGM", "LongMidRangeFGA", "Corner3FGM", "Corner3FGA",
    "SecondChanceCorner3FGM", "SecondChanceCorner3FGA", "PenaltyCorner3FGM", "PenaltyCorner3FGA",
    "Arc3FGM", "Arc3FGA", "SecondChanceArc3FGM", "SecondChanceArc3FGA", "PenaltyArc3FGM",
    "PenaltyArc3FGA", "FG2M", "FG2A", "FG3M", "FG3A", "FtPoints", "Points", "OpponentPoints",
    "SecondChanceFG2M", "SecondChanceFG2A", "SecondChanceFG3M", "SecondChanceFG3A", "SecondChanceFtPoints",
    "SecondChancePoints", "PenaltyFG2M", "PenaltyFG2A", "PenaltyFG3M", "PenaltyFG3A", "PenaltyFtPoints",
    "PenaltyPoints", "PtsAssisted2s", "PtsUnassisted2s", "PtsAssisted3s", "PtsUnassisted3s",
    "PtsPutbacks", "NonHeaveArc3FGA", "NonHeaveArc3FGM", "Fg2aBlocked", "Fg3aBlocked", "TwoPtAssists",
    "ThreePtAssists", "Assists", "Arc3Assists", "Corner3Assists", "AtRimAssists", "ShortMidRangeAssists",
    "LongMidRangeAssists", "AssistPoints", "OffThreePtRebounds", "OffTwoPtRebounds", "FTOffRebounds",
    "DefThreePtRebounds", "DefTwoPtRebounds", "FTDefRebounds", "DefRebounds", "OffRebounds", "Rebounds",
    "SelfOReb", "Steals", "BadPassSteals", "LostBallSteals", "LiveBallTurnovers",
    "BadPassOutOfBoundsTurnovers", "BadPassTurnovers", "DeadBallTurnovers", "LostBallOutOfBoundsTurnovers",
    "LostBallTurnovers", "StepOutOfBoundsTurnovers", "Travels", "Turnovers", "SecondChanceTurnovers",
    "PenaltyTurnovers", "ShootingFouls", "Fouls", "Charge Fouls", "Loose Ball Fouls",
    "Offensive Fouls", "FoulsDrawn", "Charge Fouls Drawn", "Loose Ball Fouls Drawn",
    "Offensive Fouls Drawn", "FTA", "2pt And 1 Free Throw Trips", "TwoPtShootingFoulsDrawn",
    "NonShootingFoulsDrawn", "Blocked2s", "Blocked3s", "BlockedArc3", "BlockedAtRim",
    "BlockedCorner3", "BlockedLongMidRange", "BlockedShortMidRange", "Blocks", "RecoveredBlocks",
    "FirstChancePoints", "PenaltyPointsExcludingTakeFouls", "PenaltyOffPossExcludingTakeFouls",
    "NonShootingPenaltyNonTakeFouls", "NonShootingPenaltyNonTakeFoulsDrawn", "Period1Fouls2Minutes",
    "Period2Fouls2Minutes", "Period3Fouls3Minutes", "Period4Fouls4Minutes", "OnOffRtg", "OnDefRtg",
    "Assisted2sPct", "NonPutbacksAssisted2sPct", "Assisted3sPct", "Fg3Pct", "SecondChanceFg3Pct",
    "PenaltyFg3Pct", "NonHeaveFg3Pct", "Fg2Pct", "SecondChanceFg2Pct", "PenaltyFg2Pct", "EfgPct",
    "SecondChanceEfgPct", "PenaltyEfgPct", "TsPct", "SecondChanceTsPct", "PenaltyTsPct", "FG3APct",
    "FG3APctBlocked", "FG2APctBlocked", "AtRimPctBlocked", "ShortMidRangePctBlocked", "LongMidRangePctBlocked",
    "Corner3PctBlocked", "Arc3PctBlocked", "Usage", "LiveBallTurnoverPct", "DefFTReboundPct",
    "OffFTReboundPct", "DefTwoPtReboundPct", "OffTwoPtReboundPct", "DefThreePtReboundPct",
    "OffThreePtReboundPct", "DefFGReboundPct", "OffFGReboundPct", "OffAtRimReboundPct",
    "OffShortMidRangeReboundPct", "OffLongMidRangeReboundPct", "OffArc3ReboundPct", "OffCorner3ReboundPct",
    "DefAtRimReboundPct", "DefShortMidRangeReboundPct", "DefLongMidRangeReboundPct", "DefArc3ReboundPct",
    "DefCorner3ReboundPct", "SelfORebPct", "BlocksRecoveredPct", "AtRimFrequency", "AtRimAccuracy",
    "UnblockedAtRimAccuracy", "AtRimPctAssisted", "ShortMidRangeFrequency", "ShortMidRangeAccuracy",
    "UnblockedShortMidRangeAccuracy", "ShortMidRangePctAssisted", "LongMidRangeFrequency",
    "LongMidRangeAccuracy", "UnblockedLongMidRangeAccuracy", "LongMidRangePctAssisted",
    "Corner3Frequency", "Corner3Accuracy", "UnblockedCorner3Accuracy", "Corner3PctAssisted",
    "Arc3Frequency", "Arc3Accuracy", "UnblockedArc3Accuracy", "Arc3PctAssisted", "SecondChanceAtRimFrequency",
    "SecondChanceAtRimAccuracy", "SecondChanceAtRimPctAssisted", "SecondChanceCorner3Frequency",
    "SecondChanceCorner3Accuracy", "SecondChanceCorner3PctAssisted", "SecondChanceArc3Frequency",
    "SecondChanceArc3Accuracy", "SecondChanceArc3PctAssisted", "PenaltyAtRimFrequency",
    "PenaltyAtRimAccuracy", "PenaltyCorner3Frequency", "PenaltyCorner3Accuracy", "PenaltyArc3Frequency",
    "PenaltyArc3Accuracy", "AtRimFG3AFrequency", "NonHeaveArc3Accuracy", "ShotQualityAvg",
    "SecondChanceShotQualityAvg", "PenaltyShotQualityAvg", "ShootingFoulsDrawnPct", "TwoPtShootingFoulsDrawnPct",
    "SecondChancePointsPct", "PenaltyPointsPct", "Avg2ptShotDistance", "Avg3ptShotDistance",
    "AtRimOffReboundedPct", "ShortMidRangeOffReboundedPct", "LongMidRangeOffReboundedPct",
    "ThreePtOffReboundedPct", "PenaltyOffPossPct", "HeaveAttempts", "Technical Free Throw Trips",
    "DefensiveGoaltends", "OffensiveGoaltends", "Defensive 3 Seconds Violations", "Period2Fouls3Minutes",
    "Period3Fouls4Minutes", "Period4Fouls5Minutes", "PeriodOTFouls4Minutes", "PeriodOTFouls5Minutes",
    "3pt And 1 Free Throw Trips", "ThreePtShootingFoulsDrawn", "ThreePtShootingFoulsDrawnPct",
    "Clear Path Fouls", "HeaveMakes", "3SecondViolations", "Period1Fouls3Minutes", "Period3Fouls5Minutes",
    "Period2Fouls4Minutes"
]


index_col=["PLAYER_ID","PLAYER_NAME","W","GP","year","POSS","TEAM_ABBREVIATION","TEAM_ID","AGE",'PLAYER_HEIGHT_INCHES', 'PLAYER_WEIGHT', 'COLLEGE',
       'COUNTRY', 'DRAFT_YEAR', 'DRAFT_ROUND', 'DRAFT_NUMBER']
other_col = index_col+sum_metrics+pct_metrics
pbp_col =[ col for col in pbp_columns if col not in other_col]
data=collect_yeardata(ps=ps)

data['POINTS'] = data['POINTS'].fillna(data['Points'])

# Fill Points nulls with POINTS
data['Points'] = data['Points'].fillna(data['POINTS'])
total_columns = index_col+sum_metrics+pct_metrics+pbp_columns

total_columns=list(set(total_columns))
data=data[total_columns]


columns_to_front = ["PLAYER_ID", "PLAYER_NAME", "W", "GP", "year", "POSS", "TEAM_ABBREVIATION", "TEAM_ID", "AGE"]

# Reorder the DataFrame
data = data[columns_to_front + [col for col in data.columns if col not in columns_to_front]]

total_path = 'all_totals.csv' if ps else 'all_totals_ps.csv'
data.to_csv(total_path,index=False)
modern = data[data.year>=2014]

modern_path='modern_totals.csv' if ps else 'modern_totals_ps.csv'
data.to_csv(modern_path,index=False)
def perc_save(df, ps=True):
    trail = '_ps' if ps else ''
    player_ids = df['PLAYER_ID'].unique().tolist()

    frame = df[index_col + sum_metrics + pct_metrics].reset_index().fillna(0)

    # Calculate percentages
    for col in sum_metrics:
        frame[col] = 100 * frame[col].astype(int) / frame['POSS']

    # Rank metrics
    all_metrics = sum_metrics + pct_metrics

    # Create a dictionary to store new columns
    rank_cols = {col + '_rank': frame.groupby('year')[col].rank(pct=True) for col in all_metrics}

    # Use pd.concat to add all rank columns at once
    frame = pd.concat([frame, pd.DataFrame(rank_cols)], axis=1)

    # De-fragment the DataFrame
    frame = frame.copy()

    # Save individual player files
    for player_id in player_ids:
        player_frame = frame[frame['PLAYER_ID'] == player_id]
        player_frame.to_csv(f'../perc/{player_id}{trail}.csv', index=False)

def total_save(df,ps=False):
    trail ='_ps' if ps else ''
    player_ids=df['PLAYER_ID'].unique().tolist()

    frame=df[index_col+sum_metrics+pct_metrics].reset_index()
    frame=frame.fillna(0)


    for id in player_ids:

        player_frame=frame[frame.PLAYER_ID==id]

        player_frame.to_csv('../totals/'+str(id)+trail+'.csv',index=False)


def player_factors(df, year):
    # Copy relevant columns
    df['FTM'] = df['FtPoints']
    df['RimFGA'] = df['AtRimFGA']
    df['RimFGM'] = df['AtRimFGM']

    # Load average season data
    avg = AVERAGE.copy()
    avg = avg.rename(columns={'oreb%': 'ORB%', 'ortg': 'ORtg', 'season': 'Season'})
    avg = avg[avg.year == year]
    avg['fgoreb%'] = avg['total_fgoreb'].astype(int) / (avg['total_fgoreb'].astype(int) + avg['total_opp_fgdreb'].astype(int))
    aorb, aortg = avg[['ORB%', 'ORtg']].iloc[0]

    # Add average values to df
    df['avg_ortg'] = aortg
    df['avg_orb%'] = aorb

    # Compute FGA and FGM
    df['FG2_miss'] = df['FG2A'] - df['FG2M']
    df['FG3_miss'] = df['FG3A'] - df['FG3M']
    df['FG2_points'] = df['FG2M'] * 2
    df['FG3_points'] = df['FG3M'] * 3
    df['FGA'] = df['FG2A'] + df['FG3A']

    # Rim and non-rim values
    df['RimPoints'] = df['RimFGM'] * 2
    df['RimMiss'] = df['RimFGA'] - df['RimFGM']
    df['NonRim2FGA'] = df['FG2A'] - df['RimFGA']
    df['NonRim2FGM'] = df['FG2M'] - df['RimFGM']
    df['NonRimPoints'] = df['NonRim2FGM'] * 2

    # Factors (still using avg_orb%)
    df['2shooting_factor'] = (
        df['FG2_points'] -
        df['FG2M'] * df['avg_ortg'] -
        (1 - df['avg_orb%']) * df['FG2_miss'] * df['avg_ortg']
    )
    df['3shooting_factor'] = (
        df['FG3_points'] -
        df['FG3M'] * df['avg_ortg'] -
        (1 - df['avg_orb%']) * df['FG3_miss'] * df['avg_ortg']
    )
    df['rimfactor'] = (
        df['RimPoints'] -
        df['RimFGM'] * df['avg_ortg'] -
        (1 - df['avg_orb%']) * df['RimMiss'] * df['avg_ortg']
    )
    df['nonrim2factor'] = (
        df['NonRimPoints'] -
        df['NonRim2FGM'] * df['avg_ortg'] -
        (1 - df['avg_orb%']) * (df['NonRim2FGA'] - df['NonRim2FGM']) * df['avg_ortg']
    )
    df['morey_factor'] = (
        df['RimPoints'] + df['FG3_points'] -
        (df['RimFGM'] + df['FG3M']) * df['avg_ortg'] -
        (1 - df['avg_orb%']) * (df['RimMiss'] + df['FG3_miss']) * df['avg_ortg']
    )

    df['turnover_factor'] = -df['Turnovers'] * df['avg_ortg']
    df['ft_factor'] = df['FTM'] - 0.4 * df['FTA'] * df['avg_ortg'] + 0.06 * (df['FTA'] - df['FTM']) * df['avg_ortg']

    df['shooting_factor']=df['3shooting_factor']+df['2shooting_factor']
    df['scoring_factor']=df['ft_factor']+df['3shooting_factor']+df['2shooting_factor']

    return df


perc_save(data,ps=ps)

total_save(data,ps=ps)


# In[21]:


data[data.PLAYER_NAME.str.upper()=='KEVIN DURANT']



# In[ ]:





# In[ ]:


directory = "../totals"


totals=pd.read_csv(total_path)
totals.columns = totals.columns.str.replace(' ', '_')
totals.rename(columns={'3SecondViolations':'ThreeSecondViolations'},inplace=True)


lebron=pd.read_csv('https://raw.githubusercontent.com/gabriel1200/site_Data/refs/heads/master/lebron.csv')
start_year=1997
end_year=SEASON_YEAR
trail = '_ps' if ps else ''
modern_years=[] 
for year in range(start_year,end_year+1):
    print(year)

    yearframe=totals[totals.year==year].reset_index(drop=True)
    if trail =='' and year>=2010:
        lebronyear=lebron[lebron.year==year].reset_index(drop=True)

        lebronyear=lebronyear[['WAR','LEBRON','O-LEBRON','D-LEBRON','year','NBA ID','Pos', 'Offensive Archetype','Defensive Role']]

        lebronyear.rename(columns={'WAR':'LEBRON_WAR','NBA ID':'PLAYER_ID','O-LEBRON':'O_LEBRON','D-LEBRON':'D_LEBRON',},inplace=True)


        yearframe=yearframe.merge(lebronyear,on=['PLAYER_ID','year'],how='left')
        if len(lebronyear)==0:
            yearframe.drop(columns='Pos',inplace=True)
            temp_index=pd.read_csv('modern_index.csv')
            temp_index=temp_index[temp_index.year==year]
            if len(temp_index)>0:
                temp_index = temp_index[['nba_id','Pos']]
                temp_index.rename(columns={'nba_id':'PLAYER_ID'},inplace=True)
                yearframe = yearframe.merge(temp_index, on='PLAYER_ID')

                print(yearframe[yearframe.Pos.isna()])

    if year>=2014:

        yearframe['on-ball-time%'] = 100 * 2 * (yearframe['TIME_OF_POSS']) / (yearframe['Minutes'])
        yearframe['ON_BALL_TIME_PCT'] =  100 * 2 * (yearframe['TIME_OF_POSS']) / (yearframe['Minutes'])
    if year>=2001:

        yearframe['Stops'] = (
        yearframe["Charge_Fouls_Drawn"].fillna(0) +
        yearframe["Offensive_Fouls_Drawn"].fillna(0) +
        yearframe["Steals"].fillna(0) +
        yearframe["RecoveredBlocks"].fillna(0)    
    )


        # Fill missing values with 0 for all involved columns
        cols_to_fill = [
            "FG2A", "FG3A", "FTA", "2pt_And_1_Free_Throw_Trips", "3pt_And_1_Free_Throw_Trips",
            "Turnovers", "BadPassTurnovers", "Points", "SelfOReb"
        ]
        yearframe[cols_to_fill] = yearframe[cols_to_fill].fillna(0)

        # Calculate improved TSA
        yearframe["improved_tsa"] = (
            yearframe["FG2A"]
            + yearframe["FG3A"]
            + (0.5 * (yearframe["FTA"]     - yearframe["2pt_And_1_Free_Throw_Trips"]
            - yearframe["3pt_And_1_Free_Throw_Trips"]))

        )

        # Calculate Non-Pass Turnovers
        yearframe["NonPassTurnover"] = yearframe["Turnovers"] - yearframe["BadPassTurnovers"]



        # Avoid divide-by-zero
        yearframe["adjusted_trueshooting_pct"] =yearframe['PTS']/(
            (yearframe["improved_tsa"] - yearframe["SelfOReb"]
              + yearframe["NonPassTurnover"]))/2

        # Compute league totals
        total_points = yearframe["PTS"].sum()
        total_possessions = yearframe["improved_tsa"].sum()- yearframe["SelfOReb"].sum()+ yearframe["NonPassTurnover"].sum()


        league_avg_ts = total_points / (total_possessions )/2

        # Add relative adjusted true shooting column
        yearframe["relative_adjusted_ts_pct"] = yearframe["adjusted_trueshooting_pct"] - league_avg_ts

        yearframe=player_factors(yearframe,year)

        # Sort and save
    yearframe.sort_values(by=['Points', 'Minutes'], inplace=True)
    print('Saving to ' + '../year_totals/' + str(year) + trail + '.csv')
    yearframe.to_csv('../year_totals/' + str(year) + trail + '.csv', index=False)
    yearframe.to_parquet('../year_totals/' + str(year) + trail + '.parquet', index=False)



    if year>=2014:
        modern_years.append(yearframe)
modern = pd.concat(modern_years)
modern.to_csv('../year_totals/modern'+trail+'.csv',index=False)


# In[23]:


modern['adjusted_trueshooting_pct'].value_counts()


# In[ ]:





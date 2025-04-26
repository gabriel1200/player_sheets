#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import pandas as pd
from datetime import datetime
import time
from typing import List, Dict
import logging

class PBPStatsAPI:
    def __init__(self, start_year: int = 2013, end_year: int = 2024):
        self.base_url = "https://api.pbpstats.com/get-game-logs/nba?"
        self.season_types = ["Regular Season", "Playoffs"]
        self.start_year = start_year
        self.end_year = end_year

    def get_season_years(self) -> List[str]:
        """Generate season strings from start_year to end_year."""
        return [f"{year-1}-{str(year)[2:]}" for year in range(self.start_year, self.end_year + 1)]
    
    def get_team_game_logs(self, team_id: str,entity_type: str ="Team") -> pd.DataFrame:
        """
        Fetch all game logs for a specific team within the specified year range.
        
        Args:
            team_id (str): The team ID in PBP Stats format
            
        Returns:
            pd.DataFrame: DataFrame containing all game logs
        """
        all_games = []
        seasons = self.get_season_years()
        #print(seasons)
        current_season="2024-25"
        for season in seasons:
            for season_type in self.season_types:
         
        
                try:
                    params = {
                        "Season": season,
                        "SeasonType": season_type,
                        "EntityId": team_id,
                        "EntityType": entity_type
                    }
                    
                    response = requests.get(self.base_url, params=params)
                    data_response = response.json()
                    raw_data = data_response["multi_row_table_data"]
                    games_data = pd.DataFrame(raw_data)
                    
                    games_data['Season'] = season
                    games_data['year'] = int(season.split('-')[0]) + 1
                    games_data['SeasonType'] = season_type
                    all_games.append(games_data)
                            
                    # Respect API rate limits
                    time.sleep(5)
                    
                except requests.exceptions.RequestException as e:
                    logging.error(f"Error fetching data for {team_id} in {season} {season_type}: {str(e)}")
                    continue
                    
        return pd.concat(all_games) if all_games else pd.DataFrame()

def get_team_abbreviations():
    """
    Returns a dictionary mapping team IDs to team abbreviations.
    """
    return {
        '1610612755': 'PHI', 
        '1610612744': 'GSW', 
        '1610612752': 'NYK', 
        '1610612737': 'ATL', 
        '1610612758': 'SAC', 
        '1610612738': 'BOS', 
        '1610612765': 'DET', 
        '1610612747': 'LAL', 
        '1610612741': 'CHI', 
        '1610612764': 'WAS', 
        '1610612745': 'HOU', 
        '1610612760': 'OKC', 
        '1610612749': 'MIL', 
        '1610612756': 'PHX', 
        '1610612757': 'POR', 
        '1610612739': 'CLE', 
        '1610612761': 'TOR', 
        '1610612762': 'UTA', 
        '1610612754': 'IND', 
        '1610612751': 'BRK', 
        '1610612743': 'DEN', 
        '1610612759': 'SAS', 
        '1610612746': 'LAC', 
        '1610612742': 'DAL', 
        '1610612766': 'CHO', 
        '1610612748': 'MIA', 
        '1610612753': 'ORL', 
        '1610612750': 'MIN', 
        '1610612763': 'MEM', 
        '1610612740': 'NOP'
    }

def fetch_all_teams_game_logs(team_ids: List[str], start_year: int, end_year: int,entity_type:str ="Team") -> Dict[str, pd.DataFrame]:
    """
    Fetch game logs for multiple teams within the specified year range.
    
    Args:
        team_ids (List[str]): List of team IDs
        start_year (int): Starting year for data collection
        end_year (int): Ending year for data collection
        
    Returns:
        Dict[str, pd.DataFrame]: Dictionary with team IDs as keys and their game logs as values
    """
    api = PBPStatsAPI(start_year=start_year, end_year=end_year)
    team_games = {}
    
    for team_id in team_ids:
        logging.info(f"Fetching game logs for team {team_id}")
        team_games[team_id] = api.get_team_game_logs(team_id,entity_type)
        
    return team_games

# Example usage
if __name__ == "__main__":
    # Example team IDs (you'll need to use the correct IDs from PBP Stats)
    start_year = 2025
    end_year =2025

    
    df = pd.read_csv('index_master.csv')
    df = df[df.year >= start_year]
    df = df[df.team != 'TOT']
    team_ids = df['team_id'].unique().tolist()

    # Input start and end years

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    # Fetch game logs
    team_game_logs = fetch_all_teams_game_logs(team_ids, start_year, end_year)
    #frames=[]
    frames=[]
    # Example: Save to CSV files
    team_dict = get_team_abbreviations()
   
    for team_id, games_df in team_game_logs.items():
        if not games_df.empty:
            team_all=[]
            for year in range(start_year, end_year + 1):
                
                team_df = games_df[games_df.year == year]
                team_df['team_id']=team_id
                
                if len(team_df)!=0:
                    team_df.to_csv(f"team/{year}/{team_id}.csv", index=False)


            
            logging.info(f"Saved game logs for {team_id}")
          
    entity_type="Opponent"
    team_game_logs = fetch_all_teams_game_logs(team_ids, start_year, end_year,entity_type)
    for team_id, games_df in team_game_logs.items():
        if not games_df.empty:
            team_all_vs=[]
            for year in range(start_year, end_year + 1):
                team_df = games_df[games_df.year == year]
                team_df['team_id']=team_id
                team_df['team']=team_dict[str(team_id)]
                if len(team_df)!=0:
                    team_df.to_csv(f"team/{year}/{team_id}vs.csv", index=False)

                    #frames.append(team_df)
            logging.info(f"Saved game logs for {team_id}")
    
    


# In[2]:


import os
import pandas as pd
for year in range(2014,2026):
    directory = "team/"+str(year)
    files = os.listdir(directory)
    files =[file for file in files if 'game_logs' not in file and '.csv' in file and 'vs' not in file]
    
    totals=[]
    for file in files:
        df=pd.read_csv(directory+'/'+file)
        df['TeamId']=file.split('.')[0]
        totals.append(df)
    master= pd.concat(totals)

    team_dict={'OKC': '1610612760',
            'MIL': '1610612749',
            'SAC': '1610612758',
            'LAL': '1610612747',
            'BOS': '1610612738',
            'DEN': '1610612743',
            'MIN': '1610612750',
            'NYK': '1610612752',
            'PHO': '1610612756',
            'PHX': '1610612756',
            'ORL': '1610612753',
            'CHA': '1610612766',
            'CHO': '1610612766',
            'CLE': '1610612739',
            'LAC': '1610612746',
            'ATL': '1610612737',
            'MIA': '1610612748',
            'DAL': '1610612742',
            'DET': '1610612765',
            'MEM': '1610612763',
            'TOR': '1610612761',
            'CHI': '1610612741',
            'IND': '1610612754',
            'SAS': '1610612759',
            'HOU': '1610612745',
            'BRK': '1610612751',
            'BKN': '1610612751',
            'WAS': '1610612764',
            'GSW': '1610612744',
            'PHI': '1610612755',
            'UTA': '1610612762',
            'POR': '1610612757',
            'NOP': '1610612740'}
    master.to_csv(directory+'all_logs.csv')
    
   
for year in range(2014,2026):
    directory = "team/"+str(year)
    files = os.listdir(directory)
    files =[file for file in files if 'game_logs' not in file and '.csv' in file and 'vs' in file]
    
    totals=[]
    for file in files:
        df=pd.read_csv(directory+'/'+file)
        split1=file.split('.')[0]
        split2=split1.split('vs')[0]
     
        df['TeamId']=split2
        totals.append(df)
    master= pd.concat(totals)

    team_dict={'OKC': '1610612760',
            'MIL': '1610612749',
            'SAC': '1610612758',
            'LAL': '1610612747',
            'BOS': '1610612738',
            'DEN': '1610612743',
            'MIN': '1610612750',
            'NYK': '1610612752',
            'PHO': '1610612756',
            'PHX': '1610612756',
            'ORL': '1610612753',
            'CHA': '1610612766',
            'CHO': '1610612766',
            'CLE': '1610612739',
            'LAC': '1610612746',
            'ATL': '1610612737',
            'MIA': '1610612748',
            'DAL': '1610612742',
            'DET': '1610612765',
            'MEM': '1610612763',
            'TOR': '1610612761',
            'CHI': '1610612741',
            'IND': '1610612754',
            'SAS': '1610612759',
            'HOU': '1610612745',
            'BRK': '1610612751',
            'BKN': '1610612751',
            'WAS': '1610612764',
            'GSW': '1610612744',
            'PHI': '1610612755',
            'UTA': '1610612762',
            'POR': '1610612757',
            'NOP': '1610612740'}
    master.to_csv(directory+'vs_all_logs.csv')
    
    print(year)


# In[ ]:





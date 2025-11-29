import pandas as pd
import requests 
import time
url='https://stats.nba.com/stats/leaguedashplayerbiostats?College=&Conference=&Country=&DateFrom=&DateTo=&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PerMode=PerGame&Period=0&PlayerExperience=&PlayerPosition=F&Season=2025-26&SeasonSegment=&SeasonType=Regular%20Season&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='

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
frame = pull_data(url)
print(frame.columns)
print(frame)


#!/usr/bin/env python
# coding: utf-8

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import numpy as np
import sys
# The nba_api import is kept though the logic using it (process_player_ids)
# is downstream from the pulled code and not included here,
# but it's part of your original file structure.
from nba_api.stats.endpoints import commonallplayers 

# Configuration (Minimal inclusion for functionality)
class Config:
    """
    Central configuration class for the scraper.
    """
    # Set to True for playoffs data, False for regular season
    PLAYOFFS_MODE = False 
    
    # Current year to scrape (e.g., 2026 for 2025-26 season)
    CURRENT_YEAR = 2026
    # Current season in NBA API format
    CURRENT_SEASON = "2025-26"
    
    # File paths with dynamic playoff suffix
    @property
    def trail(self):
        return '_ps' if self.PLAYOFFS_MODE else ''

    @property
    def totals_path(self):
        # This path is used by the main function outside this block
        return f'totals{self.trail}.csv'

    # Hard-coded player IDs for players that might be missing
    SEARCH_DICT = {}

# Initialize config
config = Config()

# --- Utility Functions ---

def fetch_data(url):
    """Fetch data from a URL with proper error handling"""
    print(f"Fetching data from: {url}")
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
        response.encoding = 'utf-8'
        response.raise_for_status() # Check for HTTP errors
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"ERROR: Failed to fetch data: {e}")
        return None

# --- Robust Scraping Helpers ---

def get_stat_from_row(row_obj, stat_name, default_value="0"):
    """
    Extracts a stat value from a table row <tr> using its 'data-stat' attribute.
    """
    cell = row_obj.find(['td', 'th'], {'data-stat': stat_name})
    if cell:
        text_content = cell.text.strip()
        return text_content if text_content else default_value
    return default_value

def get_player_url_from_row(row_obj, stat_name="name_display", default_value="N/A"):
    """
    Extracts the player's b-ref URL from the row.
    """
    cell = row_obj.find(['td', 'th'], {'data-stat': stat_name})
    if cell and cell.a and 'href' in cell.a.attrs:
        return "https://www.basketball-reference.com" + cell.a['href']
    return default_value

# --- Core Logic Function (Modified to include 'Pos') ---

def pull_bref_data(totals=False):
    """
    Pull data from Basketball Reference using the robust 'data-stat' scraping method.
    This function has been modified to scrape the player Position ('Pos').
    """
    leagues = "playoffs" if config.PLAYOFFS_MODE else "leagues"
    frames = []

    # Determine URL and stat names based on data type
    if totals:
        url_pattern = f"https://www.basketball-reference.com/{leagues}/NBA_{{year}}_totals.html"
        data_type = "totals"
        stat_suffix = ""
        pts_stat = "pts"
    else:
        url_pattern = f"https://www.basketball-reference.com/{leagues}/NBA_{{year}}_per_poss.html"
        data_type = "per possession"
        stat_suffix = "_per_poss"
        pts_stat = "pts_per_poss"

    # Only scrape current year
    url = url_pattern.format(year=config.CURRENT_YEAR)
    html_content = fetch_data(url)
    if not html_content:
        print(f"Skipping {config.CURRENT_YEAR}, couldn't fetch data")
        return pd.DataFrame()

    # Parse HTML
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Find the main data table
    table_id = "totals_stats" if totals else "per_poss_stats"
    table = soup.find('table', id=table_id)
    if not table:
        table = soup.find('table') # Fallback to generic table search
    
    if not table:
        print(f"No table found for {config.CURRENT_YEAR}")
        return pd.DataFrame()

    # Find table body
    tbody = table.find('tbody')
    if not tbody:
        print(f"No table body found for {config.CURRENT_YEAR}")
        return pd.DataFrame()

    # Get all rows
    rows = tbody.find_all('tr')
    print(f"Found {len(rows)} player entries for {config.CURRENT_YEAR}")

    # Extract data
    data = []
    for row in rows:
        # Skip header rows that are sometimes repeated in the body
        if 'thead' in row.get('class', []):
            continue

        # Get values using robust helper functions
        player_name = get_stat_from_row(row, 'name_display', "N/A")
        
        # Skip non-player rows (like "Player" headers)
        if player_name == "N/A" or player_name == "Player":
            continue
            
        player_url = get_player_url_from_row(row, 'name_display')
        team_acronym = get_stat_from_row(row, 'team_name_abbr', "N/A")
        
        # --- ADDED: Extract Player Position ('Pos') ---
        position = get_stat_from_row(row, 'pos', "N/A")
        
        gp = get_stat_from_row(row, 'g', "0")
        mp = get_stat_from_row(row, 'mp', "0")

        # Get shooting stats with dynamic suffixes
        fg = get_stat_from_row(row, f'fg{stat_suffix}', "0")
        fga = get_stat_from_row(row, f'fga{stat_suffix}', "0")
        tp = get_stat_from_row(row, f'fg3{stat_suffix}', "0")
        tpa = get_stat_from_row(row, f'fg3a{stat_suffix}', "0")
        ft = get_stat_from_row(row, f'ft{stat_suffix}', "0")
        fta = get_stat_from_row(row, f'fta{stat_suffix}', "0")
        pts = get_stat_from_row(row, pts_stat, "0")

        data.append([
            player_name, player_url, team_acronym, config.CURRENT_YEAR, 
            position,  # <-- NEW COLUMN
            gp, mp, fga, fg, tpa, tp, fta, ft, pts
        ])

    # Create DataFrame - Note: 'Pos' column added here
    year_data = pd.DataFrame(
        data=data, 
        columns=['player', 'url', 'team', 'year', 'Pos', 'G', 'MP', 'FGA', 'FG', '3PA', '3P', 'FTA', 'FT', 'PTS']
    )

    if not year_data.empty:
        print(f"Successfully processed {len(year_data)} players for {config.CURRENT_YEAR} ({data_type})")
        if len(year_data) > 0:
            print(f"Sample player: {year_data.iloc[0]['player']} - {year_data.iloc[0]['team']} (Pos: {year_data.iloc[0]['Pos']})")
    else:
        print(f"WARNING: No data found for {config.CURRENT_YEAR}")
        if not config.PLAYOFFS_MODE:
             print("This may be normal if the regular season has not started.")
        return pd.DataFrame()

    frames.append(year_data)
    time.sleep(2)

    return pd.concat(frames) if frames else pd.DataFrame()


# Example of how you would call it:
if __name__ == "__main__":
    # Get the Totals data frame, which will now include 'Pos'
    totals_data_with_pos = pull_bref_data(totals=True)
    totals_data_with_pos['bref_id']= totals_data_with_pos['url'].str.split('/').str[-1]
    totals_data_with_pos['bref_id']= totals_data_with_pos['bref_id'].str.split('.').str[0]
    print(totals_data_with_pos.head())
    totals_data_with_pos=totals_data_with_pos[['Pos','bref_id']]
    index_master= pd.read_csv('https://raw.githubusercontent.com/gabriel1200/site_Data/refs/heads/master/index_master.csv')
    index_master=index_master[index_master.year==2026]

    player_index=index_master.merge(totals_data_with_pos,on='bref_id')
    player_index.to_csv('modern_index.csv',index=False)
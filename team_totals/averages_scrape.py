#!/usr/bin/env python
# coding: utf-8

"""
Optimized NBA Team Statistics Scraper
Usage: python team_average_scrape.py --start 2024 --end 2026 --playoffs
"""

from nba_api.stats.static import players, teams
import pandas as pd
import requests
import sys
import os
import time
from datetime import datetime
import argparse

# ============================================================================
# CONFIGURATION - Easy season specification
# ============================================================================

def parse_args():
    """Parse command line arguments for easy season specification"""
    parser = argparse.ArgumentParser(description='Scrape NBA team statistics')
    parser.add_argument('--start', type=int, default=2025, 
                        help='Start year (default: 2025)')
    parser.add_argument('--end', type=int, default=2026,
                        help='End year (default: 2026)')
    parser.add_argument('--playoffs', action='store_true',
                        help='Scrape playoffs data instead of regular season')
    parser.add_argument('--aggregation-start', type=int, default=2001,
                        help='Start year for aggregation (default: 2001)')
    parser.add_argument('--aggregation-end', type=int, default=2026,
                        help='End year for aggregation (default: 2026)')
    return parser.parse_args()

# For notebook use, set these manually if not using command line
class Config:
    def __init__(self):
        # Scraping configuration
        self.scrape_start_year = 2025
        self.scrape_end_year = 2026
        self.playoffs = True
        
        # Aggregation configuration (for combining historical data)
        self.agg_start_year = 2001
        self.agg_end_year = 2026

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def format_date_to_url(date):
    """Convert date from YYYYMMDD to MM%2FDD%2FYYYY format"""
    date_obj = datetime.strptime(str(date), '%Y%m%d')
    return date_obj.strftime('%m%%2F%d%%2F%Y')

def pull_data(url, max_retries=3, base_delay=2.0, timeout=30):
    """
    Pull data from NBA stats API with proper headers, retry logic, and error handling
    
    Args:
        url: API endpoint URL
        max_retries: Maximum number of retry attempts (default: 3)
        base_delay: Base delay between retries in seconds (default: 2.0)
        timeout: Request timeout in seconds (default: 30)
    
    Returns:
        DataFrame with API data
        
    Raises:
        Exception: After all retries exhausted with detailed error message
    """
    headers = {
        "Host": "stats.nba.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
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
    
    last_error = None
    
    for attempt in range(max_retries):
        try:
            # Make request with timeout
            response = requests.get(url, headers=headers, timeout=timeout)
            
            # Check HTTP status code
            if response.status_code == 429:
                # Rate limited - use exponential backoff
                retry_delay = base_delay * (2 ** attempt)
                print(f"  ⚠️  Rate limited (429). Waiting {retry_delay:.1f}s before retry {attempt + 1}/{max_retries}...")
                time.sleep(retry_delay)
                continue
                
            elif response.status_code == 404:
                raise Exception(f"API endpoint not found (404). URL may be invalid or data not available.")
                
            elif response.status_code == 403:
                raise Exception(f"Access forbidden (403). Headers may need updating or IP may be blocked.")
                
            elif response.status_code >= 500:
                retry_delay = base_delay * (2 ** attempt)
                print(f"  ⚠️  Server error ({response.status_code}). Waiting {retry_delay:.1f}s before retry {attempt + 1}/{max_retries}...")
                time.sleep(retry_delay)
                continue
                
            elif response.status_code != 200:
                raise Exception(f"HTTP {response.status_code}: {response.reason}")
            
            # Parse JSON response
            try:
                json_data = response.json()
            except ValueError as e:
                raise Exception(f"Invalid JSON response: {str(e)}")
            
            # Check for API error messages
            if "resultSets" not in json_data:
                if "message" in json_data:
                    raise Exception(f"API Error: {json_data['message']}")
                else:
                    raise Exception(f"Unexpected API response format: {list(json_data.keys())}")
            
            # Extract data from response
            if len(json_data["resultSets"]) == 0:
                raise Exception("API returned empty resultSets - no data available for this query")
                
            if len(json_data["resultSets"]) == 1:
                data = json_data["resultSets"][0]["rowSet"]
                columns = json_data["resultSets"][0]["headers"]
                
                if not data:
                    print(f"  ℹ️  Warning: API returned no data rows for this endpoint")
                    return pd.DataFrame(columns=columns)
                    
                df = pd.DataFrame.from_records(data, columns=columns)
            else:
                data = json_data["resultSets"]["rowSet"]
                columns = json_data["resultSets"]["headers"][1]['columnNames']
                
                if not data:
                    print(f"  ℹ️  Warning: API returned no data rows for this endpoint")
                    return pd.DataFrame(columns=columns)
                    
                df = pd.DataFrame.from_records(data, columns=columns)
            
            # Success - add delay and return
            time.sleep(1.2)
            print('returned data')
            return df
            
        except requests.exceptions.Timeout:
            last_error = f"Request timeout after {timeout}s"
            retry_delay = base_delay * (2 ** attempt)
            if attempt < max_retries - 1:
                print(f"  ⚠️  {last_error}. Retrying in {retry_delay:.1f}s... ({attempt + 1}/{max_retries})")
                time.sleep(retry_delay)
            
        except requests.exceptions.ConnectionError as e:
            last_error = f"Connection error: {str(e)}"
            retry_delay = base_delay * (2 ** attempt)
            if attempt < max_retries - 1:
                print(f"  ⚠️  {last_error}. Retrying in {retry_delay:.1f}s... ({attempt + 1}/{max_retries})")
                time.sleep(retry_delay)
                
        except requests.exceptions.RequestException as e:
            last_error = f"Request error: {str(e)}"
            retry_delay = base_delay * (2 ** attempt)
            if attempt < max_retries - 1:
                print(f"  ⚠️  {last_error}. Retrying in {retry_delay:.1f}s... ({attempt + 1}/{max_retries})")
                time.sleep(retry_delay)
                
        except Exception as e:
            last_error = str(e)
            # For non-network errors, only retry if it might be transient
            if any(x in last_error.lower() for x in ['timeout', 'connection', 'temporary', 'server error']):
                retry_delay = base_delay * (2 ** attempt)
                if attempt < max_retries - 1:
                    print(f"  ⚠️  {last_error}. Retrying in {retry_delay:.1f}s... ({attempt + 1}/{max_retries})")
                    time.sleep(retry_delay)
            else:
                # Non-retryable error, raise immediately
                raise Exception(f"API Error: {last_error}\nURL: {url[:100]}...")
    
    # All retries exhausted
    raise Exception(
        f"Failed after {max_retries} attempts. Last error: {last_error}\n"
        f"URL: {url[:100]}...\n"
        f"Possible causes:\n"
        f"  - Rate limiting (try increasing delays)\n"
        f"  - Invalid season/parameters\n"
        f"  - NBA.com API changes\n"
        f"  - Network connectivity issues"
    )

# ============================================================================
# NBA.COM SCRAPING
# ============================================================================

def pull_game_avg(start_year, end_year, ps=False, unit='Team'):
    """Pull team statistics from NBA.com for specified years"""
    stype = 'Playoffs' if ps else 'Regular%20Season'
    trail = 'ps' if ps else ''
    if unit.lower() == 'team':
        trail += '_team'
    
    year_frames = []
    shotcolumns = ['FGA_FREQUENCY', 'FGM', 'FGA', 'FG_PCT', 'EFG_PCT', 
                   'FG2A_FREQUENCY', 'FG2M', 'FG2A', 'FG2_PCT', 
                   'FG3A_FREQUENCY', 'FG3M', 'FG3A', 'FG3_PCT']
    
    for year in range(start_year, end_year):
        print(f"Scraping {year} {stype}...")
        season = f"{year - 1}-{str(year)[-2:]}"
        date = ''
        
        try:
            # Base stats
            url = f'https://stats.nba.com/stats/leaguedashteamstats?DateFrom={date}&DateTo={date}&LastNGames=0&LeagueID=00&MeasureType=Base&Month=0&OpponentTeamID=0&PaceAdjust=N&PerMode=Totals&Period=0&PlusMinus=N&Rank=N&Season={season}&SeasonType={stype}&TeamID=0'
            df = pull_data(url)
            
            # Advanced stats
            url2 = f'https://stats.nba.com/stats/leaguedashteamstats?DateFrom={date}&DateTo={date}&LastNGames=0&LeagueID=00&MeasureType=Advanced&Month=0&OpponentTeamID=0&PaceAdjust=N&PerMode=Totals&Period=0&PlusMinus=N&Rank=N&Season={season}&SeasonType={stype}&TeamID=0'
            df2 = pull_data(url2)
            
            # Tracking stats
            tracking_types = ['Passing', 'Drives', 'Possessions', 'Rebounding']
            tracking_dfs = []
            for pt_type in tracking_types:
                url = f'https://stats.nba.com/stats/leaguedashptstats?DateFrom={date}&DateTo={date}&LastNGames=0&LeagueID=00&Month=0&OpponentTeamID=0&PerMode=Totals&PlayerOrTeam={unit}&PtMeasureType={pt_type}&Season={season}&SeasonType={stype}&TeamID=0'
                tracking_dfs.append(pull_data(url))
            
            # Shot defense stats
            shot_dists = ["0-2%20Feet%20-%20Very%20Tight", "2-4%20Feet%20-%20Tight", 
                          "4-6%20Feet%20-%20Open", "6%2B%20Feet%20-%20Wide%20Open"]
            shot_terms = ['very_tight_', 'tight_', 'open_', 'wide_open_']
            
            shot_dfs = []
            opp_shot_dfs = []
            
            for shot, term in zip(shot_dists, shot_terms):
                # Team shots
                url = f'https://stats.nba.com/stats/leaguedashteamptshot?CloseDefDistRange={shot}&DateFrom={date}&DateTo={date}&LastNGames=0&LeagueID=00&PerMode=Totals&Season={season}&SeasonType={stype}&TeamID=0'
                shot_df = pull_data(url)
                shot_df.rename(columns={col: term + col for col in shotcolumns}, inplace=True)
                shot_dfs.append(shot_df)
                
                # Opponent shots
                url_opp = f'https://stats.nba.com/stats/leaguedashoppptshot?CloseDefDistRange={shot}&DateFrom={date}&DateTo={date}&LastNGames=0&LeagueID=00&PerMode=Totals&Season={season}&SeasonType={stype}&TeamID=0'
                opp_shot_df = pull_data(url_opp)
                opp_shot_df.rename(columns={col: 'opp_' + term + col for col in shotcolumns}, inplace=True)
                opp_shot_dfs.append(opp_shot_df)
            
            # Pull-up shots
            url11 = f'https://stats.nba.com/stats/leaguedashptstats?DateFrom={date}&DateTo={date}&LastNGames=0&LeagueID=00&PerMode=Totals&PlayerOrTeam=Team&PtMeasureType=PullUpShot&Season={season}&SeasonType={stype}&TeamID=0'
            df11 = pull_data(url11)
            shotcolumns2 = shotcolumns + ['EFG%']
            df11.rename(columns={col: 'pullup_' + col for col in shotcolumns2}, inplace=True)
            
            # Efficiency
            url12 = f'https://stats.nba.com/stats/leaguedashptstats?DateFrom={date}&DateTo={date}&LastNGames=0&LeagueID=00&PerMode=Totals&PlayerOrTeam=Team&PtMeasureType=Efficiency&Season={season}&SeasonType={stype}&TeamID=0'
            df12 = pull_data(url12)
            
            # Shot locations by zone
            url13 = f"https://stats.nba.com/stats/leaguedashteamshotlocations?DateFrom={date}&DateTo={date}&DistanceRange=By%20Zone&LastNGames=0&MeasureType=Base&Month=0&OpponentTeamID=0&PaceAdjust=N&PerMode=Totals&Period=0&PlusMinus=N&Rank=N&Season={season}&SeasonType={stype}&TeamID=0"
            df13 = pull_data(url13)
            zone_columns = ['TEAM_ID', 'TEAM_ABBREVIATION', 'RA_FGM', 'RA_FGA', 'RA_FG_PCT',
                           'ITP_FGM', 'ITP_FGA', 'ITP_FG_PCT', 'MID_FGM', 'MID_FGA', 'MID_FG_PCT',
                           'LEFT_CORNER_3_FGM', 'LEFT_CORNER_3_FGA', 'LEFT_CORNER_3_FG_PCT',
                           'RIGHT_CORNER_3_FGM', 'RIGHT_CORNER_3_FGA', 'RIGHT_CORNER_3_FG_PCT',
                           'ABOVE_BREAK_3_FGM', 'ABOVE_BREAK_3_FGA', 'ABOVE_BREAK_3_FG_PCT',
                           'BACKCOURT_FGM', 'BACKCOURT_FGA', 'BACKCOURT_FG_PCT',
                           'CORNER_3_FGM', 'CORNER_3_FGA', 'CORNER_3_FG_PCT']
            df13.columns = zone_columns
            
            # Defense tracking
            url14 = f"https://stats.nba.com/stats/leaguedashptteamdefend?DateFrom={date}&DateTo={date}&DefenseCategory=Less%20Than%206Ft&LastNGames=0&LeagueID=00&PerMode=Totals&Period=0&Season={season}&SeasonType={stype}&TeamID=0"
            df14 = pull_data(url14)
            
            # Shot locations by distance
            url15 = f"https://stats.nba.com/stats/leaguedashteamshotlocations?DateFrom={date}&DateTo={date}&DistanceRange=5ft%20Range&LastNGames=0&MeasureType=Base&Month=0&OpponentTeamID=0&PaceAdjust=N&PerMode=Totals&Period=0&PlusMinus=N&Rank=N&Season={season}&SeasonType={stype}&TeamID=0"
            df15 = pull_data(url15)
            df15.columns = ['TEAM_ID', 'TEAM_NAME', 'FGM_LT_5', 'FGA_LT_5', 'FGP_LT_5',
                           'FGM_5_9', 'FGA_5_9', 'FGP_5_9', 'FGM_10_14', 'FGA_10_14', 'FGP_10_14',
                           'FGM_15_19', 'FGA_15_19', 'FGP_15_19', 'FGM_20_24', 'FGA_20_24', 'FGP_20_24',
                           'FGM_25_29', 'FGA_25_29', 'FGP_25_29', 'FGM_30_34', 'FGA_30_34', 'FGP_30_34',
                           'FGM_35_39', 'FGA_35_39', 'FGP_35_39', 'FGM_40_PLUS', 'FGA_40_PLUS', 'FGP_40_PLUS']
            
            # Catch and shoot
            url16 = f'https://stats.nba.com/stats/leaguedashptstats?DateFrom={date}&DateTo={date}&LastNGames=0&LeagueID=00&PerMode=Totals&PlayerOrTeam={unit}&PtMeasureType=CatchShoot&Season={season}&SeasonType={stype}&TeamID=0'
            df16 = pull_data(url16)
            
            # Possessions for normalization
            url17 = f'https://stats.nba.com/stats/leaguedashteamstats?DateFrom={date}&DateTo={date}&LastNGames=0&LeagueID=00&MeasureType=Advanced&Month=0&OpponentTeamID=0&PaceAdjust=N&PerMode=Totals&Period=0&PlusMinus=N&Rank=N&Season={season}&SeasonType={stype}&TeamID=0'
            df17 = pull_data(url17)
            df17 = df17[['TEAM_ID', 'POSS']]
            df17.columns = ['TEAM_ID', 'team_poss']
            
            # Misc stats
            url22 = f'https://stats.nba.com/stats/leaguedashteamstats?DateFrom={date}&DateTo={date}&LastNGames=0&LeagueID=00&MeasureType=Misc&Month=0&OpponentTeamID=0&PaceAdjust=N&PerMode=Totals&Period=0&PlusMinus=N&Rank=N&Season={season}&SeasonType={stype}&TeamID=0'
            df22 = pull_data(url22)
            
            # Opponent stats
            url23 = f'https://stats.nba.com/stats/leaguedashteamstats?DateFrom={date}&DateTo={date}&LastNGames=0&LeagueID=00&MeasureType=Opponent&Month=0&OpponentTeamID=0&PaceAdjust=N&PerMode=PerGame&Period=0&PlusMinus=N&Rank=N&Season={season}&SeasonType={stype}&TeamID=0'
            df23 = pull_data(url23)
            
            # Merge all dataframes
            poss_map = dict(zip(df17['TEAM_ID'], df17['team_poss']))
            df['team_poss'] = df['TEAM_ID'].map(poss_map)
            
            all_frames = [df2] + tracking_dfs + shot_dfs + opp_shot_dfs + [df11, df12, df13, df14, df15, df16, df22, df23]
            
            for frame in all_frames:
                joined_columns = list(set(frame.columns) - set(df.columns))
                joined_columns.append('TEAM_ID')
                frame = frame[joined_columns]
                df = df.merge(frame, on='TEAM_ID', how='left').reset_index(drop=True)
            
            df['year'] = year
            df['playoffs'] = ps
            
            year_frames.append(df)
            df.to_csv(f"{year}{trail}_games.csv", index=False)
            
        except Exception as e:
            print(f"Error scraping {year}: {str(e)}")
            time.sleep(1)
            continue
    
    if year_frames:
        yeardata = pd.concat(year_frames)
        return yeardata
    return pd.DataFrame()

# ============================================================================
# PBPSTATS SCRAPING
# ============================================================================

def scrape_teams(start_year, end_year, ps=False):
    """Scrape team data from pbpstats API"""
    base_url = "https://api.pbpstats.com/get-totals/nba"
    stype = "Playoffs" if ps else "Regular Season"
    carry = "ps" if ps else ""
    
    params = {
        "SeasonType": stype,
        "Season": "",
        "Type": "Team",
    }
    
    all_data = []
    
    for season in range(start_year, end_year + 1):
        params["Season"] = f"{season}-{str(season + 1)[-2:]}"
        print(f"Scraping pbpstats {params['Season']} {stype}...")
        
        response = requests.get(base_url, params=params)
        data = response.json()
        
        df = pd.DataFrame(data['multi_row_table_data'])
        df['season'] = season
        df['year'] = season + 1
        year = season + 1
        df.to_csv(f"{year}{carry}.csv", index=False)
        
        time.sleep(2.5)
        all_data.append(df)
    
    df = pd.concat(all_data)
    df['TeamId'] = df['TeamId'].astype(int)
    
    for team_id in df['TeamId'].unique().tolist():
        teamdf = df[df['TeamId'] == team_id]
        
        try:
            olddf = pd.read_csv(f"{team_id}{carry}.csv")
            new_years = teamdf['year'].unique().tolist()
            olddf = olddf[~olddf.year.isin(new_years)]
            teamdf = pd.concat([olddf, teamdf])
            teamdf.drop_duplicates(inplace=True)
        except FileNotFoundError:
            pass
        
        teamdf.to_csv(f"{team_id}{carry}.csv", index=False)
    
    return df

def scrape_teams_vs(start_year, end_year, ps=False):
    """Scrape opponent team data from pbpstats API"""
    base_url = "https://api.pbpstats.com/get-totals/nba"
    stype = "Playoffs" if ps else "Regular Season"
    carry = "ps" if ps else ""
    
    params = {
        "SeasonType": stype,
        "Season": "",
        "Type": "Opponent",
    }
    
    all_data = []
    
    for season in range(start_year, end_year + 1):
        params["Season"] = f"{season}-{str(season + 1)[-2:]}"
        print(f"Scraping pbpstats opponent {params['Season']} {stype}...")
        
        response = requests.get(base_url, params=params)
        data = response.json()
        
        df = pd.DataFrame(data['multi_row_table_data'])
        df['season'] = season
        df['year'] = season + 1
        year = season + 1
        
        time.sleep(2.5)
        df.to_csv(f"{year}vs{carry}.csv", index=False)
        all_data.append(df)
    
    df = pd.concat(all_data)
    df['TeamId'] = df['TeamId'].astype(int)
    
    for team_id in df['TeamId'].unique().tolist():
        teamdf = df[df['TeamId'] == team_id]
        
        try:
            olddf = pd.read_csv(f"{team_id}vs{carry}.csv")
            new_years = teamdf['year'].unique().tolist()
            olddf = olddf[~olddf.year.isin(new_years)]
            teamdf = pd.concat([olddf, teamdf])
            teamdf.drop_duplicates(inplace=True)
        except FileNotFoundError:
            pass
        
        teamdf.to_csv(f"{team_id}vs{carry}.csv", index=False)
    
    return df

# ============================================================================
# PLAYTYPE DATA
# ============================================================================

def get_playtype_summary(ps=False):
    """Get playtype summary data"""
    url = 'https://raw.githubusercontent.com/gabriel1200/site_Data/refs/heads/master/teamplay_p.csv' if ps else 'https://raw.githubusercontent.com/gabriel1200/site_Data/refs/heads/master/teamplay.csv'
    
    df = pd.read_csv(url)
    cols_to_keep = ['Team', 'year', 'playtype', 'PPP', 'POSS', 'FREQ%']
    df = df[cols_to_keep]
    
    pivoted = df.pivot_table(
        index=['Team', 'year'],
        columns='playtype',
        values=['PPP', 'POSS', 'FREQ%']
    )
    
    pivoted.columns = [f"{stat}_{ptype}" for stat, ptype in pivoted.columns]
    pivoted = pivoted.reset_index()
    
    return pivoted

# ============================================================================
# AGGREGATION FUNCTIONS
# ============================================================================

def aggregate_team_averages(start_year, end_year, ps=False):
    """Aggregate team averages across years"""
    trail = 'ps' if ps else ''
    
    ortg = []
    orebperc = []
    total_points = []
    total_fg_miss = []
    total_oreb = []
    total_off_poss = []
    total_opp_dreb = []
    total_ftoreb = []
    total_fgoreb = []
    total_opp_fgdreb = []
    total_opp_ftdreb = []
    
    for year in range(start_year, end_year):
        try:
            df = pd.read_csv(f"{year}{trail}.csv")
            opp = pd.read_csv(f"{year}vs{trail}.csv")
            
            opp_def_rebounds = opp['DefRebounds'].sum()
            opp_def_rebounds_fg = opp['DefThreePtRebounds'].sum() + opp['DefTwoPtRebounds'].sum()
            opp_def_ftrebounds = opp['FTDefRebounds'].sum()
            
            total_opp_fgdreb.append(opp_def_rebounds_fg)
            total_opp_dreb.append(opp_def_rebounds)
            total_opp_ftdreb.append(opp_def_ftrebounds)
            
            df['OREB'] = df['OffRebounds']
            df['FGA'] = df['FG2A'] + df['FG3A']
            df['FGM'] = df['FG2M'] + df['FG3M']
            df['FG2_miss'] = df['FG2A'] - df['FG2M']
            df['FG3_miss'] = df['FG3A'] - df['FG3M']
            df['FG_miss'] = df['FG3_miss'] + df['FG2_miss']
            df['fg_OffRebounds'] = df['OffThreePtRebounds'] + df['OffTwoPtRebounds']
            
            ortg.append(df['Points'].sum() / df['OffPoss'].sum())
            orebperc.append(df['OREB'].sum() / (opp_def_rebounds + df['OREB'].sum()))
            total_points.append(df['Points'].sum())
            total_fg_miss.append(df['FG_miss'].sum())
            total_oreb.append(df['OREB'].sum())
            total_off_poss.append(df['OffPoss'].sum())
            total_fgoreb.append(df['fg_OffRebounds'].sum())
            total_ftoreb.append(df['FTOffRebounds'].sum())
        except FileNotFoundError:
            print(f"Warning: Data files for {year} not found")
            continue
    
    years = list(range(start_year, end_year))
    seasons = [f"{year-1}-{str(year)[-2:]}" for year in years]
    
    testdf = pd.DataFrame({
        'year': years,
        'season': seasons,
        'ortg': ortg,
        'oreb%': orebperc,
        'total_points': total_points,
        'total_fg_miss': total_fg_miss,
        'total_oreb': total_oreb,
        'total_opp_dreb': total_opp_dreb,
        'total_off_poss': total_off_poss,
        'total_opp_ft_dreb': total_opp_ftdreb,
        'total_ftoreb': total_ftoreb,
        'total_fgoreb': total_fgoreb,
        'total_opp_fgdreb': total_opp_fgdreb
    })
    
    filename = 'team_averages_ps.csv' if ps else 'team_averages.csv'
    testdf.to_csv(filename, index=False)
    print(f"Saved {filename}")
    
    return testdf

def create_master_files(start_year, end_year, ps=False):
    """Create master aggregated files"""
    trail = 'ps' if ps else ''
    
    shooting_columns = [
        "TeamId", "TEAM_NAME", "TeamAbbreviation", "GamesPlayed", "OffPoss", "DefPoss",
        "FG3M", "FG3A", "tight_FG3A", "tight_FG3M", "tight_FG3_PCT", "tight_FG3A_FREQUENCY",
        "very_tight_FG3A", "very_tight_FG3M", "very_tight_FG3_PCT", "very_tight_FG3A_FREQUENCY",
        "wide_open_FG3A_FREQUENCY", "wide_open_FG3A", "wide_open_FG3M", "wide_open_FG3_PCT",
        "open_FG3M", "open_FG3A_FREQUENCY", "open_FG3A", "open_FG3_PCT",
        "PULL_UP_FG3A", "PULL_UP_FG3M", "PULL_UP_FG3_PCT",
        "opp_very_tight_FG3A", "opp_very_tight_FG3M", "opp_very_tight_FG3_PCT", "opp_very_tight_FG3A_FREQUENCY",
        "opp_tight_FG3A_FREQUENCY", "opp_tight_FG3A", "opp_tight_FG3_PCT", "opp_tight_FG3M",
        "opp_open_FG3A", "opp_open_FG3M", "opp_open_FG3_PCT", "opp_open_FG3A_FREQUENCY",
        "opp_wide_open_FG3M", "opp_wide_open_FG3_PCT", "opp_wide_open_FG3A", "opp_wide_open_FG3A_FREQUENCY",
        "year"
    ]
    
    frames = []
    for year in range(start_year, end_year):
        try:
            df = pd.read_csv(f"{year}{trail}_team_totals.csv")
            df['year'] = year
            frames.append(df)
        except FileNotFoundError:
            print(f"Warning: {year}{trail}_team_totals.csv not found")
            continue
    
    if frames:
        master = pd.concat(frames)
        master.to_csv(f'all_teamyears{("_" + trail) if trail else ""}.csv', index=False)
        
        shooting = master[shooting_columns]
        shooting.to_csv(f'team_threes{("_" + trail) if trail else ""}.csv', index=False)
        
        # Copy to web app directory if it exists
        if os.path.exists('../../web_app/data'):
            shooting.to_csv(f'../../web_app/data/team_threes{("_" + trail) if trail else ""}.csv', index=False)
        
        print(f"Created master files for {trail if trail else 'regular season'}")
    else:
        print(f"No data found to create master files for {start_year}-{end_year}")

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution function"""
    # Try to parse command line arguments, fall back to Config if in notebook
    try:
        args = parse_args()
        config = Config()
        config.scrape_start_year = args.start
        config.scrape_end_year = args.end
        config.playoffs = args.playoffs
        config.agg_start_year = args.aggregation_start
        config.agg_end_year = args.aggregation_end
    except:
        # Running in notebook, use Config defaults
        config = Config()
    
    print("=" * 70)
    print(f"NBA Team Statistics Scraper")
    print(f"Scraping: {config.scrape_start_year}-{config.scrape_end_year}")
    print(f"Season Type: {'Playoffs' if config.playoffs else 'Regular Season'}")
    print("=" * 70)
    
    # Step 1: Scrape NBA.com data
    print("\n[1/5] Scraping NBA.com statistics...")
    df = pull_game_avg(config.scrape_start_year, config.scrape_end_year, 
                       unit='Team', ps=config.playoffs)
    
    # Step 2: Scrape pbpstats data
    print("\n[2/5] Scraping pbpstats team data...")
    scrape_teams(config.scrape_start_year - 1, config.scrape_end_year - 1, 
                ps=config.playoffs)
    
    print("\n[3/5] Scraping pbpstats opponent data...")
    scrape_teams_vs(config.scrape_start_year - 1, config.scrape_end_year - 1, 
                   ps=config.playoffs)
    
    # Step 3: Process four factors (requires fourfactors module)
    print("\n[4/5] Processing four factors data...")
    try:
        from fourfactors import four_factors_data
        
        trail = 'ps' if config.playoffs else ''
        
        for year in range(config.scrape_start_year, config.scrape_end_year):
            try:
                pbp = pd.read_csv(f"{year}{trail}.csv")
                pbpvs = pd.read_csv(f"{year}vs{trail}.csv")
                
                pbp = four_factors_data(pbp, pbpvs, year, ps=config.playoffs)
                pbpvs = four_factors_data(pbpvs, pbp, year, ps=config.playoffs)
                
                columns = ['ft_factor', 'oreb_factor', 'turnover_factor', '2shooting_factor',
                          '3shooting_factor', 'rimfactor', 'nonrim2factor', 'morey_factor', 'TeamId']
                oppcolumns = {c: 'opp_' + c for c in columns if c != 'TeamId'}
                
                vsframe = pbpvs[columns].reset_index(drop=True)
                vsframe.rename(columns=oppcolumns, inplace=True)
                
                pbp = pbp.merge(vsframe, on='TeamId')
                pbp['OffMinutes'] = (pbp['SecondsPerPossOff'] * pbp['OffPoss']) / 60
                pbp['DefMinutes'] = (pbp['SecondsPerPossDef'] * pbp['DefPoss']) / 60
                pbp['OPace'] = 48 * (pbp['OffPoss']) / (2 * pbp['OffMinutes'])
                pbp['DPace'] = 48 * (pbp['DefPoss']) / (2 * pbp['DefMinutes'])
                
                pbp['o_rating'] = 100 * pbp['Points'] / pbp['OffPoss']
                pbp['d_rating'] = 100 * pbp['OpponentPoints'] / pbp['DefPoss']
                pbp['3p_rate'] = 100 * pbp['FG3A'] / pbp['FGA']
                pbp['TEAM_ID'] = pbp['TeamId']
                
                if year >= 2014:
                    nba = pd.read_csv(f"{year}{trail}_team_games.csv")
                    nba['opp_3p_rate'] = 100 * nba['OPP_FG3A'] / nba['OPP_FGA']
                    keepcol = [col for col in nba.columns if col not in pbp.columns]
                    keepcol.append('TEAM_ID')
                    nba = nba[keepcol]
                    
                    total = pbp.merge(nba, on='TEAM_ID')
                    
                    # Merge playtype data
                    playtype_df = get_playtype_summary(config.playoffs)
                    playtype_df = playtype_df[playtype_df['year'] == year]
                    
                    if 'TeamAbbreviation' in total.columns:
                        playtype_df.rename(columns={'Team': 'TeamAbbreviation'}, inplace=True)
                        total = total.merge(playtype_df, how='left', on=['TeamAbbreviation', 'year'])
                else:
                    total = pbp.reset_index()
                
                total.rename(columns={'3SecondViolations': 'ThreeSecondViolations'}, inplace=True)
                total.to_csv(f"{year}{trail}_team_totals.csv", index=False)
                print(f"  Processed {year}")
            except FileNotFoundError as e:
                print(f"  Skipping {year}: Required files not found ({e})")
            except Exception as e:
                print(f"  Error processing {year}: {str(e)}")
    except ImportError:
        print("  Warning: fourfactors module not found. Skipping four factors calculation.")
        print("  You can still use the scraped data, but advanced metrics won't be calculated.")
    
    # Step 4: Aggregate team averages
    print("\n[5/5] Aggregating team averages...")
    aggregate_team_averages(config.agg_start_year, config.agg_end_year, ps=config.playoffs)
    
    # Step 5: Create master files
    print("\n[6/6] Creating master aggregated files...")
    create_master_files(config.agg_start_year, config.agg_end_year, ps=config.playoffs)
    
    print("\n" + "=" * 70)
    print("Scraping complete!")
    print("=" * 70)

if __name__ == "__main__":
    main()


# ============================================================================
# NOTEBOOK-FRIENDLY EXECUTION
# For running in Jupyter notebooks, use this instead of main()
# ============================================================================

def notebook_run(scrape_start=2025, scrape_end=2026, playoffs=True, 
                agg_start=2001, agg_end=2026):
    """
    Convenience function for notebook execution
    
    Args:
        scrape_start: First year to scrape (e.g., 2025 for 2024-25 season)
        scrape_end: Last year to scrape (exclusive)
        playoffs: True for playoffs, False for regular season
        agg_start: First year for aggregation
        agg_end: Last year for aggregation
    
    Example:
        # Scrape 2024-25 playoffs
        notebook_run(scrape_start=2025, scrape_end=2026, playoffs=True)
        
        # Scrape multiple regular seasons
        notebook_run(scrape_start=2023, scrape_end=2026, playoffs=False)
    """
    config = Config()
    config.scrape_start_year = scrape_start
    config.scrape_end_year = scrape_end
    config.playoffs = playoffs
    config.agg_start_year = agg_start
    config.agg_end_year = agg_end
    
    # Temporarily replace sys.argv to prevent argparse errors in notebooks
    import sys
    old_argv = sys.argv
    sys.argv = ['notebook']
    
    try:
        print("=" * 70)
        print(f"NBA Team Statistics Scraper")
        print(f"Scraping: {config.scrape_start_year}-{config.scrape_end_year}")
        print(f"Season Type: {'Playoffs' if config.playoffs else 'Regular Season'}")
        print("=" * 70)
        
        # Step 1: Scrape NBA.com data
        print("\n[1/5] Scraping NBA.com statistics...")
        df = pull_game_avg(config.scrape_start_year, config.scrape_end_year, 
                          unit='Team', ps=config.playoffs)
        
        # Step 2: Scrape pbpstats data
        print("\n[2/5] Scraping pbpstats team data...")
        scrape_teams(config.scrape_start_year - 1, config.scrape_end_year - 1, 
                    ps=config.playoffs)
        
        print("\n[3/5] Scraping pbpstats opponent data...")
        scrape_teams_vs(config.scrape_start_year - 1, config.scrape_end_year - 1, 
                       ps=config.playoffs)
        
        # Step 3: Process four factors
        print("\n[4/5] Processing four factors data...")
        try:
            from fourfactors import four_factors_data
            
            trail = 'ps' if config.playoffs else ''
            
            for year in range(config.scrape_start_year, config.scrape_end_year):
                try:
                    pbp = pd.read_csv(f"{year}{trail}.csv")
                    pbpvs = pd.read_csv(f"{year}vs{trail}.csv")
                    
                    pbp = four_factors_data(pbp, pbpvs, year, ps=config.playoffs)
                    pbpvs = four_factors_data(pbpvs, pbp, year, ps=config.playoffs)
                    
                    columns = ['ft_factor', 'oreb_factor', 'turnover_factor', '2shooting_factor',
                              '3shooting_factor', 'rimfactor', 'nonrim2factor', 'morey_factor', 'TeamId']
                    oppcolumns = {c: 'opp_' + c for c in columns if c != 'TeamId'}
                    
                    vsframe = pbpvs[columns].reset_index(drop=True)
                    vsframe.rename(columns=oppcolumns, inplace=True)
                    
                    pbp = pbp.merge(vsframe, on='TeamId')
                    pbp['OffMinutes'] = (pbp['SecondsPerPossOff'] * pbp['OffPoss']) / 60
                    pbp['DefMinutes'] = (pbp['SecondsPerPossDef'] * pbp['DefPoss']) / 60
                    pbp['OPace'] = 48 * (pbp['OffPoss']) / (2 * pbp['OffMinutes'])
                    pbp['DPace'] = 48 * (pbp['DefPoss']) / (2 * pbp['DefMinutes'])
                    
                    pbp['o_rating'] = 100 * pbp['Points'] / pbp['OffPoss']
                    pbp['d_rating'] = 100 * pbp['OpponentPoints'] / pbp['DefPoss']
                    pbp['3p_rate'] = 100 * pbp['FG3A'] / pbp['FGA']
                    pbp['TEAM_ID'] = pbp['TeamId']
                    
                    if year >= 2014:
                        nba = pd.read_csv(f"{year}{trail}_team_games.csv")
                        nba['opp_3p_rate'] = 100 * nba['OPP_FG3A'] / nba['OPP_FGA']
                        keepcol = [col for col in nba.columns if col not in pbp.columns]
                        keepcol.append('TEAM_ID')
                        nba = nba[keepcol]
                        
                        total = pbp.merge(nba, on='TEAM_ID')
                        
                        playtype_df = get_playtype_summary(config.playoffs)
                        playtype_df = playtype_df[playtype_df['year'] == year]
                        
                        if 'TeamAbbreviation' in total.columns:
                            playtype_df.rename(columns={'Team': 'TeamAbbreviation'}, inplace=True)
                            total = total.merge(playtype_df, how='left', on=['TeamAbbreviation', 'year'])
                    else:
                        total = pbp.reset_index()
                    
                    total.rename(columns={'3SecondViolations': 'ThreeSecondViolations'}, inplace=True)
                    total.to_csv(f"{year}{trail}_team_totals.csv", index=False)
                    print(f"  Processed {year}")
                except FileNotFoundError as e:
                    print(f"  Skipping {year}: Required files not found")
                except Exception as e:
                    print(f"  Error processing {year}: {str(e)}")
        except ImportError:
            print("  Warning: fourfactors module not found. Skipping four factors calculation.")
        
        # Step 4: Aggregate team averages
        print("\n[5/5] Aggregating team averages...")
        aggregate_team_averages(config.agg_start_year, config.agg_end_year, ps=config.playoffs)
        
        # Step 5: Create master files
        print("\n[6/6] Creating master aggregated files...")
        create_master_files(config.agg_start_year, config.agg_end_year, ps=config.playoffs)
        
        print("\n" + "=" * 70)
        print("Scraping complete!")
        print("=" * 70)
        
        return df
        
    finally:
        sys.argv = old_argv


# ============================================================================
# USAGE EXAMPLES
# ============================================================================

"""
COMMAND LINE USAGE:
-------------------
# Scrape 2024-25 playoffs
python team_average_scrape.py --start 2025 --end 2026 --playoffs

# Scrape 2023-24 and 2024-25 regular season
python team_average_scrape.py --start 2023 --end 2026

# Custom aggregation range
python team_average_scrape.py --start 2025 --end 2026 --playoffs --aggregation-start 2015


NOTEBOOK USAGE:
---------------
# Scrape 2024-25 playoffs
notebook_run(scrape_start=2025, scrape_end=2026, playoffs=True)

# Scrape multiple regular seasons
notebook_run(scrape_start=2023, scrape_end=2026, playoffs=False)

# Custom aggregation range
notebook_run(scrape_start=2025, scrape_end=2026, playoffs=True, 
            agg_start=2015, agg_end=2026)


QUICK CONFIG (edit Config class):
----------------------------------
config = Config()
config.scrape_start_year = 2025  # Change this
config.scrape_end_year = 2026    # And this
config.playoffs = True           # And this
main()
"""
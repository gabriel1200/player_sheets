import os
import pandas as pd
from nba_api.stats.static import teams

def get_dates(start_year, end_year, ps=False):
    trail = 'ps' if ps else ''
    dates = []
    for year in range(start_year, end_year):
        for team in teams.get_teams():
            team_id = team['id']
            base = 'https://raw.githubusercontent.com/gabriel1200/shot_data/refs/heads/master/team/'
            path = f"{base}{year}{trail}/{team_id}.csv"
            try:
                df = pd.read_csv(path, usecols=['PLAYER_ID', 'TEAM_ID', 'HTM', 'VTM', 'GAME_DATE', 'GAME_ID'])
                df.sort_values(by='GAME_DATE', inplace=True)
                df.drop_duplicates(inplace=True)
                df['year'] = year
                dates.append(df)
            except Exception:
                continue
    return pd.concat(dates, ignore_index=True) if dates else pd.DataFrame()

def build_all_games(year=2026, ps=False):
    trail = 'ps' if ps else ''
    year_file = f'year_files/{year}{trail}_games.csv'
    
    if not os.path.exists(year_file):
        print(f"File not found: {year_file}")
        return

    print(f"\n--- Processing all_{year}{trail} ---")
    os.makedirs('all_games', exist_ok=True)
    os.makedirs(f'{year}', exist_ok=True)

    # 1. Load Schedule / Dateframe
    print("Loading schedule & master index...")
    dateframe = get_dates(year, year + 1, ps=ps)
    
    url_index = 'https://raw.githubusercontent.com/gabriel1200/site_Data/refs/heads/master/index_master_ps.csv' if ps else 'https://raw.githubusercontent.com/gabriel1200/site_Data/refs/heads/master/index_master.csv'
    index_master = pd.read_csv(url_index)
    index_master = index_master[index_master.team != 'TOT']
    index_master['team_id'] = index_master['team_id'].astype(int)
    index_master['nba_id'] = index_master['nba_id'].astype(int)
    year_index = index_master[index_master['year'] == year].reset_index(drop=True)

    game_dates = pd.read_csv('https://raw.githubusercontent.com/gabriel1200/shot_data/refs/heads/master/game_dates.csv')
    team_id_map = game_dates[['team', 'TEAM_ID']].drop_duplicates().set_index('team')['TEAM_ID'].to_dict()
    game_dates['year'] = game_dates['season'].apply(lambda x: int(x.split('-')[0]) + 1)
    game_dates['OPP_TEAM_ID'] = game_dates['opp_team'].map(team_id_map)

    # 2. Process Dates & Align Game IDs
    print(f"Loading corrected year file: {year_file}")
    df = pd.read_csv(year_file)
    team_map = dict(zip(df['TEAM_ID'], df['TEAM_ABBREVIATION']))

    games_collected = []

    for date in df['date'].unique().tolist():
        datedf = df[df.date == date].reset_index(drop=True)
        datedf = datedf.drop_duplicates(subset=['PLAYER_ID', 'date'])

        gameframe = dateframe[dateframe['GAME_DATE'] == date].reset_index(drop=True)
        gameframe.rename(columns={'GAME_DATE': 'date'}, inplace=True)

        to_merge = gameframe[['TEAM_ID', 'GAME_ID', 'date', 'year']].drop_duplicates().reset_index(drop=True)
        save_frame = datedf.merge(to_merge, on=['TEAM_ID', 'date', 'year'], how='left')
        save_frame.drop_duplicates(inplace=True)

        # Fallback Level 1: Match by player/date directly in gameframe
        if save_frame['GAME_ID'].isna().any():
            missing = save_frame[save_frame['GAME_ID'].isna()].reset_index(drop=True)
            save_frame.dropna(subset=['GAME_ID'], inplace=True)
            missing.drop(columns=['GAME_ID', 'TEAM_ID', 'TEAM_ABBREVIATION'], inplace=True)
            missing = missing.merge(gameframe, on=['PLAYER_ID', 'year', 'date'], how='left')
            missing['TEAM_ABBREVIATION'] = missing['TEAM_ID'].map(team_map)
            save_frame = pd.concat([save_frame, missing], ignore_index=True)

        # Fallback Level 2, 3, 4: Sequential index_master team fallbacks for traded players
        for trade_idx in range(3):
            if not save_frame['GAME_ID'].isna().any():
                break
            missing = save_frame[save_frame['GAME_ID'].isna()].reset_index(drop=True)
            missing.drop(columns=['GAME_ID'], inplace=True)
            save_frame.dropna(subset=['GAME_ID'], inplace=True)
            
            missed = []
            for missed_player in missing['PLAYER_ID'].unique().tolist():
                missing_frame = missing[missing.PLAYER_ID == missed_player].reset_index(drop=True)
                temp_index = year_index[year_index.nba_id == missed_player].reset_index(drop=True)
                
                if len(temp_index) > trade_idx:
                    t_id = temp_index.iloc[trade_idx]['team_id']
                    t_abbr = temp_index.iloc[trade_idx]['team']
                    missing_frame['TEAM_ID'] = int(t_id)
                    missing_frame['TEAM_ABBREVIATION'] = t_abbr
                    missing_frame = missing_frame.merge(to_merge, on=['TEAM_ID', 'date', 'year'], how='left')
                
                missed.append(missing_frame)

            if missed:
                save_frame = pd.concat([save_frame] + missed, ignore_index=True)

        # Drop any remaining unmapped rows
        save_frame.dropna(subset=['GAME_ID'], inplace=True)
        save_frame.drop_duplicates(inplace=True)
        save_frame['GAME_ID'] = save_frame['GAME_ID'].astype(int)

        # Save individual game CSVs
        for game_id in save_frame['GAME_ID'].unique():
            gameid_frame = save_frame[save_frame['GAME_ID'] == game_id].reset_index(drop=True)
            gameid_frame.to_csv(f'{year}/{game_id}.csv', index=False)
            games_collected.append(gameid_frame)

    # 3. Add Opponent Data & Final Export
    print("Finalizing master tables...")
    all_games = pd.concat(games_collected, ignore_index=True)

    year_dates = game_dates[game_dates.year == year][['GAME_ID', 'TEAM_ID', 'opp_team', 'OPP_TEAM_ID']].copy()
    year_dates.rename(columns={'opp_team': 'opp_team_abbr', 'OPP_TEAM_ID': 'opp_team_id'}, inplace=True)

    all_games = all_games.merge(year_dates, how='left', on=['GAME_ID', 'TEAM_ID'])

    csv_out = f'all_games/all_{year}{trail}.csv'
    parquet_out = f'all_games/all_{year}{trail}.parquet'

    all_games.to_csv(csv_out, index=False)
    all_games.to_parquet(parquet_out, index=False)
    print(f"[✓] Generated {csv_out} and {parquet_out} with shape {all_games.shape}!")

if __name__ == '__main__':
    # Run for 2026 Regular Season
    for year in range(2024,2025):
        build_all_games(year,ps=False)
        build_all_games(year,ps=True)
        

    #build_all_games(2023, ps=False)
    
    # Run for 2026 Playoffs
    #build_all_games(2023, ps=True)

    #build_all_games(2022, ps=False)
    
    # Run for 2026 Playoffs
    #build_all_games(2022, ps=True)

    #build_all_games(2021, ps=False)
    
    # Run for 2026 Playoffs
    #build_all_games(2021, ps=True)

    #build_all_games(2020, ps=True)

    #build_all_games(2020, ps=True)

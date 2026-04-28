#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import requests
import os
from nba_api.stats.static import teams
import io

def get_dates(start_year=2025, end_year=2026):
    dates = []
    for year in range(start_year, end_year):
        for team in teams.get_teams():
            team_id = team['id']
            path = f'../../shot_data/team/{year}ps/{team_id}.csv'
            if os.path.exists(path):
                try:
                    df = pd.read_csv(path)
                    required_cols = {'TEAM_ID', 'HTM', 'VTM', 'GAME_DATE', 'GAME_ID'}
                    if required_cols.issubset(df.columns):
                        df = df[['TEAM_ID', 'HTM', 'VTM', 'GAME_DATE', 'GAME_ID']]
                        df['year'] = year
                        df.drop_duplicates(inplace=True)
                        dates.append(df)
                except Exception:
                    continue
    return pd.concat(dates).drop_duplicates(subset='GAME_ID') if dates else pd.DataFrame()

def fetch_team_game_csvs(dateframe, save_dir='team_game_data'):
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
    all_game_data = []
    for _, row in dateframe.iterrows():
        year = int(row['year'])
        game_id = row['GAME_ID']
        url = f'https://raw.githubusercontent.com/gabriel1200/player_sheets/refs/heads/master/teamgame_report/games/{year}/{game_id}.csv'
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            if len(response.text.strip()) == 0:
                continue
            df = pd.read_csv(io.StringIO(response.text))
            df['GAME_ID'] = game_id
            df['date'] = row['GAME_DATE']
            df['HTM'] = row['HTM']
            df['VTM'] = row['VTM']
            df['year'] = year
            all_game_data.append(df)
            with open(os.path.join(save_dir, f'{year}_{game_id}.csv'), 'w', encoding='utf-8') as f:
                f.write(response.text)
        except Exception:
            print('failed')
            continue
    return pd.concat(all_game_data, ignore_index=True) if all_game_data else pd.DataFrame()

def process_and_save_team_series_data(df):
    df.rename(columns={'GAME_DATE': 'date'}, inplace=True)
    home = df[df.HTM == df.TEAM_ABBREVIATION].copy()
    away = df[df.VTM == df.TEAM_ABBREVIATION].copy()
    none = df[df.HTM.isna()].copy().reset_index(drop=True)

    home.drop(columns='HTM', inplace=True)
    home.rename(columns={'VTM': 'opp_team'}, inplace=True)
    away.drop(columns='VTM', inplace=True)
    away.rename(columns={'HTM': 'opp_team'}, inplace=True)

    home.drop_duplicates(inplace=True)
    away.drop_duplicates(inplace=True)

    frames = [home, away]
    if not none.empty:
        frames.append(none)

    df = pd.concat(frames, ignore_index=True)
    df.dropna(subset=['opp_team'], inplace=True)

    df['team'] = df['TEAM_ABBREVIATION']
    teammap = dict(zip(df['TEAM_ABBREVIATION'], df['TEAM_ID']))
    df['opp_id'] = df['opp_team'].map(teammap)
    df.sort_values(by='date', inplace=True)

    df['series_key'] = df['team'] + '_' + df['opp_team'] + '_' + df['year'].astype(str)

    # Save series index
    series_index = df[['series_key', 'team', 'opp_team', 'TEAM_ID', 'opp_id', 'year']].drop_duplicates()
    series_index_path = 'series_index_teams.csv'
    if os.path.exists(series_index_path):
        existing = pd.read_csv(series_index_path)
        combined = pd.concat([existing, series_index], ignore_index=True)
        combined.drop_duplicates(subset=['series_key', 'team', 'TEAM_ID', 'opp_team', 'opp_id', 'year'], inplace=True)
        combined.to_csv(series_index_path, index=False)
    else:
        series_index.to_csv(series_index_path, index=False)

    # Save each series
    series_dir = '../series/team'
    os.makedirs(series_dir, exist_ok=True)
    for key, group in df.groupby('series_key'):
        safe_key = key.replace('/', '-').replace('\\', '-')
        group.to_csv(os.path.join(series_dir, f'{safe_key}.csv'), index=False)
    return df

def run_team_series_pipeline(start_year=2024, end_year=2025):
    dates = get_dates(start_year, end_year)
    print(dates)
    if dates.empty:
        return None
    raw_df = fetch_team_game_csvs(dates)
    if raw_df.empty:
        return None
    return process_and_save_team_series_data(raw_df)

if __name__ == "__main__":
    for year in range(2026, 2027):
        run_team_series_pipeline(year, year + 1)


# In[ ]:





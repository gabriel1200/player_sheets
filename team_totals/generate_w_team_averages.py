import pandas as pd
import requests
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- 1. SETUP SESSION WITH CUSTOM HEADERS ---
def get_session():
    session = requests.Session()
    
    # Injecting your specific browser headers
    session.headers.update({
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
    })

    retry = Retry(
        total=5, 
        backoff_factor=1, 
        status_forcelist=[429, 500, 502, 503, 504]
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session

# --- 2. WNBA SCRAPE & SUMMARY FUNCTION ---
def generate_wteam_averages(start_year, end_year, ps=False):
    """
    Scrapes pbpstats WNBA data and generates wteam_averages.csv.
    Identical logic to the NBA team_averages.csv portion of your file.
    """
    http = get_session()
    base_url = "https://api.pbpstats.com/get-totals/wnba"
    stype = "Playoffs" if ps else "Regular Season"
    trail = 'ps' if ps else ''
    
    all_summary_stats = []

    for year in range(start_year, end_year + 1):
        # pbpstats uses the "2025-26" format for its Season parameter
        season_str = f"{year}"
        print(f"Scraping WNBA {stype}: {season_str}")

        try:
            # 1. Pull Team Totals
            t_res = http.get(base_url, params={"Season": season_str, "SeasonType": stype, "Type": "Team"}, timeout=15)
            # 2. Pull Opponent Totals (to get Opponent Defensive Rebounds for OREB%)
            o_res = http.get(base_url, params={"Season": season_str, "SeasonType": stype, "Type": "Opponent"}, timeout=15)
            
            t_data = t_res.json().get('multi_row_table_data', [])
            o_data = o_res.json().get('multi_row_table_data', [])

            if not t_data or not o_data:
                print(f"No data found for {season_str}")
                continue

            df_t = pd.DataFrame(t_data)
            df_o = pd.DataFrame(o_data)

            # 3. Calculate League Aggregates (NBA logic from Cell 4)
            points = df_t['Points'].sum()
            poss = df_t['OffPoss'].sum()
            oreb = df_t['OffRebounds'].sum()
            opp_dreb = df_o['DefRebounds'].sum()
            
            # Missing shots calculation: (2PA + 3PA) - (2PM + 3PM)
            fg_miss = (df_t['FG2A'] + df_t['FG3A']).sum() - (df_t['FG2M'] + df_t['FG3M']).sum()
            
            # Rebound sub-types
            fg_oreb = (df_t['OffThreePtRebounds'] + df_t['OffTwoPtRebounds']).sum()
            ft_oreb = df_t['FTOffRebounds'].sum()

            all_summary_stats.append({
                'year': year + 1,
                'season': season_str,
                'ortg': points / poss,
                'oreb%': oreb / (opp_dreb + oreb),
                'total_points': points,
                'total_fg_miss': fg_miss,
                'total_oreb': oreb,
                'total_opp_dreb': opp_dreb,
                'total_off_poss': poss,
                'total_ftoreb': ft_oreb,
                'total_fgoreb': fg_oreb
            })

            # Save raw files with 'w' prefix to avoid NBA collisions
            df_t.to_csv(f"{year+1}w{trail}.csv", index=False)
            df_o.to_csv(f"{year+1}vsw{trail}.csv", index=False)

        except Exception as e:
            print(f"Error processing {season_str}: {e}")
        
        time.sleep(2.5) # Compliance sleep

    # 4. Final Export
    summary_df = pd.DataFrame(all_summary_stats)
    out_file = f"wteam_averages{'_ps' if ps else ''}.csv"
    summary_df.to_csv(out_file, index=False)
    
    print(f"Successfully generated {out_file}")
    return summary_df

# Example run
generate_wteam_averages(2020, 2026, ps=False)
generate_wteam_averages(2020, 2025, ps=True)
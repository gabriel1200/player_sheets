"""
Backfill per-game "defense within 6 ft" columns (and optionally team_poss).

NOT YET RUN. See BACKFILL_TODO.md for why this exists.

Until 2026-09, url14 in teamgame_report_scrape.py had `DateFrom{date}=`
instead of `DateFrom={date}`, so leaguedashptteamdefend returned season-to-date
totals for every date instead of that day's numbers. Those columns were deleted
from every year file (repair_teamgames.py drop-columns --columns DEF6FT). This
script refetches them per date with the fixed URL and writes them back into:

    year_files/{year}{trail}_teamgames.csv       (the scrape's cache)
    year_files/all_games_{year}{trail}.csv/.parquet
    games/{year}/{GameId}.csv

--team-poss also fills team_poss for regular-season files, which was always
blank because url17 hardcoded SeasonType=Playoffs.

Resumable: dates whose rows already have the columns filled are skipped, and
the teamgames cache is saved every 10 dates.

    python backfill_def6ft.py --years 2014-2026                 # regular season
    python backfill_def6ft.py --years 2014-2026 --ps            # playoffs
    python backfill_def6ft.py --years 2026 --team-poss --dry-run

Roughly 1-1.5 s per request with the pacing below: ~2,100 regular-season and
~250 playoff dates, so plan on about an hour per pass (two with --team-poss).
"""
import argparse
import os
import time
from datetime import datetime

import pandas as pd
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
YEAR_FILES = os.path.join(HERE, 'year_files')
GAMES_DIR = os.path.join(HERE, 'games')

DEF6FT_COLUMNS = ['FREQ', 'FGM_LT_06', 'FGA_LT_06', 'LT_06_PCT', 'NS_LT_06_PCT', 'PLUSMINUS']

# Keep in sync with pull_data() in teamgame_report_scrape.ipynb.
HEADERS = {
    "Host": "stats.nba.com",
    "Connection": "keep-alive",
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
    "Dnt": "1",
    "Sec-Ch-Ua": '"Not=A?Brand";v="99", "Google Chrome";v="151", "Chromium";v="151"',
    "Sec-Ch-Ua-Mobile": "?1",
    "Sec-Ch-Ua-Platform": '"Android"',
    "User-Agent": "Mozilla/5.0 (Linux; Android 15; Pixel 9) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Mobile Safari/537.36",
    "Accept": "*/*",
    "Origin": "https://www.nba.com",
    "Sec-Fetch-Site": "same-site",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Dest": "empty",
    "Referer": "https://www.nba.com/",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-US,en;q=0.9",
}

DEF6FT_URL = (
    "https://stats.nba.com/stats/leaguedashptteamdefend?College=&Conference=&Country="
    "&DateFrom={date}&DateTo={date}&DefenseCategory=Less%20Than%206Ft&Division=&DraftPick=&DraftYear="
    "&GameSegment=&Height=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0"
    "&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&Season={season}&SeasonSegment="
    "&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight="
)
ADVANCED_URL = (
    "https://stats.nba.com/stats/leaguedashteamstats?College=&Conference=&Country="
    "&DateFrom={date}&DateTo={date}&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height="
    "&ISTRound=&LastNGames=0&LeagueID=00&Location=&MeasureType=Advanced&Month=0&OpponentTeamID=0&Outcome="
    "&PORound=&PaceAdjust=N&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N"
    "&Season={season}&SeasonSegment=&SeasonType={stype}&ShotClockRange=&StarterBench=&TeamID=0"
    "&VsConference=&VsDivision=&Weight="
)


def fetch(url, retries=3, pause=1.2):
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            r.raise_for_status()
            rs = r.json()['resultSets'][0]
            time.sleep(pause)
            return pd.DataFrame(rs['rowSet'], columns=rs['headers'])
        except Exception as e:
            print(f"  attempt {attempt} failed: {e}")
            if attempt == retries:
                raise
            time.sleep(pause * attempt * 2)


def url_date(yyyymmdd):
    return datetime.strptime(str(int(yyyymmdd)), '%Y%m%d').strftime('%m%%2F%d%%2F%Y')


def backfill_year(year, ps, team_poss, dry_run):
    trail = 'ps' if ps else ''
    stype = 'Playoffs' if ps else 'Regular%20Season'
    season = f"{year - 1}-{str(year)[-2:]}"
    cache_path = os.path.join(YEAR_FILES, f"{year}{trail}_teamgames.csv")
    if not os.path.exists(cache_path):
        print(f"{year}{trail}: no teamgames file, skipping")
        return
    tg = pd.read_csv(cache_path, low_memory=False)
    tg['date'] = tg['date'].astype(int)

    fill_cols = list(DEF6FT_COLUMNS) + (['team_poss'] if team_poss and not ps else [])
    for c in fill_cols:
        if c not in tg.columns:
            tg[c] = pd.NA
    done = tg.groupby('date')[fill_cols].apply(lambda g: g.notna().all().all())
    todo = sorted(done.index[~done])
    print(f"{year}{trail}: {len(todo)} of {done.size} dates to fetch")
    if dry_run or not todo:
        return

    for n, d in enumerate(todo, 1):
        sel = tg['date'] == d
        frame = fetch(DEF6FT_URL.format(date=url_date(d), season=season, stype=stype))
        frame = frame[['TEAM_ID', *DEF6FT_COLUMNS]].set_index('TEAM_ID')
        if 'team_poss' in fill_cols:
            adv = fetch(ADVANCED_URL.format(date=url_date(d), season=season, stype=stype))
            frame = frame.join(adv[['TEAM_ID', 'POSS']].set_index('TEAM_ID').rename(columns={'POSS': 'team_poss'}))
        for c in fill_cols:
            tg.loc[sel, c] = tg.loc[sel, 'TEAM_ID'].map(frame[c]).values
        print(f"  {d}: {int(sel.sum())} teams")
        if n % 10 == 0:
            tg.to_csv(cache_path, index=False)
    tg.to_csv(cache_path, index=False)

    patch_published(year, trail, tg[['TEAM_ID', 'date', *fill_cols]])


def patch_published(year, trail, values):
    """Write the refreshed columns into all_games and the per-game files."""
    cols = [c for c in values.columns if c not in ('TEAM_ID', 'date')]
    csv_path = os.path.join(YEAR_FILES, f"all_games_{year}{trail}.csv")
    pq_path = os.path.join(YEAR_FILES, f"all_games_{year}{trail}.parquet")
    ag = pd.read_parquet(pq_path)
    ag = ag.drop(columns=[c for c in cols if c in ag.columns]).merge(values, on=['TEAM_ID', 'date'], how='left')
    ag.to_parquet(pq_path, index=False)
    ag.to_csv(csv_path, index=False)

    folder = os.path.join(GAMES_DIR, str(year))
    by_game = ag.set_index(['GameId', 'TEAM_ID'])[cols]
    for gid in ag['GameId'].unique():
        path = os.path.join(folder, f"{gid}.csv")
        if not os.path.exists(path):
            continue
        g = pd.read_csv(path, low_memory=False)
        g = g.drop(columns=[c for c in cols if c in g.columns])
        g = g.join(by_game, on=['GameId', 'TEAM_ID'])
        g.to_csv(path, index=False)
    print(f"  patched {os.path.basename(pq_path)}, .csv and games/{year}/")


def parse_years(spec):
    if '-' in spec:
        a, b = spec.split('-')
        return list(range(int(a), int(b) + 1))
    return [int(y) for y in spec.split(',')]


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--years', required=True, help='e.g. 2014-2026 or 2025,2026')
    p.add_argument('--ps', action='store_true', help='playoff files instead of regular season')
    p.add_argument('--team-poss', action='store_true', help='also fill team_poss (regular season only)')
    p.add_argument('--dry-run', action='store_true', help='only report how many dates need fetching')
    args = p.parse_args()
    for year in parse_years(args.years):
        backfill_year(year, args.ps, args.team_poss, args.dry_run)


if __name__ == '__main__':
    main()

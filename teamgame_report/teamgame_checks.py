"""
Completeness checks for year_files/{year}{trail}_teamgames.csv.

Shared by teamgame_report_scrape.py (self-healing refetch of recent dates) and
repair_teamgames.py (one-off repairs and verification).
"""
import os

import pandas as pd

# A date scraped before stats.nba.com finishes processing tracking comes back
# partial: tracking endpoints return only some teams (blank after the left
# merge), or a finished game is missing from Base entirely. Every team that
# played has drives, passes and touches, so blanks there mean "not processed
# yet", not "zero". (Closest-defender buckets can legitimately be missing
# when a team took no shots in that bucket, so they're not used here.)
TRACKING_CHECK_COLS = ['DRIVES', 'PASSES_MADE', 'TOUCHES']

SHOT_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'shot_data', 'team')


def expected_team_dates(year, ps=False):
    """(GAME_DATE, TEAM_ID, GAME_ID) per team-game from shot_data, same source as get_dates()."""
    folder = os.path.join(SHOT_DATA_DIR, f"{year}{'ps' if ps else ''}")
    frames = []
    if os.path.isdir(folder):
        for f in os.listdir(folder):
            if f.endswith('.csv') and f[:-4].isdigit():
                frames.append(pd.read_csv(os.path.join(folder, f), usecols=['TEAM_ID', 'GAME_DATE', 'GAME_ID']))
    if not frames:
        return pd.DataFrame(columns=['TEAM_ID', 'GAME_DATE', 'GAME_ID'])
    return pd.concat(frames, ignore_index=True).drop_duplicates()


def incomplete_dates(saved, expected):
    """Saved dates with blank tracking columns or fewer teams than scheduled."""
    cols = [c for c in TRACKING_CHECK_COLS if c in saved.columns]
    blank = set(saved.loc[saved[cols].isna().any(axis=1), 'date']) if cols else set()
    exp = expected.groupby('GAME_DATE')['TEAM_ID'].nunique()
    got = saved.groupby('date')['TEAM_ID'].nunique().reindex(exp.index).fillna(0)
    short = set(exp.index[(got > 0) & (got < exp)])
    return sorted(int(d) for d in blank | short)


def date_report(saved, expected):
    """Per incomplete date: scheduled teams, saved teams, rows with blank tracking."""
    cols = [c for c in TRACKING_CHECK_COLS if c in saved.columns]
    exp = expected.groupby('GAME_DATE')['TEAM_ID'].nunique()
    got = saved.groupby('date')['TEAM_ID'].nunique()
    blank = saved[saved[cols].isna().any(axis=1)].groupby('date').size() if cols else pd.Series(dtype=int)
    rows = [{'date': d,
             'scheduled_teams': int(exp.get(d, 0)),
             'saved_teams': int(got.get(d, 0)),
             'blank_tracking_rows': int(blank.get(d, 0))}
            for d in incomplete_dates(saved, expected)]
    return pd.DataFrame(rows, columns=['date', 'scheduled_teams', 'saved_teams', 'blank_tracking_rows'])


# stats.nba.com also reprocesses tracking after a date is scraped, with no
# blanks to give it away: in Oct 2026 the drives saved for Oct 20-27 were about
# 40% of what it reported a few days later (Oct 21: 576 vs 1,401). A date counts
# as revised when a team's drives there differ from stats.nba.com by more than
# this many. Twin of game_checks.revised_dates.
REVISION_TOL = 2


def revised_dates(saved, dates, team_drives, tol=REVISION_TOL):
    """Saved dates whose team drives no longer match stats.nba.com.

    saved: the year file (date, TEAM_ID, DRIVES). dates: saved dates to check
    (YYYYMMDD ints). team_drives(first, last): {TEAM_ID: drives} over
    first..last from the Team drives endpoint, or None if the pull failed. One
    pull covers all the dates; single dates are pulled only for teams whose
    total over the range is off.
    """
    dates = sorted(int(d) for d in dates)
    if not dates or 'DRIVES' not in saved.columns:
        return []
    ours = saved[saved['date'].isin(dates)].groupby(['date', 'TEAM_ID'])['DRIVES'].sum()

    def off(theirs, mine):
        return {t for t, n in theirs.items() if abs(n - mine.get(t, 0)) > tol}

    window = team_drives(dates[0], dates[-1])
    if not window:
        return []
    teams = off(window, ours.groupby(level='TEAM_ID').sum())
    revised = []
    for d in dates:
        day = ours.xs(d, level='date')
        if teams & set(day.index):
            theirs = team_drives(d, d)
            if theirs and off(theirs, day):
                revised.append(d)
    return revised

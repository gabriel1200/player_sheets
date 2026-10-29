"""
Completeness checks for year_files/{year}{trail}_games.csv (the player game report).

The player-level twin of ../teamgame_report/teamgame_checks.py, used by
game_report_scrape.py to re-fetch recent dates that were scraped before
stats.nba.com finished processing tracking.
"""
import pandas as pd

# A date scraped too early keeps its box score but comes back with blank
# tracking (the tracking endpoints return no rows, blank after the left
# merge), for every team or for the games not processed yet. Unlike the team
# file, a few player rows are blank on a normal date (up to about 5%: players
# the tracking endpoints leave out), so the test is per team: a scheduled team
# with no player carrying drives, passes and touches that day, or a missing team.
TRACKING_CHECK_COLS = ['DRIVES', 'PASSES_MADE', 'TOUCHES']


def incomplete_dates(saved, expected):
    """Saved dates with a team that has no tracked player, or fewer teams than scheduled.

    saved: the year file (columns date, TEAM_ID and the tracking columns).
    expected: scheduled player-games or team-games (columns GAME_DATE, TEAM_ID).
    """
    exp = expected.groupby('GAME_DATE')['TEAM_ID'].nunique()
    got = saved.groupby('date')['TEAM_ID'].nunique().reindex(exp.index).fillna(0)
    short = set(exp.index[(got > 0) & (got < exp)])
    cols = [c for c in TRACKING_CHECK_COLS if c in saved.columns]
    if not cols:
        return sorted(int(d) for d in short)
    tracked = saved[saved[cols].notna().all(axis=1)].groupby('date')['TEAM_ID'].nunique().reindex(exp.index).fillna(0)
    untracked = set(exp.index[(got > 0) & (tracked < got)])
    return sorted(int(d) for d in short | untracked)


# stats.nba.com also reprocesses tracking after a date is scraped, with no
# blanks to give it away: in Oct 2026 the drives saved for Oct 20-27 were about
# 40% of what it reported a few days later (Oct 21: 576 vs 1,401). A date counts
# as revised when a team's drives there differ from stats.nba.com by more than
# this many.
REVISION_TOL = 2


def revised_dates(saved, dates, team_drives, tol=REVISION_TOL):
    """Saved dates whose team drive totals no longer match stats.nba.com.

    saved: the year file (date, TEAM_ID, DRIVES); players are summed per team.
    dates: saved dates to check (YYYYMMDD ints).
    team_drives(first, last): {TEAM_ID: drives} over first..last from the Team
    drives endpoint, or None if the pull failed. One pull covers all the dates;
    single dates are pulled only for teams whose total over the range is off.
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

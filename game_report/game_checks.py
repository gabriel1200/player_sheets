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

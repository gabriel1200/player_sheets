#!/usr/bin/env python
# coding: utf-8

# WNBA gen_totals - simplified.
#
# Produces, in one run:
#   a) A merged file per season and per postseason: {year}_totals.csv / {year}_ps_totals.csv
#      (each is that year's leaguedash averages + that year's pbpstats totals, merged)
#   b) Two combined files: all_totals.csv (every regular season, concatenated) and
#      all_totals_ps.csv (every postseason, concatenated)
#
# Everything (this script, w_averages.py, the _avg/_pbp source CSVs, and these outputs)
# lives flat in one w_totals folder.
#
# pbp_columns below is pulled directly from your most recent 2026_pbp.csv headers (243
# columns, minus EntityId/year) rather than carried over from the NBA version - so it's an
# exact match to what fetch_wnba_data is actually producing right now, not an assumption.
#
# Dropped vs. earlier versions:
#   - The incremental/manifest caching from the previous pass - unnecessary complexity for
#     what you actually need. This just recomputes everything each run, which is
#     what you asked for. Straightforward.
#   - LEBRON dashboard merge, salary/cap merge, player_factors() (external
#     team_averages.csv), the 2014+ on-ball-time% calc, the "modern" era split - all
#     NBA-only dependencies with no WNBA equivalent (same reasoning as before).
#   - Per-player perc/totals exports - dropped per your last call, functions kept below
#     (unused) in case you want them back.
#
# Kept:
#   - The adjusted true-shooting / Stops calc - self-contained, computed from each year's
#     own pbp totals, no external file needed.

import pandas as pd
import glob
import os
import re

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ---------------------------------------------------------------------------
# Column groups - filtered against whatever's actually present in the merged data at
# runtime, so no harm in over-including here.
# ---------------------------------------------------------------------------
index_col = [
    "PLAYER_ID", "PLAYER_NAME", "W", "GP", "year", "POSS", "TEAM_ABBREVIATION", "TEAM_ID", "AGE",
    'PLAYER_HEIGHT_INCHES', 'PLAYER_WEIGHT', 'COLLEGE', 'COUNTRY', 'DRAFT_YEAR', 'DRAFT_ROUND', 'DRAFT_NUMBER'
]

sum_metrics = [
    "MIN", "FGM", "FGA", "FG3M", "FG3A", "FTM", "FTA", "OREB", "DREB", "REB",
    "AST", "TOV", "STL", "BLK", "BLKA", "PF", "PFD", "PTS", "PLUS_MINUS",
    "DD2", "TD3", "FGM_PG", "sp_work_PACE", "sp_work_DEF_RATING", "DEF_RATING",
    "sp_work_NET_RATING", "E_DEF_RATING", "OFF_RATING", "PACE_PER40",
    "AST_RATIO", "sp_work_OFF_RATING", "E_PACE", "NET_RATING", "E_NET_RATING",
    "PACE", "E_OFF_RATING", "FGA_PG", "AST_TO", "ITP_FGM",
    "ABOVE_BREAK_3_FGA", "MID_FGM", "RIGHT_CORNER_3_FGM", "MID_FGA",
    "RA_FGA", "LEFT_CORNER_3_FGM", "ITP_FGA", "LEFT_CORNER_3_FGA",
    "BACKCOURT_FGA", "RA_FGM", "CORNER_3_FGM", "RIGHT_CORNER_3_FGA",
    "BACKCOURT_FGM", "CORNER_3_FGA", "ABOVE_BREAK_3_FGM",
    "FGM_LT_06", "FGA_LT_06", "PLUSMINUS", "NBA_FANTASY_PTS", "WNBA_FANTASY_PTS",
    "team_poss",
    'two_pt_def_FG2A', 'two_pt_def_FG2M', 'three_pt_def_FG3M', 'three_pt_def_FG3A',
    'overall_def_D_FGA', 'overall_def_D_FGM',
    'less_10ft_def_FGM_LT_10', 'less_10ft_def_FGA_LT_10',
    'less_6ft_def_FGM_LT_06', 'less_6ft_def_FGA_LT_06',
    'more_15ft_def_FGM_GT_15', 'more_15ft_def_FGA_GT_15',
]

pct_metrics = [
    "W_PCT", "FG_PCT", "FG3_PCT", "FT_PCT",
    "TM_TOV_PCT", "REB_PCT", "AST_PCT", "DREB_PCT",
    "E_TOV_PCT", "TS_PCT", "EFG_PCT", "E_USG_PCT",
    "OREB_PCT", "USG_PCT", "MID_FG_PCT", "BACKCOURT_FG_PCT",
    "ABOVE_BREAK_3_FG_PCT", "CORNER_3_FG_PCT", "RA_FG_PCT", "LEFT_CORNER_3_FG_PCT",
    "RIGHT_CORNER_3_FG_PCT", "ITP_FG_PCT",
    "LT_06_PCT", "NS_LT_06_PCT", "overall_def_NORMAL_FG_PCT",
    'overall_def_D_FG_PCT', 'overall_def_PCT_PLUSMINUS',
    'less_6ft_def_LT_06_PCT', 'less_6ft_def_NS_LT_06_PCT', 'less_6ft_def_FREQ',
    'less_10ft_def_FREQ', 'less_10ft_def_NS_LT_10_PCT', 'less_10ft_def_LT_10_PCT',
    'more_15ft_def_GT_15_PCT', 'more_15ft_def_NS_GT_15_PCT', 'more_15ft_def_FREQ',
    'two_pt_def_FG2_PCT', 'two_pt_def_FREQ', 'two_pt_def_NS_FG2_PCT',
    'three_pt_def_FG3_PCT', 'three_pt_def_FREQ', 'three_pt_def_NS_FG3_PCT',
]

# Exact headers from your most recently uploaded pbpstats file (2026_pbp.csv), minus
# EntityId (-> PLAYER_ID) and year (handled separately).
pbp_columns = [
    'TeamId', 'Name', 'ShortName', 'RowId', 'TeamAbbreviation', 'SecondsPlayed',
    'GamesPlayed', 'Minutes', 'PlusMinus', 'OffPoss', 'DefPoss', 'PenaltyOffPoss',
    'PenaltyDefPoss', 'SecondChanceOffPoss', 'TotalPoss', 'AtRimFGM', 'AtRimFGA',
    'SecondChanceAtRimFGM', 'SecondChanceAtRimFGA', 'PenaltyAtRimFGM', 'PenaltyAtRimFGA',
    'ShortMidRangeFGM', 'ShortMidRangeFGA', 'LongMidRangeFGA', 'FG2M', 'FG2A', 'FtPoints',
    'Points', 'OpponentPoints', 'SecondChanceFG2M', 'SecondChanceFG2A', 'SecondChanceFtPoints',
    'SecondChancePoints', 'PenaltyFG2M', 'PenaltyFG2A', 'PenaltyFtPoints', 'PenaltyPoints',
    'PtsAssisted2s', 'PtsUnassisted2s', 'PtsPutbacks', 'Fg2aBlocked', 'TwoPtAssists',
    'ThreePtAssists', 'Assists', 'Arc3Assists', 'Corner3Assists', 'AtRimAssists',
    'ShortMidRangeAssists', 'LongMidRangeAssists', 'AssistPoints', 'OffThreePtRebounds',
    'OffTwoPtRebounds', 'FTOffRebounds', 'DefThreePtRebounds', 'DefTwoPtRebounds',
    'FTDefRebounds', 'DefRebounds', 'OffRebounds', 'Rebounds', 'SelfOReb', 'Steals',
    'BadPassSteals', 'LostBallSteals', 'LiveBallTurnovers', 'BadPassOutOfBoundsTurnovers',
    'BadPassTurnovers', 'DeadBallTurnovers', 'LostBallOutOfBoundsTurnovers',
    'LostBallTurnovers', 'Travels', 'Turnovers', 'SecondChanceTurnovers', 'PenaltyTurnovers',
    'ShootingFouls', 'Fouls', 'Charge Fouls', 'Loose Ball Fouls', 'Offensive Fouls',
    'FoulsDrawn', 'Loose Ball Fouls Drawn', 'Offensive Fouls Drawn', 'FTA',
    '2pt And 1 Free Throw Trips', 'TwoPtShootingFoulsDrawn', 'NonShootingFoulsDrawn',
    'Blocked2s', 'BlockedAtRim', 'BlockedShortMidRange', 'Blocks', 'RecoveredBlocks',
    'Defensive 3 Seconds Violations', 'FirstChancePoints', 'PenaltyPointsExcludingTakeFouls',
    'PenaltyOffPossExcludingTakeFouls', 'NonShootingPenaltyNonTakeFouls',
    'NonShootingPenaltyNonTakeFoulsDrawn', 'Period2Fouls2Minutes', 'Period2Fouls3Minutes',
    'Period3Fouls3Minutes', 'Period3Fouls4Minutes', 'Period4Fouls4Minutes',
    'Period4Fouls5Minutes', 'PeriodOTFouls4Minutes', 'OnOffRtg', 'OnDefRtg',
    'Assisted2sPct', 'NonPutbacksAssisted2sPct', 'Fg2Pct', 'SecondChanceFg2Pct',
    'PenaltyFg2Pct', 'EfgPct', 'SecondChanceEfgPct', 'PenaltyEfgPct', 'TsPct',
    'SecondChanceTsPct', 'PenaltyTsPct', 'FG2APctBlocked', 'AtRimPctBlocked',
    'ShortMidRangePctBlocked', 'Usage', 'LiveBallTurnoverPct', 'DefFTReboundPct',
    'OffFTReboundPct', 'DefTwoPtReboundPct', 'OffTwoPtReboundPct', 'DefThreePtReboundPct',
    'OffThreePtReboundPct', 'DefFGReboundPct', 'OffFGReboundPct', 'OffAtRimReboundPct',
    'OffShortMidRangeReboundPct', 'OffLongMidRangeReboundPct', 'OffArc3ReboundPct',
    'OffCorner3ReboundPct', 'DefAtRimReboundPct', 'DefShortMidRangeReboundPct',
    'DefLongMidRangeReboundPct', 'DefArc3ReboundPct', 'DefCorner3ReboundPct',
    'SelfORebPct', 'BlocksRecoveredPct', 'AtRimFrequency', 'AtRimAccuracy',
    'UnblockedAtRimAccuracy', 'AtRimPctAssisted', 'ShortMidRangeFrequency',
    'ShortMidRangeAccuracy', 'UnblockedShortMidRangeAccuracy', 'ShortMidRangePctAssisted',
    'LongMidRangeFrequency', 'SecondChanceAtRimFrequency', 'SecondChanceAtRimAccuracy',
    'SecondChanceAtRimPctAssisted', 'PenaltyAtRimFrequency', 'PenaltyAtRimAccuracy',
    'AtRimFG3AFrequency', 'ShotQualityAvg', 'SecondChanceShotQualityAvg',
    'PenaltyShotQualityAvg', 'ShootingFoulsDrawnPct', 'TwoPtShootingFoulsDrawnPct',
    'SecondChancePointsPct', 'PenaltyPointsPct', 'Avg2ptShotDistance',
    'AtRimOffReboundedPct', 'ShortMidRangeOffReboundedPct', 'PenaltyOffPossPct',
    'LongMidRangeFGM', 'Corner3FGA', 'Arc3FGM', 'Arc3FGA', 'PenaltyArc3FGA', 'FG3M', 'FG3A',
    'PenaltyFG3A', 'PtsAssisted3s', 'NonHeaveArc3FGA', 'NonHeaveArc3FGM',
    'StepOutOfBoundsTurnovers', 'Blocked3s', 'BlockedCorner3', 'Period1Fouls2Minutes',
    'Assisted3sPct', 'Fg3Pct', 'NonHeaveFg3Pct', 'FG3APct', 'LongMidRangeAccuracy',
    'UnblockedLongMidRangeAccuracy', 'LongMidRangePctAssisted', 'Corner3Frequency',
    'Arc3Frequency', 'Arc3Accuracy', 'UnblockedArc3Accuracy', 'Arc3PctAssisted',
    'PenaltyArc3Frequency', 'NonHeaveArc3Accuracy', 'Avg3ptShotDistance',
    'LongMidRangeOffReboundedPct', 'ThreePtOffReboundedPct', 'Corner3FGM',
    'PenaltyCorner3FGM', 'PenaltyCorner3FGA', 'SecondChanceArc3FGA', 'PenaltyArc3FGM',
    'SecondChanceFG3A', 'PenaltyFG3M', 'PtsUnassisted3s', 'Fg3aBlocked',
    'Technical Free Throw Trips', 'BlockedLongMidRange', 'PenaltyFg3Pct', 'FG3APctBlocked',
    'Arc3PctBlocked', 'Corner3Accuracy', 'UnblockedCorner3Accuracy', 'Corner3PctAssisted',
    'SecondChanceArc3Frequency', 'PenaltyCorner3Frequency', 'PenaltyCorner3Accuracy',
    'PenaltyArc3Accuracy', 'BlockedArc3', 'SecondChanceCorner3FGA', 'LongMidRangePctBlocked',
    'SecondChanceCorner3Frequency', 'SecondChanceArc3FGM', 'SecondChanceFG3M',
    'Charge Fouls Drawn', 'SecondChanceFg3Pct', 'SecondChanceArc3Accuracy',
    'SecondChanceArc3PctAssisted', 'SecondChanceCorner3FGM', 'Clear Path Fouls',
    'ThreePtShootingFoulsDrawn', 'Corner3PctBlocked', 'SecondChanceCorner3Accuracy',
    'SecondChanceCorner3PctAssisted', 'ThreePtShootingFoulsDrawnPct',
    '3pt And 1 Free Throw Trips', 'PeriodOTFouls5Minutes', 'Period3Fouls5Minutes',
    'Period1Fouls3Minutes', 'OffensiveGoaltends', 'Period2Fouls4Minutes',
]


# ---------------------------------------------------------------------------
def discover_years(ps=False):
    """Years found on disk via {year}{trail}_avg.csv."""
    trail = '_ps' if ps else ''
    avg_files = sorted(glob.glob(os.path.join(BASE_DIR, f'*{trail}_avg.csv')))
    if trail == '':
        # '*_avg.csv' would also match '2026_ps_avg.csv' when trail is ''.
        avg_files = [f for f in avg_files if '_ps_avg.csv' not in f]
    return sorted(int(re.match(r'(\d{4})', os.path.basename(f)).group(1)) for f in avg_files)


def merge_single_year(year, ps=False):
    """
    Reads {year}{trail}_avg.csv and merges in the matching {year}{trail}_pbp.csv, when
    it exists. Returns None if the avg file is missing or empty (e.g. a playoff year
    before the playoffs have happened).
    """
    trail = '_ps' if ps else ''
    avg_file = os.path.join(BASE_DIR, f'{year}{trail}_avg.csv')
    if not os.path.exists(avg_file):
        return None

    df = pd.read_csv(avg_file)
    if len(df) == 0:
        return None
    df['PLAYER_ID'] = df['PLAYER_ID'].astype(str)

    pbp_file = os.path.join(BASE_DIR, f'{year}{trail}_pbp.csv')
    if os.path.exists(pbp_file):
        df2 = pd.read_csv(pbp_file)
        df2.rename(columns={'EntityId': 'PLAYER_ID'}, inplace=True)

        curcol = [col.lower() for col in df.columns]
        keepcol = [col for col in df2.columns if col.lower() not in curcol]
        keepcol.append('PLAYER_ID')
        keepcol.append('year')
        df2 = df2[keepcol]
        df2['PLAYER_ID'] = df2['PLAYER_ID'].astype(str)
        if ps:
            # fetch_wnba_data writes 'year' as e.g. "2026_ps" for playoffs
            df2['year'] = df2['year'].astype(str).str.split('_').str[0].astype(int)

        df = df.merge(df2, on=['PLAYER_ID', 'year'], how='left')
    else:
        print(f"  No pbp file for {year}{trail} ({pbp_file} not found) - avg data only for this year.")

    df['year'] = year
    return df


def add_adjusted_true_shooting(df):
    """
    Self-contained adjusted true-shooting / Stops calc, computed entirely from this
    year's own pbp totals (no external reference file needed).
    """
    required = ["Charge Fouls Drawn", "Offensive Fouls Drawn", "Steals", "RecoveredBlocks",
                "FG2A", "FG3A", "FTA", "2pt And 1 Free Throw Trips", "3pt And 1 Free Throw Trips",
                "Turnovers", "BadPassTurnovers", "Points", "SelfOReb"]
    if not all(col in df.columns for col in required):
        return df

    df = df.copy()
    df['Stops'] = (
        df["Charge Fouls Drawn"].fillna(0) +
        df["Offensive Fouls Drawn"].fillna(0) +
        df["Steals"].fillna(0) +
        df["RecoveredBlocks"].fillna(0)
    )

    cols_to_fill = ["FG2A", "FG3A", "FTA", "2pt And 1 Free Throw Trips",
                     "3pt And 1 Free Throw Trips", "Turnovers", "BadPassTurnovers",
                     "Points", "SelfOReb"]
    df[cols_to_fill] = df[cols_to_fill].fillna(0)

    df["improved_tsa"] = (
        df["FG2A"] + df["FG3A"]
        + (0.5 * (df["FTA"] - df["2pt And 1 Free Throw Trips"] - df["3pt And 1 Free Throw Trips"]))
    )
    df["NonPassTurnover"] = df["Turnovers"] - df["BadPassTurnovers"]

    df["adjusted_trueshooting_pct"] = df['Points'] / (
        (df["improved_tsa"] - df["SelfOReb"] + df["NonPassTurnover"])
    ) / 2

    total_points = df["Points"].sum()
    total_possessions = df["improved_tsa"].sum() - df["SelfOReb"].sum() + df["NonPassTurnover"].sum()
    league_avg_ts = total_points / total_possessions / 2

    df["relative_adjusted_ts_pct"] = df["adjusted_trueshooting_pct"] - league_avg_ts
    return df


def finalize(df):
    """Cross-fill, add derived columns, select/order columns for a single year's frame."""
    if 'POINTS' in df.columns and 'Points' in df.columns:
        df['POINTS'] = df['POINTS'].fillna(df['Points'])
        df['Points'] = df['Points'].fillna(df['POINTS'])

    df = add_adjusted_true_shooting(df)

    total_columns = index_col + sum_metrics + pct_metrics + pbp_columns + \
        ['Stops', 'adjusted_trueshooting_pct', 'relative_adjusted_ts_pct']
    total_columns = [c for c in dict.fromkeys(total_columns) if c in df.columns]
    df = df[total_columns]

    columns_to_front = [c for c in
                         ["PLAYER_ID", "PLAYER_NAME", "W", "GP", "year", "POSS",
                          "TEAM_ABBREVIATION", "TEAM_ID", "AGE"]
                         if c in df.columns]
    df = df[columns_to_front + [col for col in df.columns if col not in columns_to_front]]
    return df


# ---------------------------------------------------------------------------
# perc_save/total_save - kept but not called (per-player exports dropped for now).
# Uncomment the calls at the bottom to bring them back.
# ---------------------------------------------------------------------------
def perc_save(df, ps=False, out_dir=None):
    """Per-player CSV with rate stats (per-100-poss) and percentile ranks within-year."""
    trail = '_ps' if ps else ''
    out_dir = out_dir or os.path.join(BASE_DIR, 'perc')
    os.makedirs(out_dir, exist_ok=True)
    player_ids = df['PLAYER_ID'].unique().tolist()

    cols = [c for c in (index_col + sum_metrics + pct_metrics) if c in df.columns]
    frame = df[cols].reset_index(drop=True).fillna(0)

    present_sum_metrics = [c for c in sum_metrics if c in frame.columns]
    for col in present_sum_metrics:
        frame[col] = 100 * frame[col].astype(float) / frame['POSS'].replace(0, pd.NA)

    all_metrics = present_sum_metrics + [c for c in pct_metrics if c in frame.columns]
    rank_cols = {col + '_rank': frame.groupby('year')[col].rank(pct=True) for col in all_metrics}
    frame = pd.concat([frame, pd.DataFrame(rank_cols)], axis=1).copy()

    for player_id in player_ids:
        player_frame = frame[frame['PLAYER_ID'] == player_id]
        player_frame.to_csv(os.path.join(out_dir, f'{player_id}{trail}.csv'), index=False)


def total_save(df, ps=False, out_dir=None):
    """Per-player CSV of raw totals."""
    trail = '_ps' if ps else ''
    out_dir = out_dir or os.path.join(BASE_DIR, 'totals')
    os.makedirs(out_dir, exist_ok=True)
    player_ids = df['PLAYER_ID'].unique().tolist()

    cols = [c for c in (index_col + sum_metrics + pct_metrics) if c in df.columns]
    frame = df[cols].reset_index(drop=True).fillna(0)

    for player_id in player_ids:
        player_frame = frame[frame['PLAYER_ID'] == player_id]
        player_frame.to_csv(os.path.join(out_dir, f'{player_id}{trail}.csv'), index=False)


# ---------------------------------------------------------------------------
# Run: for regular season AND postseason, merge each year, save a per-year file, then
# concatenate into one totals file for that trail.
# ---------------------------------------------------------------------------
for ps in (False, True):
    trail = '_ps' if ps else ''
    label = 'postseason' if ps else 'regular season'

    years = discover_years(ps=ps)
    if not years:
        print(f"No {label} _avg.csv files found - skipping {label}.")
        continue

    frames = []
    for year in years:
        df_year = merge_single_year(year, ps=ps)
        if df_year is None:
            print(f"Skipping {year}{trail}: no data (missing or empty avg file).")
            continue
        df_year = finalize(df_year)

        year_path = os.path.join(BASE_DIR, f'{year}{trail}_totals.csv')
        df_year.to_csv(year_path, index=False)
        print(f"Saved {year_path} ({df_year.shape[0]} rows, {df_year.shape[1]} cols)")
        frames.append(df_year)

    if not frames:
        print(f"No usable {label} data - skipping combined all_totals{trail}.csv.")
        continue

    all_data = pd.concat(frames, ignore_index=True)
    all_data.sort_values(by=[c for c in ['PTS', 'MIN'] if c in all_data.columns], inplace=True)

    total_path = os.path.join(BASE_DIR, f'all_totals{trail}.csv')
    all_data.to_csv(total_path, index=False)
    print(f"Saved {total_path} ({all_data.shape[0]} rows, {all_data.shape[1]} cols)")
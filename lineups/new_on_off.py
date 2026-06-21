#!/usr/bin/env python
# coding: utf-8
"""
on_off_gen.py

Generate on/off combination CSVs for any set of years, for both the regular
season and the postseason.

Usage
-----
    # Regular season + postseason for 2025 and 2026:
    python on_off_gen.py --years 2025 2026

    # Only a couple of years, regular season only:
    python on_off_gen.py --years 2024 2025 --no-postseason

    # Only postseason:
    python on_off_gen.py --years 2025 --no-regular

    # A full range:
    python on_off_gen.py --start 2020 --end 2026

Postseason files follow the same naming convention as regular-season files
but with a `_ps` suffix before `.csv` (e.g. `1610612737_ps.csv`,
`1610612737_vs_ps.csv`). Postseason output files are written with a `_ps`
suffix as well (e.g. `2025_ps.csv`, `2025vs_ps.csv`).
"""

import argparse
import time
from functools import lru_cache

import numpy as np
import pandas as pd

# --------------------------------------------------------------------------- #
#  Paths / config
# --------------------------------------------------------------------------- #
CONTRACT_DIR = "../../contract/nba_rapm/on-off/years"
INDEX_RS = "https://raw.githubusercontent.com/gabriel1200/site_Data/refs/heads/master/index_master.csv"
INDEX_PS = "https://raw.githubusercontent.com/gabriel1200/site_Data/refs/heads/master/index_master_ps.csv"

# Every raw column that calculate_basketball_percentages() reads but does not
# itself create. Postseason team files sometimes omit some of these (e.g.
# Fg3aBlocked, the opp_Blocked* columns), so we backfill any that are missing
# with 0 before computing percentages. Keep in sync with the function below.
_PCT_REQUIRED_COLS = [
    "2pt And 1 Free Throw Trips", "3pt And 1 Free Throw Trips",
    "Arc3Assists", "Arc3FGA", "Arc3FGM",
    "AtRimAssists", "AtRimFGA", "AtRimFGM",
    "Blocked3s", "Blocks",
    "Corner3Assists", "Corner3FGA", "Corner3FGM",
    "DefRebounds", "DefThreePtRebounds", "DefTwoPtRebounds",
    "FG2A", "FG2M", "FG3A", "FG3M", "FTA",
    "FTDefRebounds", "FTOffRebounds",
    "Fg2aBlocked", "Fg3aBlocked", "HeaveAttempts",
    "LiveBallTurnovers",
    "LongMidRangeAssists", "LongMidRangeFGA", "LongMidRangeFGM",
    "NonHeaveArc3FGA", "NonHeaveArc3FGM",
    "OffPoss", "OffRebounds", "OffThreePtRebounds", "OffTwoPtRebounds",
    "PenaltyArc3FGA", "PenaltyArc3FGM",
    "PenaltyAtRimFGA", "PenaltyAtRimFGM",
    "PenaltyCorner3FGA", "PenaltyCorner3FGM",
    "PenaltyFG2A", "PenaltyFG2M", "PenaltyFG3A", "PenaltyFG3M",
    "PenaltyOffPoss", "PenaltyPoints", "Points",
    "PtsAssisted2s", "PtsAssisted3s", "PtsPutbacks",
    "RecoveredBlocks",
    "SecondChanceArc3FGA", "SecondChanceArc3FGM",
    "SecondChanceAtRimFGA", "SecondChanceAtRimFGM",
    "SecondChanceCorner3FGA", "SecondChanceCorner3FGM",
    "SecondChanceFG2A", "SecondChanceFG2M",
    "SecondChanceFG3A", "SecondChanceFG3M", "SecondChancePoints",
    "SelfOReb", "ShootingFouls",
    "ShortMidRangeAssists", "ShortMidRangeFGA", "ShortMidRangeFGM",
    "ThreePtShootingFoulsDrawn", "Turnovers", "TwoPtShootingFoulsDrawn",
    # opp_ columns the calc reads directly
    "opp_BlockedAtRim", "opp_BlockedLongMidRange", "opp_BlockedShortMidRange",
    "opp_FG2A", "opp_FG2M", "opp_FG3A", "opp_FG3M",
    "opp_FTDefRebounds", "opp_FTOffRebounds",
]


# --------------------------------------------------------------------------- #
#  Percentage calculations (unchanged math)
# --------------------------------------------------------------------------- #
def calculate_basketball_percentages(df):
    """Calculate basketball percentage statistics from raw totals."""
    result = df.copy()

    # Basic shooting percentages
    result['Fg3Pct'] = (result['FG3M'] / result['FG3A']).fillna(0)
    result['Fg2Pct'] = (result['FG2M'] / result['FG2A']).fillna(0)
    result['FGA'] = result['FG2A'] + result['FG3A']
    result['FGM'] = result['FG2M'] + result['FG3M']
    result['PenaltyFGA'] = result['PenaltyFG2A'] + result['PenaltyFG3A']
    result['SecondChanceFGA'] = result['SecondChanceFG2A'] + result['SecondChanceFG3A']
    result['NonHeaveFg3Pct'] = (result['FG3M'] / (result['FG3A'] - result['HeaveAttempts'])).fillna(0)

    # Advanced shooting percentages
    result['EfgPct'] = ((result['FG2M'] + 1.5 * result['FG3M']) /
                        (result['FG2A'] + result['FG3A'])).fillna(0)

    # True shooting with and-1 adjustment
    points = result['Points']
    fga = result['FGA']
    fta = result['FTA']
    and1_2pt = result['2pt And 1 Free Throw Trips']
    and1_3pt = result['3pt And 1 Free Throw Trips']
    w = (and1_2pt + 1.5 * and1_3pt + 0.44 * (fta - and1_2pt - and1_3pt)) / fta
    result['TsPct'] = points / (2 * (fga + w * fta))

    # Second chance percentages
    result['SecondChanceFg3Pct'] = (result['SecondChanceFG3M'] / result['SecondChanceFG3A']).fillna(0)
    result['SecondChanceFg2Pct'] = (result['SecondChanceFG2M'] / result['SecondChanceFG2A']).fillna(0)
    result['SecondChanceEfgPct'] = ((result['SecondChanceFG2M'] + 1.5 * result['SecondChanceFG3M']) /
                                    (result['SecondChanceFG2A'] + result['SecondChanceFG3A'])).fillna(0)
    result['SecondChanceTsPct'] = (result['SecondChancePoints'] /
                                   (2 * (result['SecondChanceFG2A'] + result['SecondChanceFG3A']))).fillna(0)
    result['SecondChancePointsPct'] = (result['SecondChancePoints'] / result['Points']).fillna(0)

    # Shot distribution
    result['FG3APct'] = (result['FG3A'] / (result['FG2A'] + result['FG3A'])).fillna(0)
    result['FG2APctBlocked'] = (result['Fg2aBlocked'] / result['FG2A']).fillna(0)
    result['AtRimPctBlocked'] = (result['opp_BlockedAtRim'] / result['AtRimFGA']).fillna(0)
    result['LongMidRangePctBlocked'] = (result['opp_BlockedLongMidRange'] / result['LongMidRangeFGA']).fillna(0)
    result['ShortMidRangePctBlocked'] = (result['opp_BlockedShortMidRange'] / result['ShortMidRangeFGA']).fillna(0)
    result['FG3APctBlocked'] = (result['Fg3aBlocked'] / result['FG3A']).fillna(0)
    result['Corner3PctBlocked'] = (result['Blocked3s'] / result['Corner3FGA']).fillna(0)
    result['Arc3PctBlocked'] = (result['Blocked3s'] / result['Arc3FGA']).fillna(0)

    # Rebound percentages - Field Goals
    result['DefFGReboundPct'] = (result['DefRebounds'] /
                                 (result['opp_FG2A'] - result['opp_FG2M'] +
                                  result['opp_FG3A'] - result['opp_FG3M'])).fillna(0)
    result['OffFGReboundPct'] = (result['OffRebounds'] /
                                 (result['FG2A'] - result['FG2M'] +
                                  result['FG3A'] - result['FG3M'])).fillna(0)

    # Rebound percentages by shot location
    result['OffLongMidRangeReboundPct'] = (result['OffTwoPtRebounds'] / (result['LongMidRangeFGA'] - result['LongMidRangeFGM'])).fillna(0)
    result['DefLongMidRangeReboundPct'] = (result['DefTwoPtRebounds'] / (result['LongMidRangeFGA'] - result['LongMidRangeFGM'])).fillna(0)
    result['DefArc3ReboundPct'] = (result['DefThreePtRebounds'] / (result['Arc3FGA'] - result['Arc3FGM'])).fillna(0)
    result['OffArc3ReboundPct'] = (result['OffThreePtRebounds'] / (result['Arc3FGA'] - result['Arc3FGM'])).fillna(0)
    result['DefAtRimReboundPct'] = (result['DefTwoPtRebounds'] / (result['AtRimFGA'] - result['AtRimFGM'])).fillna(0)
    result['OffAtRimReboundPct'] = (result['OffTwoPtRebounds'] / (result['AtRimFGA'] - result['AtRimFGM'])).fillna(0)
    result['DefShortMidRangeReboundPct'] = (result['DefTwoPtRebounds'] / (result['ShortMidRangeFGA'] - result['ShortMidRangeFGM'])).fillna(0)
    result['OffShortMidRangeReboundPct'] = (result['OffTwoPtRebounds'] / (result['ShortMidRangeFGA'] - result['ShortMidRangeFGM'])).fillna(0)
    result['DefCorner3ReboundPct'] = (result['DefThreePtRebounds'] / (result['Corner3FGA'] - result['Corner3FGM'])).fillna(0)
    result['OffCorner3ReboundPct'] = (result['OffThreePtRebounds'] / (result['Corner3FGA'] - result['Corner3FGM'])).fillna(0)

    # Assist percentages
    result['Assisted2sPct'] = (result['PtsAssisted2s'] / (2 * result['FG2M'])).fillna(0)
    result['Assisted3sPct'] = (result['PtsAssisted3s'] / (3 * result['FG3M'])).fillna(0)
    result['NonPutbacksAssisted2sPct'] = (result['PtsAssisted2s'] / (2 * (result['FG2M'] - result['PtsPutbacks'] / 2))).fillna(0)
    result['Corner3PctAssisted'] = (result['Corner3Assists'] / result['Corner3FGM']).fillna(0)
    result['Arc3PctAssisted'] = (result['Arc3Assists'] / result['Arc3FGM']).fillna(0)
    result['SecondChanceCorner3PctAssisted'] = (result['Corner3Assists'] / result['SecondChanceCorner3FGM']).fillna(0)
    result['SecondChanceArc3PctAssisted'] = (result['Arc3Assists'] / result['SecondChanceArc3FGM']).fillna(0)
    result['SecondChanceAtRimPctAssisted'] = (result['AtRimAssists'] / result['SecondChanceAtRimFGM']).fillna(0)
    result['AtRimPctAssisted'] = (result['AtRimAssists'] / result['AtRimFGM']).fillna(0)
    result['ShortMidRangePctAssisted'] = (result['ShortMidRangeAssists'] / result['ShortMidRangeFGM']).fillna(0)
    result['LongMidRangePctAssisted'] = (result['LongMidRangeAssists'] / result['LongMidRangeFGM']).fillna(0)

    # Penalty percentages
    result['PenaltyPointsPct'] = (result['PenaltyPoints'] / result['Points']).fillna(0)
    result['PenaltyOffPossPct'] = (result['PenaltyOffPoss'] / result['OffPoss']).fillna(0)
    result['PenaltyFg2Pct'] = (result['PenaltyFG2M'] / result['PenaltyFG2A']).fillna(0)
    result['PenaltyFg3Pct'] = (result['PenaltyFG3M'] / result['PenaltyFG3A']).fillna(0)
    result['PenaltyEfgPct'] = ((result['PenaltyFG2M'] + 1.5 * result['PenaltyFG3M']) /
                               (result['PenaltyFG2A'] + result['PenaltyFG3A'])).fillna(0)
    result['PenaltyTsPct'] = (result['PenaltyPoints'] /
                              (2 * (result['PenaltyFG2A'] + result['PenaltyFG3A'] + 0.44 * result['FTA']))).fillna(0)

    # Miscellaneous percentages
    result['BlocksRecoveredPct'] = (result['RecoveredBlocks'] / result['Blocks']).fillna(0)
    result['LiveBallTurnoverPct'] = (result['LiveBallTurnovers'] / result['Turnovers']).fillna(0)
    result['SelfORebPct'] = (result['SelfOReb'] / (result['FGA'] - result['FGM'])).fillna(0)

    # Fouls percentages
    result['ShootingFoulsDrawnPct'] = (result['ShootingFouls'] / (result['FG2A'] + result['FG3A'])).fillna(0)
    result['TwoPtShootingFoulsDrawnPct'] = (result['TwoPtShootingFoulsDrawn'] /
                                            (result['FG2A'] + result['2pt And 1 Free Throw Trips'])).fillna(0)
    result['ThreePtShootingFoulsDrawnPct'] = result['ThreePtShootingFoulsDrawn'] / result['FG3A']

    total_def_rebounds = result['DefTwoPtRebounds'] + result['DefThreePtRebounds']
    total_off_rebounds = result['OffTwoPtRebounds'] + result['OffThreePtRebounds']
    result['DefTwoPtReboundPct'] = (result['DefTwoPtRebounds'] / total_def_rebounds).fillna(0)
    result['DefThreePtReboundPct'] = (result['DefThreePtRebounds'] / total_def_rebounds).fillna(0)
    result['OffTwoPtReboundPct'] = (result['OffTwoPtRebounds'] / (result['FG2A'] - result['FG2M'])).fillna(0)
    result['OffThreePtReboundPct'] = (result['OffThreePtRebounds'] / total_off_rebounds).fillna(0)

    result['OffFTReboundPct'] = result['FTOffRebounds'] / (result['opp_FTDefRebounds'] + result['FTOffRebounds'])
    result['DefFTReboundPct'] = result['FTDefRebounds'] / (result['opp_FTOffRebounds'] + result['FTDefRebounds'])

    # Frequencies
    result['AtRimFrequency'] = result['AtRimFGA'] / result['FGA']
    result['ShortMidRangeFrequency'] = result['ShortMidRangeFGA'] / result['FGA']
    result['LongMidRangeFrequency'] = result['LongMidRangeFGA'] / result['FGA']
    result['Corner3Frequency'] = result['Corner3FGA'] / result['FGA']
    result['Arc3Frequency'] = result['Arc3FGA'] / result['FGA']
    result['SecondChanceArc3Frequency'] = result['SecondChanceArc3FGA'] / result['SecondChanceFGA']
    result['AtRimFG3AFrequency'] = (result['AtRimFGA'] + result['FG3A']) / result['FGA']
    result['SecondChanceAtRimFrequency'] = result['SecondChanceAtRimFGA'] / result['SecondChanceFGA']
    result['SecondChanceCorner3Frequency'] = result['SecondChanceCorner3FGA'] / result['SecondChanceFGA']
    result['PenaltyAtRimFrequency'] = result['PenaltyAtRimFGA'] / result['PenaltyFGA']
    result['PenaltyArc3Frequency'] = result['PenaltyArc3FGA'] / result['PenaltyFGA']
    result['PenaltyCorner3Frequency'] = result['PenaltyCorner3FGA'] / result['PenaltyFGA']

    # Accuracy metrics
    result['AtRimAccuracy'] = result['AtRimFGM'] / result['AtRimFGA']
    result['UnblockedAtRimAccuracy'] = (result['AtRimFGM'] - result['Fg2aBlocked']) / result['AtRimFGA']
    result['ShortMidRangeAccuracy'] = result['ShortMidRangeFGM'] / result['ShortMidRangeFGA']
    result['UnblockedShortMidRangeAccuracy'] = (result['ShortMidRangeFGM'] - result['Fg2aBlocked']) / result['ShortMidRangeFGA']
    result['LongMidRangeAccuracy'] = result['LongMidRangeFGM'] / result['LongMidRangeFGA']
    result['UnblockedLongMidRangeAccuracy'] = (result['LongMidRangeFGM'] - result['Fg2aBlocked']) / result['LongMidRangeFGA']
    result['Corner3Accuracy'] = result['Corner3FGM'] / result['Corner3FGA']
    result['UnblockedCorner3Accuracy'] = (result['Corner3FGM'] - result['Fg3aBlocked']) / result['Corner3FGA']
    result['Arc3Accuracy'] = result['Arc3FGM'] / result['Arc3FGA']
    result['UnblockedArc3Accuracy'] = (result['Arc3FGM'] - result['Fg3aBlocked']) / result['Arc3FGA']

    # Second-chance accuracy
    result['SecondChanceAtRimAccuracy'] = result['SecondChanceAtRimFGM'] / result['SecondChanceAtRimFGA']
    result['SecondChanceCorner3Accuracy'] = result['SecondChanceCorner3FGM'] / result['SecondChanceCorner3FGA']
    result['SecondChanceArc3Accuracy'] = result['SecondChanceArc3FGM'] / result['SecondChanceArc3FGA']

    # Penalty accuracy
    result['PenaltyAtRimAccuracy'] = result['PenaltyAtRimFGM'] / result['PenaltyAtRimFGA']
    result['PenaltyCorner3Accuracy'] = result['PenaltyCorner3FGM'] / result['PenaltyCorner3FGA']
    result['PenaltyArc3Accuracy'] = result['PenaltyArc3FGM'] / result['PenaltyArc3FGA']

    # Non-heave accuracy
    result['NonHeaveArc3Accuracy'] = result['NonHeaveArc3FGM'] / result['NonHeaveArc3FGA']

    # De-fragment: the ~90 single-column insertions above fragment the frame,
    # which pandas warns about. One copy consolidates the block layout.
    return result.copy()


def calculate_weighted_average(df, value_col, weight_col, group_by=None):
    """Calculate weighted average of a value column based on a weight column."""
    if value_col not in df.columns:
        raise ValueError(f"Value column '{value_col}' not found in dataframe")
    if weight_col not in df.columns:
        raise ValueError(f"Weight column '{weight_col}' not found in dataframe")
    if (df[weight_col] < 0).any():
        raise ValueError("Negative weights found. Please ensure all weights are non-negative")

    df = df.dropna(subset=[value_col, weight_col])

    if (df[weight_col] == 0).all():
        return np.nan

    if group_by is None:
        weighted_sum = (df[value_col] * df[weight_col]).sum()
        weight_sum = df[weight_col].sum()
        return weighted_sum / weight_sum if weight_sum != 0 else np.nan
    else:
        # Vectorized: avoids the deprecated DataFrameGroupBy.apply-on-grouping
        # warning and is faster than a per-group lambda.
        prod = (df[value_col] * df[weight_col]).groupby(df[group_by]).sum()
        weight_sum = df[weight_col].sum()
        return prod / weight_sum


# --------------------------------------------------------------------------- #
#  Cached team-file loading
#
#  player_rows() was re-reading the same per-team CSVs from disk for *every*
#  player on that team (and again for on/off and vs). For a team with 15
#  players that's ~60 redundant reads of the same 4 files. We cache the merged
#  per-(year, team, ps) frame so each team file is read exactly once.
# --------------------------------------------------------------------------- #
@lru_cache(maxsize=None)
def _load_team_merged(year, team_id, ps):
    """
    Read and merge a team's on (df1) and opponent/vs (df2) files once.
    Returns the merged frame with all the derived "misses" columns added,
    BEFORE the on/off player filter is applied.

    `vs` orientation is handled by the caller by swapping which logical side
    is treated as 'self' vs 'opp'. To keep a single cache entry per physical
    file pair, we cache both orientations.
    """
    pstring = "_ps" if ps else ""
    base = pd.read_csv(f"data/{year}/{team_id}{pstring}.csv")
    vs = pd.read_csv(f"data/{year}/{team_id}_vs{pstring}.csv")
    return base, vs


def _build_merged(year, team_id, vs, ps):
    """Build the merged df for a given (year, team, vs, ps) from cached reads."""
    base, vs_frame = _load_team_merged(year, team_id, ps)

    # Orientation: regular -> self=base, opp=vs ; vs=True -> swapped
    if not vs:
        df1, df2 = base.copy(), vs_frame.copy()
    else:
        df1, df2 = vs_frame.copy(), base.copy()

    notfound = set(df2.columns) - set(df1.columns)

    if 'team_vs' in df2.columns:
        df2.drop(columns='team_vs', inplace=True)

    id_col = ['EntityId']
    df2.columns = [('opp_' + c if c not in id_col else c) for c in df2.columns]
    df = df1.merge(df2, on=id_col)

    for col in notfound:
        df[col] = 0

    return df


def player_rows(year, player_id, team_id, vs=False, on=True, ps=False):
    """Compute one aggregated on/off row for a single player."""
    df = _build_merged(year, team_id, vs, ps)

    if on:
        df = df[df['EntityId'].apply(lambda x: player_id in x.split('-'))]
    else:
        df = df[~df['EntityId'].apply(lambda x: player_id in x.split('-'))]

    df = df.copy()
    df.fillna(0, inplace=True)

    df['FGA'] = df['FG2A'] + df['FG3A']
    df['FGM'] = df['FG2M'] + df['FG3M']
    df['opp_FGA'] = df['opp_FG2A'] + df['opp_FG3A']
    df['opp_FGM'] = df['opp_FG2M'] + df['opp_FG3M']

    missing = ['3pt And 1 Free Throw Trips', 'opp_BlockedLongMidRange', 'opp_FTOffRebounds']
    for col in missing:
        if col not in df.columns:
            df[col] = 0

    df.drop(columns=['opp_Name', 'opp_ShortName', 'opp_RowId',
                     'opp_TeamAbbreviation', 'opp_season'],
            inplace=True, errors='ignore')

    # Miss columns
    df['two_point_misses'] = df['FG2A'] - df['FG2M']
    df['opp_two_point_misses'] = df['opp_FG2A'] - df['opp_FG2M']
    df['at_rim_misses'] = df['AtRimFGA'] - df['AtRimFGM']
    df['opp_at_rim_misses'] = df['opp_AtRimFGA'] - df['opp_AtRimFGM']
    df['short_midrange_misses'] = df['ShortMidRangeFGA'] - df['ShortMidRangeFGM']
    df['opp_short_midrange_misses'] = df['opp_ShortMidRangeFGA'] - df['opp_ShortMidRangeFGM']
    df['long_midrange_misses'] = df['LongMidRangeFGA'] - df['LongMidRangeFGM']
    df['opp_long_midrange_misses'] = df['opp_LongMidRangeFGA'] - df['opp_LongMidRangeFGM']
    df['corner3_misses'] = df['Corner3FGA'] - df['Corner3FGM']
    df['opp_corner3_misses'] = df['opp_Corner3FGA'] - df['opp_Corner3FGM']
    df['arc3_misses'] = df['Arc3FGA'] - df['Arc3FGM']
    df['opp_arc3_misses'] = df['opp_Arc3FGA'] - df['opp_Arc3FGM']
    df['ft_misses'] = df['FTA'] - df['FtPoints']
    df['opp_ft_misses'] = df['opp_FTA'] - df['opp_FtPoints']
    df['fg_misses'] = df['FGA'] - df['FGM']
    df['opp_fg_misses'] = df['opp_FGA'] - df['opp_FGM']

    weight_mapping = {
        'DefTwoPtReboundPct': 'opp_two_point_misses',
        'OffTwoPtReboundPct': 'two_point_misses',
        'DefThreePtReboundPct': 'opp_FG3A',
        'DefFGReboundPct': 'opp_fg_misses',
        'OffFGReboundPct': 'fg_misses',
        'OffLongMidRangeReboundPct': 'long_midrange_misses',
        'DefLongMidRangeReboundPct': 'opp_long_midrange_misses',
        'OffThreePtReboundPct': 'opp_FG3A',
        'OffArc3ReboundPct': 'arc3_misses',
        'DefArc3ReboundPct': 'opp_arc3_misses',
        'DefAtRimReboundPct': 'opp_at_rim_misses',
        'DefShortMidRangeReboundPct': 'opp_short_midrange_misses',
        'DefCorner3ReboundPct': 'opp_corner3_misses',
        'OffAtRimReboundPct': 'at_rim_misses',
        'SelfORebPct': 'fg_misses',
        'OffShortMidRangeReboundPct': 'short_midrange_misses',
        'OffCorner3ReboundPct': 'corner3_misses',
        'SecondChanceTsPct': 'SecondChanceOffPoss',
        'SecondChanceCorner3PctAssisted': 'SecondChanceCorner3FGM',
        'SecondChanceArc3PctAssisted': 'SecondChanceArc3FGM',
        'SecondChanceAtRimPctAssisted': 'SecondChanceAtRimFGM',
    }

    values = []
    for key, weight_col in weight_mapping.items():
        # Postseason team files may not carry every per-lineup pct column
        # (key) or weight column. If either is absent, or the weights sum to
        # zero, the aggregate for that metric is 0.
        if key not in df.columns or weight_col not in df.columns:
            val = 0
        elif df[weight_col].sum() == 0:
            val = 0
        else:
            val = calculate_weighted_average(df, key, weight_col, 'team_id').iloc[0]
        values.append(val)

    weight_list = list(weight_mapping.keys())
    pct = [col for col in df.columns if 'pct' in col.lower()]

    grouped_sums = df.groupby('TeamId').sum(numeric_only=True)
    # After groupby, 'TeamId' is the index. Select the numeric, non-pct totals
    # that actually survived the aggregation (i.e. are present as columns).
    sum_cols = [
        col for col in grouped_sums.columns
        if col not in pct and col != 'TeamId'
    ]
    sums = grouped_sums[sum_cols].reset_index(drop=True)

    # Postseason team files can be missing some raw columns that the
    # percentage calc references (e.g. Fg3aBlocked). Backfill any required
    # column with 0 so calculate_basketball_percentages never KeyErrors.
    for col in _PCT_REQUIRED_COLS:
        if col not in sums.columns:
            sums[col] = 0

    newframe = calculate_basketball_percentages(sums)
    newframe[weight_list] = values

    to_drop = [col for col in newframe if 'opp_' in col.lower()]
    newframe.drop(columns=to_drop, inplace=True)

    year_i = int(year)
    # Assign all metadata columns at once (avoids the fragmentation warning
    # from many sequential single-column insertions) and de-fragment via copy.
    newframe = newframe.assign(
        player_id=player_id,
        player_on=on,
        player_vs=vs,
        season=f"{year_i - 1}-{str(year_i)[-2:]}",
        team_id=team_id,
    ).copy()

    return newframe


# --------------------------------------------------------------------------- #
#  Year-level aggregation
# --------------------------------------------------------------------------- #
def get_year(year, ps=False, vs=False):
    """Build the full on/off frame for a single year (regular or postseason)."""
    index = pd.read_csv(INDEX_PS if ps else INDEX_RS)
    index = index[index.year == year]

    # Skip rows missing a player or team id (NaN) so int() doesn't blow up.
    before = len(index)
    index = index.dropna(subset=['nba_id', 'team_id'])
    dropped = before - len(index)
    if dropped:
        print(f"  [warn] skipped {dropped} index row(s) with missing nba_id/team_id")

    rows = []
    count = 0
    for player_id, team_id in zip(index['nba_id'], index['team_id']):
        player_id = str(int(player_id))
        team_id = str(int(team_id))

        rows.append(player_rows(str(year), player_id, team_id, vs=vs, on=True, ps=ps))
        rows.append(player_rows(str(year), player_id, team_id, vs=vs, on=False, ps=ps))

        count += 1
        if count % 100 == 0:
            print(f"  ...{count} players")

    frame = pd.concat(rows)
    frame['year'] = year
    return frame


def generate_for_year(year, do_regular=True, do_postseason=True):
    """Generate and write outputs for a single year."""
    # Cache is per physical file; clearing between years keeps memory bounded.
    _load_team_merged.cache_clear()

    if do_regular:
        print(f"[{year}] regular season...")
        frame = get_year(year, ps=False, vs=False)
        frame_vs = get_year(year, ps=False, vs=True)

        frame.to_csv(f"shifts/{year}.csv", index=False)
        frame_vs.to_csv(f"shifts/{year}vs.csv", index=False)

        print(f"[{year}] regular season -> {year}.csv, {year}vs.csv")

    if do_postseason:
        print(f"[{year}] postseason...")
        _load_team_merged.cache_clear()
        frame_ps = get_year(year, ps=True, vs=False)
        frame_ps_vs = get_year(year, ps=True, vs=True)

        frame_ps.to_csv(f"shifts/{year}_ps.csv", index=False)
        frame_ps_vs.to_csv(f"shifts/{year}vs_ps.csv", index=False)

        print(f"[{year}] postseason -> {year}_ps.csv, {year}vs_ps.csv")


# --------------------------------------------------------------------------- #
#  CLI
# --------------------------------------------------------------------------- #
def parse_args():
    p = argparse.ArgumentParser(description="Generate on/off combo CSVs by year.")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--years", nargs="+", type=int, help="Explicit list of years, e.g. --years 2025 2026")
    g.add_argument("--start", type=int, help="Start year (use with --end)")
    p.add_argument("--end", type=int, help="End year inclusive (use with --start)")
    p.add_argument("--no-regular", action="store_true", help="Skip regular season")
    p.add_argument("--no-postseason", action="store_true", help="Skip postseason")
    return p.parse_args()


def main():
    args = parse_args()

    if args.years:
        years = sorted(set(args.years))
    else:
        if args.end is None:
            raise SystemExit("--start requires --end")
        years = list(range(args.start, args.end + 1))

    do_regular = not args.no_regular
    do_postseason = not args.no_postseason
    if not do_regular and not do_postseason:
        raise SystemExit("Nothing to do: both --no-regular and --no-postseason set.")

    start_time = time.time()

    for year in years:
        generate_for_year(year, do_regular=do_regular, do_postseason=do_postseason)

    print(f"\nDone. Years: {years} | regular={do_regular} postseason={do_postseason}")
    print(f"Time taken: {time.time() - start_time:.1f} seconds")


if __name__ == "__main__":
    main()
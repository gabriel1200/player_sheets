"""One-off repair for the gen_totals case-insensitive merge bug (fixed in gen_totals.ipynb, 2026-10-17).

collect_yeardata() dropped any PBP column whose name matched an existing column ignoring case, so
PBP `Points` and `PlusMinus` were discarded in favor of the tracking `POINTS` / `PLUSMINUS` columns.
`Points` was then filled from tracking `POINTS` (Booker 2016-17: 1648 instead of 1726) and
`PlusMinus` stayed empty. This script rewrites only those columns in the existing outputs, the same way a
rerun of the fixed notebook would:

    POINTS    = POINTS.fillna(pbp Points)
    Points    = (pbp Points).fillna(POINTS)
    PlusMinus = pbp PlusMinus

Year files (and year_totals/modern*) get cell 3's Points blank -> 0 fill and are re-sorted by Points, Minutes as the notebook does. CSVs are
read and written as text so every other cell is left byte-for-byte unchanged.

    python fix_points_plusminus_2026_10.py            # dry run: report changes
    python fix_points_plusminus_2026_10.py --write    # rewrite the files

Delete this script once a full gen_totals run has regenerated the outputs.
"""
import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
YEAR_TOTALS = HERE.parent / 'year_totals'
FIRST_PBP_YEAR = 2001  # collect_yeardata merges PBP only for year > 2000


def pbp_values(year, playoffs):
    path = HERE / f'{year}{"_ps" if playoffs else ""}_pbp.csv'
    if year < FIRST_PBP_YEAR or not path.exists():
        return None
    frame = pd.read_csv(path, usecols=['EntityId', 'Points', 'PlusMinus'])
    frame = frame.rename(columns={'EntityId': 'PLAYER_ID'})
    frame['PLAYER_ID'] = frame['PLAYER_ID'].astype(str)
    if frame['PLAYER_ID'].duplicated().any():
        raise ValueError(f'{path.name} has duplicate players')
    frame['year'] = str(year)
    return frame


def as_text(values):
    """Format like pandas writes a float column: 1726.0, -225.0, blank for missing."""
    return values.map(lambda v: '' if pd.isna(v) else repr(float(v)))


def repair(frame, playoffs, sort_years):
    """frame holds text columns; returns (repaired frame, rows whose Points changed)."""
    years = sorted({int(y) for y in frame['year'].unique() if y})
    pbp = [p for p in (pbp_values(y, playoffs) for y in years) if p is not None]
    if not pbp:
        return frame, 0
    pbp = pd.concat(pbp, ignore_index=True)
    key = frame['PLAYER_ID'].astype(str).str.removesuffix('.0') + '|' + frame['year'].astype(str)
    lookup = pbp.set_index(pbp['PLAYER_ID'] + '|' + pbp['year'])
    pbp_points = key.map(lookup['Points'])
    pbp_pm = key.map(lookup['PlusMinus'])
    has_pbp = key.isin(lookup.index)

    tracking = pd.to_numeric(frame['POINTS'], errors='coerce')
    new_tracking = tracking.fillna(pbp_points)
    new_points = pbp_points.fillna(new_tracking)

    out = frame.copy()
    # Rows from years without PBP keep their values; the notebook leaves them untouched too.
    out.loc[has_pbp, 'POINTS'] = as_text(new_tracking)[has_pbp]
    out.loc[has_pbp, 'Points'] = as_text(new_points)[has_pbp]
    out.loc[has_pbp, 'PlusMinus'] = as_text(pbp_pm)[has_pbp]
    if sort_years:
        # Cell 3 fills Points blanks with 0 before writing year files (cols_to_fill).
        out.loc[has_pbp & out['Points'].eq(''), 'Points'] = '0.0'
        order = pd.DataFrame({
            'year': out['year'].astype(int),
            'Points': pd.to_numeric(out['Points'], errors='coerce'),
            'Minutes': pd.to_numeric(out['Minutes'], errors='coerce'),
        })
        out = out.loc[order.sort_values(['year', 'Points', 'Minutes'], kind='mergesort').index]
    old_points = pd.to_numeric(frame['Points'], errors='coerce')
    final_points = pd.to_numeric(out['Points'], errors='coerce').reindex(frame.index)
    changed = int((~np.isclose(old_points.fillna(-1e9), final_points.fillna(-1e9))).sum())
    return out, changed


def check(frame, label):
    """Regular-season files: Points should now track box-score PTS. The bug left gaps of 50-78 points;
    pbpstats itself is sometimes a few points short (2024-25: 18 players by 1-8), which a rerun keeps."""
    pts = pd.to_numeric(frame['PTS'], errors='coerce')
    points = pd.to_numeric(frame['Points'], errors='coerce')
    fga = pd.to_numeric(frame['FGA'], errors='coerce')
    rows = frame['year'].astype(int).ge(2014) & fga.gt(300)
    gap = (points - pts).abs()[rows]
    share = gap.le(5).mean()
    print(f'    {label}: Points within 5 of PTS on {share:.1%} of 2014+ 300+ FGA seasons (largest gap {gap.max():.0f})')
    if share < .99:
        raise SystemExit(f'{label}: agreement {share:.1%} is too low; not writing.')


def patch_parquet(parquet, repaired, sort_years):
    """Copy the three repaired columns into the parquet by player and year; leave its other columns as they are."""
    frame = pd.read_parquet(parquet)
    key = lambda f: f['PLAYER_ID'].astype(str).str.removesuffix('.0') + '|' + f['year'].astype(str)
    source = repaired.set_index(key(repaired))
    if source.index.duplicated().any():
        raise SystemExit(f'{parquet.name}: duplicate player-years; not writing.')
    rows = key(frame)
    if not rows.isin(source.index).all():
        raise SystemExit(f'{parquet.name}: rows missing from the CSV; not writing.')
    for column in ('POINTS', 'Points', 'PlusMinus'):
        values = pd.to_numeric(rows.map(source[column]), errors='coerce')
        frame[column] = values.astype(frame[column].dtype) if frame[column].dtype.kind == 'f' else values
    if sort_years:
        frame = frame.sort_values(['year', 'Points', 'Minutes'], kind='mergesort')
    frame.to_parquet(parquet, index=False)


def targets():
    # gen_totals names these backwards: ps=False writes all_totals_ps.csv / modern_totals_ps.csv.
    yield HERE / 'all_totals_ps.csv', False, False
    yield HERE / 'modern_totals_ps.csv', False, False
    yield HERE / 'all_totals.csv', True, False
    yield HERE / 'modern_totals.csv', True, False
    for path in sorted(YEAR_TOTALS.glob('*.csv')):
        stem = path.stem
        playoffs = stem.endswith('_ps')
        base = stem.removesuffix('_ps')
        if base == 'modern' or (base.isdigit() and int(base) >= FIRST_PBP_YEAR):
            yield path, playoffs, True


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--write', action='store_true', help='rewrite the files (default is a dry run)')
    args = parser.parse_args()
    for path, playoffs, sort_years in targets():
        if not path.exists():
            continue
        frame = pd.read_csv(path, dtype=str, keep_default_na=False)
        if not {'Points', 'POINTS', 'PlusMinus'} <= set(frame.columns):
            print(f'skip {path.relative_to(HERE.parent)} (no Points/POINTS/PlusMinus)')
            continue
        repaired, changed = repair(frame, playoffs, sort_years)
        print(f'{path.relative_to(HERE.parent)}: {len(frame)} rows, Points changed on {changed}')
        if not playoffs:
            check(repaired, path.name)
        if args.write:
            repaired.to_csv(path, index=False)
            parquet = path.with_suffix('.parquet')
            if parquet.exists():
                patch_parquet(parquet, repaired, sort_years)
    if not args.write:
        print('\nDry run only. Re-run with --write to apply.')


if __name__ == '__main__':
    sys.exit(main())

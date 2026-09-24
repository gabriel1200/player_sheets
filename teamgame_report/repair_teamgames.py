"""
One-off repairs for the team-game pipeline. Dry run by default; --apply writes.

Run from player_sheets/teamgame_report/:

    # 1. Restore regular-season pbpstats logs that a Playoffs-only
    #    pbp_gamelogs.py run overwrote (per-team files + all_logs rebuilds).
    python repair_teamgames.py restore-logs --year 2026 --commit 7740a45754 [--apply]

    # 2. Drop incomplete dates from {year}{trail}_teamgames.csv so the next
    #    teamgame_report_scrape.py run refetches them (any age).
    python repair_teamgames.py drop-incomplete --year 2026 [--ps] [--apply]

    # 3. After rerunning the scrape: check the published all_games file.
    python repair_teamgames.py verify --year 2026 [--ps]

    # 4. Remove columns from every teamgames / all_games / games/{year} file.
    #    Used for the defense-within-6ft columns, which were season-to-date
    #    totals (url14 DateFrom typo) instead of per game. See BACKFILL_TODO.md.
    python repair_teamgames.py drop-columns --columns DEF6FT [--apply] [--allow-dirty]

--apply refuses to touch files with uncommitted git changes, so `git checkout
-- <file>` is always a clean undo.
"""
import argparse
import csv
import io
import os
import subprocess
import sys

import pandas as pd
import pyarrow.parquet as pq

from teamgame_checks import TRACKING_CHECK_COLS, date_report, expected_team_dates

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
TEAM_LOG_DIR = os.path.join(REPO, 'game_report', 'team')


def _git(*args):
    return subprocess.run(['git', *args], cwd=REPO, capture_output=True, text=True, check=True).stdout


def _require_clean(paths):
    rel = [os.path.relpath(p, REPO) for p in paths]
    dirty = _git('status', '--porcelain', '--', *rel).strip()
    if dirty:
        sys.exit(f"Refusing to write: uncommitted changes in\n{dirty}\nCommit or stash them first.")


def _log_files(year_dir):
    return sorted(f for f in os.listdir(year_dir)
                  if f.endswith('.csv') and f.replace('vs', '')[:-4].isdigit())


def _rebuild_all_logs(year_dir):
    """Same aggregation as pbp_gamelogs.py: concat per-team files, TeamId from filename."""
    out = {}
    for vs in (False, True):
        files = [f for f in _log_files(year_dir) if ('vs' in f) == vs]
        frames = []
        for f in files:
            df = pd.read_csv(os.path.join(year_dir, f))
            df['TeamId'] = f[:-4].split('vs')[0]
            frames.append(df)
        out['vs_all_logs.csv' if vs else 'all_logs.csv'] = pd.concat(frames, ignore_index=True)
    return out


def _season_summary(df, label):
    d = df.dropna(subset=['GameId'])
    for st, g in d.groupby(d['SeasonType'].fillna('?')):
        per_game = g.groupby('GameId').size().value_counts().to_dict()
        print(f"  {label:18s} {st:15s} rows {len(g):5d}  games {g.GameId.nunique():5d}  "
              f"teams {g.TeamId.nunique():2d}  rows/game {per_game}")


def restore_logs(args):
    year_dir = os.path.join(TEAM_LOG_DIR, str(args.year))
    git_dir = f"game_report/team/{args.year}"
    git_files = {os.path.basename(p) for p in _git('ls-tree', '--name-only', args.commit, git_dir + '/').split()}
    files = sorted(set(_log_files(year_dir)) | {f for f in git_files if f.replace('vs', '')[:-4].isdigit()})

    merged, restored_rows = {}, 0
    for f in files:
        path = os.path.join(year_dir, f)
        cur = pd.read_csv(path) if os.path.exists(path) else pd.DataFrame()
        old = pd.DataFrame()
        if f in git_files:
            old = pd.read_csv(io.StringIO(_git('show', f"{args.commit}:{git_dir}/{f}")))
        if cur.empty:
            new = old
        elif old.empty or 'GameId' not in old.columns:
            new = cur
        else:
            # Current rows win; bring back any game the current file lost.
            missing = old[~old['GameId'].isin(cur['GameId'])]
            new = pd.concat([missing, cur], ignore_index=True)
            if 'Date' in new.columns:
                new = new.sort_values('Date', kind='stable').reset_index(drop=True)
        restored_rows += len(new) - len(cur)
        merged[f] = new

    print(f"{args.year}: {len(files)} team files, {restored_rows} rows restored from {args.commit}")
    print("Before:")
    for name in ('all_logs.csv', 'vs_all_logs.csv'):
        p = os.path.join(year_dir, name)
        if os.path.exists(p):
            _season_summary(pd.read_csv(p, usecols=['GameId', 'TeamId', 'SeasonType']), name)

    if not args.apply:
        # Preview the rebuilt aggregates without writing.
        frames = {False: [], True: []}
        for f, df in merged.items():
            df = df.copy()
            df['TeamId'] = f[:-4].split('vs')[0]
            frames['vs' in f].append(df)
        print("After (preview):")
        _season_summary(pd.concat(frames[False], ignore_index=True), 'all_logs.csv')
        _season_summary(pd.concat(frames[True], ignore_index=True), 'vs_all_logs.csv')
        print("\nDry run. Re-run with --apply to write.")
        return

    targets = [os.path.join(year_dir, f) for f in merged] + \
              [os.path.join(year_dir, n) for n in ('all_logs.csv', 'vs_all_logs.csv')]
    _require_clean([t for t in targets if os.path.exists(t)])
    for f, df in merged.items():
        df.to_csv(os.path.join(year_dir, f), index=False)
    print("After:")
    for name, df in _rebuild_all_logs(year_dir).items():
        df.to_csv(os.path.join(year_dir, name), index=False)
        _season_summary(df, name)


def drop_incomplete(args):
    trail = 'ps' if args.ps else ''
    path = os.path.join(HERE, 'year_files', f"{args.year}{trail}_teamgames.csv")
    saved = pd.read_csv(path, low_memory=False)
    saved['date'] = saved['date'].astype(int)
    expected = expected_team_dates(args.year, args.ps)
    report = date_report(saved, expected)
    if report.empty:
        print(f"{args.year}{trail}: no incomplete dates.")
        return
    print(f"{args.year}{trail}: {len(report)} incomplete dates "
          f"(blank {'/'.join(TRACKING_CHECK_COLS)} or fewer teams than scheduled)")
    print(report.to_string(index=False))
    drop = saved['date'].isin(report['date'])
    print(f"{int(drop.sum())} of {len(saved)} rows would be removed; the next scrape run refetches these dates.")
    if not args.apply:
        print("\nDry run. Re-run with --apply to write.")
        return
    _require_clean([path])
    saved[~drop].to_csv(path, index=False)
    print(f"Wrote {path}")


# Columns contributed only by url14 (leaguedashptteamdefend, Less Than 6Ft).
DEF6FT_COLUMNS = ['FREQ', 'FGM_LT_06', 'FGA_LT_06', 'LT_06_PCT', 'NS_LT_06_PCT', 'PLUSMINUS']


def _drop_csv_columns(path, drop):
    """Remove columns by streaming rows, so every other value stays byte-for-byte identical."""
    with open(path, newline='') as fh:
        rows = list(csv.reader(fh))
    keep = [i for i, name in enumerate(rows[0]) if name not in drop]
    tmp = path + '.tmp'
    with open(tmp, 'w', newline='') as fh:
        w = csv.writer(fh, lineterminator='\n')
        for row in rows:
            w.writerow([row[i] for i in keep if i < len(row)])
    os.replace(tmp, path)


def drop_columns(args):
    cols = DEF6FT_COLUMNS if args.columns == 'DEF6FT' else [c.strip() for c in args.columns.split(',') if c.strip()]
    year_dir = os.path.join(HERE, 'year_files')
    targets = sorted(
        os.path.join(year_dir, f) for f in os.listdir(year_dir)
        if (f.endswith('_teamgames.csv') and f[:4].isdigit())
        or (f.startswith('all_games_') and f.endswith(('.csv', '.parquet')))
    )
    games_root = os.path.join(HERE, 'games')
    game_files = sorted(
        os.path.join(games_root, y, f)
        for y in os.listdir(games_root) if os.path.isdir(os.path.join(games_root, y))
        for f in os.listdir(os.path.join(games_root, y)) if f.endswith('.csv')
    )
    print(f"Columns: {cols}")
    if not args.allow_dirty:
        _require_clean(targets + [games_root])

    changed = 0
    for path in targets + game_files:
        if path.endswith('.parquet'):
            names = pq.read_schema(path).names
        else:
            with open(path, newline='') as fh:
                names = next(csv.reader(fh), [])
        present = [c for c in cols if c in names]
        if not present:
            continue
        changed += 1
        if path in targets:
            print(f"  {os.path.relpath(path, HERE)}: drop {present}")
        if args.apply:
            if path.endswith('.parquet'):
                table = pq.read_table(path)
                pq.write_table(table.drop(present), path)
            else:
                _drop_csv_columns(path, present)
    print(f"{changed} files contain these columns ({len(game_files)} per-game files checked).")
    if not args.apply:
        print("\nDry run. Re-run with --apply to write.")


def verify(args):
    trail = 'ps' if args.ps else ''
    path = os.path.join(HERE, 'year_files', f"all_games_{args.year}{trail}.parquet")
    df = pd.read_parquet(path, columns=['GameId', 'TEAM_ID', 'TEAM_ABBREVIATION', 'date', *TRACKING_CHECK_COLS])
    expected = expected_team_dates(args.year, args.ps)
    exp_games = set(expected['GAME_ID'].astype(int))
    got_games = set(df['GameId'].astype(int))
    pairs = df.groupby('GameId').size().value_counts().to_dict()
    blank = df[df[TRACKING_CHECK_COLS].isna().any(axis=1)]
    print(f"{os.path.basename(path)}: {len(df)} rows, {len(got_games)} games (schedule {len(exp_games)}), rows/game {pairs}")
    missing = sorted(exp_games - got_games)
    print(f"  games missing: {len(missing)} {missing[:10]}")
    print(f"  rows with blank tracking: {len(blank)} on dates {sorted(blank['date'].unique().tolist())[:12]}")
    ok = not missing and set(pairs) == {2} and blank.empty
    print("  OK" if ok else "  NOT CLEAN")
    sys.exit(0 if ok else 1)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest='cmd', required=True)
    r = sub.add_parser('restore-logs')
    r.add_argument('--year', type=int, required=True)
    r.add_argument('--commit', required=True, help='commit with complete regular-season logs')
    r.add_argument('--apply', action='store_true')
    d = sub.add_parser('drop-incomplete')
    d.add_argument('--year', type=int, required=True)
    d.add_argument('--ps', action='store_true')
    d.add_argument('--apply', action='store_true')
    v = sub.add_parser('verify')
    v.add_argument('--year', type=int, required=True)
    v.add_argument('--ps', action='store_true')
    c = sub.add_parser('drop-columns')
    c.add_argument('--columns', required=True, help="comma-separated names, or DEF6FT for the url14 columns")
    c.add_argument('--apply', action='store_true')
    c.add_argument('--allow-dirty', action='store_true', help='skip the clean-git check (e.g. right after a rescrape)')
    args = p.parse_args()
    {'restore-logs': restore_logs, 'drop-incomplete': drop_incomplete, 'verify': verify,
     'drop-columns': drop_columns}[args.cmd](args)


if __name__ == '__main__':
    main()

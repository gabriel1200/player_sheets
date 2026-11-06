#!/usr/bin/env python
"""Time of possession's default value. Owner's call, 2026-11-05 (nba-pipeline/docs/tracking-vintages.md, "Time of possession").

From 2024-25 the NBA's TIME_OF_POSS carries lumps of about 3.7 extra minutes on random player-games, so it can't show how a player changes from season to season:
- Through 2023-24: the NBA's number, untouched.
- 2024-25 and 2025-26: the values saved before the revision, written once (nba-pipeline/tools/top_default/apply_saved.py).
- 2026-27 on: an ESTIMATE, from this file. Seconds = 1.42 x touches + 0.88 x dribbles - 2.77 per player-game, times the player's frozen factor (top_factors.csv).
  The factor is his 2025-26 real number over this formula, shrunk toward 1. It can never be refreshed, because real values stop at 2025-26. New players get 1.
  Seconds per touch is the estimate over touches, x 0.92 to stay on the older touch count every earlier season uses (owner's suggestion). So touches x seconds per touch is about 8% off time of possession on these rows, as it already is in the 2024-25 and 2025-26 game logs.

The NBA's own numbers stay, in TIME_OF_POSS_NBA and AVG_SEC_PER_TOUCH_NBA.

    python top_default.py players    # after game_report_scrape.py rebuilds all_games and the per-game files
    python top_default.py teams      # after teamgame_report_scrape.py
"""
import csv, datetime as _dt, glob, os, sys
import numpy as np, pandas as pd, pyarrow as pa, pyarrow.parquet as pq

csv.field_size_limit(10 ** 9)
HERE = os.path.dirname(os.path.abspath(__file__))
_today = _dt.date.today()
SEASON_YEAR = int(os.environ.get('NBA_SEASON_YEAR') or _today.year + (_today.month >= 10))
PLAYOFFS = os.environ.get('NBA_PLAYOFFS') == '1'
FIRST_ESTIMATED = 2027
SEC_PER_TOUCH, SEC_PER_DRIBBLE, SEC_CONSTANT = 1.42, 0.88, -2.77     # fit on 2024-25 and 2025-26 against the saved values
PARTIAL_TOUCHES, DOUBLED_TOUCHES_PER_MIN = 200, 2.4                  # team-games outside these aren't real tracking (web_app/scripts/build_game_finder.py)
TOUCH_SCALE = 0.92     # the NBA's touches now per the older count, the same team-games both ways: 0.922 in 2024-25, 0.915 in 2025-26. Unscaled, a 2025-26 test read 0.26 s too high league-wide and 27% of players moved the wrong way; scaled, 0.01 s and 16%.
ON, SEC, ON_NBA, SEC_NBA = 'TIME_OF_POSS', 'AVG_SEC_PER_TOUCH', 'TIME_OF_POSS_NBA', 'AVG_SEC_PER_TOUCH_NBA'


def factors():
    path = os.path.join(HERE, 'top_factors.csv')
    return pd.read_csv(path).set_index('PLAYER_ID').factor if os.path.exists(path) else pd.Series(dtype=float)


def formula(touches, drib_per_touch):
    """Minutes, before any player factor. A player with no touches recorded reads 0."""
    touches = pd.to_numeric(touches, errors='coerce').astype(float); dribbles = touches * pd.to_numeric(drib_per_touch, errors='coerce').fillna(0)
    return ((SEC_PER_TOUCH * touches + SEC_PER_DRIBBLE * dribbles + SEC_CONSTANT) / 60).clip(lower=0).where(touches.notna())


def real_tracking(frame):
    """False for team-games with touches missing or double-counted: the formula would read their error."""
    team = frame.groupby(['GAME_ID', 'TEAM_ABBREVIATION'])[['TOUCHES', 'MIN']].transform('sum')
    return (team.TOUCHES >= PARTIAL_TOUCHES) & (team.TOUCHES <= DOUBLED_TOUCHES_PER_MIN * team.MIN)


def estimate(frame, table=None):
    """The estimated minutes for each player-game of a frame. Blank where the game's tracking isn't real."""
    table = factors() if table is None else table
    est = formula(frame.TOUCHES, frame.AVG_DRIB_PER_TOUCH) * frame.PLAYER_ID.map(table).fillna(1.0).values
    return est.where(real_tracking(frame)).round(1)


def write_columns(frame, default, seconds, base):
    """Write the default into TIME_OF_POSS and AVG_SEC_PER_TOUCH and keep the NBA's beside them, in the parquet and the csv of one file. Run twice, the result is the same."""
    for col, new, keep in ((ON, default, ON_NBA), (SEC, seconds, SEC_NBA)):
        nba = frame[keep].combine_first(frame[col]) if keep in frame else frame[col]
        frame[keep] = nba
        frame[col] = new.values
    frame.to_parquet(base + '.parquet.tmp', index=False); os.replace(base + '.parquet.tmp', base + '.parquet')
    edit_csv(base + '.csv', frame)


def edit_csv(path, frame):
    """Only these four columns change, so every other cell keeps its exact text."""
    with open(path, newline='', encoding='utf-8', errors='surrogateescape') as f:
        rows = list(csv.reader(f))
    head = rows[0]
    if len(rows) - 1 != len(frame):
        raise ValueError(f'{path}: {len(rows) - 1} rows, the parquet has {len(frame)}')
    for col in (ON_NBA, SEC_NBA):
        if col not in head:
            head.append(col); [r.append('') for r in rows[1:]]
    key = 'PLAYER_ID' if 'PLAYER_ID' in head else 'TEAM_ID'; k = head.index(key)
    for row, want in zip(rows[1:], frame[key].values):      # the same rows in the same order, or nothing is written
        if int(float(row[k])) != int(want):
            raise ValueError(f'{path}: row order differs from the parquet')
    at = {c: head.index(c) for c in (ON, SEC, ON_NBA, SEC_NBA)}
    vals = {c: frame[c].values for c in at}
    for i, row in enumerate(rows[1:]):
        for c, j in at.items():
            v = vals[c][i]
            row[j] = '' if v != v else repr(round(float(v), 3)).rstrip('0').rstrip('.') if float(v) != int(v) else str(int(v))
    with open(path + '.tmp', 'w', newline='', encoding='utf-8', errors='surrogateescape') as f:
        csv.writer(f, lineterminator='\n').writerows(rows)
    os.replace(path + '.tmp', path)


def seconds_per_touch(minutes, touches):
    return (minutes * 60 * TOUCH_SCALE / pd.to_numeric(touches, errors='coerce').replace(0, np.nan)).round(2)


def apply_players(year=SEASON_YEAR, ps=PLAYOFFS):
    trail = 'ps' if ps else ''
    base = os.path.join(HERE, f'game_report/all_games/all_{year}{trail}')
    if year < FIRST_ESTIMATED or not os.path.exists(base + '.parquet'):
        print('top_default players: nothing to do for', year, trail); return
    frame = pd.read_parquet(base + '.parquet')
    frame['GAME_ID'] = pd.to_numeric(frame.GAME_ID, errors='coerce')
    est = estimate(frame); write_columns(frame, est, seconds_per_touch(est, frame.TOUCHES), base)
    games = apply_game_files(frame, os.path.join(HERE, f'game_report/{year}'))
    print(f'top_default players {year}{trail}: {len(frame)} rows, {est.isna().sum()} blank, {games} per-game files, per team-game {est.sum() / frame.groupby(["GAME_ID", "TEAM_ABBREVIATION"]).ngroups:.1f} (NBA {frame[ON_NBA].sum() / frame.groupby(["GAME_ID", "TEAM_ABBREVIATION"]).ngroups:.1f})')


def apply_game_files(frame, folder):
    """The per-game files read by Box & Tracking. nba-pipeline's report tables read the _NBA columns back out of them."""
    done = 0
    for gid, rows in frame.groupby('GAME_ID'):
        for name in (f'{int(gid)}.csv', f'00{int(gid)}.csv'):
            path = os.path.join(folder, name)
            if os.path.exists(path):
                edit_game_file(path, rows); done += 1; break
    return done


def edit_game_file(path, rows):
    with open(path, newline='', encoding='utf-8', errors='surrogateescape') as f:
        lines = list(csv.reader(f))
    head = lines[0]
    for col in (ON_NBA, SEC_NBA):
        if col not in head:
            head.append(col); [r.append('') for r in lines[1:]]
    pid = head.index('PLAYER_ID'); at = {c: head.index(c) for c in (ON, SEC, ON_NBA, SEC_NBA)}; by = rows.drop_duplicates('PLAYER_ID').set_index('PLAYER_ID')
    for row in lines[1:]:
        try:
            r = by.loc[int(float(row[pid]))]
        except (KeyError, ValueError):
            continue
        for c, j in at.items():
            v = r[c]; row[j] = '' if v != v else repr(round(float(v), 3)).rstrip('0').rstrip('.') if float(v) != int(v) else str(int(v))
    with open(path + '.tmp', 'w', newline='', encoding='utf-8', errors='surrogateescape') as f:
        csv.writer(f, lineterminator='\n').writerows(lines)
    os.replace(path + '.tmp', path)


def team_sums(year, ps):
    """Each team-game's default: its players' sum."""
    d = pd.read_parquet(os.path.join(HERE, f'game_report/all_games/all_{year}{"ps" if ps else ""}.parquet'), columns=['GAME_ID', 'TEAM_ABBREVIATION', 'PLAYER_ID', ON])
    d['GAME_ID'] = pd.to_numeric(d.GAME_ID, errors='coerce')
    return d.groupby(['GAME_ID', 'TEAM_ABBREVIATION'])[ON].sum(min_count=1).rename('new').reset_index()


def apply_teams(year=SEASON_YEAR, ps=PLAYOFFS):
    trail = 'ps' if ps else ''
    if year < FIRST_ESTIMATED:
        print('top_default teams: nothing to do for', year, trail); return
    apply_team_file(year, ps)
    apply_team_totals(year, trail)


def apply_team_file(year, ps):
    trail = 'ps' if ps else ''
    base = os.path.join(HERE, f'teamgame_report/year_files/all_games_{year}{trail}')
    if not os.path.exists(base + '.parquet'):
        return
    frame = pd.read_parquet(base + '.parquet'); s = team_sums(year, ps)
    j = frame[['GameId', 'TEAM_ABBREVIATION']].merge(s, left_on=['GameId', 'TEAM_ABBREVIATION'], right_on=['GAME_ID', 'TEAM_ABBREVIATION'], how='left')
    new = pd.Series(j.new.values, index=frame.index).round(1)
    write_columns(frame, new, seconds_per_touch(new, frame.TOUCHES), base)
    print(f'top_default teams {year}{trail}: {len(frame)} team-games, {new.isna().sum()} blank, mean {new.mean():.1f} (NBA {frame[ON_NBA].mean():.1f})')


def apply_team_totals(year, trail):
    """team_totals/<year><trail>_team_totals.csv and _team_games.csv: one season row per team."""
    s = team_sums(year, trail == 'ps'); t = s.groupby('TEAM_ABBREVIATION').new.sum(min_count=1)
    for name in ('team_totals', 'team_games'):
        path = os.path.join(HERE, f'team_totals/{year}{trail}_{name}.csv')
        if not os.path.exists(path):
            continue
        d = pd.read_csv(path, low_memory=False)
        if ON not in d or 'TEAM_ABBREVIATION' not in d:
            continue
        d[ON_NBA] = d[ON_NBA].combine_first(d[ON]) if ON_NBA in d else d[ON]
        d[ON] = d.TEAM_ABBREVIATION.map(t).round(1).values
        if SEC in d and 'TOUCHES' in d:
            d[SEC_NBA] = d[SEC_NBA].combine_first(d[SEC]) if SEC_NBA in d else d[SEC]; d[SEC] = seconds_per_touch(d[ON], d.TOUCHES)
        d.to_csv(path + '.tmp', index=False); os.replace(path + '.tmp', path)


def season_override(df, year, ps):
    """For gen_totals: a season file's time of possession is the sum of its game logs' default, from 2026-27 on. Earlier seasons' _avg files already hold the old numbers."""
    if year < FIRST_ESTIMATED:
        return df
    path = os.path.join(HERE, f'game_report/all_games/all_{year}{"ps" if ps else ""}.parquet')
    if not os.path.exists(path):
        return df
    if ON_NBA not in pq.ParquetFile(path).schema.names:     # the daily step didn't run: never sum the NBA's lumps into a season
        apply_players(year, ps)
    g = pd.read_parquet(path, columns=['PLAYER_ID', ON]); by = g.groupby(g.PLAYER_ID.astype(int).astype(str))[ON].sum(min_count=1)
    ids = df.PLAYER_ID.astype(float).astype(int).astype(str)
    df = df.copy(); df[ON] = ids.map(by).values
    if 'TOUCHES' in df and SEC in df:
        df[SEC] = seconds_per_touch(df[ON], df.TOUCHES).values
    return df


if __name__ == '__main__':
    which = sys.argv[1] if len(sys.argv) > 1 else ''
    if which == 'players':
        apply_players()
    elif which == 'teams':
        apply_teams()
    else:
        raise SystemExit('python top_default.py players|teams')

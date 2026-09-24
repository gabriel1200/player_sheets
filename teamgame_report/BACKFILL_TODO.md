# Backfill needed: per-game "defense within 6 ft" (and `team_poss`)

Status: **columns deleted 2026-09-23, backfill not yet run.**

## What was wrong

`teamgame_report_scrape.ipynb` pulls `leaguedashptteamdefend` (Less Than 6Ft) once per date as
`url14`. The URL had `DateFrom{date}=` instead of `DateFrom={date}`, so the start date was blank and
every call returned **season-to-date totals through that date** instead of that day's game.

Checked against the live API for 2026-04-01:

| URL | teams | `FGA_LT_06` | `GP` |
|---|---|---|---|
| `DateFrom{date}=` (old) | 30 | 1,964–2,703 | 75–77 |
| `DateFrom={date}` (fixed) | 18 (teams that played) | 22–43 | 1 |

Separately, `url17` (Advanced, for `team_poss`) hardcoded `SeasonType=Playoffs`, so `team_poss` was
blank in every regular-season file. Playoff files were correct.

(`gen_totals/averages_scrape.py` and `team_totals/team_average_scrape.py` use the same
`DateFrom{date}=` pattern on purpose: they want season-to-date totals. Those are unchanged.)

## What was done

1. Both URLs fixed in the notebook and the generated `.py`, effective from the next scrape
   (2026-27 onward is correct from day one).
2. The six columns that only `url14` supplies were deleted from every file so no wrong values
   remain anywhere:

   `FREQ`, `FGM_LT_06`, `FGA_LT_06`, `LT_06_PCT`, `NS_LT_06_PCT`, `PLUSMINUS`

   ```
   python repair_teamgames.py drop-columns --columns DEF6FT --apply --allow-dirty
   ```

   Files: `year_files/{2014..2026}{,ps}_teamgames.csv`, `year_files/all_games_{year}{,ps}.csv/.parquet`,
   `games/{year}/*.csv`.

   Nothing in `web_app` reads these team-level columns (its `less_6ft_def_*` columns come from the
   player game reports). The `games/` files are read by `web_app/visuals/game_comparison_logic.py`
   and `teamseries.py`, neither of which uses these six columns.

## To do: refetch per date

`backfill_def6ft.py` refetches the columns with the fixed URL, date by date, and writes them back
into the teamgames cache, `all_games_*` and `games/{year}/`. It skips dates that are already filled,
so it can be stopped and restarted.

```
cd player_sheets/teamgame_report
python backfill_def6ft.py --years 2014-2026 --dry-run          # counts only
python backfill_def6ft.py --years 2014-2026 --team-poss        # regular season (+ team_poss)
python backfill_def6ft.py --years 2014-2026 --ps               # playoffs
./quick_commit.sh
```

Expect about an hour per regular-season pass at ~1.2 s between requests (twice that with
`--team-poss`, which makes a second call per date). Run it outside the daily scrape window.

After it runs, check a few games by hand: `FGA_LT_06` should be a single-game count (roughly 20–50),
and `GP` in the raw response 1. Then update the status line at the top of this file.

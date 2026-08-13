"""
Re-runs the averages scrape across ALL historical years and both season
types, using the fixed pull_avg()/pull_avg_classic() functions (url23
fix + fanout dedup applied 2026-08).

Purpose: purge the old buggy _avg.csv files (mislabeled more_15ft_def_*
columns for every year the script has ever run, plus duplicate rows for
the specific player-seasons where a source endpoint returned >1 row per
player) and regenerate them correctly.

PREREQUISITE: save the fixed averages_scrape.py (the one with url23 and
the drop_duplicates() fix) over your existing averages_scrape.py in the
gen_totals/ directory -- this script imports from that filename.

Boundary between pull_avg (modern, tracking-stats era) and
pull_avg_classic (pre-tracking-stats era) follows the split already
sketched in your own commented-out code: 2014+ uses pull_avg, pre-2014
uses pull_avg_classic.

Each of the 4 calls (classic RS, classic playoffs, modern RS, modern
playoffs) writes its own per-year CSVs as a side effect (that's what
pull_avg/pull_avg_classic already do internally), so this script doesn't
need to handle file output itself -- just trigger all 4 passes and not
let one failure kill the others.
"""

from averages_scrape import pull_avg, pull_avg_classic

CLASSIC_START = 1997
CLASSIC_END = 2014       # exclusive -- covers 1997-2013

MODERN_START = 2014
MODERN_END = 2027        # exclusive -- covers 2014-2026


def run(label, fn, *args, **kwargs):
    print(f"\n{'='*60}\nSTARTING: {label}\n{'='*60}")
    try:
        fn(*args, **kwargs)
        print(f"DONE: {label}")
    except Exception as e:
        # Don't let one failed pass (e.g. a transient NBA.com API error)
        # kill the other three -- print and move on, then you can re-run
        # just the failed one afterward.
        print(f"*** FAILED: {label} -- {e!r}")


if __name__ == "__main__":
    run("Classic era, Regular Season (1997-2013)",
        pull_avg_classic, [], CLASSIC_START, CLASSIC_END, ps=False)

    run("Classic era, Playoffs (1997-2013)",
        pull_avg_classic, [], CLASSIC_START, CLASSIC_END, ps=True)

    run("Modern era, Regular Season (2014-2026)",
        pull_avg, [], MODERN_START, MODERN_END, ps=False)

    run("Modern era, Playoffs (2014-2026)",
        pull_avg, [], MODERN_START, MODERN_END, ps=True)

    print("\nAll passes attempted. Check output above for any FAILED "
          "entries -- re-run just those years/season-types individually "
          "if needed rather than the whole backfill again.")
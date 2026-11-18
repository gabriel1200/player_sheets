#!/bin/bash
# lineups.py (pbpstats WOWY on/off) in the background, started by ../daily_scrape.sh (2026-11-18), so a slow pbpstats
# day no longer holds up all_scrapes (that morning: 1.5 h of timeouts and 502s). Then pushes player_sheets, after
# all_scrapes is done so this commit can't collide with its quick_commits. A team that fails keeps its last file.
# CourtSketch reads these files from GitHub (app.py find_lineups, visuals/breakdown.py, visuals/data_fetchers.py);
# /wowy_combos doesn't (it runs on the play-by-play engine). Log: ~/basketball/lineups_<date>_<time>.log.
#
#   bash ~/basketball/player_sheets/lineups/background.sh      (by hand: run in a detached shell, it waits)
cd "$(dirname "$(readlink -f "$0")")" || exit 1
exec 9>/tmp/player_sheets_lineups.lock
flock -n 9 || { echo "lineups.py is already running"; exit 0; }

echo "=== $(date +%H:%M:%S) lineups.py"
python -u lineups.py
echo "=== $(date +%H:%M:%S) lineups.py exited $?"

while pgrep -f "all_scrapes\.sh" > /dev/null; do sleep 30; done  # however it was started (./, bash, full path)
echo "=== $(date +%H:%M:%S) pushing player_sheets"
cd .. && ./quick_commit.sh
echo "=== $(date +%H:%M:%S) done"

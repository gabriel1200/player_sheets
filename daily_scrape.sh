#!/bin/bash

# Convert Jupyter notebooks to Python scripts in their respective directories
cd game_report
python ~/basketball/nb2py.py game_report_scrape.ipynb
python ~/basketball/nb2py.py pbp_gamelogs.ipynb
cd ..


cd game_report/all_games
python ~/basketball/nb2py.py master_record.ipynb
cd ../..
cd teamgame_report
python ~/basketball/nb2py.py teamgame_report_scrape.ipynb
cd ..

cd team_totals
python ~/basketball/nb2py.py team_average_scrape.ipynb
cd ..

cd gen_totals

python ~/basketball/nb2py.py gen_totals.ipynb
cd ..

cd lineups
cd ..

# Run the Python scripts in their respective directories
cd game_report
python game_report_scrape.py
python pbp_gamelogs.py

cd all_games/

python master_record.py
cd ../..

# Time of possession's default on this season's game logs, before gen_totals sums them (see top_default.py)
python top_default.py players


cd team_totals
python team_average_scrape.py
cp team_averages.csv ../teamgame_report/team_averages.csv
cp team_averages_ps.csv ../teamgame_report/team_averages_ps.csv
cd ..

cd gen_totals
python bio.py
python averages_scrape.py
python gen_totals.py
cd ..

cd teamgame_report
python teamgame_report_scrape.py
python ~/basketball/nb2py.py teamseries.ipynb
python teamseries.py
cd ..

python top_default.py teams

# Refresh this season's SRS/HCA sheet (web_app/data/srs_generated.csv) now
# that this season's teamgame_report parquet is current. Reads that parquet
# from the local file above (not the GitHub raw URL), so this doesn't need
# to wait for quick_commit.sh's push. Also depends on game_dates.csv, which
# shot_data/daily_scrape.sh already refreshes earlier in all_scrapes.sh.
python ../web_app/srs_by_season.py --current

# pbpstats WOWY on/off: in the background since 2026-11-18 (it held the whole run 1.5 h when pbpstats was down); it
# pushes player_sheets itself once all_scrapes is done. See lineups/background.sh.
(setsid nohup bash lineups/background.sh > "$HOME/basketball/lineups_$(date +%Y%m%d_%H%M).log" 2>&1 < /dev/null &)


cp team_totals/team_averages.csv ../web_app/data/team_averages.csv


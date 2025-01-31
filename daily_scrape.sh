jupyter nbconvert --to script game_report/game_report_scrape.ipynb

jupyter nbconvert --to script game_report/pbp_gamelogs.ipynb


jupyter nbconvert --to script team_totals/team_average_scrape.ipynb
jupyter nbconvert --to script gen_totals/averages_scrape.ipynb
jupyter nbconvert --to script gen_totals/gen_totals.ipynb

jupyter nbconvert --to script lineups/lineups.ipynb



python game_report/game_report_scrape.py

python game_report/pbp_gamelogs.py
python team_totals/team_average_scrape.py
python gen_totals/averages_scrape.py
python gen_totals/gen_totals.py

python lineups/lineups.py

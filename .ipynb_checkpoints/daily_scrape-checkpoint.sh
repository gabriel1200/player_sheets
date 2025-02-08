#!/bin/bash

# Convert Jupyter notebooks to Python scripts in their respective directories
cd game_report
jupyter nbconvert --to script game_report_scrape.ipynb
jupyter nbconvert --to script pbp_gamelogs.ipynb
cd ..

cd team_totals
jupyter nbconvert --to script team_average_scrape.ipynb
cd ..

cd gen_totals
jupyter nbconvert --to script averages_scrape.ipynb
jupyter nbconvert --to script gen_totals.ipynb
cd ..

cd lineups
jupyter nbconvert --to script lineups.ipynb
cd ..

# Run the Python scripts in their respective directories
cd game_report
python game_report_scrape.py
python pbp_gamelogs.py
cd ..

cd team_totals
python team_average_scrape.py
cd ..

cd gen_totals
python averages_scrape.py
python gen_totals.py
cd ..

cd lineups
python lineups.py
cd ..
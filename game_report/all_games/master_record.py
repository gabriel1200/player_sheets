#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import pandas as pd

# Define the directory containing the CSV files
csv_dir = '.'  # or provide a full path like '/Users/yourname/data/'

# Get all CSV file names in the directory
csv_files = ['all_'+str(year)+'.csv' for year in range (2014,2027)]

csv_files_ps=['all_'+str(year)+'ps.csv' for year in range (2014,2027)]
csv_files=csv_files+csv_files_ps
# Initialize a list to hold DataFrames
dfs = []
print(csv_files)
# Loop through and read each file
for filename in csv_files:
    file_path = os.path.join(csv_dir, filename)
    try:
        df = pd.read_csv(file_path)
        dfs.append(df)
    except Exception as e:
        print(f"Could not read {filename}: {e}")

# Concatenate all DataFrames into one
if dfs:
    combined_df = pd.concat(dfs, ignore_index=True)
    print(f"Combined DataFrame shape: {combined_df.shape}")
else:
    print("No CSV files were loaded.")


# In[7]:


index_col=['PLAYER_ID', 'PLAYER_NAME','TEAM_ABBREVIATION','TEAM_ID','GAME_ID','year','MIN','date']

game_record=combined_df[index_col]
game_record.drop_duplicates(inplace=True)
game_record.to_csv('master_record.csv',index=False)
game_record


# In[3]:


game_record


# In[ ]:


game_record.sort_values(by='date',inplace=True)
game_record


# In[ ]:





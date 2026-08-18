import pandas as pd

# Convert Regular Season
df = pd.read_csv("modern.csv", low_memory=False)
df.to_parquet("modern.parquet", engine="pyarrow", compression="snappy", index=False)

# Convert Playoffs
df_ps = pd.read_csv("modern_ps.csv", low_memory=False)
df_ps.to_parquet("modern_ps.parquet", engine="pyarrow", compression="snappy", index=False)
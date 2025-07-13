import pandas as pd
import numpy as np
from itertools import combinations
import time
from collections import defaultdict
from tqdm import tqdm
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing as mp
from functools import partial
import pickle
import os

# Set up logging for detailed progress tracking
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('basketball_analytics.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def extract_player_ids(entity_id):
    """Extract player IDs from EntityId string"""
    # Ensure entity_id is a string
    entity_id_str = str(entity_id)
    return entity_id_str.split('-')

def generate_combinations_vectorized(entity_ids, sizes=[2, 3, 4]):
    """Generate all combinations using vectorized operations where possible"""
    combo_to_rows = defaultdict(list)
    
    for idx, entity_id in enumerate(entity_ids):
        # Ensure entity_id is a string
        entity_id_str = str(entity_id)
        player_ids = entity_id_str.split('-')
        
        # Generate combinations for all sizes at once
        for size in sizes:
            if len(player_ids) >= size:
                for combo in combinations(player_ids, size):
                    # Ensure all player IDs are strings before sorting and joining
                    combo_strs = [str(pid) for pid in combo]
                    key = '-'.join(sorted(combo_strs))
                    combo_to_rows[key].append(idx)
    
    return combo_to_rows

def precompute_basketball_calculations(df):
    """Precompute all basketball-related calculations once"""
    # Calculate all derived fields in one pass
    df = df.copy()
    
    # Basic calculations
    df['FGA'] = df['FG2A'] + df['FG3A']
    df['FGM'] = df['FG2M'] + df['FG3M']
    df['opp_FGA'] = df['opp_FG2A'] + df['opp_FG3A']
    df['opp_FGM'] = df['opp_FG2M'] + df['opp_FG3M']
    
    # Calculate all miss columns at once
    miss_calculations = {
        'two_point_misses': ('FG2A', 'FG2M'),
        'opp_two_point_misses': ('opp_FG2A', 'opp_FG2M'),
        'at_rim_misses': ('AtRimFGA', 'AtRimFGM'),
        'opp_at_rim_misses': ('opp_AtRimFGA', 'opp_AtRimFGM'),
        'short_midrange_misses': ('ShortMidRangeFGA', 'ShortMidRangeFGM'),
        'opp_short_midrange_misses': ('opp_ShortMidRangeFGA', 'opp_ShortMidRangeFGM'),
        'long_midrange_misses': ('LongMidRangeFGA', 'LongMidRangeFGM'),
        'opp_long_midrange_misses': ('opp_LongMidRangeFGA', 'opp_LongMidRangeFGM'),
        'corner3_misses': ('Corner3FGA', 'Corner3FGM'),
        'opp_corner3_misses': ('opp_Corner3FGA', 'opp_Corner3FGM'),
        'arc3_misses': ('Arc3FGA', 'Arc3FGM'),
        'opp_arc3_misses': ('opp_Arc3FGA', 'opp_Arc3FGM'),
        'ft_misses': ('FTA', 'FtPoints'),
        'opp_ft_misses': ('opp_FTA', 'opp_FtPoints'),
        'fg_misses': ('FGA', 'FGM'),
        'opp_fg_misses': ('opp_FGA', 'opp_FGM')
    }
    
    for miss_col, (attempts, makes) in miss_calculations.items():
        if attempts in df.columns and makes in df.columns:
            df[miss_col] = df[attempts] - df[makes]
    
    return df

def process_team_data_optimized(team_id, year, ps=False, vs=False):
    """Optimized team data processing with caching"""
    # Ensure team_id is a string for consistent cache naming
    team_id_str = str(team_id)
    cache_key = f"{team_id_str}_{year}_{ps}_{vs}"
    cache_file = f"cache/{cache_key}.pkl"
    
    # Check cache first
    if os.path.exists(cache_file):
        try:
            with open(cache_file, 'rb') as f:
                return pickle.load(f)
        except:
            pass  # Cache corrupted, rebuild
    
    try:
        pstring = "_ps" if ps else ""
        
        # Load data - use team_id_str for file paths
        if vs == False:
            df1 = pd.read_csv(f"data/{year}/{team_id_str}{pstring}.csv")
            df2 = pd.read_csv(f"data/{year}/{team_id_str}_vs{pstring}.csv")
        else:
            df2 = pd.read_csv(f"data/{year}/{team_id_str}{pstring}.csv")
            df1 = pd.read_csv(f"data/{year}/{team_id_str}_vs{pstring}.csv")
        
        # Merge data efficiently
        df2 = df2.drop(columns=['team_vs'], errors='ignore')
        
        # Rename opponent columns in one operation
        opp_rename = {col: f'opp_{col}' for col in df2.columns if col != 'EntityId'}
        df2 = df2.rename(columns=opp_rename)
        
        # Merge
        df = pd.merge(df1, df2, on='EntityId', how='left')
        df = df.fillna(0)
        
        # Precompute all basketball calculations
        df = precompute_basketball_calculations(df)
        
        # Generate combinations
        combo_to_rows = generate_combinations_vectorized(df['EntityId'].values)
        
        # Cache the result
        os.makedirs('cache', exist_ok=True)
        with open(cache_file, 'wb') as f:
            pickle.dump((df, combo_to_rows), f)
        
        return df, combo_to_rows
        
    except Exception as e:
        logger.error(f"Error processing team {team_id}: {e}")
        return None, None

def compute_combo_stats_vectorized(df_subset, combo_key):
    """Vectorized computation of combination statistics - NO WEIGHTED AVERAGES"""
    if len(df_subset) == 0:
        return None
    
    # Define column groups - exclude percentage columns from initial aggregation
    id_cols = ['EntityId', 'TeamId', 'Name', 'ShortName', 'RowId', 'TeamAbbreviation', 
               'team_id', 'year', 'season', 'team_vs']
    pct_cols = [col for col in df_subset.columns if 'pct' in col.lower()]
    sum_cols = [col for col in df_subset.columns if col not in id_cols and col not in pct_cols]
    
    # Aggregate by summing (vectorized) - only non-percentage columns
    sums = df_subset[sum_cols].sum().to_frame().T
    
    # REMOVED: All weighted average calculations for efficiency
    # The following weighted metrics are no longer calculated:
    # - DefTwoPtReboundPct, OffTwoPtReboundPct, DefThreePtReboundPct
    # - DefFGReboundPct, OffFGReboundPct, OffLongMidRangeReboundPct
    # - DefLongMidRangeReboundPct, OffThreePtReboundPct, OffArc3ReboundPct
    # - DefArc3ReboundPct, DefAtRimReboundPct, DefShortMidRangeReboundPct
    # - DefCorner3ReboundPct, OffAtRimReboundPct, SelfORebPct
    # - OffShortMidRangeReboundPct, OffCorner3ReboundPct, SecondChanceTsPct
    # - SecondChanceCorner3PctAssisted, SecondChanceArc3PctAssisted
    # - SecondChanceAtRimPctAssisted
    
    # Calculate all percentage columns at the end for efficiency
    newframe = calculate_basketball_percentages_vectorized(sums)
    
    # Clean up opponent columns
    opp_cols = [col for col in newframe.columns if col.startswith('opp_')]
    newframe = newframe.drop(columns=opp_cols)
    
    # Add metadata
    newframe['combo_key'] = combo_key
    newframe['combo_size'] = len(str(combo_key).split('-'))
    
    return newframe

def calculate_basketball_percentages_vectorized(df):
    """Vectorized calculation of basketball percentages - moved to end for efficiency"""
    result = df.copy()
    
    # Use numpy operations for speed
    with np.errstate(divide='ignore', invalid='ignore'):
        # Basic shooting percentages
        result['Fg3Pct'] = np.where(result['FG3A'] > 0, result['FG3M'] / result['FG3A'], 0)
        result['Fg2Pct'] = np.where(result['FG2A'] > 0, result['FG2M'] / result['FG2A'], 0)
        
        # Ensure FGA and FGM are calculated
        result['FGA'] = result['FG2A'] + result['FG3A']
        result['FGM'] = result['FG2M'] + result['FG3M']
        result['PenaltyFGA'] = result['PenaltyFG2A'] + result['PenaltyFG3A']
        result['SecondChanceFGA'] = result['SecondChanceFG2A'] + result['SecondChanceFG3A']
        
        # NonHeaveFg3Pct calculation
        heave_adjusted_3pa = result['FG3A'] - result['HeaveAttempts']
        result['NonHeaveFg3Pct'] = np.where(heave_adjusted_3pa > 0, 
                                          result['FG3M'] / heave_adjusted_3pa, 0)
        
        # EfgPct calculation
        result['EfgPct'] = np.where(result['FGA'] > 0,
                                  (result['FG2M'] + 1.5 * result['FG3M']) / result['FGA'], 0)
        
        # True Shooting Percentage
        and1_2pt = result['2pt And 1 Free Throw Trips']
        and1_3pt = result['3pt And 1 Free Throw Trips']
        
        w = np.where(result['FTA'] > 0,
                    (and1_2pt + 1.5 * and1_3pt + 0.44 * (result['FTA'] - and1_2pt - and1_3pt)) / result['FTA'],
                    0.44)
        
        tss_denominator = 2 * (result['FGA'] + w * result['FTA'])
        result['TsPct'] = np.where(tss_denominator > 0,
                                 result['Points'] / tss_denominator, 0)
        
        # Additional percentage calculations can be added here
        # Free throw percentage
        result['FtPct'] = np.where(result['FTA'] > 0, result['FtPoints'] / result['FTA'], 0)
        
        # Overall field goal percentage
        result['FgPct'] = np.where(result['FGA'] > 0, result['FGM'] / result['FGA'], 0)
        
        # Shot zone percentages
        result['AtRimPct'] = np.where(result['AtRimFGA'] > 0, result['AtRimFGM'] / result['AtRimFGA'], 0)
        result['ShortMidRangePct'] = np.where(result['ShortMidRangeFGA'] > 0, 
                                            result['ShortMidRangeFGM'] / result['ShortMidRangeFGA'], 0)
        result['LongMidRangePct'] = np.where(result['LongMidRangeFGA'] > 0, 
                                           result['LongMidRangeFGM'] / result['LongMidRangeFGA'], 0)
        result['Corner3Pct'] = np.where(result['Corner3FGA'] > 0, 
                                      result['Corner3FGM'] / result['Corner3FGA'], 0)
        result['Arc3Pct'] = np.where(result['Arc3FGA'] > 0, 
                                   result['Arc3FGM'] / result['Arc3FGA'], 0)
    
    return result

def process_combinations_parallel(args):
    """Process combinations for a single team in parallel"""
    team_id, year, combo_keys, vs, ps = args
    
    # Ensure team_id is properly handled as string
    team_id_str = str(team_id)
    
    # Load preprocessed team data
    df, combo_to_rows = process_team_data_optimized(team_id_str, year, ps, vs)
    
    if df is None:
        return []
    
    results = []
    
    for combo_key in combo_keys:
        # Ensure combo_key is a string
        combo_key_str = str(combo_key)
        
        if combo_key_str in combo_to_rows:
            relevant_rows = combo_to_rows[combo_key_str]
            df_subset = df.iloc[relevant_rows]
            
            result = compute_combo_stats_vectorized(df_subset, combo_key_str)
            if result is not None:
                # Add team metadata
                result['team_id'] = team_id_str
                result['year'] = year
                result['combo_vs'] = vs
                result['season'] = f"{year-1}-{str(year)[-2:]}"
                results.append(result)
    
    return results

def get_year_combinations_parallel(year, ps=False, vs=False, sizes=[2, 3, 4], max_workers=None):
    """Parallel processing version of get_year_combinations with separate files per size"""
    logger.info(f"Starting parallel combination generation for year {year}")
    logger.info(f"Parameters: ps={ps}, vs={vs}, sizes={sizes}")
    
    # Load index
    index_file = 'index_master_ps.csv' if ps else 'index_master.csv'
    index = pd.read_csv(index_file)
    index = index.dropna(subset=['team_id'])
    index = index[index.year == year]
    
    unique_teams = index['team_id'].unique()
    logger.info(f"Found {len(unique_teams)} unique teams for year {year}")
    
    # First pass: collect all possible combinations, grouped by size
    logger.info("Collecting all possible combinations...")
    combos_by_size = defaultdict(set)
    
    for team_id in tqdm(unique_teams, desc="Collecting combinations"):
        try:
            # Ensure team_id is string
            team_id_str = str(team_id)
            _, combo_to_rows = process_team_data_optimized(team_id_str, year, ps, vs)
            if combo_to_rows:
                for combo_key in combo_to_rows.keys():
                    combo_key_str = str(combo_key)
                    combo_size = len(combo_key_str.split('-'))
                    if combo_size in sizes:
                        combos_by_size[combo_size].add(combo_key_str)
        except Exception as e:
            logger.warning(f"Error collecting combinations for team {team_id}: {e}")
            continue
    
    # Log combination counts by size
    for size in sizes:
        logger.info(f"Found {len(combos_by_size[size])} combinations of size {size}")
    
    # Process each size separately
    results_by_size = {}
    
    for size in sizes:
        if len(combos_by_size[size]) == 0:
            logger.warning(f"No combinations found for size {size}")
            results_by_size[size] = pd.DataFrame()
            continue
            
        logger.info(f"Processing size {size} combinations...")
        
        # Prepare parallel processing arguments for this size
        if max_workers is None:
            max_workers = min(mp.cpu_count() - 1, len(unique_teams))
        
        size_combos = list(combos_by_size[size])
        combo_chunks = np.array_split(size_combos, max_workers)
        
        args_list = []
        for team_id in unique_teams:
            for chunk in combo_chunks:
                if len(chunk) > 0:
                    # Ensure team_id is string when passing to args
                    args_list.append((str(team_id), year, chunk.tolist(), vs, ps))
        
        logger.info(f"Starting parallel processing for size {size} with {max_workers} workers")
        logger.info(f"Processing {len(args_list)} work units")
        
        # Process in parallel
        size_results = []
        
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            futures = [executor.submit(process_combinations_parallel, args) 
                      for args in args_list]
            
            # Collect results with progress bar
            for future in tqdm(as_completed(futures), 
                              total=len(futures), 
                              desc=f"Processing size {size}"):
                try:
                    results = future.result()
                    size_results.extend(results)
                except Exception as e:
                    logger.error(f"Error in parallel processing for size {size}: {e}")
                    continue
        
        logger.info(f"Size {size} processing complete. Generated {len(size_results)} results")
        
        if size_results:
            # Concatenate results for this size
            size_frame = pd.concat(size_results, ignore_index=True)
            size_frame['year'] = year
            results_by_size[size] = size_frame
            logger.info(f"Size {size} dataset shape: {size_frame.shape}")
        else:
            logger.warning(f"No results generated for size {size}")
            results_by_size[size] = pd.DataFrame()
    
    return results_by_size

def print_summary_stats_by_size(results_by_size, title):
    """Print summary statistics for results organized by size"""
    logger.info(f"\n{title} Summary:")
    logger.info("-" * 50)
    
    for size in sorted(results_by_size.keys()):
        frame = results_by_size[size]
        if not frame.empty:
            logger.info(f"Size {size}: {len(frame):,} combinations")
            logger.info(f"  Columns: {len(frame.columns)}")
            logger.info(f"  Memory usage: {frame.memory_usage(deep=True).sum() / 1024 / 1024:.1f} MB")
        else:
            logger.info(f"Size {size}: No data")

def save_results_by_size(results_by_size, year, vs=False):
    """Save results by size to separate CSV files"""
    vs_suffix = "_vs" if vs else ""
    
    for size in sorted(results_by_size.keys()):
        frame = results_by_size[size]
        if not frame.empty:
            filename = f"{year}_combinations_size_{size}{vs_suffix}.csv"
            frame.to_csv(filename, index=False)
            logger.info(f"Saved {len(frame):,} size-{size} combinations to {filename}")

# Optimized main execution
if __name__ == "__main__":
    # Set up timing
    overall_start = time.time()
    
    logger.info("="*60)
    logger.info("STARTING OPTIMIZED BASKETBALL ANALYTICS PROCESSING")
    logger.info("*** NO WEIGHTED AVERAGES FOR MAXIMUM EFFICIENCY ***")
    logger.info("="*60)
    
    # Clear cache if needed (uncomment to force refresh)
    # import shutil
    # if os.path.exists('cache'):
    #     shutil.rmtree('cache')
    
    try:
        # Generate regular combination statistics
        logger.info("Phase 1: Generating regular combination statistics...")
        phase1_start = time.time()
        
        results_combos = get_year_combinations_parallel(2025, sizes=[2, 3, 4])
        
        phase1_end = time.time()
        phase1_duration = phase1_end - phase1_start
        logger.info(f"Phase 1 completed in {phase1_duration:.2f} seconds")
        
        # Generate vs combination statistics
        logger.info("Phase 2: Generating vs combination statistics...")
        phase2_start = time.time()
        
        results_combos_vs = get_year_combinations_parallel(2025, vs=True, sizes=[2, 3, 4])
        
        phase2_end = time.time()
        phase2_duration = phase2_end - phase2_start
        logger.info(f"Phase 2 completed in {phase2_duration:.2f} seconds")
        
        # Print summary statistics
        print_summary_stats_by_size(results_combos, "Regular Combinations")
        print_summary_stats_by_size(results_combos_vs, "VS Combinations")
        
        # Save results by size
        logger.info("Saving results by size...")
        save_start = time.time()
        
        save_results_by_size(results_combos, 2025, vs=False)
        save_results_by_size(results_combos_vs, 2025, vs=True)
        
        save_end = time.time()
        logger.info(f"Results saved in {save_end - save_start:.2f} seconds")
        
        # Calculate totals for final summary
        total_regular = sum(len(frame) for frame in results_combos.values())
        total_vs = sum(len(frame) for frame in results_combos_vs.values())
        
        # Final summary
        overall_end = time.time()
        total_duration = overall_end - overall_start
        
        logger.info("="*60)
        logger.info("PROCESSING COMPLETE!")
        logger.info(f"Total execution time: {total_duration:.2f} seconds ({total_duration/60:.2f} minutes)")
        logger.info(f"Regular combinations: {total_regular:,} rows")
        logger.info(f"VS combinations: {total_vs:,} rows")
        logger.info(f"Total rows generated: {total_regular + total_vs:,}")
        
        # Log individual file details
        logger.info("\nGenerated files:")
        for size in [2, 3, 4]:
            reg_count = len(results_combos.get(size, pd.DataFrame()))
            vs_count = len(results_combos_vs.get(size, pd.DataFrame()))
            logger.info(f"  Size {size}: {reg_count:,} regular, {vs_count:,} vs")
            if reg_count > 0:
                logger.info(f"    -> 2025_combinations_size_{size}.csv")
            if vs_count > 0:
                logger.info(f"    -> 2025_combinations_size_{size}_vs.csv")
        
        logger.info("="*60)
        
    except Exception as e:
        logger.error(f"Fatal error during processing: {e}")
        raise
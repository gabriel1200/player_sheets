analyze_stat_win_rate <- function(stat_column, year=2025, playoffs=FALSE, by_team=FALSE) {
  # Build URL
  suffix <- if(playoffs) 'ps' else ''
  url <- paste0('https://raw.githubusercontent.com/gabriel1200/player_sheets/refs/heads/master/teamgame_report/year_files/all_games_', year, suffix, '.csv')
  
  # Load dataset
  print(paste0("Loading data from: ", url))
  df <- read.csv(url)
  df <- df[!duplicated(df[, c("GameId", "TEAM_ID")]), ]
  print(paste0("Games loaded: ", nrow(df), " rows"))
  
  # Make sure stat column exists
  if (!stat_column %in% colnames(df)) {
    print(paste0("Error: Column '", stat_column, "' not found."))
    return(NULL)
  }
  
  # Process games: each row = one game, columns for each team
  results <- data.frame()
  
  # Group by GameId
  game_ids <- unique(df$GameId)
  
  for (game_id in game_ids) {
    group <- df[df$GameId == game_id, ]
    group <- group[order(group$TEAM_ID), ]
    
    if (nrow(group) != 2) {
      next  # skip incomplete games
    }
    
    team1 <- group[1, ]
    team2 <- group[2, ]
    
    # Compare stat
    if (is.na(team1[[stat_column]]) || is.na(team2[[stat_column]])) {
      next
    }
    
    if (team1[[stat_column]] == team2[[stat_column]]) {
      next  # skip ties
    }
    
    # Determine which team had higher stat
    if (team1[[stat_column]] > team2[[stat_column]]) {
      higher_team <- team1
      lower_team <- team2
    } else {
      higher_team <- team2
      lower_team <- team1
    }
    
    result_row <- data.frame(
      TEAM_NAME = higher_team$TEAM_NAME,
      TEAM_ID = higher_team$TEAM_ID,
      won = higher_team$W,
      stat_value = higher_team[[stat_column]],
      stringsAsFactors = FALSE
    )
    names(result_row)[4] <- stat_column
    
    results <- rbind(results, result_row)
  }
  
  if (nrow(results) == 0) {
    print("No valid games found.")
    return(NULL)
  }
  
  # Summary
  avg_stat <- mean(results[[stat_column]])
  win_rate <- mean(results$won) * 100
  
  cat("\n--- Stat:", stat_column, "---\n")
  cat("Games where team had higher", stat_column, ":", nrow(results), "\n")
  cat("Average", stat_column, "in those games:", sprintf("%.2f", avg_stat), "\n")
  cat("Win rate when team had higher", stat_column, ":", sprintf("%.2f%%", win_rate), "\n")
  
  if (by_team == TRUE) {
    cat("\n--- Team Breakdown ---\n")
    
    # In R we use aggregate or dplyr for grouping operations
    library(dplyr)
    
    summary <- results %>%
      group_by(TEAM_NAME) %>%
      summarize(
        games_with_higher_stat = n(),
        wins = sum(won),
        avg_stat = mean(!!sym(stat_column))
      ) %>%
      mutate(win_rate = wins / games_with_higher_stat * 100) %>%
      arrange(desc(win_rate))
    
    # Format the win_rate and avg_stat for display
    summary$win_rate <- sprintf("%.2f%%", summary$win_rate)
    summary$avg_stat <- sprintf("%.2f", summary$avg_stat)
    
    print(summary)
  }
}

# Run the analysis
analyze_stat_win_rate('turnover_factor', year=2025, playoffs=FALSE, by_team=TRUE)
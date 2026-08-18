import pandas as pd
import requests

date_str = "03%2F09%2F2024"
season = "2023-24"
stype = "Regular%20Season"

headers = {
    "Host": "stats.nba.com",
    "Connection": "keep-alive",
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
    "Dnt": "1",
    "Sec-Ch-Ua": '"Not=A?Brand";v="99", "Google Chrome";v="151", "Chromium";v="151"',
    "Sec-Ch-Ua-Mobile": "?1",
    "Sec-Ch-Ua-Platform": '"Android"',
    "User-Agent": "Mozilla/5.0 (Linux; Android 15; Pixel 9) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Mobile Safari/537.36",
    "Accept": "*/*",
    "Origin": "https://www.nba.com",
    "Sec-Fetch-Site": "same-site",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Dest": "empty",
    "Referer": "https://www.nba.com/",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-US,en;q=0.9",
}

print(f"--- Probing NBA Stats API for Date: 20240309 ({season}) ---")

# 1. Base Stats
url_base = f"https://stats.nba.com/stats/leaguedashplayerstats?College=&Conference=&Country=&DateFrom={date_str}&DateTo={date_str}&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=&PaceAdjust=N&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={season}&SeasonSegment=&SeasonType={stype}&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight="
r_base = requests.get(url_base, headers=headers).json()
rows_base = len(r_base['resultSets'][0]['rowSet'])
print(f"1. Base Player Stats:      {rows_base} players returned")

# 2. Passing Tracking
url_pass = f"https://stats.nba.com/stats/leaguedashptstats?College=&Conference=&Country=&DateFrom={date_str}&DateTo={date_str}&Division=&DraftPick=&DraftYear=&GameScope=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PerMode=Totals&PlayerExperience=&PlayerOrTeam=Player&PlayerPosition=&PtMeasureType=Passing&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight="
r_pass = requests.get(url_pass, headers=headers).json()
rows_pass = len(r_pass['resultSets'][0]['rowSet'])
print(f"2. Passing Tracking:        {rows_pass} players returned")

# 3. Possessions Tracking
url_poss = f"https://stats.nba.com/stats/leaguedashptstats?College=&Conference=&Country=&DateFrom={date_str}&DateTo={date_str}&Division=&DraftPick=&DraftYear=&GameScope=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PerMode=Totals&PlayerExperience=&PlayerOrTeam=Player&PlayerPosition=&PtMeasureType=Possessions&Season={season}&SeasonSegment=&SeasonType={stype}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight="
r_poss = requests.get(url_poss, headers=headers).json()
rows_poss = len(r_poss['resultSets'][0]['rowSet'])
print(f"3. Possessions Tracking:    {rows_poss} players returned")

# 4. Hustle Stats
url_hustle = f"https://stats.nba.com/stats/leaguehustlestatsplayer?College=&Conference=&Country=&DateFrom={date_str}&DateTo={date_str}&Division=&DraftPick=&DraftYear=&GameScope=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=PerGame&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={season}&SeasonSegment=&SeasonType={stype}&TeamID=0&VsConference=&VsDivision=&Weight="
r_hustle = requests.get(url_hustle, headers=headers).json()
rows_hustle = len(r_hustle['resultSets'][0]['rowSet'])
print(f"4. Hustle Stats:            {rows_hustle} players returned")
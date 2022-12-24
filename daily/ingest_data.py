# --- SET UP --- #

# Load libraries
import sys
from os.path import dirname, join, abspath
import psycopg2
import warnings
warnings.filterwarnings("ignore")

# Set relative path
sys.path.insert(0, join(dirname(abspath(__file__)), '../'))

# Load functions
from functions.nba_api_functions import *
from functions.dk_api_functions import *

## Set up SQL connection
# Connect to DB
con = psycopg2.connect(
   database="nba_odds", user='postgres', password='password', host='127.0.0.1', port= '5432'
)

# Create cursor
cursor = con.cursor()

# Auto commit
con.autocommit = True
##

# Assign nba_header_data
nba_header_data = {
    'Connection': 'keep-alive',
    'Accept': 'application/json, text/plain, */*',
    'x-nba-stats-token': 'true',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36',
    'x-nba-stats-origin': 'stats',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-Mode': 'cors',
    'Referer': 'https://stats.nba.com/',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9',
}

# --- INGEST DATA --- #

## DraftKings API
# Game df and team game lines
nba_team_game_lines, nba_game_df, offer_length = get_nba_team_game_lines()

# Team Odds 
nba_team_odds_df = pd.concat(
    [create_nba_team_odds_df(nba_team_game_lines, game) for game in range(offer_length)], 
    axis=0
)

# Update in SQL
update_nba_team_odds(cursor, nba_game_df, nba_team_odds_df)

## NBA API
# Get today's schedule
nba_games_today = get_nba_games_today(nba_header_data)

# get team game logs
nba_api_team_game_logs = get_nba_api_team_game_logs(nba_header_data)

# get player game logs
nba_api_player_game_logs = get_nba_api_player_game_logs(nba_header_data)

# Update Data
update_nba_api_data(cursor, nba_games_today, nba_api_team_game_logs, nba_api_player_game_logs)

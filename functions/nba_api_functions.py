# Load libraries
import pandas as pd
import requests
from datetime import date
from re import sub

# Functions
def convert_camel_case(string: str):

    # Function to convert string to camelCase
    # Args:
    # string (str): string to convert to camelCase
    # Returns:
    # string converted to camelCase
    ###

    # First level regex + manipulation
    s = sub(r"(_|-)+", " ", string).title().replace(" ", "")

    # Return
    return ''.join([s[0].lower(), s[1:]])

def get_nba_games(nba_header_data: dict, day = date.today()):

    # Function to scrape NBA API for specified date
    # Args:
    # nba_header_data (dict): headers for NBA API request
    # day (str): date to get games for, format 'YYYY-MM-DD'
    # Returns
    # nba_games_today (df): dataframe with games for given date
    ###

    # Paste into url
    nba_schedule_url = 'https://stats.nba.com/stats/scoreboardv3?GameDate=' + \
        str(day) + '&LeagueID=00'

    # Send request
    r = requests.get(nba_schedule_url, headers=nba_header_data)

    # Get JSON response
    resp = r.json()

    # Set json resp columns to grab
    nba_json_schedule_cols = ['gameId', 'gameEt', 'awayTeam.teamId', 'awayTeam.teamTricode', 'awayTeam.teamName',
                              'homeTeam.teamId', 'homeTeam.teamTricode', 'homeTeam.teamName']

    # Normalize json and select columns
    nba_games_today = pd.json_normalize(resp["scoreboard"]['games'])

    # If games
    if len(nba_games_today) > 0:
        try:
            # Try to get games today
            nba_games_today = nba_games_today[nba_json_schedule_cols]

            # Create awayteamName and homeTeamName
            nba_games_today['awayTeamName'] = nba_games_today['awayTeam.teamTricode'] + \
                ' ' + nba_games_today['awayTeam.teamName']
            nba_games_today['homeTeamName'] = nba_games_today['homeTeam.teamTricode'] + \
                ' ' + nba_games_today['homeTeam.teamName']

            # Rename some columns
            nba_games_today.rename({'awayTeam.teamId': 'awayTeamId', 'awayTeam.teamTricode': 'awayTeamSlug',
                                    'homeTeam.teamId': 'homeTeamId', 'homeTeam.teamTricode': 'homeTeamSlug'}, axis=1, inplace=True)

            # Set columns to select
            nba_schedule_cols = ['gameId', 'gameEt', 'awayTeamId', 'awayTeamSlug', 'awayTeamName',
                                 'homeTeamId', 'homeTeamSlug', 'homeTeamName']

            # Select columns
            nba_games_today = nba_games_today[nba_schedule_cols]

            # Convert gameEt to datetime
            nba_games_today['gameEt'] = pd.to_datetime(
                nba_games_today['gameEt'])

            ## --- INSERT/UPDATE SQL --- ##
            # - Update all other columns where game id = gameId - #

            return nba_games_today
        except:
            # Error -> return print statement
            print("Error Returning Today's Games")
            return pd.DataFrame()

    else:
        # No Games Today -> return print statement
        print('No Games Found Today')
        return pd.DataFrame()

def get_nba_api_player_game_logs(nba_header_data: dict, date_from: str = None, date_to: str = None):

    # Function to scrape NBA API for Player game logs
    # Args:
    # nba_header_data (dict): headers for NBA API request
    # season_type (str): Season type, one of 'Regular Season', 'Playoffs', 'PlayIn'
    # date_from (str): Date from, format 'YYYY-MM-DD'
    # date_to (str): Date to, format 'YYYY-MM-DD'
    # Returns
    # nba_api_player_game_logs (df): dataframe with team game logs for given params
    ###

    # Set default dates if date_from or date_to is None
    # date_from
    if date_from is None:
        # Set to yesterday
        date_from = date.today() - pd.Timedelta(days=1)
    else:
        # If supplied, convert to date YYYY-MM-DD
        date_from = pd.to_datetime(date_from).date()

    # date_to
    if date_to is None:
        # Set to yesterday
        date_to = date.today() - pd.Timedelta(days=1)
    else:
        # If supplied, convert to date YYYY-MM-DD
        date_to = pd.to_datetime(date_to).date()
    ##

    # Determine season
    # Get current year and month
    current_year = date.today().year
    current_month = date.today().month

    # Derive season
    if current_month > 8:
        season_url = str(current_year) + '-' + str(current_year + 1)[2:4]
    else:
        season_url = str(current_year - 1) + '-' + str(current_year)[2:4]
    ##

    try:
        # Try to retrieve game logs

        # encode dates with %2F
        date_from_url = str(date_from.month) + '%2F' + \
            str(date_from.day) + '%2F' + str(date_from.year)
        date_to_url = str(date_to.month) + '%2F' + \
            str(date_to.day) + '%2F' + str(date_to.year)

        # Construct advanced url to get possessions
        nba_game_log_url_adv = f"https://stats.nba.com/stats/playergamelogs?DateFrom={date_from_url}&DateTo={date_to_url}&GameSegment=&LastNGames=0&LeagueID=00&Location=&MeasureType=Advanced&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=Totals&Period=0&PlusMinus=N&Rank=N&Season={season_url}&SeasonSegment=&SeasonType=&ShotClockRange=&TeamID=0&VsConference=&VsDivision="

        # Send request
        r = requests.get(nba_game_log_url_adv, headers=nba_header_data)

        # Get JSON response
        resp = r.json()

        # get column names of advanced response
        nba_api_player_game_logs_headers_adv = resp['resultSets'][0]['headers']

        # Set columns to select for advanced box
        nba_api_player_game_logs_columns_adv = ['PLAYER_ID', 'TEAM_ID', 'GAME_ID', 'POSS']

        # Turn rowSet into dataframe, set column names
        nba_api_player_poss_counts = pd.DataFrame(resp['resultSets'][0]['rowSet'], columns=nba_api_player_game_logs_headers_adv)[
            nba_api_player_game_logs_columns_adv]

        # Construct base game log url
        nba_game_log_url_base = f"https://stats.nba.com/stats/playergamelogs?DateFrom={date_from_url}&DateTo={date_to_url}&GameSegment=&LastNGames=0&LeagueID=00&Location=&MeasureType=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=Totals&Period=0&PlusMinus=N&Rank=N&Season={season_url}&SeasonSegment=&SeasonType=&ShotClockRange=&TeamID=0&VsConference=&VsDivision="

        # Send requets
        r = requests.get(nba_game_log_url_base, headers=nba_header_data)

        # Get JSON response
        resp = r.json()

        # get column names of base response
        nba_api_player_game_logs_headers_base = resp['resultSets'][0]['headers']

        # set columns to select for base game logs
        nba_api_player_game_logs_columns_base = ['GAME_ID', 'PLAYER_ID', 'TEAM_ID', 'WL', 'MIN', 'PTS', 'FGM',
                                               'FGA', 'FG3M', 'FG3A', 'FTM', 'FTA', 'OREB', 'DREB',
                                               'REB', 'AST', 'TOV', 'STL', 'BLK', 'BLKA', 'PF', 'PFD']
        # Turn rowSet into dataframe, set column names
        nba_api_player_game_logs = pd.DataFrame(resp['resultSets'][0]['rowSet'], columns=nba_api_player_game_logs_headers_base)[
            nba_api_player_game_logs_columns_base]

        # Join game logs with poss counts
        nba_api_player_game_logs = nba_api_player_game_logs.merge(
            nba_api_player_poss_counts, on=['GAME_ID', 'TEAM_ID', 'PLAYER_ID'])

        # Convert to camel case
        nba_api_player_game_logs.columns = [convert_camel_case(
            x) for x in nba_api_player_game_logs.columns]

        return nba_api_player_game_logs

    except:
        # Error -> return empty dataframe
        return pd.DataFrame()

def get_nba_api_team_game_logs(nba_header_data: dict, date_from: str = None, date_to: str = None):

    # Function to scrape NBA API for Team game logs
    # Args:
    # nba_header_data (dict): headers for NBA API request
    # season_type (str): Season type, one of 'Regular Season', 'Playoffs', 'PlayIn'
    # date_from (str): Date from, format 'YYYY-MM-DD'
    # date_to (str): Date to, format 'YYYY-MM-DD'
    # Returns
    # nba_api_team_game_logs (df): dataframe with team game logs for given params
    ###

    # Set default dates if date_from or date_to is None
    # date_from
    if date_from is None:
        # Set to yesterday
        date_from = date.today() - pd.Timedelta(days=1)
    else:
        # If supplied, convert to date YYYY-MM-DD
        date_from = pd.to_datetime(date_from).date()

    # date_to
    if date_to is None:
        # Set to yesterday
        date_to = date.today() - pd.Timedelta(days=1)
    else:
        # If supplied, convert to date YYYY-MM-DD
        date_to = pd.to_datetime(date_to).date()
    ##

    # Determine season
    # Get current year and month
    current_year = date.today().year
    current_month = date.today().month

    # Derive season
    if current_month > 8:
        season_url = str(current_year) + '-' + str(current_year + 1)[2:4]
    else:
        season_url = str(current_year - 1) + '-' + str(current_year)[2:4]
    ##

    try:
        # Try to retrieve game logs

        # encode dates with %2F
        date_from_url = str(date_from.month) + '%2F' + \
            str(date_from.day) + '%2F' + str(date_from.year)
        date_to_url = str(date_to.month) + '%2F' + \
            str(date_to.day) + '%2F' + str(date_to.year)

        # Construct advanced url to get possessions
        nba_game_log_url_adv = f"https://stats.nba.com/stats/teamgamelogs?DateFrom={date_from_url}&DateTo={date_to_url}&GameSegment=&LastNGames=0&LeagueID=00&Location=&MeasureType=Advanced&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=Totals&Period=0&PlusMinus=N&Rank=N&Season={season_url}&SeasonSegment=&SeasonType=&ShotClockRange=&TeamID=0&VsConference=&VsDivision="

        # Send request
        r = requests.get(nba_game_log_url_adv, headers=nba_header_data)

        # Get JSON response
        resp = r.json()

        # get column names of advanced response
        nba_api_team_game_logs_headers_adv = resp['resultSets'][0]['headers']

        # Set columns to select for advanced box
        nba_api_team_game_logs_columns_adv = ['TEAM_ID', 'GAME_ID', 'POSS']

        # Turn rowSet into dataframe, set column names
        nba_api_team_poss_counts = pd.DataFrame(resp['resultSets'][0]['rowSet'], columns=nba_api_team_game_logs_headers_adv)[
            nba_api_team_game_logs_columns_adv]

        # Construct base game log url
        nba_game_log_url_base = f"https://stats.nba.com/stats/teamgamelogs?DateFrom={date_from_url}&DateTo={date_to_url}&GameSegment=&LastNGames=0&LeagueID=00&Location=&MeasureType=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=Totals&Period=0&PlusMinus=N&Rank=N&Season={season_url}&SeasonSegment=&SeasonType=&ShotClockRange=&TeamID=0&VsConference=&VsDivision="

        # Send requets
        r = requests.get(nba_game_log_url_base, headers=nba_header_data)

        # Get JSON response
        resp = r.json()

        # get column names of base response
        nba_api_team_game_logs_headers_base = resp['resultSets'][0]['headers']

        # set columns to select for base game logs
        nba_api_team_game_logs_columns_base = ['GAME_ID', 'TEAM_ID', 'WL', 'MIN' ,'PTS', 'FGM',
                                               'FGA', 'FG3M', 'FG3A', 'FTM', 'FTA', 'OREB', 'DREB',
                                               'REB', 'AST', 'TOV', 'STL', 'BLK', 'BLKA', 'PF', 'PFD']
        # Turn rowSet into dataframe, set column names
        nba_api_team_game_logs = pd.DataFrame(resp['resultSets'][0]['rowSet'], columns=nba_api_team_game_logs_headers_base)[
            nba_api_team_game_logs_columns_base]

        # Join game logs with poss counts on GAME_ID and TEAM_ID
        nba_api_team_game_logs = nba_api_team_game_logs.merge(
            nba_api_team_poss_counts, on=['GAME_ID', 'TEAM_ID'])

        # Convert to camel case
        nba_api_team_game_logs.columns = [convert_camel_case(
            x) for x in nba_api_team_game_logs.columns]

        # Convert min to int
        nba_api_team_game_logs['min'] = nba_api_team_game_logs['min'].astype(int)
        
        return nba_api_team_game_logs

    except:
        # Error -> return empty dataframe
        return pd.DataFrame()

def update_nba_api_data(cursor, nba_games_today, nba_api_team_game_logs, nba_api_player_game_logs):
    
    ### Function to update data from the NBA API
    ## Args:
    # nba_games_today (df): dataframe from get_nba_games_today()
    # nba_api_team_game_logs (df): dataframe from get_nba_api_team_game_logs()
    # nba_api_player_game_logs (df): dataframe from get_nba_api_player_game_logs()
    ##
    ###
    
    try:
        # Try to update nba_games_today
        if len(nba_games_today) > 0:
            for index, row in nba_games_today.iterrows():
                # Query
                query = "CALL update_nbaapi_events(%s, %s, %s, %s, %s, %s, %s, %s)"
                # Execute
                cursor.execute(query, (row['gameId'], row['gameEt'], row['awayTeamId'],row['awayTeamSlug'], 
                                       row['awayTeamName'], row['homeTeamId'], row['homeTeamSlug'], row['homeTeamName']))
            print('Updated nba_api_events')
    except:
        print('Error updating nba_api_events')
        
    try:
        # Try to update nba_api_team_game_logs
        if len(nba_api_team_game_logs) > 0:
            for index, row in nba_api_team_game_logs.iterrows():
                # Query
                query = """
                            CALL update_nbaapi_team_game_logs(%s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """
                # Execute
                cursor.execute(query, (row['gameId'], row['teamId'], row['wl'],
                                    row['pts'], row['fgm'], row['fga'], row['fg3M'],
                                    row['fg3A'], row['ftm'], row['fta'], row['oreb'],
                                    row['dreb'], row['reb'], row['ast'], row['tov'],
                                    row['stl'], row['blk'], row['blka'], row['pf'],
                                    row['pfd'], row['poss'], row['min']))
            print('Updated nba_api_team_game_logs')
    except:
        print('Error updating nba_api_team_game_logs')
    
    try:
        # TRY to update nba_api_player_game_logs
        if len(nba_api_player_game_logs) > 0:
            for index, row in nba_api_player_game_logs.iterrows():
                # Query
                query = """
                CALL update_nbaapi_player_game_logs(%s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                # Execute
                cursor.execute(query, (row['gameId'], row['playerId'], row['teamId'], 
                                       row['wl'], row['min'], row['pts'], row['fgm'], 
                                       row['fga'], row['fg3M'], row['fg3A'], row['ftm'], row['fta'], 
                                       row['oreb'], row['dreb'], row['reb'], row['ast'],
                                       row['tov'], row['stl'],  row['blk'], row['blka'], 
                                       row['pf'], row['pfd'], row['poss']))
            print('Updated nba_api_player_game_logs')
                
    except:
        print('Error updating nba_api_player_game_logs')   
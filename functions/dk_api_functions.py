"""
Functions to pull data from DraftKings API
"""
# Import packages
import pandas as pd
import requests
import numpy as np

# Functions
def get_nba_team_game_lines():
    """
    Function to get offer subcategory 4511 from DK API (NBA team game lines)
    Returns:
    nba_team_game_lines (df): dataframe of available NBA team game lines
    nba_game_df (df): dataframe of available NBA games
    offer_length (int): number of offers to pull
    """
    try:
        # Set the API URL for NBA teams
        dk_nba_team_url = "https://sportsbook-us-ny.draftkings.com//sites/" + \
        "US-NY-SB/api/v5/eventgroups/42648?format=json"

        # Get team data from the API
        dk_nba_team_data = requests.get(dk_nba_team_url).json()["eventGroup"] #pylint: disable=line-too-long, missing-timeout

        # Construct NBA Game Dataframe
        # Select columns for games dataframe
        nba_game_df_cols = [
            "eventId",
            "nameIdentifier",
            "startDate",
            "teamShortName1",
            "teamShortName2",
            "eventStatus.state",
        ]

        # Create dataframe of games available
        nba_game_df = pd.json_normalize(dk_nba_team_data["events"])[
            nba_game_df_cols
        ]

        # Convert startDate to datetime,
        # then convert to EST by subtracting 5 hours
        nba_game_df["startDate"] = pd.to_datetime(nba_game_df["startDate"])
        nba_game_df["startDate"] = nba_game_df["startDate"] - pd.Timedelta(
            hours=5
        )

        # Rename eventStatus.state to gameState
        nba_game_df.rename(
            columns={"eventStatus.state": "gameState"}, inplace=True
        )

        # Only take games that have not started
        nba_game_df = nba_game_df[nba_game_df["gameState"] == "NOT_STARTED"]

        # Create away column from first word of nameIdentifier, trim whitespace
        nba_game_df["awayTeamName"] = (
            nba_game_df["nameIdentifier"].str.split(" @ ").str[0].str.strip()
        )
        # Create home column from second word of nameIdentifier, trim whitespace
        nba_game_df["homeTeamName"] = (
            nba_game_df["nameIdentifier"].str.split(" @ ").str[1].str.strip()
        )

        # Add NBA type column
        nba_game_df["leagueSlug"] = "NBA"

        # Rename teamShortName1 and teamShortName2
        nba_game_df.rename(
            columns={
                "teamShortName1": "awayTeamSlug",
                "teamShortName2": "homeTeamSlug",
            },
            inplace=True,
        )

        # Drop nameIdentifier and gameState
        nba_game_df.drop(columns=["nameIdentifier", "gameState"], inplace=True)

        # Make eventId an int
        nba_game_df["eventId"] = nba_game_df["eventId"].astype(int)
        ###

        ### Get offers df from offerCategories and offerSubcategoryDescriptors
        # Game lines first level
        nba_team_offer_cats = [
            x
            for x in dk_nba_team_data["offerCategories"]
            if x["name"] == "Game Lines"
        ][0]["offerSubcategoryDescriptors"]

        # Filter for normal game lines ID (total/point spread/moneyline)
        nba_team_game_lines = [
            x for x in nba_team_offer_cats if x["subcategoryId"] == 4511
        ][0]

        # Get number of offers to loop through
        offer_length = len(nba_team_game_lines["offerSubcategory"]["offers"])
        ###

        return nba_team_game_lines, nba_game_df, offer_length

    except RuntimeError:
        # Error retrieving data -> return empty
        return pd.DataFrame(), pd.DataFrame(), 0


def create_nba_team_odds_df(nba_team_game_lines, event_ind):
    """
    Function to create NBA team odds dataframe for a given event index
    Args:
    nba_team_game_lines (df): nba_team_game_lines from get_nba_team_game_lines()
    event_ind (int): index of event to get odds for
    Returns:
    event_odds_df (df): dataframe of available odds for passed event index
    """
    # Try to get event dataframe
    try:
        # Get event dataframe
        event_df = pd.json_normalize(
            nba_team_game_lines["offerSubcategory"]["offers"][event_ind]
        )
    except: # pylint: disable=bare-except
        # If live, return empty dataframe
        return pd.DataFrame()

    ## Get various odds
    # Spread
    try:
        spread_lines = pd.json_normalize(
            event_df["outcomes"][
                event_df[event_df["label"] == "Spread"].index[0]
            ]
        )[["oddsAmerican", "label", "line"]]
        spread_lines["oddType"] = "Spread"
    except: # pylint: disable=bare-except
        # Error -> return empty dataframe
        spread_lines = pd.DataFrame()

    # Moneyline
    try:
        ml_lines = pd.json_normalize(
            event_df["outcomes"][
                event_df[event_df["label"] == "Moneyline"].index[0]
            ]
        )[["oddsAmerican", "label"]]
        ml_lines["oddType"] = "Moneyline"
    except: # pylint: disable=bare-except
        # Error -> return empty dataframe
        ml_lines = pd.DataFrame()

    # Total
    try:
        total_lines = pd.json_normalize(
            event_df["outcomes"][
                event_df[event_df["label"] == "Total"].index[0]
            ]
        )[["oddsAmerican", "label", "line"]]
        total_lines["oddType"] = "Total"
    except: # pylint: disable=bare-except
        # Error -> return empty dataframe
        total_lines = pd.DataFrame()
    ##

    # Try to construct event_odds_df
    try:
        # Combine all odds into one dataframe
        event_odds_df = pd.concat([spread_lines, ml_lines, total_lines], axis=0)

        # Add event id to dataframe
        event_odds_df["eventId"] = event_df["eventId"][0]

    except: # pylint: disable=bare-except
        # Error -> return empty dataframe
        event_odds_df = pd.DataFrame()

    return event_odds_df


def update_nba_team_odds(cursor, nba_game_df, nba_team_odds_df, con):
    """
    Function to join meta info to nba_team_odds_df + update SQL tables
    Args:
    cursor (cursor): cursor to SQL database
    nba_game_df (df): nba_game_df from get_nba_team_game_lines()
    nba_team_odds_df (df): nba_team_odds_df from create_nba_team_odds_df()
    con (connection): connection to SQL database
    """
    try:
        # Try to join meta info to nba_team_odds_df
        if len(nba_team_odds_df) > 0:

            # Make eventId an int
            nba_team_odds_df["eventId"] = nba_team_odds_df["eventId"].astype(
                int
            )

            # join nba_game_df
            nba_team_odds_df = nba_team_odds_df.merge(
                nba_game_df[["eventId", "awayTeamName", "homeTeamName"]],
                on="eventId",
                how="inner",
            )

            # Create teamType column
            nba_team_odds_df["teamType"] = np.where(
                nba_team_odds_df["label"] == nba_team_odds_df["homeTeamName"],
                "Home",
                "Away",
            )

            # filter for over/under labels (which will always
            # be the same so can just take Over)
            over_under_lines = nba_team_odds_df[
                nba_team_odds_df["label"].isin(["Over"])
            ][["eventId", "line"]]
            over_under_lines.rename(
                columns={"line": "totalPointsLine"}, inplace=True
            )

            # Get spread lines
            spread_lines = nba_team_odds_df[
                nba_team_odds_df["oddType"] == "Spread"
            ][["eventId", "line", "teamType"]]
            spread_lines.rename(columns={"line": "spreadLine"}, inplace=True)

            # Filter out Over/Under labels
            nba_team_odds_df = nba_team_odds_df[
                ~nba_team_odds_df["label"].isin(["Over", "Under"])
            ]

            # Pivot wider on team type and eventId
            nba_team_odds_df = nba_team_odds_df.pivot_table(
                index=["eventId", "teamType"],
                columns=["oddType"],
                values=["oddsAmerican"],
                aggfunc="first",
            )

            # Fix colummn names then reset index
            nba_team_odds_df.columns = [
                "".join(col) for col in nba_team_odds_df.columns
            ]
            nba_team_odds_df = nba_team_odds_df.reset_index()

            # Join spread_lines and over_under_lines
            nba_team_odds_df = nba_team_odds_df.merge(
                spread_lines, on=["eventId", "teamType"], how="left"
            )
            nba_team_odds_df = nba_team_odds_df.merge(
                over_under_lines, on="eventId", how="left"
            )

            # Remove American from columns names
            nba_team_odds_df.columns = nba_team_odds_df.columns.str.replace(
                "American", ""
            )

            # Convert spreadLine, oddsSpread, and totalPoints to float
            nba_team_odds_df[
                ["spreadLine", "oddsSpread", "totalPointsLine", "oddsMoneyline"]
            ] = nba_team_odds_df[
                ["spreadLine", "oddsSpread", "totalPointsLine", "oddsMoneyline"]
            ].astype(
                float
            )

            # If event/teamType combo in SQL, update, else insert
            for (
                index, #pylint: disable=unused-variable
                row,
            ) in nba_team_odds_df.iterrows():
                # Query
                query = "CALL update_dkodds_nba_team(%s, %s, %s, %s, %s, %s)"
                # Execute
                cursor.execute(
                    query,
                    (
                        row["eventId"],
                        row["teamType"],
                        row["oddsMoneyline"],
                        row["oddsSpread"],
                        row["spreadLine"],
                        row["totalPointsLine"],
                    ),
                )

        print("Inserted/Updated dk_nba_team_odds")

    except: #pylint: disable=bare-except
        # No offers today
        print("No offers today")

    try:
        # Try to update nba game df
        # Query team_slug_lk to get correct team names
        team_fix = pd.read_sql(
            """
            SELECT
                dk_slug,
                team_slug
            FROM
                team_slug_lk
            WHERE
                league_slug = 'NBA'
            """,
            con=con
        )

        # Join team_fix to nba_team_odds_df
        # Replace away team names with correct names
        nba_game_df = nba_game_df.merge(
            team_fix,
            left_on="awayTeamSlug",
            right_on="dk_slug",
            how="left",
        )
        # Replace awayTeamSlug with team_slug
        nba_game_df["awayTeamSlug"].update(nba_game_df["team_slug"])

        # Drop joined columns
        nba_game_df.drop(
            columns=["team_slug", "dk_slug"],
            inplace=True,
        )

        # Replace home team names with correct names
        nba_game_df = nba_game_df.merge(
            team_fix,
            left_on="homeTeamSlug",
            right_on="dk_slug",
            how="left",
        )

        # Replace homeTeamSlug with team_slug
        nba_game_df["homeTeamSlug"].update(nba_game_df["team_slug"])


        if len(nba_game_df) > 0:
            for index, row in nba_game_df.iterrows():
                # Query
                query = "CALL update_dkevents(%s, %s, %s, %s, %s, %s, %s)"
                # Execute
                cursor.execute(
                    query,
                    (
                        row["eventId"],
                        row["startDate"],
                        row["awayTeamSlug"],
                        row["homeTeamSlug"],
                        row["awayTeamName"],
                        row["homeTeamName"],
                        row["leagueSlug"],
                    ),
                )

            print("Inserted/Updated dk_events")

    except: #pylint: disable=bare-except
        # No games today
        print("No games today")

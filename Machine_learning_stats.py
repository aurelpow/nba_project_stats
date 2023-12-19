import numpy as np
import pandas as pd
from tqdm.notebook import tqdm # tqdm allows to have a progress bar of the loop
import asyncio
from datetime import datetime
# Import PYDRIVE lybraries
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
#Import sklearn lybraries
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
import statistics
from sklearn.metrics import mean_squared_error


startTime = datetime.now()
## Identifying with google API to put the NBA_DB table into a variable : 
    # Specify the path to your credentials JSON file
json_keyfile = "C:/Users/aureb/OneDrive - Sport-Data/Documents/COURS/DATABIRD/PROJECT/imposing-bee-389610-823a1fac476d.json"
    # Define the scope
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    # Authenticate using the credentials
creds = ServiceAccountCredentials.from_json_keyfile_name(json_keyfile, scope)
client = gspread.authorize(creds)

def get_NBA_db(sheet_name,json_keyfile,scope):
    # Authenticate using the credentials
    creds = ServiceAccountCredentials.from_json_keyfile_name(json_keyfile, scope)
    client = gspread.authorize(creds)
    # Open the Google Sheet by title
    sheet_title = "NBA_DB"
    spreadsheet = client.open(sheet_name)
    # Get the first (and presumably only) sheet in the spreadsheet
    worksheet = spreadsheet.get_worksheet(0)
    # Convert the sheet data to a Pandas DataFrame
    return pd.DataFrame(worksheet.get_all_records())

def get_PLAYER_db(sheet_name,json_keyfile,scope):
    # Authenticate using the credentials
    creds = ServiceAccountCredentials.from_json_keyfile_name(json_keyfile, scope)
    client = gspread.authorize(creds)
    # Open the Google Sheet by title
    sheet_title = "NBA_DB"
    spreadsheet = client.open(sheet_name)
    # Get the first (and presumably only) sheet in the spreadsheet
    worksheet = spreadsheet.get_worksheet(0)
    return worksheet
#Create a function to have the deviation for each player
def players_deviations(nba_db,player_db):
  player_column = player_db.col_values(player_db.find("Player").col)# Get the values from the 'Player' column
  player_list = player_column[1:]# Remove the header row
  fantasy_deviation_l = []
  player_list_final = []
  for p in tqdm(player_list):
    if p in list(nba_db["player_name"]):
       if list(nba_db["player_name"]).count(p) > 2:
        player_list_final.append(p)
        player_filter =  nba_db["player_name"] == p
        player_db = nba_db[player_filter]
        player_db = player_db.sort_values(by='date', ascending = False)
        # Calculating avg : 
        fantasy_avg = player_db["Fantasy_ttfl"].mean()
        # Calculating standard variations:
        if fantasy_avg > 0: #if mean == 0 then we can't calculate the deviation
           fantasy_deviation = player_db["Fantasy_ttfl"].std() /fantasy_avg
        elif fantasy_avg < 0:
           fantasy_deviation = (player_db["Fantasy_ttfl"].std() /fantasy_avg) *-1
        else:
           fantasy_deviation = ""
        # Adding the deviation variables into each corresponding list :
        fantasy_deviation_l.append(fantasy_deviation)
       continue
    continue
  d = {'Players':player_list_final,'fantasy_dev':fantasy_deviation_l}
  return pd.DataFrame(d)


#Creating a function to have the projection of the fantasy for each player :
def get_player_projections(player_db,  features, target,time_window):
    if len(player_db) <= 1:
        # If the dataset is too small, use the entire dataset for training
        X = player_db[features]
        y = player_db[target]
        X_train, y_train = X, y
    else:
        player_db = player_db.sort_values(by='date', ascending=True).tail(time_window)
        X = player_db[features]
        y = player_db[target]
        X_train, _, y_train, _ = train_test_split(X, y, test_size=0.2, random_state=42)

    model = LinearRegression()
    model.fit(X_train, y_train)

    # Make predictions on the entire dataset
    y_pred = model.predict(X)
    average_projection = round(np.mean(y_pred), 2)

    return average_projection


def player_projections(nba_db, player_db):
    player_list = player_db.col_values(player_db.find("Player").col)[1:]

    # Define features for Fantasy_pts_+
    features_positive = ['MP','PTS', 'TRB', 'AST', 'STL', 'BLK', 'FT', '_3P', 'FT']
    # Create columns for Fantasy_pts_-
    nba_db['Field_goals_missed'] = nba_db['FGA'] - nba_db['FG']
    nba_db['Free_throws_missed'] = nba_db['FTA'] - nba_db['FT']
    nba_db['_3_points_missed'] = nba_db['_3PA'] - nba_db['_3P']

    # Define features for Fantasy_pts_-
    features_negative = ['MP','TOV', 'Field_goals_missed', '_3_points_missed', 'Free_throws_missed']

    projections_full_season = []
    projections_last_10_games = []
    player_list_final = []

    for p in tqdm(player_list):
        if p in list(nba_db["player_name"]):
            player_list_final.append(p)
            
            # Get the subset of nba_db for the current player
            player_subset = nba_db[nba_db["player_name"] == p]
            
            if len(player_subset) == 0:
                continue
            
            # Predict Fantasy_pts_+ and Fantasy_pts_-
            projection_positive = get_player_projections(player_subset, features_positive, 'Fantasy_pts_+', len(player_subset))
            projection_negative = get_player_projections(player_subset, features_negative, 'Fantasy_pts_-', len(player_subset))
            projection_season = projection_positive - projection_negative
            projections_full_season.append(projection_season)
            
            if len(player_subset) >= 10:
                # Predict for the last 10 games if available
                projection_last_10_pos = get_player_projections(player_subset, features_positive, 'Fantasy_pts_+', 10)
                projection_last_10_neg = get_player_projections(player_subset, features_negative, 'Fantasy_pts_-', 10)
                projection_last_10 = projection_last_10_pos - projection_last_10_neg
            else:
                # If less than 10 games, use the same projection as full season
                projection_last_10 = projection_season

            projections_last_10_games.append(projection_last_10)

    d = {'Players': player_list_final,
         'Projections_full': projections_full_season,
         'Projections_last_10_games': projections_last_10_games}

    return pd.DataFrame(d)


def teams_impact_by_position(nba_db,player_db):
    player_db = pd.DataFrame(player_db.get_all_records())
   # JOIN the two databases :
    nba_players_db = pd.merge(nba_db,player_db, left_on = ["player_name"], right_on = ["Player"])
    # Clean up the database removing the NAN, only stats from 2022-2023 season :
    final_db = nba_players_db[nba_players_db['MP'].notna()]
    final_db = final_db[final_db["MP"] >= 25 ]
    # Use the Position_corrected column to have only 3 positions : Guard, Forward and Center
    # create a list of our conditions
    condlist = [
        final_db['Pos'].str.contains('G', case=False, regex=True),
        final_db['Pos'].str.contains('F', case=False, regex=True),
        final_db['Pos'].str.contains('C', case=False, regex=True)
        ]
    # create a list of our 3 player positions :
    choicelist = ['G', 'F', 'C']
    # create the new column :
    final_db['Position'] = np.select(condlist, choicelist)
    # Once we have the database ready, we can write a function to have the impact of the number of points for each team by position
    df = pd.DataFrame() # Create an empty DataFrame
    position_list = ["G","F","C"]
    for pos in position_list:
        db_points = final_db.groupby(["Position", "Against"]).agg(Fantasy_ttfl_mean=("Fantasy_ttfl", "mean")) # Group by position first and also by team with the average by fantasy points
        db_points = db_points.reset_index() # Add a index to put Position and Against index as Columns
        df_filtered = db_points[db_points["Position"] == str(pos)].copy()  # Create a copy of the subset
        # Modify the copied DataFrame without affecting the original DataFrame
        df_filtered["impact"] = (df_filtered["Fantasy_ttfl_mean"]- df_filtered["Fantasy_ttfl_mean"].mean() ) / df_filtered["Fantasy_ttfl_mean"]
        # Add the modified DataFrame to the final DataFrame
        df = pd.concat([df, df_filtered])
    return df
        

def write_google_sheet(client,url,df_to_write):
   # Open the Google Sheets document by URL
   spreadsheet = client.open_by_url(url)
   # Select the worksheet to which you want to write the data
   worksheet = spreadsheet.get_worksheet(0)  # Get the first worksheet (index 0)
   # Clear the specified range
   worksheet.clear()
    # Get the column names from the DataFrame
   column_names = df_to_write.columns.tolist()
    # Insert the column names as the first row in the worksheet
   worksheet.insert_rows([column_names], 1)
    # Export the DataFrame data to Google Sheets starting from the second row (row 2)
   data_to_insert = df_to_write.values.tolist()
   worksheet.insert_rows(data_to_insert, 2)
   
async def main():
   nba_db = get_NBA_db("NBA_DB", json_keyfile,scope)
   player_db = get_PLAYER_db("NBA_PLAYERS",json_keyfile,scope)
   deviations_df = players_deviations(nba_db, player_db)
   projections_df = player_projections(nba_db,player_db)
   impact_df = teams_impact_by_position(nba_db,player_db)
   url_deviation_sheet = 'https://docs.google.com/spreadsheets/d/14y6ZDDybilliRA946WF0ixNb0AowC_gEIZh8jhWKpMY/edit#gid=0'
   write_google_sheet(client,url_deviation_sheet,deviations_df)
   url_projection_sheet = 'https://docs.google.com/spreadsheets/d/19U-zX1D8v3-r3aaN4vUN2-vhJ4rfEnnJ5H25FFk_J-w/edit#gid=0'
   write_google_sheet(client,url_projection_sheet,projections_df)
   url_impact_sheet = 'https://docs.google.com/spreadsheets/d/1W2quFG6_slcoMFSFahKJBQy-tZM-jWD9P8BS_6bKSzs/edit#gid=0'
   write_google_sheet(client,url_impact_sheet,impact_df)


if __name__ == "__main__":
    asyncio.run(main())
    print("Script ended")
    print(datetime.now() - startTime)
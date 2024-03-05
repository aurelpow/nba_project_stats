import numpy as np
import pandas as pd
from tqdm import tqdm # tqdm allows to have a progress bar of the loop
import asyncio
from datetime import datetime
import warnings
# Import PYDRIVE lybraries
import gspread
from oauth2client.service_account import ServiceAccountCredentials
#Import sklearn lybraries
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error

#Create a function to can read sheet from a google drive folder with JSON file:
def authenticate_google_sheet(json_keyfile, scope):
    creds = ServiceAccountCredentials.from_json_keyfile_name(json_keyfile, scope)
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=DeprecationWarning)
        client = gspread.authorize(creds)
    return client

#Create a function to get the full stats database we scrapped, built and wrote into a google sheet : 
def get_nba_db(sheet_name,client):
    spreadsheet = client.open(sheet_name)
    # Get the first (and presumably only) sheet in the spreadsheet
    worksheet = spreadsheet.get_worksheet(0)
    # Convert the sheet data to a Pandas DataFrame
    return pd.DataFrame(worksheet.get_all_records())

#Create a function to get the players list we scrapped, built and wrote into a google sheet :
def get_player_db(sheet_name,client):
    # Open the Google Sheet by title
    spreadsheet = client.open(sheet_name)
    # Get the first (and presumably only) sheet in the spreadsheet
    worksheet = spreadsheet.get_worksheet(0)
    return worksheet

#Create a function to write into a google sheet without removing the existing data: 
def write_to_google_sheet_add(client, url, df_to_write):
    # Open the Google Sheets document by URL
    spreadsheet = client.open_by_url(url)
    # Select the worksheet to which you want to write the data
    worksheet = spreadsheet.get_worksheet(0)  # Get the first worksheet (index 0)

    # Check if the worksheet is empty
    if worksheet.row_count == 0:
        # If the worksheet is empty, insert the header separately
        header = df_to_write.columns.tolist()
        worksheet.insert_rows([header])
        last_row = 1  # The header was just added, so the last row is 1
    else:
        # Find the last non-empty row in column A
        non_empty_rows = [i for i, value in enumerate(worksheet.col_values(1)) if value != '']
        last_row = non_empty_rows[-1] + 2 if non_empty_rows else 1  # Move to the first empty row if there's no non-empty row
    # Convert the DataFrame to a list of lists
    data_to_insert = df_to_write.values.tolist()

    # Convert Timestamp objects to string representation
    for row in data_to_insert:
        for i, value in enumerate(row):
            if isinstance(value, pd.Timestamp):
                row[i] = value.strftime('%Y-%m-%d')  # Adjust the date format as needed

    # Append the new data below the last row with data
    worksheet.add_rows(len(data_to_insert))

    # Write the new data to the worksheet
    worksheet.insert_rows(data_to_insert, last_row)   

#Create a function to have the deviation for each player
def calculate_deviations(nba_db,player_db):
  player_column = player_db.col_values(player_db.find("Player").col)# Get the values from the 'Player' column
  player_list = player_column[1:]# Remove the header row
  fantasy_deviation_l = []
  stats_deviation_l = []
  player_list_final = []
  print("calculate_deviations started")
  for p in tqdm(player_list):
    if p in list(nba_db["player_name"]):
       if list(nba_db["player_name"]).count(p) > 2:
        player_list_final.append(p)
        player_filter =  nba_db["player_name"] == p
        player_db = nba_db[player_filter]
        player_db = player_db.sort_values(by='date', ascending = False)
        # Calculating avg : 
        fantasy_avg = player_db["Fantasy_ttfl"].mean()
        pts_avg = player_db["PTS"].mean()
        rbs_avg = player_db["TRB"].mean()
        assists_avg = player_db["AST"].mean()
        # Calculating standard variations:
        if fantasy_avg > 0: #if mean == 0 then we can't calculate the deviation
           fantasy_deviation = player_db["Fantasy_ttfl"].std() /fantasy_avg
        elif fantasy_avg < 0:
           fantasy_deviation = (player_db["Fantasy_ttfl"].std() /fantasy_avg) *-1
        else:
           fantasy_deviation = ""
        if pts_avg +rbs_avg + assists_avg >0:
            stats_deviations = (player_db["PTS"].std() + player_db["TRB"].std() + player_db["AST"].std()) / (pts_avg +rbs_avg + assists_avg)
        elif pts_avg +rbs_avg + assists_avg < 0:
            stats_deviations = (player_db["PTS"].std() + player_db["TRB"].std() + player_db["AST"].std()) / (pts_avg +rbs_avg + assists_avg) * -1
        else :
            stats_deviations = ""

        # Adding the deviation variables into each corresponding list :
        fantasy_deviation_l.append(fantasy_deviation)
        stats_deviation_l.append(stats_deviations)
       continue
    continue
  d = {'Players':player_list_final,'fantasy_dev':fantasy_deviation_l, 'stats_dev': stats_deviation_l}
  return pd.DataFrame(d)

#Creating a function that will be used to calculate projections :
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
        X_train, _, y_train, _ = train_test_split(X, y, test_size=0.15, random_state=42)

    model = LinearRegression()
    model.fit(X_train, y_train)

    # Make predictions on the entire dataset
    y_pred = model.predict(X)
    average_projection = round(np.mean(y_pred), 2)

    return average_projection

#Creating a function to calculate fantasy projections: 
def calculate_fantasy_projections(nba_db, player_db):
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
    print("calculate_fantasy_pr0jections started")
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

#Creating a function to calculate the defense impact of each team for each player positions(G,F,C):
def calculate_team_impact(nba_db,player_db):
    player_db = pd.DataFrame(player_db.get_all_records())
    # Sort the nba_db by date before filtering
    nba_db_sorted = nba_db.sort_values(by='date')
    # Filter the nba_db with the last 12 games for each team
    last_12_games = nba_db_sorted.groupby('date').tail(45)
   # JOIN the two databases :
    nba_players_db = pd.merge(last_12_games,player_db, left_on = ["player_name"], right_on = ["Player"])
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
        db_fantasy = final_db.groupby(["Position", "Against"]).agg(Fantasy_ttfl_mean=("Fantasy_ttfl", "mean"),
                                                                   points_mean= ("PTS","mean"),
                                                                   rbds_mean= ("TRB","mean"),
                                                                   assists_mean= ("AST","mean")) # Group by position first and also by team with the average by the features we want 
        db_fantasy = db_fantasy.reset_index() # Add a index to put Position and Against index as Columns
        df_filtered = db_fantasy[db_fantasy["Position"] == str(pos)].copy()  # Create a copy of the subset
        # Modify the copied DataFrame without affecting the original DataFrame
        df_filtered["fantasy_impact"] = (df_filtered["Fantasy_ttfl_mean"]- df_filtered["Fantasy_ttfl_mean"].mean() ) / df_filtered["Fantasy_ttfl_mean"]
        df_filtered["pts_impact"] = (df_filtered["points_mean"]- df_filtered["points_mean"].mean() ) / df_filtered["points_mean"]
        df_filtered["rbds_impact"] = (df_filtered["rbds_mean"]- df_filtered["rbds_mean"].mean() ) / df_filtered["rbds_mean"]
        df_filtered["assists_impact"] = (df_filtered["assists_mean"]- df_filtered["assists_mean"].mean() ) / df_filtered["assists_mean"]         
        # Add the modified DataFrame to the final DataFrame
        df = pd.concat([df, df_filtered])
    return df  

#Creating a function to evaluate the predictions with real data:
def evaluate_projections(predictions, actual):
    # Convert predictions and actual to NumPy arrays if they are Pandas Series
    if isinstance(predictions, pd.Series):
        predictions = predictions.to_numpy()
    if isinstance(actual, pd.Series):
        actual = actual.to_numpy()

    return mean_absolute_error(actual, predictions)

#Creating the main function of the test_model: 
def test_model(nba_db,player_db):
    # Convert the 'date' column to datetime
    nba_db['date'] = pd.to_datetime(nba_db['date'])
    # Sort the DataFrame by 'player_name' and 'date' in descending order
    nba_db = nba_db.sort_values(by=[ 'date'], ascending=[True])
    # Remove the last date for each player
    nba_db_filtered = nba_db.groupby('player_name').apply(lambda group: group.iloc[:-1]).reset_index(drop=True)
    # Keep only the last date for each player
    nba_db_last_date = nba_db.groupby('player_name').apply(lambda group: group.iloc[-1]).reset_index(drop=True)
    fantasy_projection_df = calculate_fantasy_projections(nba_db_filtered, player_db)
    impact_df = calculate_team_impact(nba_db_filtered,player_db)

    player_list = player_db.col_values(player_db.find("Player").col)[1:]
    player_df = pd.DataFrame(player_db.get_all_records())  # Convert worksheet to DataFrame
    test_data = []
    date_column = nba_db_last_date["date"].max()  # Get the highest date

    print("calculate_test_model started")
    for p in tqdm(player_list):
        if p in list(nba_db["player_name"]):
            real_scores = nba_db_last_date.loc[(nba_db_last_date["player_name"] == p) & (nba_db_last_date["date"] == date_column), "Fantasy_ttfl"].tolist()
            Real_score = real_scores[0] if real_scores else None
            player_projections = fantasy_projection_df.loc[fantasy_projection_df['Players'] == p,"Projections_full"].values
            fantasy_full_games = player_projections[0] if len(player_projections) > 0 else None
            player_10_games_projections  = fantasy_projection_df.loc[fantasy_projection_df['Players'] == p,"Projections_last_10_games"].values
            fantasy_10_games = player_10_games_projections [0] if len(player_10_games_projections ) > 0 else None
            if fantasy_full_games is not None and fantasy_10_games is not None and Real_score is not None :
                player_position = player_df.loc[player_df["Player"] == p, "Pos"].apply(lambda x: "G" if x == 'SG' or x == 'PG' 
                                                                                   else('F' if x== 'SF' or x== 'PF' 
                                                                                        else 'C')
                                                                                        ).values[0]
                team_against = nba_db_last_date.loc[nba_db_last_date["player_name"] == p, "Against"].values[0]
                impact_coef = impact_df.loc[(impact_df['Against'] == team_against) & (impact_df['Position'] == player_position),'fantasy_impact'].values[0]
                final_projection = (fantasy_full_games + (fantasy_10_games*2) / 3 ) * (1+ impact_coef)
                # Append the real score and the final projection to your test data
                test_data.append({"Players": p, "Real_Score": Real_score, "Final_Projection": final_projection})
    # Convert your test data to a DataFrame
    test_data_df = pd.DataFrame(test_data)
    # Evaluate the projections using the chosen metric
    mae = evaluate_projections(test_data_df["Final_Projection"], test_data_df["Real_Score"])
    # Create a DataFrame with the results
    result_df = pd.DataFrame({"Date": [date_column], "MAE": mae })
    return result_df
   
async def main():
    startTime = datetime.now()
    ## Identifying with google API to put the NBA_DB table into a variable : 
     # Specify the path to your credentials JSON file
    json_keyfile = "C:/Users/aureb/OneDrive - Sport-Data/Documents/COURS/DATABIRD/PROJECT/imposing-bee-389610-823a1fac476d.json"
        # Define the scope
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    # Authenticate using the credentials
    client = authenticate_google_sheet(json_keyfile, scope)
    
    nba_sheet_name = "NBA_DB"
    nba_db = get_nba_db(nba_sheet_name, client)
    player_sheet_name = "NBA_PLAYERS"
    player_db = get_player_db(player_sheet_name,client)
    test_df = test_model(nba_db, player_db)
    url_test_model_sheet = 'https://docs.google.com/spreadsheets/d/1TsN5CBCBceWZtFZGLzk0XqHurcirFJYJMumiBrM1mrw/edit#gid=0'
    write_to_google_sheet_add(client,url_test_model_sheet,test_df)

    print("Script ended")
    print(datetime.now() - startTime)

if __name__ == "__main__":
    asyncio.run(main())
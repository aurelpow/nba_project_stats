import asyncio
import os
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup, Comment
from urllib.request import urlopen
import re
import datetime
import unidecode
import io
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
startTime = datetime.now()

## Identifying with google API to write the final database to a google sheet : 
    # Specify the path to your credentials JSON file
json_keyfile =  "C:/Users/aureb/OneDrive - Sport-Data/Documents/COURS/DATABIRD/PROJECT/imposing-bee-389610-823a1fac476d.json"
    # Define the scope
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    # Authenticate using the credentials
creds = ServiceAccountCredentials.from_json_keyfile_name(json_keyfile, scope)
client = gspread.authorize(creds)


## Creating variables : 
SEASONS = list(range(2024, 2025))#Select the NBA season(s) to scrape
STANDINGS_DIR = r'C:\Users\aureb\OneDrive - Sport-Data\Documents\COURS\DATABIRD\PROJECT\data\standings' #Final destination for the seasons .html files
GAMES_DIR = r'C:\Users\aureb\OneDrive - Sport-Data\Documents\COURS\DATABIRD\PROJECT\data\games' # Final destination for the games .html files
box_scores = os.listdir(GAMES_DIR)
box_scores = [os.path.join(GAMES_DIR, f) for f in box_scores if f.endswith(".html")]

## Create a function to "clean" the html code of the game page before extracting the tables we need  
def parse_html(soup_box_score):
    try:
        with open(soup_box_score, 'r', encoding='utf-8') as f:
            html = f.read()
            soup = BeautifulSoup(html, 'html.parser')
            [s.decompose() for s in soup.select("tr.over_header")]
            [s.decompose() for s in soup.select("tr.thead")]
            return soup
    except FileNotFoundError:
        print(f"File not found: {soup_box_score}")
    except Exception as e:
        print(f"An error occurred while reading the file: {e}")

## Creating a function to have the final result of the game : 
def read_line_score(soup) : 
    line_score = pd.read_html(io.StringIO(str(soup)), attrs={'id': 'line_score'})[0]
    cols = list(line_score.columns)#create a list to modify columns name
    cols[0] = "team"
    cols[-1] = "total"
    line_score.columns = cols #renamed the columns
    line_score = line_score[["team", "total"]]#we only keep the teams and total columns
    return line_score

## Creating a function to have the box_score table in a numeric version
def read_stats(soup, team, stat):
    df = pd.read_html(io.StringIO(str(soup)), attrs={"id" : f"box-{team}-game-{stat}"},index_col = 0)[0]
    for i in [i for i in list(range(len(df.columns))) if i not in [0,1]]:
        df.iloc[:,i]=pd.to_numeric(df.iloc[:,i],errors = "coerce") # convert to numeric columns
    df = df[:-2]#remove the last rows (totals)
    return df 
def nba_df(box_scores):
    total_scores = [] # empty list for the scores for each team
    games = [] # empty list for the boxscores
    base_cols = None 
    for bx_s in box_scores :
        soup = parse_html(bx_s) # Fonction n°1
        line_score = read_line_score(soup) # Fonction n°2
        # Define a regular expression pattern to match the game ID 
        pattern = r'\\([^\\]+)\.'
        match = re.search(pattern, bx_s) #Use re.search to find the pattern in the input string
        game_id = match.group(1)  # Get the first captured group
        line_score["game_ID"] = game_id # Add a new column with the game id
        teams = list(line_score["team"]) #Create a variable with the name of the two teams
        total_scores.append(line_score) # Add the line score to the list "total_scores"
        for team in teams: # loop to extract the two boxscores, using the "teams" list
            basic = read_stats(soup, team, "basic")
            basic["team"] = team #Add a new column with the team's name
            basic["game_ID"] = game_id #Add a new column with the game id
            basic.reset_index() 
            games.append(basic)           
        if len(games) % 100 == 0:
            print(f"{len(games)/2} / {len(box_scores)}") # IF loop to have the level of progress of the code
    #Creating the SCORE dataframe from the data we gathered : 
    scores_df = pd.concat(total_scores) # Convert list to dataframe with Pandas 
    #Cleaning the dataframe : 
    scores_df = scores_df.groupby('game_ID').agg(lambda x: x.tolist()) #Same row for a same gameID
    scores_df[['Away', 'Home']] = scores_df["team"].apply(lambda x: pd.Series(str(x).split(","))) #Spliting into 2 columns
    scores_df[['A_score', 'H_score']] = scores_df["total"].apply(lambda x: pd.Series(str(x).split(",")))#Spliting into 2 columns
    scores_df =  scores_df.drop(columns = ['team', 'total']) #Dropping extra columns
    scores_df["Away"] = scores_df["Away"].str[2:5] #Taking only the 3 letters of the team 
    scores_df["Home"] = scores_df["Home"].str[2:5]#Taking only the 3 letters of the team 
    scores_df["A_score"] = scores_df["A_score"].str[1:]#Taking only the digitals 
    scores_df["H_score"] = scores_df["H_score"].str[0:-1]#Taking only the digitals 
    scores_df["game_ID"] = scores_df.index # Putting gameID as a column 
    scores_df.index = np.arange(1, len(scores_df) + 1) 
    scores_df["date"] = scores_df["game_ID"].str[:8] #Extracting the date from the gameID
    scores_df["date"] = scores_df["date"].apply(lambda x: pd.to_datetime(str(x), format='%Y%m%d')) #string to datetime format
    #Creating the games dataframe from the data we gathered : 
    games_df = pd.concat(games) # Convert list to dataframe with Pandas
    #Cleaning the dataframe : 
    games_df = games_df.replace('Did Not Play', np.nan)
    games_df = games_df.reset_index()
    #MERGE the TWO dataframes into one : 
    nba_db = pd.merge(games_df,scores_df, on = ["game_ID"])
    # Create a new column to have the opposing team 
    nba_db["Against"] = np.nan
    nba_db["Against"] = nba_db.apply(lambda x : x["Home"] if x["team"] == x["Away"]  else x["Away"],axis=1 )
    # Clean the MP (Minute Played) and FG(Field Goals) columns with only INT or NaN values : 
    # MP converting to float : Loop to replace "did Not Dress", "Not with Team" and "Player Suspended" values into NaN value
    MP_list = []
    for mp in  list(nba_db["MP"]):
        if bool(re.search("Did Not Dress",str(mp))) == True or bool(re.search("Not With Team",str(mp))) == True or bool(re.search("Player Suspended",str(mp))) == True :
            MP_list.append(np.nan)
        else:
            MP_list.append(mp)
    nba_db["MP"] = MP_list
    nba_db["MP"] = nba_db["MP"].str.replace(':','.',regex=True)
    nba_db["MP"] = nba_db["MP"].astype(float)
    #FG converting to float : Loop to replace "did Not Dress", "Not with Team" and "Player Suspended" values into NaN value
    FG_list = []
    for fg in  list(nba_db["FG"]):
        if bool(re.search("Did Not Dress",str(fg))) == True or bool(re.search("Not With Team",str(fg))) == True or bool(re.search("Player Suspended",str(fg))) == True :
            FG_list.append(np.nan)
        else:
            FG_list.append(fg)
    nba_db["FG"] = FG_list
    nba_db["FG"] = nba_db["FG"].astype(float)
    # Convert Date column to datetime format / remove duplicates / change "Starter" column name : 
    nba_db["date"] = pd.to_datetime(nba_db["date"])# Date column to datetime format : 
    # Clean and prepare data
    nba_db = nba_db.drop_duplicates()
    nba_db.dropna(subset=['MP'], inplace=True)
    nba_db["A_score"] = nba_db["A_score"].astype(int)
    nba_db["H_score"] = nba_db["H_score"].astype(int)
    #Rename player names column : 
    nba_db = nba_db.rename(columns={'Starters': 'player_name'})
    # Create a new column Season to make out 2022 and 2023 seasons : 
    nba_db["Season"] = nba_db.apply(lambda x : 2024 if x["date"] > datetime(2023, 9, 1)  else  2023, axis=1)
    # Spelling issue for Russian special caracters : 
    nba_db["player_name"] = nba_db["player_name"].apply(unidecode.unidecode)
    #rename the columns : 
    nba_db = nba_db.rename(columns={'FG%': 'FG_per', '3P%': '_3P_per','FT%': 'FT_per','+/-': 'plus_minus',
                                    '3P': '_3P','3PA': '_3PA'})
    # Converting the statistic columns to integer : 
    integer_columns = ["PTS", "TRB", "AST", "STL", "BLK", "FG", "_3P", "FT","_3PA","FTA","ORB","DRB","TOV","PF","plus_minus"]
    for col in integer_columns:
        nba_db[col] = pd.to_numeric(nba_db[col], errors='coerce').fillna(0).astype(int)
    #Suming the bonus columns for the fantasy score : 
    fantasy_bonus_columns = ["PTS", "TRB", "AST", "STL", "BLK", "FG", "_3P", "FT"]
    nba_db["Fantasy_pts_+"] = nba_db[fantasy_bonus_columns].sum(axis=1, skipna=True)
    #Suming the malus columns for the fantasy score : 
    nba_db["Fantasy_pts_-"] = nba_db.apply(lambda x: 
                                        x["TOV"] + 
                                        (x["FGA"] - x["FG"]) + 
                                        (x["_3PA"]-x["_3P"]) + 
                                        (x["FTA"]-x["FT"]), axis=1).astype(int)
    #Final calcul to have the fantasy TTFL result : 
    nba_db["Fantasy_ttfl"] = nba_db.apply(lambda x: x["Fantasy_pts_+"] - x["Fantasy_pts_-"], axis = 1 ) 
    return nba_db
## creating a function to have the calendar for each season defined : 
def nba_calendar(SEASONS,STANDINGS_DIR):
    game_id_list = []
    teams = []
    standings_files = os.listdir(STANDINGS_DIR)
    for season in SEASONS:#loop to execute only one season
        files = [s for s in standings_files if str(season) in s]
        for f in files:#loop to go to each html file in the folder ""
            filepath = os.path.join(STANDINGS_DIR, f)
            with open(filepath, 'r') as f:
                html = f.read()
                soup = BeautifulSoup(html, 'html.parser')
                soup_id = soup.find_all("th", {"class" : "left"})
                for x in soup_id:
                    game_ID = x.text
                    game_id_list.append(game_ID)
                soup_visitor = soup.find_all("td", {"class" : "left"})
                for v in soup_visitor:
                    visitor = v.text
                    teams.append(visitor)
    #Creating lists for the visitor/home teams & stadium : 
    visitor_team = [teams[x:x+1] for x in range(0, len(teams), 4)]
    home_team = [teams[x:x+1] for x in range(1, len(teams), 4)]
    stadium = [teams[x:x+1] for x in range(2, len(teams), 4)]
    #Creating a Dataframe with the 4 final lists : 
    d = {'Date':game_id_list,'Away':visitor_team, "Home" :home_team, "Arena" : stadium}
    NBA_Calendar = pd.DataFrame(d)
     #Cleaning the dataframe : 
    NBA_Calendar["Away"] = NBA_Calendar["Away"].str[0]
    NBA_Calendar["Home"] = NBA_Calendar["Home"].str[0]
    NBA_Calendar["Arena"] = NBA_Calendar["Arena"].str[0]
    #Convert "Date" column into date format : 
    NBA_Calendar["Date"]=NBA_Calendar["Date"].apply(lambda x: datetime.strptime(x, '%a, %b %d, %Y').strftime('%Y-%m-%d'))
    return NBA_Calendar 

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
    # Run function and export the nba_df database to a google drive spreedsheet :
    nba_db = nba_df(box_scores)
        #Convert date to string and NA to '' (necessary to transfer the data to a drive sheet): 
    nba_db['date'] = nba_db['date'].astype(str)
    nba_db = nba_db.fillna('')
    url_nba_db_sheet = 'https://docs.google.com/spreadsheets/d/1Rgy6ZGjkT99PvYjhbRdSGeSDgMLXDyO62eaHHVOC_Nk/edit#gid=0'
    write_google_sheet(client,url_nba_db_sheet,nba_db)
    # Run function and export the nba_calendar database to a google drive spreedsheet :
    nba_calendar_db = nba_calendar(SEASONS,STANDINGS_DIR)
    url_nba_calendar_sheet = 'https://docs.google.com/spreadsheets/d/1X_p-4PQN8prHiw4nKykEx7pRLqH44uXRbVJKgZokDu0/edit#gid=0'
    write_google_sheet(client,url_nba_calendar_sheet,nba_calendar_db)
if __name__ == "__main__":
    asyncio.run(main())
    print("Script ended")
    print(datetime.now() - startTime)
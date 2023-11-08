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
startTime = datetime.now()


GAMES_DIR = r'C:\Users\aureb\Documents\COURS\DATABIRD\PROJECT\data\games' # Final destination for the games .html files
box_scores = os.listdir(GAMES_DIR)
box_scores = [os.path.join(GAMES_DIR, f) for f in box_scores if f.endswith(".html")]


# Create a function to "clean" the html code of the game page before extracting the tables we need  
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

# function to have the final result of the game : 
def read_line_score(soup) : 
    line_score = pd.read_html(io.StringIO(str(soup)), attrs={'id': 'line_score'})[0]
    cols = list(line_score.columns)#create a list to modify columns name
    cols[0] = "team"
    cols[-1] = "total"
    line_score.columns = cols #renamed the columns
    line_score = line_score[["team", "total"]]#we only keep the teams and total columns
    return line_score

# function to have the box_score table in a numeric version
def read_stats(soup, team, stat):
    df = pd.read_html(io.StringIO(str(soup)), attrs={"id" : f"box-{team}-game-{stat}"},index_col = 0)[0]
    for i in [i for i in list(range(len(df.columns))) if i not in [0,1]]:
        df.iloc[:,i]=pd.to_numeric(df.iloc[:,i],errors = "coerce") # convert to numeric columns
    df = df[:-2]#remove the last rows (totals)
    return df 

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
# Date column to datetime format : 
nba_db["date"] = pd.to_datetime(nba_db["date"])
# Clean and prepare data
nba_db = nba_db.drop_duplicates()
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
# Export the database into a csv file :
nba_db.to_csv(r'C:\Users\aureb\Documents\COURS\DATABIRD\PROJECT\data\nba_db.csv', index=False)
print("Script ended")
print(datetime.now() - startTime)
import os
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup, Comment
from urllib.request import urlopen
import re
import datetime
import unidecode
import io
import asyncio
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
startTime = datetime.now()
# Specify the path to your credentials JSON file
json_keyfile = "C:/Users/aureb/OneDrive - Sport-Data/Documents/COURS/DATABIRD/PROJECT/imposing-bee-389610-823a1fac476d.json"
# Define the scope
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
# Authenticate using the credentials
creds = ServiceAccountCredentials.from_json_keyfile_name(json_keyfile, scope)
client = gspread.authorize(creds)

season = 2024
TEAMS_DIR = r'C:\Users\aureb\OneDrive - Sport-Data\Documents\COURS\DATABIRD\PROJECT\data\teams' # direction of the .html team files
teams = os.listdir(TEAMS_DIR) # list of all the html files with the team name + .html (Ex : 'ATL.html')
teams = [os.path.join(TEAMS_DIR, f) for f in teams if f.endswith(".html")] # join the directions to have the full direction of each team file

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

# function to have the roster : 
def read_roster(soup) : 
    roster = pd.read_html(io.StringIO(str(soup)), attrs={'id': 'roster'})[0]
    cols = list(roster.columns)#create a list to modify columns name
    cols[0] = "Number"
    cols[1] = "Player"
    cols[2] = "Pos"
    cols[3] = "Ht"
    cols[4] = "Wt"
    cols[5] = "Birth Date"
    cols[6] = "From"
    cols[7] = "Exp"
    cols[8] = "College"
    roster.columns = cols #renamed the columns
    roster = roster[["Number","Player","Pos","Ht","Wt","Birth Date","From", "College","Exp"]]#we only keep the teams and total columns
    return roster

# function to have the injury report : 
def read_injury(soup):
    try:
        tables = pd.read_html(io.StringIO(str(soup)), attrs={"id": "injuries"})
        if not tables:
            # The "injuries" table was not found in the HTML
            return None
        else:
            # The table was found, process it
            df = tables[0]
            # Additional processing logic for the table
            return df
    except ValueError:
        return None
    
async def main():
    fullRoster = [] # empty list for the scores for each team
    fullInjuryreport = [] # empty list for the boxscores
    base_cols = None 
    for team in teams :
        soup = parse_html(team) # Call the parse_html function
        roster= read_roster(soup) # Call the roster function
        # Define a regular expression pattern to match the team name
        pattern = r'\\([^\\]+)\.'
        match = re.search(pattern, team) #Use re.search to find the pattern in the input string
        team = match.group(1)  # Get the first captured group
        roster["team"] = team # Add a new column with the team name
        roster.reset_index() 
        fullRoster.append(roster)
        injury_report = read_injury(soup) # Call the injury function
        if injury_report is not None: # The table was found, and injury_report contains the DataFrame
            injury_report["team"] = team # Add a new column with the team name
            injury_report.reset_index()
            fullInjuryreport.append(injury_report)
        else:
            pass 
    #Creating the roster_dataframe dataframe from the data we gathered : 
    roster_df = pd.concat(fullRoster) # Convert list to dataframe with Pandas
    injury_df = pd.concat(fullInjuryreport) # Convert list to dataframe with Pandas
    #Cleaning the roster dataframe : 
    roster_df["Number"] = roster_df["Number"].fillna(0).astype(np.int64)# Convert player numbers to float64 in int64
    roster_df["Wt"] = roster_df["Wt"].apply(lambda x : round(x*0.453592,0)).fillna(0).astype(np.int64) #convert weight from pounds to kilos  
    conversions = [30.48, 2.54] #Create a variable to have the conversion from foot to centimeters 
    roster_df['Ht'] = roster_df['Ht'].apply(lambda x: round(pd.Series(map(int, x.split('-'))).dot(conversions),0)).fillna(0).astype(np.int64) #convert foot to centimeters using the conversion variable
    roster_df["Birth Date"] = pd.to_datetime(roster_df["Birth Date"], format='%B %d, %Y') # Birth date to date format
    roster_df["Exp"] = roster_df["Exp"].apply(lambda x : 0 if x == "R" else x).astype(np.int64) # Rookie year = 0 and pass the column from object to int64
    roster_df["Player"] = roster_df["Player"].apply(unidecode.unidecode) # Spelling issue for Russian special caracters : 
    roster_df['Player'] = roster_df['Player'].str.replace(r'\s*\(TW\)\s*$', '', regex=True)# Remove "(TW)" from the 'Player' column
    roster_df.index = np.arange(1, len(roster_df) + 1)# index reset
    ## replace specific player names : 
    roster_df['Player'].replace('KJ Martin', 'Kenyon Martin Jr.', inplace=True)

    #Cleaning the injury dataframe : 
    injury_df[['Type', 'Details']] = injury_df["Description"].str.split('-', n=1, expand=True) #Split the Description column in 2 columns to have the type and details
    injury_df =  injury_df.drop(columns = ['Description'])#Remove Description column 
    injury_df["Update"] = pd.to_datetime(injury_df["Update"], format='%a, %b %d, %Y') #date to date format
    injury_df.index = np.arange(1, len(injury_df) + 1)# index reset

    # Export the roster _df to a google drive spreedsheet :
    roster_df["Birth Date"] =  roster_df["Birth Date"].astype(str)
    roster_df = roster_df.fillna('')
    spreadsheet = client.open_by_url('https://docs.google.com/spreadsheets/d/1KRqGNSVsHU_VQOBplL_-V4BXkdRKhGkjRUi0qFb1pYc/edit#gid=0')# Open the Google Sheets document by URL
    worksheet = spreadsheet.get_worksheet(0) #Select the worksheet to which you want to write the data
    worksheet.clear() # Clear the specified range
    column_names = roster_df.columns.tolist() # Get the column names from the DataFrame
    worksheet.insert_rows([column_names], 1)  # Insert the column names as the first row in the worksheet
    data_to_insert = roster_df.values.tolist() # Export the DataFrame data to Google Sheets
    worksheet.insert_rows(data_to_insert, 2) # starting from the second row (row 2)
    # Export the injury _df to a google drive spreedsheet :
    injury_df["Update"] =  injury_df["Update"].astype(str)
    spreadsheet = client.open_by_url('https://docs.google.com/spreadsheets/d/1O8PrMTxk34qh872XMlTyfQJdqfajtuJxcuT_WLYy_DE/edit#gid=0')# Open the Google Sheets document by URL
    worksheet = spreadsheet.get_worksheet(0) #Select the worksheet to which you want to write the data
    worksheet.clear() # Clear the specified range
    column_names = injury_df.columns.tolist() # Get the column names from the DataFrame
    worksheet.insert_rows([column_names], 1)  # Insert the column names as the first row in the worksheet
    data_to_insert = injury_df.values.tolist() # Export the DataFrame data to Google Sheets
    worksheet.insert_rows(data_to_insert, 2) # starting from the second row (row 2)



if __name__ == "__main__":
    asyncio.run(main())
    print("Script ended")
    print(datetime.now() - startTime)
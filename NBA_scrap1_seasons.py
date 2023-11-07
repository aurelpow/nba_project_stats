
import asyncio
import os
import time
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
import requests
import re
import pandas as pd 
from datetime import datetime
startTime = datetime.now()

#Creating some variables : 
SEASONS = list(range(2024, 2025))#Select the NBA season(s) to scrape
STANDINGS_DIR = r'C:\Users\aureb\Documents\COURS\DATABIRD\PROJECT\data\standings' #Final destination for the seasons .html files
GAMES_DIR = r'C:\Users\aureb\Documents\COURS\DATABIRD\PROJECT\data\games' # Final destination for the games .html files


async def get_html(url, selector, sleep=5, retries=3):
    """Function to get html links with Playwright, using Chromium browser
    imput : URL 
    """
    html = None
    for i in range(1, retries + 1):
        time.sleep(sleep * i)
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch()
                page = await browser.new_page()
                await page.goto(url)
                print(await page.title())
                html = await page.inner_html(selector)
        except asyncio.TimeoutError:
            print(f"Timeout error on {url}")
            continue
        else:
            break
    return html
#Creating a function to get the season resume by month for each season in the variable SEASONS : 
async def scrape_season(season):
    url = f"https://www.basketball-reference.com/leagues/NBA_{season}_games.html"
    html = await get_html(url, "#content .filter")
    soup = BeautifulSoup(html, 'html.parser')
    links = soup.find_all("a")
    standings_pages = [f"https://www.basketball-reference.com{l['href']}" for l in links]

    for url in standings_pages:
        save_path = os.path.join(STANDINGS_DIR, url.split("/")[-1])
        if os.path.exists(save_path):
            continue
        
        html = await get_html(url, "#all_schedule")
        with open(save_path, "w+") as f:
            f.write(html)

# Delete the files in the standings folder
standings_files = os.listdir(STANDINGS_DIR)
for f in standings_files:
    file_path = os.path.join(STANDINGS_DIR, f)
    if os.path.exists(file_path):
        os.remove(file_path)
    else:
        print("The file does not exist")

#Creating a Fonction to scrap all the "Schedule and result" websites by month and by season and save them into the folder
async def scrape_game(standings_file):
    with open(standings_file, 'r') as f:
        html = f.read()

    soup = BeautifulSoup(html, 'html.parser')
    links = soup.find_all("a")
    hrefs = [l.get('href') for l in links]
    box_scores = [f"https://www.basketball-reference.com{l}" for l in hrefs if l and "boxscore" in l and '.html' in l]

    for url in box_scores:
        save_path = os.path.join(GAMES_DIR, url.split("/")[-1])
        if os.path.exists(save_path):
            continue

        html = await get_html(url, "#content")
        if not html:
            continue
        with open(save_path, "w+", encoding='utf-8') as f:
            f.write(html)
   
#Creating a function to execute the scraoe season function for all the seasons defined : 
async def scrape_all_seasons():
    for season in SEASONS:
        await scrape_season(season)

    standings_files = os.listdir(STANDINGS_DIR)
    
    for season in SEASONS:
        files = [s for s in standings_files if str(season) in s]
        for f in files:
            file_path = os.path.join(STANDINGS_DIR, f)
            await scrape_game(file_path)

# create a function to have the calendar for each season defined : 
def nba_calendar(SEASONS,standings_files,STANDINGS_DIR):
    game_id_list = []
    teams = []
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
    # Save the dataframe to a CSV file : 
    NBA_Calendar.to_csv(r'C:\Users\aureb\Documents\COURS\DATABIRD\PROJECT\data\NBA_Calendar.csv', index=False)


if __name__ == "__main__":
    asyncio.run(scrape_all_seasons())
    nba_calendar(SEASONS, standings_files, STANDINGS_DIR)
    print("Script ended")
    print(datetime.now() - startTime)


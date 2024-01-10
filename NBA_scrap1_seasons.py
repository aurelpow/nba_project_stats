import asyncio
import os
import time
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
import pandas as pd 
from pandas_gbq import to_gbq
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

## Creating a function to record the html page with playwright : 
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
##Creating a function to get the season resume by month for each season in the variable SEASONS : 
async def scrape_season(season):
    # Deleting the files in the standings folder ; 
    standings_files = os.listdir(STANDINGS_DIR)
    for f in standings_files:
        file_path = os.path.join(STANDINGS_DIR, f)
        if os.path.exists(file_path):
            os.remove(file_path)
        else:
            print("The file does not exist")
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

##Creating a Fonction to scrap all the "Schedule and result" websites by month and by season and save them into the folder
async def scrape_game(standings_file):
    with open(standings_file, 'r') as f:
        html = f.read()

    soup = BeautifulSoup(html, 'html.parser')
    links = soup.find_all("a")
    hrefs = [l.get('href') for l in links]
    box_scores = [f"https://www.basketball-reference.com{l}" for l in hrefs if l and "boxscores" in l and '.html' in l]

    for url in box_scores:
        save_path = os.path.join(GAMES_DIR, url.split("/")[-1])
        if os.path.exists(save_path):
            continue

        html = await get_html(url, "#content")
        if not html:
            continue
        with open(save_path, "w+", encoding='utf-8') as f:
            f.write(html)
   
##Creating a function to execute the scrape season and scrape game function for all the seasons defined : 
async def scrape_all_games():
    for season in SEASONS:
        await scrape_season(season)

    standings_files = os.listdir(STANDINGS_DIR)
    
    for season in SEASONS:
        files = [s for s in standings_files if str(season) in s]
        for f in files:
            file_path = os.path.join(STANDINGS_DIR, f)
            await scrape_game(file_path)

if __name__ == "__main__":
    asyncio.run(scrape_all_games())
    print("Script ended")
    print(datetime.now() - startTime)


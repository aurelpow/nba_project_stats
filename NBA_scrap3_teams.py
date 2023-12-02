import asyncio
from playwright.async_api import async_playwright
import os
from datetime import datetime
startTime = datetime.now()

season = 2024
TEAMS_DIR = r'C:\Users\aureb\OneDrive - Sport-Data\Documents\COURS\DATABIRD\PROJECT\data\teams' # Final destination for the player lists .html files
teams = {'full_name': ['ATLANTA HAWKS', 'BOSTON CELTICS', 'BROOKLYN NETS',"CHICAGO BULLS",
                      "CHARLOTTE HORNETS","CLEVELAND CAVALIERS","DALLAS MAVERICKS","DENVER NUGGETS",
                      "DETROIT PISTONS","GOLDEN STATE WARRIORS","HOUSTON ROCKETS","INDIANA PACERS","LOS ANGELES CLIPPERS"
                      ,"LOS ANGELES LAKERS","MEMPHIS GRIZZLIES","MIAMI HEAT","MILWAUKEE BUCKS","MINNESOTA TIMBERWOLVES"
                     ,"NEW ORLEANS PELICANS","NEW YORK KNICKS","OKLAHOMA CITY THUNDER","ORLANDO MAGIC","PHILADELPHIA 76ERS"
                     ,"PHOENIX SUNS","PORTLAND TRAIL BLAZERS","SACRAMENTO KINGS","SAN ANTONIO SPURS","TORONTO RAPTORS"
                     ,"UTAH JAZZ","WASHINGTON WIZARDS"],
        '3letters': ["ATL","BOS","BRK","CHI","CHO","CLE","DAL","DEN","DET","GSW","HOU","IND","LAC","LAL","MEM"
                    ,"MIA","MIL","MIN","NOP","NYK","OKC","ORL","PHI","PHO","POR","SAC","SAS","TOR","UTA","WAS"]}


async def scrape_and_save_html(url, save_path):
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()
        await page.goto(url)
        html = await page.content()
        await browser.close()
        with open(save_path, "w", encoding="utf-8") as file:
            file.write(html)

async def main():
    for team in teams["3letters"]:
        url = f"https://www.basketball-reference.com/teams/{team}/{season}.html"   # Replace with the URL you want to scrape
        save_path = os.path.join(TEAMS_DIR, url.split("/")[-2] + ".html") # Replace with the desired file path and name
        await scrape_and_save_html(url, save_path)

if __name__ == "__main__":
    asyncio.run(main())
    print("Script ended")
    print(datetime.now() - startTime)
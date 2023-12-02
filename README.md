# nba_project_stats
Web scraping to have all NBA data to be able to do Machine Learning and create a interactive Dashboard in Power BI. 
**- NBA_scrap1_seasons : ** 
First of all, we use the module playwright to exctract the standing html sites for each month of each seasons we are interessed in. 
The html files are saved in the local windows folder defined by the variable "STANDINGS_DIR". 
After that the function scrape_game has been created to enter in each .html sites present into the standing folder and enter in every single game to save the box_score page into a .html file in the local windows folder defined by the variable "GAMES_DIR". 
**- NBA_scrap2_games : **
This script is scraping two tables (score dataframe
- read_line_score() : taking 

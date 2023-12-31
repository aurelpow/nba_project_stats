# nba_project_stats
Web scraping to have all NBA data to be able to do Machine Learning and create a interactive Dashboard in Power BI. 

**- NBA_scrap1_seasons : ** 
First of all, we use the module playwright to extract the standing html sites for each month of each seasons we are interessed in. 
The html files are saved in the local windows folder defined by the variable "STANDINGS_DIR". 
After that the function scrape_game has been created to enter in each .html sites present into the standing folder and enter in every single game to save the box_score page into a .html file in the local windows folder defined by the variable "GAMES_DIR".

**- NBA_scrap2_games : **
This script is scraping 3 tables (Line Score and basic stats for both teams) from the box score .html files saved in the games folder.
After that we created some functions to create final dataframes. These df are saved into a google drive folder. 
- read_line_score() : scraping the line score
- read_stats() : scraping the two basic stats tables (one for each team)
- nba_df() : Using the two functions above to have the data we need and after a merge and some cleaning is return the nba database with 33 columns :
player_name	MP	FG	FGA	FG_per	_3P	_3PA	_3P_per	FT	FTA	FT_per	ORB	DRB	TRB	AST	STL	BLK	TOV	PF	PTS	plus_minus	team	game_ID	Away	Home	A_score	H_score	date	Against	Season	Fantasy_pts_+	Fantasy_pts_-	Fantasy_ttfl
- nba_calendar() : Creating a calendar with 4 columns :
  Date	Away	Home	Arena
- write_google_sheet() : Using API google drive with gspread and oauth2client.service_account to write the dataframes into the sheets with the right url.
  
**- NBA_scrap3_teams : **
This script is saving the Team "Roster and Stats" page for each NBA teams(30) in a local windows folder defined by the variable "TEAMS_DIR".

**- NBA_scrap4_roster_injuries : **
This script is scraping 2 tables (Roster and Injury Report) from the team .html files saved in the teams folder.
After that we created some functions to create final dataframes. These df are saved into a google drive folder.

**- Machine_learning_stats : **
This script is calculating some KPIs and put them into dataframes saved into several google drive sheets.
- players_deviations() : Calculating standard variations for each player who played at less 2 games.
- player_projections() : Using sklearn to define a projection (fantasy points, points, rebounds, assists...) based on a linear model.
  3 projections are done for each figure we want to project :
  - Projection with all stats 
  - Projection with stats from the ten last games
  - Projection with stats from the five last games
  In power Bi the DAX is doing a weighted average (all stats + 10 games + (5 games*2) / 4 )
- teams_impact_by_position(): Calculating team's defensive impacts according to stat category and player positions.
  The final result is a table with 3 rows per team (corresponding to the 3 positions Guard, Forward and Center) and a percentage based on the   
  opponents' statistics, allowing you to judge whether the team is above or below the defensive level of the other teams in the league. A negative 
  impact means that the team in question defends well against players in the position under analysis, and the opposite for a positive impact. 

**- NBA_test_models : **
This script measure the average size of the mistakes in a collection of predictions(fantasy, points, rebounds etc.)
To do this we filter the last database scrapted with removing the last game played for each player, after that we compare with stats for the last nba games date played 

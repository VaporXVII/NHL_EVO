# Overview 
This folder contains notebooks responsible for processing NHL player identfication data through the Bronze, Silver, and Gold layers of the Medallion Architecture

## Purpose
- Ingests raw player data from two different NHL API endpoints into the Bronze layer for all current and historical players
- Transforms, validates, and cleanses the raw data within the Silver layer
- Loads analytics-ready data into the Gold layer for downstream use

## Important Note
- Player data can be aggregated from two primary sources:
    - searching by first letter of the player's first name (aka player_search endpoint)
    - searching by season and whether or not the player is a skater or goalie (aka player_season_search endpoint)
- As seasons progress, teams may call up players from minor league or junior affiliate teams. To accompany such changes, both endpoints mentioned above are scraped every 15 days between the months of October and April (the typical timeframe for an NHL season). However,
  roster calls-ups are not typically permitted in the postseason, which is why the scrape doesn't occur in the months of May, June, or in rare circumstances of a long season, July.
- `nhl_data_staged.players.master_ids` serves as the primary source of truth and normalization for player names, positions, and the side from which the player shoots or catches the puck. After researching responses given from multiple endpoints, it was found that
  some sources would use different naming conventions for the same player identification number (e.g. `player_id` = 123 may come across as John Smith from player_search endpoint, but would then appear as Johnathan Smith in the shift NH REST API endpoint). 
  
## Relevant Tables
- `nhl_data_raw.players.player_search` (bronze): stores raw player biographic information from NHL API that uses player's first letter of first name as the search mechanism
- `nhl_data_raw.players.player_search_season` (bronze): stores raw player information from NHL API that searches based on the season and if the player is a skater or goalie
- `nhl_data_staged.players.master_ids` (silver): cleanses and transforms the data from both `nhl_data_raw.players.player_search` and `nhl_data_raw.players.player_search_season`
- `nhl_data.players.master_ids` (gold): lightweight, simplified lookup table containing player identification information sourced from `nhl_data_staged.players.master_ids`

## Other Source Tables 
- `nhl_data_staged.players.current_rosters_staged` (silver): contains a snapshot of the roster for team's that are playing on a given date. In the event that a player was somehow not loaded into the `nhl_data_staged.players.master_ids` table, this will capture
  the player's necessary information (`player_id`, `player_name`, `team_id`, `team_abbrev`) and insert them into `nhl_data_staged.players.master_ids`. 
- `nhl_data_staged.games.pbp_data` (silver): play-by-play NHL API endpoint contains a compartment that reveals the team's roster information for the game in question. In the event that a player was somehow not loaded into
  the `nhl_data_staged.players.master_ids` table, this will capture the player's information (`player_id`, `player_name`, `team_id`, `team_abbrev`) and insert them into the `nhl_data_staged.players.master_ids` table. 
- `nhl_data_staged.games.shift_data` (silver): shift NHL API contains list of shifts that a player participated in within a particular game. In the event that a player was somehow not loaded into the `nhl_data_staged.players.master_ids` table, this will capture
  the player's information (`player_id`, `player_name`, `team_id`, `team_abbrev`) and load them into the `nhl_data_staged.players.player_game_rosters` table. 

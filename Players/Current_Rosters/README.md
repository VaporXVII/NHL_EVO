# Overview
This folder contains notebooks responsible for processing NHL player identfication data through the Bronze, Silver, and Gold layers of the Medallion Architecture based on a snapshot of the team's current roster as of the date that a game is played


## Purpose
- Ingests raw player data from the NHL API into the Bronze layer for all teams that are actively in play on the current date
- Transforms, validates, and cleanses the raw data within the Silver layer
- Loads analytics-ready data into the Gold layer for downstream use


## Relevant Tables
- `nhl_data_raw.players.player_game_rosters` (bronze): stores raw player biographic data that is sourced from the team REST API endpoint each date a team plays a game, preserving a point-in-time snapshot of the team's roster
- `nhl_data_staged.players.player_game_rosters` (silver): cleanses and transforms the data from `nhl_data_raw.players.master_ids`
- `nhl_data.players.player_game_rosters` (gold): lightweight, simplified lookup table containing player identification information sourced from `nhl_data_staged.players.player_game_rosters`

## Other Source Tables 
- `nhl_data_staged.games.pbp_data` (silver): play-by-play NHL API endpoint contains a compartment that reveals the team's roster information for the game in play. In the event that a player was somehow not loaded into the `nhl_data_staged.players.player_game_rosters` table, this will capture the player's
  information (`player_id`, `player_name`, `team_id`, `team_abbrev`) and insert them into the `nhl_data_staged.players.player_game_rosters` table to show that they participated in the game. 
- `nhl_data_staged.games.shift_data` (silver): shift NHL API contains list of shifts that a player participated in within a game. In the event that a player was somehow not loaded into the `nhl_data_staged.players.player_game_rosters` table, this will capture the player's information (`player_id`, `player_name`, `team_id`, `team_abbrev`). This table acts as a secondary source to capture the games that a player participated in.

# Overview
This folder contains the notebooks responsible for processing NHL game-level data for each team's special teams topics through the Bronze, Silver, and Gold layers of the Medallion Architecture.

# Purpose
- Ingests raw special teams data from the NHL API into the Bronze layer for all historical games (on the first pipeline run, as well as games within the last two days including the current date)
- Transforms, validates, and cleanses the raw data within the Silver layer.
- Load analytics-ready data into the Gold layer for downstream use.

## Data Ingested (aka topics)

- power-play stats
- power-play time on ice (toi) 
- penalty-kill stats 
- penalty-kill time on ice (toi)

## Important Note

- Calls to the NHL API for special teams data require passing a start and end date range, and only includes regular and playoff games
- The NHL uses the same endpoint structure for all four datasets listed above, with only the endpoint path differing (power-play, penalty-kill, power-play toi, and penalty-kill toi). As a result, all four datasets are stored in a single Bronze layer table, where the endpoint identifies the special teams topic associated with each record
- To prevent data loss, logic has been implemented in `nhl_data_raw.games.special_teams` and `nhl_data_staged.games.{special_teams_topic}` tables to ensure previously collected data is not overwritten by an empty payload returned during subsequent API calls

## Bronze Layer

The Bronze table contains the field `ingest_ts_utc`, which records when each row was initially inserted.

Whenever an existing record is modified, the field `update_ts_utc` is updated with the corresponding timestamp.

# Relevant tables
- `nhl_data_raw.games.special teams` (bronze): stores raw special teams data ingested from NHL API
- `nhl_data_staged.games.pp_stats` (silver): cleanses and transforms game-level power-play stats from `nhl_data_raw.games.special_teams`
- `nhl_data_staged.games.pk_stats` (silver): cleanses and transforms game-level penalty-kill stats from `nhl_data_raw.games.special_teams`
- `nhl_data_staged.games.pp_toi` (silver) : cleanses and transforms game-level power-play toi stats from `nhl_data_raw.games.special_teams`
- `nhl_data_staged.games.pk_toi` (silver): cleanes and transforms game-level power-play toi stats from `nhl_data_raw.games.special_teams`
- `nhl_data.games.pp_stats` (gold): lightweight, simplified lookup table for a team's power-play stats by game, sourced from `nhl_data_staged.games.pp_stats`
- `nhl_data.games.pk_stats` (gold): lightweight, simplified lookup table for a team's penalty-kill stats by game, sourced from `nhl_data_staged.games.pk_stats`
- `nhl_data.games.pp_toi` (gold): lightweight, simplified lookup table for a team's power-play toi by game, sourced from `nhl_data_staged.games.pp_toi`
- `nhl_data.games.pk_toi` (gold): lightweight, simplified lookup table for a team's penalty-kill toi by game, sourced from `nhl_data_staged.games.pk_toi`

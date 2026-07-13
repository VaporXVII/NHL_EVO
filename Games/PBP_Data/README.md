# Overview
This folder contains the notebooks responsible for processing NHL game-level play-by-play (PBP) data through the Bronze, Silver, and Gold layers of the Medallion Architecture. 

# Purpose
- Ingests raw PBP data from the NHL API into the Bronze layer for all historical games (on the first pipeline run, as well as games within the last two days including the current date)
- Transforms, validates, and cleanses the raw data within the Silver layer.
- Load analytics-ready data into the Gold layer for downstream use.

## Important Note

- Game-level PBP data is retrieved only for regular season and playoff games. Preseason games are excluded
- The NHL did not begin exposing game-level PBP data with enough meaningful information until the 2008-2009 season
- To prevent data loss, logic has been implemented in both `nhl_data_raw.games.pbp_data` and `nhl_data_staged.games.pbp_data` to ensure previously collected PBP data is not overwritten by an empty payload returned during subsequent API calls.

## Bronze Layer

The Bronze table contains the field `ingest_ts_utc`, which records when each row was initially inserted.

The NHL PBP endpoint is scraped every 30 minutes between **6:00 PM and 11:30 PM CST**. In addition to current games, the pipeline also retrieves PBP data for games played during the previous two days. This is necessary because the NHL API may revise or correct events after they are initially published.

Whenever an existing record is modified, the field `update_ts_utc` is updated with the corresponding timestamp.

### Silver Layer Lineage Fields

The Silver table contains several fields used for lineage, auditing, and MERGE/INSERT/UPDATE logic.

- **`active_row`**  
  Indicates whether an event is still considered active. Because NHL PBP events may be revised after ingestion, an event's attributes can change over time.

  **Example:** `event_id = 123` is initially reported as a `missed-shot` at 6:30 PM. At 7:00 PM, the NHL API updates the same event to `shot-on-goal`. The original `missed-shot` record is therefore marked as inactive.

- **`logic_block`**  
  Identifies the MERGE/INSERT/UPDATE logic block responsible for inserting or updating the row.

- **`failed_condition`**  
  Records the field responsible for triggering an update.

  **Example:** `event_id = 45` was originally recorded as occurring in the defensive zone but was later changed to the neutral zone.


# Relevant tables
- `nhl_data_raw.games.pbp_data` (bronze): stores raw PBP data ingested from NHL API
- `nhl_data_staged.games.pbp_data` (silver): cleanses and transforms data from `nhl_data_raw.games.pbp_data`, includes fields `active_row` and `failed_condition`
- `nhl_data.games.pbp_data` (gold): contains all rows from `nhl_data_staged.games.pbp_data` where the field `is_active` = True, thus representing the most accurate and up-to-date data from the PBP endpoint

# Other tables impacted: 
- `nhl_data_staged.players.player_game_rosters` (silver): maintains the game roster by collecting all unique player identifiers associated with PBP events and ensuring each player is represented with the corresponding `season`, `game_id`, `team_id`, and other
  necessary fields that are part of table
- `nhl_data_staged.quarantine.pbp_data` (N/A): stores PBP events that violate one or more data quality rules. Rows written to this table are considered invalid and excluded from downstream processing until reviewed.
- `nhl_data_staged.ops.games_missing_pbp` (N/A): stores all game identification numbers where the API call returned no data in the payload. Once added to the table, a scrape is performed on any day where day % 5 = 0 in an attempt to see
  if the data has been re-populated.

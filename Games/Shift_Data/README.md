# Overview
This folder contains the notebooks responsible for processing NHL game-level shift data through the Bronze, Silver, and Gold layers of the Medallion Architecture. 

# Purpose
- Ingests raw shift data from the NHL API into the Bronze layer for all historical games (on the first pipeline run, as well as games within the last two days including the current date)
- Transforms, validates, and cleanses the raw data within the Silver layer.
- Loads analytics-ready data into the Gold layer for downstream use.

## Important Note

- Game-level shift data is retrieved only for regular season and playoff games. Preseason games are excluded
- The NHL did not begin exposing game-level shift data (at least through a publicly available API) until the 2010-2011 season
- Due to the size of the shift data, if users cold start the pipeline, expect this component to be the one that takes the longest time to run 
- The shift data endpoint typically takes longer to update than the play-by-play endpoint, so users may notice a delay in data availability
- The shift data endpoint appears to be the most inconsistent of all NHL API endpoints. Users may occasionally observe that shift data is available on the NHL website one day but missing the next. The reason for this behavior is currently unknown.
  To prevent data loss, logic has been implemented in both `nhl_data_raw.games.shift_data` and `nhl_data_staged.games.shift_data` to ensure previously collected shift data is not overwritten by an empty payload returned during subsequent API calls.

## Bronze Layer

The Bronze table contains the field `ingest_ts_utc`, which records when each row was initially inserted.

The NHL shift endpoint is scraped every 30 minutes between **6:00 PM and 11:30 PM CST**. In addition to current games, the pipeline also retrieves shift data for games played during the previous two days. This is necessary because the NHL API may revise or correct shifts after they are initially published.

Whenever an existing record is modified, the field `update_ts_utc` is updated with the corresponding timestamp.

### Silver Layer Lineage Fields

The Silver table contains several fields used for lineage, auditing, and MERGE/INSERT/UPDATE logic.

- **`active_row`**  
  Indicates whether a shift is still considered active. Because NHL shift events may be revised after publication, an event's attributes can change over time.

  **Example:** `shift_id = 123 by player John Smith` is initially reported at 6:30 PM. At 7:00 PM, the NHL API updates the same shift to a new player or removes it alltogether. The original shift record is therefore marked as inactive.

- **`logic_block`**  
  Identifies the MERGE/INSERT/UPDATE logic block responsible for inserting or updating the row.

- **`failed_condition`**
  Records the field responsible for triggering an update

  **Example:** player John Smith's shift ID 123456 `shift_start_time` started at 1:17 in the first period, but then in subsequent ingestions was changed to 1:19


# Relevant tables
- `nhl_data_raw.games.shift_data` (bronze): stores raw shift data ingested from NHL API
- `nhl_data_staged.games.shift_data` (silver): cleanses and transform data from `nhl_data_raw.games.shift_data`, includes fields
- `nhl_data.games.shift_data` (gold): contains all rows from `nhl_data_staged.games.shift_data` where the field `active_row` = True, thus representing the most accurate and up-to-date data from the shift data endpoint

# Other tables impacted: 
- `nhl_data_staged.players.player_game_rosters` (silver): maintains the game roster by collecting all unique player identifiers associated with shift events and ensuring each player is represented with the corresponding `season`, `game_id`, `team_id`, and other
  necessary fields that are part of table
- `nhl_data_staged.quarantine.shift_data` (N/A): stores shifts that violate one or more data quality rules. Rows written to this table are considered invalid and excluded from downstream processing until reviewed.
- `nhl_data_staged.ops.games_missing_shift` (N/A): stores all game identification numbers where the API call returned no data in the payload. Once added to the table, a scrape is performed on any day where day % 5 = 0 in an attempt to see
  if the data has been re-populated. This does not include games that were played in seasons where shift data was not collected by the NHL.


## Overview

This section documents the operational tables used to track games where the NHL API returned an empty JSON payload. These tables are applicable only to the Play-by-Play and Shift Data endpoints and are monitored by Databricks alerts, which notify users whenever new records are written to either table

During the development and testing of NHL_EVO, it was discovered that requests to these NHL API endpoints may return no data under one of the following scenarios:

- **Initial ingestion:** The first request to the NHL API returns an empty payload because no data is available for the requested game
- **Subsequent ingestion:** A request that previously returned data later returns an empty payload. This behavior occurs periodically with the Shift Data endpoint for reasons that are currently unknown

These operational tables allow the pipeline to distinguish between games where data has never been available and games where previously available data has temporarily disappeared, preventing valid data from being overwritten while supporting scheduled retry attempts.
In the event a `game_id` is loaded into this table, calls are made every 15 days to see if the data has returned so that it can be ingested into the bronze layer tables. Additionally, a field called `attempt_count` is used to track the number of times a retry has been performed for the game in question.  

At this time there is no limit to the number of retries that can occur, but efforts will be made once a new season starts to see if limits should be imposed (i.e. testing to see if it's possible that the data returns after an extended
period of time).

## Missing Data Tables 
- `nhl_data_staged.ops.games_missing_pbp`: tracks games where play-by-play data NHL API call returned empty json
- `nhl_data_staged.ops.games_missing_shift`: tracks games where shift data NHL API call returned empty json

## Missing Data Files
- NHL_API_Games_PBP_Data_Staged.py: performs the insert into the `nhl_data_staged.ops.games_missing_pbp` table
- NHL_API_Games_Shift_Data_Raw.py: performs the insert into the `nhl_data_staged.ops.games_missing_shift` table


## Missing Data Table Schema
| Field | Data Type | Purpose 
|-------|-----------|---------|
| `season` | Integer | stores the season the game is scheduled in |
| `game_id` | Bigint | stores the game identification number for the game that has missing data | 
| `last_attempt_dte` | Date | tracks the date of the last retry attempt | 
| `next_retry_dte` | Date | tracks the date of the next retry attempt, defined as the `last_attempt_dte` + 15 days | 
| `attempt_count` | Integer | counter to gauge the number of times a retry has been performed | 
| `insert_dte` | Timestamp | stamps when the row was inserted | 
| `update_dte` | Timestamp | stamps when the row was updated | 


## Audit Tables 
- `nhl_data_staged.ops.schema_audit`: logs schema of most recent payload for each endpoint and checks to see if new fields have been added or existing fields have been renamed (to assist with schema drift and evolution)

## Audit Files 
- NHL_API_Schema_Audit.py: performs audit check of most recently ingested payloads then inserts and updates into the `nhl_data_staged.ops.schema_audit` table


## Audit Table Schema 
| Field | Data Type | Purpose 
|-------|-----------|---------|
| `table_name` | String | stores name of Bronze layer table associated with schema audit | 
| `endpoint` | String | contains unique endpoint names found in each Bronze layer table |
| `schema_json` | String | the schema of the most recent JSON payload | 
| `schema_hash` | String | string schema converted to hash |
| `schema_drift_ind` | Boolean | flags whether or not there has been a change in the JSON payload schema | 
| `last_check_dte` | Date | date of the last audit check | 
| `insert_dte` | Date | date the record was inserted | 
| `update_dte` | Date | date the record was updated |
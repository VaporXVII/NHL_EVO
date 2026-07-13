## Overview

This section documents the quarantine tables used to capture records that fail required data quality checks, and are monitored by Databricks alerts that notify users whenever new records are inserted into either table.

While some NHL API tables can tolerate missing optional fields, missing values in certain Play-by-Play and Shift data fields can break downstream processing, joins, or analytical accuracy. 
These records are isolated into quarantine tables instead of being loaded directly into the primary Silver layer tables.

## Relevant Tables
- `nhl_data_staged.quarantine.pbp_data`
- `nhl_data_staged.quarantine.shift_data`

- ## Relevant Notebooks
- NHL_API_Games_PBP_Data_Staged.ipynb: performs the insert into `nhl_data_staged.quarantine.pbp_data` table
- NHL_API_Games_Shift_Data_Staged.ipynb: performs the insert into `nhl_data_staged.quarantine.shift_data` table

## Audit & Operations
| Table | Purpose | Required Fields | Validation Rule |
|-------|---------|---------|---------|
| `nhl_data_staged.quarantine.pbp_data` | Stores Play-by-Play records where any field fails required field validation | `event_type`, `time_in_period`, `time_remaining`, `situation_code` | field is NULL |
| `nhl_data_staged.quarantine.shift_data` | Stores Shift Data records where any field fails required field validation | `player_id`, `start_time`, `end_time` | field is NULL | 

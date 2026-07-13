# Overview
This folder contains the notebooks responsible for processing NHL team data through the Bronze, Silver, and Gold layers of the Medallion Architecture.

## Purpose
- Ingest raw team data from the NHL REST API into the Bronze layer.
- Transform, validate, and cleanse the raw data within the Silver layer.
- Load analytics-ready data into the Gold layer for downstream use.


## Relevant tables
- `nhl_data_raw.teams.master_ids` (bronze): stores raw team data ingested from NHL REST API, including each team's unique identification number
- `nhl_data_staged.teams.master_ids` (silver): cleanses and transforms data from `nhl_data_raw.teams.master_ids`
- `nhl_data.teams.master_ids` (gold): stores the finalized list of teams, including `team_id`, `team_name`, `team_abbrev`
- `nhl_data_staged.teams.details` (silver): extends the master team data with additional fields, including `is_active`, `active_to`, `active_from`
- `nhl_data.teams.details` (gold): stores the finalized team details for downstream analytics


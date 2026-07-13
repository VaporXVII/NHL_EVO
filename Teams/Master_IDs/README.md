# Overview
This folder contains the notebooks responsible for processing NHL team identification number data through the Bronze, Silver, and Gold layers of the Medallion Architecture. 
This houses the master list of NHL teams (current and historical), including the team's unique identifier, abbreviation, and official name. Past teams that are no longer active are included 
in the table to be able to correctly and accurately link to the appropriate team (i.e. Utah Hockey Club has team id 123 with team abbrev UTA, while Utah Mammoth also use team abbrev UTA but have team id 456)


## Purpose 
- Ingests raw team data from the NHL REST API into the Bronze layer.
- Transforms, validates, and cleanses the raw data within the Silver layer.
- Loads analytics-ready data into the Gold layer for downstream use.


## Relevant Tables
- `nhl_data_raw.teams.master_ids` (bronze): stores raw team data ingested from NHL REST API, including each team's unique identification number
- `nhl_data_staged.teams.master_ids` (silver): cleanses and transforms data from `nhl_data_raw.teams.master_ids`, acts as source of truth for any notebooks that require the lookup of a team's metadata
- `nhl_data.teams.master_ids` (gold): lightweight, simplified lookup table sourced from `nhl_data_staged.teams.master_ids` that contains `team_id`, `team_abbrev`, `team_name`. 

# Overview 
This folder contains the notebooks responsible for processing NHL franchise details data through the Silver and Gold layers of the Medallion Architecture. 
The Team Details section also incorporates `nhl_data_staged.teams.master_ids` to include teams that were not Original Six or part of an Expansion era (e.g. Hamilton Tigers, Toronto St. Patrics). The endpoint may include values for `last_active_season` if the franchise is no longer active, hwoever a franchise relocated to a new city 
and assumed a new team name, then the `nhl_data_staged.games.schedules` table is used to derive the franchise's current team `active_from`, `active_to`, and `last_active_season` fields.

## Purpose 
- Ingests data from franchise endpoint and inserts into nhl_data_raw.teams.details table
- Transforms, validates, and cleanses the raw data from `nhl_data_raw.teams.details` bronze layer
- Loads analytics-ready data into Gold layer for downstream use

## Important Note 
- Silver and Gold layer tables contain fields called insert_dte and update_dte which stamp when a row was inserted or updated. Although the fields are labeled as "date" they are in fact **TIMESTAMP** data type fields.
- Silver layer table contains a field named `py_source`. Whenever a row is inserted or updated, this field is stamped with the name and version of the notebook that performed the
  operation (e.g., details_v1.0, details_v1.1). Since the notebook name is captured automatically, users do not need to manually update the code when versioning notebooks. Recommendation: It is strongly recommended that notebook names include a version suffix (for example, _v1.0, _v1.1, _v2.0, etc.). Doing so makes it easy to identify which notebook version inserted or updated a given row, simplifying debugging, auditing, and change tracking.
record to be traced back to the notebook version that most recently processed it. It's strongly recommended that notebook names follow a versioned naming convention (`details_v1.0`, `details_v1.1`) to better track this lineage.

## Relevant tables
- `nhl_data_raw.teams.details` (bronze): stores raw franchise data ingested from NHL API, including the franchise's current team unique identification number. 
- `nhl_data_staged.games.schedules` (silver): used to derive dates that a team was `active_from`, `active_to`, but will also derive `last_active_season`, and `is_active` fields in the event the payload from `nhl_data_raw.teams.details` doesn't contain this information
- `nhl_data_staged.teams.details` (silver): cleanses and transforms data from `nhl_data_raw.teams.details` to track whether or not a team is active or not, including fields
  such as `active_to`, `active_from`, `is_active`, and `last_active_season`
- `nhl_data.teams.details` (gold): lightweight, simplified lookup table that contains team related data from `nhl_data_staged.teams.details` but omits the NHL API `team_city` field as well as the `last_active_season` field

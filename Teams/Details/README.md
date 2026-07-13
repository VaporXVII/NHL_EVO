# Overview 
This folder contains the notebooks responsible for processing NHL team details data through the Silver and Gold layers of the Medallion Architecture. 
Since the silver and gold layers of this section are dependent on `nhl_data_raw.teams.master_ids`, this section will not contain a bronze layer. The Team Details section extends the Team Master IDs dataset by deriving the time period during which each team, both current and historical, was active. It populates `active_to`, `active_from`, and `is_active`, which were intentionally not stored in `nhl_data_staged.teams.master_ids`.


## Purpose 
- Transforms, validates, and cleanses the raw data from `nhl_data_raw.teams.master_ids` bronze layer
- Loads analytics-ready data into Gold layer for downstream use

## Important Note 
- Silver and Gold layer tables contain fields called insert_dte and update_dte which stamp when a row was inserted or updated. Although the fields end in _dte they are in fact timestamp data type fields.
- Silver layer table contains a field named `py_source`. Whenever a row is inserted or updated, this field is stamped with the name and version of the notebook that performed the
  operation (e.g., details_v1.0, details_v1.1). Since the notebook name is captured automatically, users do not need to manually update the code when versioning notebooks. Recommendation: It is strongly recommended that notebook names include a version suffix (for example, _v1.0, _v1.1, _v2.0, etc.). Doing so makes it easy to identify which notebook version inserted or updated a given row, simplifying debugging, auditing, and change tracking.
record to be traced back to the notebook version that most recently processed it. It's strongly recommended that notebook names follow a versioned naming convention (`details_v1.0`, `details_v1.1`) to better track this lineage.

## Relevant tables
- `nhl_data_raw.teams.master_ids` (bronze): stores raw team data ingested from NHL REST API, including each team's unique identification number
- `nhl_data_staged.games.schedules` (silver): used to derive dates that a team was `active_to` and `active_from`, and if the team `is_active`
- `nhl_data_staged.teams.details` (silver): cleanses and transforms data from `nhl_data_raw.teams.master_ids` to track whether or not a team is active or not, including fields
  such as `active_to`, `active_from`, `is_active`
- `nhl_data.teams.details` (gold): lightweight, simplified lookup table that contains team related data from `nhl_data_staged.teams.details` but omits the NHL REST API `team_city` field as well as the `current_season` field

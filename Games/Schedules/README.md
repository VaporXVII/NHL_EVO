# Overview
This folder contains the notebooks responsible for processing NHL schedule data through the Bronze, Silver, and Gold layers of the Medallion Architecture. 
The table `nhl_data_staged.games.schedules` serves as the foundation for all downstream tables within NHL_EVO, as they depend on it either directly or indirectly.

# Purpose
- Ingests raw schedule data from the NHL API into the Bronze layer.
- Transforms, validates, and cleanses the raw data within the Silver layer.
- Loads analytics-ready data into the Gold layer for downstream use.

## Important Note
- Bronze Layer table contains field called ingest_ts_utc which stamps when the row was inserted into the table. Since this notebook isn't ran more than once a day, there is no need to include a field called update_ts_utc since a record from the previous day should never be updated. An additional field called `scrape_plan` is also included to determine which scrape plan logic was used to insert or update rows (i.e. if the schedule is released on 9/20/2026, then there is no reason to continuously scrape every day or week. However, occassional scrapes should be done to capture game time changes, as well as capture playoff game information when the postseason takes effect).
- Silver and Gold layer tables contain fields called `insert_dte` and `update_dte` which stamp when a row was inserted or updated. Although the fields end in _dte they are in fact timestamp data type fields.
- All Silver layer tables contain a field named `py_source`. Whenever a row is inserted or updated, this field is stamped with the name and version of the notebook that performed the operation (e.g., master_ids_v1.0, master_ids_v1.1). Since the notebook name is automatically captured within the code, users don't have to worry about manually changing the underlying code to see this change take effect. This provides lightweight data lineage by allowing each record to be traced back to the notebook version that most recently processed it. This field is intentionally omitted from Gold layer tables because each Gold table is populated exclusively by its corresponding Silver layer table. Since no other notebook can modify the Gold table, the additional lineage provided by `py_source` is unnecessary. The same principle applies to the Bronze layer, where each raw table is populated by a single ingestion notebook. This provides traceability by allowing each record to be associated with the notebook version that most recently processed it.

# Relevant tables
- `nhl_data_raw.games.schedules` (bronze): stores raw schedule data ingested from NHL API
- `nhl_data_staged.games.schedules` (silver): cleanses and transforms data `from nhl_data_staged.games.schedules`, includes fields such as  `season`, `game_date`, and the unique identifier of each game, aka `game_id`
- `nhl_data.games.schedules` (gold): lightweight, finalized table with list of seasons, games, game dates, and the teams playing in the corresponding game

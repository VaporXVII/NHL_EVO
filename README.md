# Overview

The purpose of this project is to build NHL_EVO, a scalable ELT pipeline that consistently ingests data from the NHL API and loads it into Databricks Delta Lake tables using the Medallion Architecture (Bronze, Silver, and Gold), while 
also incorporating other data engineering concepts to ensure accuracy and reliability, such as: 

- incremental and late-arrival data processing
- broadcast joins for query optimization
- MERGE/INSERT/UPDATE operations
- duplication detection and handling
- data cleansing
- retry logic
- Change Data Feed (CDF)
- Deletion Vectors 


NHL_EVO fully supports incremental data ingestion while preserving historical data and maintaining accurate, up-to-date datasets throughout each NHL season. This curated data will come to be the foundation for an Expected Goals (xG) model that will be 
released at a later date, as well as additional analytics that provide deeper insight into team, player, and game performance.

The NHL provides an API that houses data for teams, players, schedules, play-by-play events, shifts, and other game-related information. Starting in the 2008-2009 season, the league began
tracking game-level events with more granularity than what was previously collected in prior years. This new data has been the foundation of many advanced analytical models that serve the NHL advanced analytics community today. 
Although users can still access game-level data from those prior seasons, it was decided that there just wasn't enough useful information to warrant including them. 

## Data Collection Timeframe
- Starting date: 2008/10/01 
- Ending date: continously updates to caputure data as of the most recent season's end date

## Tools Used
- Databricks Community Edition
- PySpark
- Spark SQL
- Databricks Jobs Orchestration
- Databricks Alerts
- Python

## Important Note
Databricks Community Edition uses a Serverless compute cluster, which does not support some capabilities that are available on standard Databricks clusters. Due to this limitation, certain 
Spark features are intentionally not used throughout this project, including:

- `DataFrame.cache()`
- `DataFrame.persist()`
- Photon
- RDD API

Databricks Community Edition includes a daily limit on free serverless compute usage. If this limit is reached, some notebooks may not run until compute availability resets. This is a platform limitation and 
is unrelated to the code or functionality of this project. Additionally, users will also notice that timestamp and date related queries utilize the 'America/Chicago' timeframe, aka American Central Standard Time (CST). This was 
due to the discovery that Databricks Community Edition relies on UTC timestamps which can impact dates that are used to retrieve queries. Lastly, unlike many modern data architectures that first land raw API data in an Amazon S3 data lake before loading into the Bronze layer, this project ingests data directly from the NHL API into the Bronze Delta tables, where the raw JSON payloads are preserved for historical reference and reporting. Because the Bronze layer already serves as a durable repository for the raw JSON payloads, introducing a separate S3 landing zone would have added infrastructure complexity and cost without providing meaningful benefits for this project's requirements.

## Audit & Operational Fields
| Field | Data Type | Purpose | Medallion Layer |
|-------|-----------|---------|-----------------|
| `endpoint` | String | NHL API endpoint associated with ingestion | Bronze |
| `request_key` | String | Unique identifier used to construct and retrieve data from the NHL API endpoint | Bronze |
| `api_url` | String | Fully constructed NHL API endpoint URL used for the request | Bronze |
| `http_status` | Integer | HTTP response code returned by the NHL API | Bronze |
| `payload` | Array | Stores the raw JSON payload returned by the NHL API | Bronze |
| `ingest_ts_utc` | Timestamp | Indicates when a row was ingested | Bronze |
| `update_ts_utc` | Timestamp | Indicates when a row was updated | Bronze |
| `insert_dte` | Timestamp | Indicates when a row was initially inserted | Silver, Gold |
| `update_dte` | Timestamp | Indicates when a row was last updated | Silver, Gold |
| `active_row` | Boolean | Indicates whether a row exists in the most recent NHL API response, used in Play-by-Play and Shift Data tables because records may appear in one ingestion but be absent in subsequent ingestions | Silver |
| `py_source` | String | Automatically captures the notebook name and version responsible for the insert or update | Silver |
| `logic_block` | String | Identifies the MERGE/INSERT/UPDATE logic block responsible for modifying a row, used in Play-by-Play and Shift Data tables to aid debugging and auditing | Silver |
| `failed_condition` | String | Captures fields whose values changed during a MERGE/UPDATE operation, used on selected tables where source data is more prone to change over time | Silver |

**Recommendation:** Version notebook names using the `_v#.#` naming convention to provide lightweight data lineage, making it easy to identify which notebook version most recently inserted or update the record in the `py_source` field.

Development is ongoing, and updates are released incrementally after they have been thoroughly tested to ensure they meet the project's standards for scalability, reliability, and accuracy.

Disclaimer: This project is an independent software engineering and analytics project, and is strictly for educational and portfolio purposes only. No data is distributed as part of this repository. It is not affiliated with, endorsed by, or sponsored by the National Hockey League (NHL). All NHL data is obtained from publicly available NHL API endpoints.

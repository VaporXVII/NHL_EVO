from datetime import datetime, UTC
from pathlib import Path
import json, time
from pyspark.sql import SparkSession, Row, DataFrame
from pipeline_funcs.user_utc_region import region_return

def s3_return(bucket_name:str) -> Path:

        bucket_name = bucket_name.lower()

        buckets = {

                "schedule": "/Volumes/nhl_evo_s3/games/schedule_data",
                "pbp": "/Volumes/nhl_evo_s3/games/pbp_data",
                "shift": "/Volumes/nhl_evo_s3/games/shift_data", 
                "team_lines": "/Volumes/nhl_evo_s3/teams/team_lines_data",
                "vegas_totals": "/Volumes/nhl_evo_s3/games/vegas_totals_data"
                
                }
        return Path(buckets[bucket_name])


def s3_create_dir(s3_dir: Path) -> None: 

        s3_dir.mkdir(parents = True, exist_ok = True)


def s3_dir_name(bucket_name: str) -> str: 

        bucket_name = bucket_name.lower()
        buckets = {

                "schedule": "game_date",
                "pbp": "game_id",
                "shift": "game_id",
                "team_lines": "team_name",
                "vegas_totals": "game_date"
        }
        return buckets[bucket_name]


def s3_schema_dir(bucket_name: str) -> str: 

        bucket_name = bucket_name.lower()
        schemas = {

                "schedule": "games",
                "pbp": "games", 
                "shift": "games", 
                "team_lines": "teams",
                "vegas_totals": "games"
        }

        return schemas[bucket_name]


def s3_legacy_table_dir(bucket_name: str) -> str: 

        bucket_name = bucket_name.lower()
        legacy_tables = {

                "schedule": "schedules",
                "pbp": "pbp_data", 
                "shift": "shift_data", 
                "team_lines": "team_lines",
                "vegas_totals": "vegas_totals"
        }

        return legacy_tables[bucket_name]

def s3_schema_extract(spark: SparkSession, bucket_name: str) -> str: 

        bucket_name = bucket_name.lower()
        s3_dir = s3_return(bucket_name = bucket_name)
        bucket_schema = s3_dir / "_payload_schema/" / f"{bucket_name}_schema.txt"

        return bucket_schema.read_text()


def s3_backfill(spark: SparkSession, bucket_name: str, season_lookback: int = None, sample_size: float = None) -> DataFrame: 

        #primary season filter to limit shift backfills to only seasons where shift data was available from NHL
        shift_season_filter = f"and concat(substring(a.request_key, 1, 4)::string, (substring(a.request_key, 1, 4)::integer + 1)::string) > 20092010" if bucket_name.lower() == 'shift' else ""
        #secondary season filter to limit backfills to a particular season if need be
        season_filter = f"and concat(substring(a.request_key, 1, 4)::string, (substring(a.request_key, 1, 4)::integer + 1)::string) = {season_lookback}" if season_lookback else ""
        #final filter to be used, if a season is provided then override the shift season filter otherwise just use shift season filter 
        final_filter = season_filter if season_filter else shift_season_filter
        sample_limit = f"limit {sample_size}" if sample_size else "" 
        team_lines_join = f"and a.game_date = b.game_date" if bucket_name.lower() == "team_lines" else ""
        #time lines dataset does not have an update_ts_utc field since it's append only
        timestamp_field = "a.ingest_ts_utc" if bucket_name.lower() == "team_lines" else "a.update_ts_utc"
        bucket_schema = s3_schema_dir(bucket_name)
        legacy_table = s3_legacy_table_dir(bucket_name)
        return spark.sql(f"""

                select 
                        a.*,
                        coalesce(

                                {timestamp_field},
                                timestampadd(hour, 48, a.ingest_ts_utc)
                        ) as scrape_ts_utc
                from nhl_data_raw.{bucket_schema}.{legacy_table} a  
                left anti join nhl_evo_raw.{bucket_schema}.{bucket_name}_raw_data b 
                        on a.request_key = b.request_key
                        {team_lines_join}
                where 1 = 1
                        and a.payload is not null 
                        {final_filter}
                {sample_limit}
                order by a.request_key desc, a.ingest_ts_utc desc
                                
        """)


def s3_delete(spark: SparkSession, bucket_name: str, lookback_window: int = None) -> None: 

        user_region = region_return()
        filter_clause = f"""
                        and try_cast(s3_ingest_ts_utc as timestamp) is not null 
                        and try_cast(s3_ingest_ts_utc as date) >= date_sub(from_utc_timestamp(current_timestamp(), '{user_region}')::date, {lookback_window})  
                        """ if lookback_window else ""
        s3_ext_vol = s3_return(bucket_name = bucket_name)
        files_to_delete = spark.sql(f"""
                                   
                        select 
                                 _metadata.file_path as s3_file_path
                        from read_files(
                                '{s3_ext_vol}',
                                format => "json"
                        )                      
                        where 1 = 1
                                {filter_clause}
                                   
        """)
        deleted = 0

        for row in files_to_delete.toLocalIterator():
                if dbutils.fs.rm(row["s3_file_path"]):
                        deleted += 1

                print(f"Deleted {deleted:,} files")

def s3_ingest(row: Row | dict, s3_ext_vol: str, backfill: bool = False) -> str:

        request_key = row["request_key"]
        # Use the historical ingest timestamp for backfills, otherwise use the API scrape timestamp,
        # to uniquely timestamp each game snapshot written to S3.
        # The PBP API returns the full set of plays available at each scrape, so successive files
        # for the same game will contain many of the same plays.
        s3_file_key = s3_dir_name(s3_ext_vol)
        s3_ext_vol_path = s3_return(s3_ext_vol)
        ingest_or_scrape_ts = row["ingest_ts_utc"] if backfill else row["scrape_ts_utc"]
        #convert for consistent file naming convention
        file_time = ingest_or_scrape_ts.strftime("%Y%m%dT%H%M%S%fZ")
        s3_file_path = (s3_ext_vol_path / f"{s3_file_key}={request_key}" / f"{file_time}.json")
        s3_game_dir = s3_ext_vol_path / f"{s3_file_key}={request_key}"
        #create directory for the game if necessary     
        if s3_file_path.exists():
                return "skipped" if backfill else f"{request_key} S3 Ingestion skipped, file path already exists...."
        s3_create_dir(s3_game_dir)
        data_record = {

                        "endpoint": row["endpoint"],
                        "request_key": request_key,
                        "http_status": row["http_status"],
                        "payload": json.loads(row["payload"]),
                        "api_url": row["api_url"],
                        #timestamp fields in original pipeline version aren't defined in json response but in the 
                        #upsert statements, safe to include here for S3 backfill and subsequent live game scrapes
                        #that are part of new NHL EVO S3 ingestion and ensure timestamps are in proper format
                        "ingest_ts_utc": row["ingest_ts_utc"].isoformat() if backfill else None,
                        "scrape_ts_utc": None if backfill else row["scrape_ts_utc"].isoformat(),
                        "backfill": backfill
                
                }
        if s3_ext_vol == "team_lines":
                data_record = {
                        "game_date": row["game_date"].isoformat(),
                        "game_id": row["game_id"],
                        **data_record
                }
        if s3_ext_vol == "schedule":
                data_record = {
                        **data_record, 
                        "scrape_plan": row["scrape_plan"]
                }

        with s3_file_path.open("w", encoding = "utf-8") as file: 
                data_record["s3_ingest_ts_utc"] = datetime.now(UTC).replace(tzinfo = None).isoformat()
                json.dump(data_record, file, ensure_ascii = False, separators = (",", ":"))
                return "written" if backfill else f"{request_key} S3 Ingestion successful" 

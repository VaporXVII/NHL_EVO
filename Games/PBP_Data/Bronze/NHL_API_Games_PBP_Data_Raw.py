import sys
username = spark.sql("select current_user()").first()[0]
sys.path.append(f"/Workspace/Users/{username}/NHL_Pipeline")

from pyspark.sql import SparkSession
from pyspark.sql import functions as f, types as t, Window as w, DataFrame
from delta.tables import DeltaTable
from zoneinfo import ZoneInfo
import requests, json, time, random, threading, math, gc, psutil, datetime
import concurrent.futures 
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque 
from pipeline_funcs.games import get_games
from pipeline_funcs.api_utils import * 
from pipeline_funcs.user_utc_region import region_return

user_region = region_return()
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", f"{user_region}")

def find_games(limit_n: int, raw_schema: str = None) -> DataFrame: 

    #SQL below is used as part of batch processing. Since pbp data is the second largest data set from the NHL API, 
    #attempting to collect data for all games, without doing batch processing, can cause the Serverless compute cluster to run out of memory
    return spark.sql(f"""
                         
            with date_param as (

                select from_utc_timestamp(current_timestamp(), '{user_region}')::date as current_run_date

            )
            , 
            games as (


                select /*+ broadcast (p) */ distinct
                        a.season,
                        a.game_id,
                        a.game_date,
                        a.team_abbrev,
                        a.start_time_utc
                from nhl_data_staged.games.schedules a 
                cross join date_param p
                where 1 = 1
                    and a.game_type in (2,3)
                    and a.game_date <= p.current_run_date


                )
                ,
                games_ended_today as (


                select /*+ broadcast(b), broadcast (p) */ distinct
                    a.season,
                    a.game_id,
                    a.game_date,
                    b.start_time_utc
                from nhl_data_staged.games.pbp_data a 
                inner join games b 
                    on a.game_id = b.game_id 
                cross join date_param p
                where 1 = 1
                    and a.game_date = p.current_run_date
                    and a.event_type = 'game-end'


                )
                ,
                games_in_play as (

                select /* + broadcast(a), broadcast (p) */ distinct
                        a.season,
                        a.game_id,
                        a.game_date,
                        a.team_abbrev,
                        a.start_time_utc
                    from games a
                    left anti join games_ended_today b 
                        on a.game_id = b.game_id 
                    cross join date_param p
                    where 1 = 1
                        and a.game_date = p.current_run_date
                        ---NHL games typically don't start until at least 15 minutes after scheduled start time
                        and from_utc_timestamp(current_timestamp(), '{user_region}') >= from_utc_timestamp(a.start_time_utc, '{user_region}') + interval 15 minutes

                )
                ,
                games_prior_two as (

                    select /*+ broadcast (p) */
                        a.season,
                        a.game_id,
                        a.game_date,
                        a.team_abbrev,
                        a.start_time_utc
                    from games a 
                    cross join date_param p
                    where 1 = 1
                        and a.game_date between 
                            date_sub(p.current_run_date, 2) 
                            and 
                            date_sub(p.current_run_date, 1)
                        

                )

                ,
                games_loaded as (

                    select /*+ broadcast (p) */ distinct
                        a.request_key as game_id
                    from nhl_data_raw.games.pbp_data a
                    cross join date_param p 
                    where 1 = 1
                        and a.request_key is not null 
                        and a.ingest_ts_utc::date < date_sub(p.current_run_date, 2)
                    

                )
                ,
                games_loaded_missing_data as (

                    ---section below creates a flag to check for games where the shift data is missing, if at least 15 days have passed hit it again
                    ---this will allow for only one scrape to happen a day since after the first attempt the last_attempt_dte field will get set to the 
                    ---current date, therefore setting the retry_scrape_ind to false 
                    select /*+ broadcast (p) */
                        a.game_id,
                        coalesce(
                            (
                            (p.current_run_date - a.last_attempt_dte::date)::integer >= 15), 
                        false
                        )::boolean as retry_scrape_ind
                    from nhl_data_staged.ops.games_missing_pbp a 
                    cross join date_param p

                )
                ,
                games_ended as (

                    select distinct
                        game_id
                    from nhl_data_staged.games.pbp_data
                    where 1 = 1
                        and game_id is not null
                        and event_type = 'game-end'

                )
                , 
                games_not_loaded as (

                    select /*+ broadcast (p) */
                        a.season,
                        a.game_id,
                        a.game_date,
                        a.team_abbrev,
                        a.start_time_utc
                    from games a
                    left anti join games_loaded b
                        on a.game_id = b.game_id
                    left anti join games_loaded_missing_data c 
                        on a.game_id = c.game_id
                        and c.retry_scrape_ind = false
                    cross join date_param p
                    where 1 = 1
                        and a.game_date < p.current_run_date

                )
                ,
                games_missing_from_api as (


                    select game_id
                    from games_loaded_missing_data

                )
                ,
                final_games as (

                select 
                    "in play" as which_game,
                    season, 
                    game_id, 
                    game_date, 
                    start_time_utc
                from games_in_play a
                union 
                select 
                    "ended today" as which_game,
                    season,
                    game_id, 
                    game_date,
                    start_time_utc
                from games_ended_today
                union 
                select 
                    "not loaded" as which_game,
                    season, 
                    game_id, 
                    game_date, 
                    start_time_utc
                from games_not_loaded a 
                left anti join games_prior_two b 
                    on a.game_id = b.game_id 
                left anti join games_missing_from_api c 
                    on a.game_id = c.game_id 
                union 
                select 
                    "last two" as which_game,
                    season, 
                    game_id, 
                    game_date,
                    start_time_utc
                from games_prior_two
                union 
                select 
                    "loaded but missing data" as which_game,
                    a.season,
                    a.game_id,
                    a.game_date,
                    a.start_time_utc
                from games a  
                inner join games_loaded_missing_data b 
                    on a.game_id = b.game_id 
                where 1 = 1

                )

                select 
                    date_format(a.start_time_utc, 'hh:mm a') as game_start_time_cst,
                    a.season,
                    a.game_date,
                    a.game_id,
                    a.which_game,
                    concat("https://api-web.nhle.com/v1/gamecenter/", a.game_id, "/play-by-play") as api_url
                from final_games a
                ---pulls in the active api url for pbp data
                where 1 = 1
                order by a.game_date, a.game_id 
                limit {limit_n}
                         
""")

def flush_api_data(api_data: list) -> int:

    if not api_data:
        return 0 
    
    api_data_df = (
            spark.createDataFrame(api_data)
            #.withColumn("game_idx", f.row_number().over(w.partitionBy("request_key").orderBy("request_key")))
            #.filter(f.col("game_idx") == 1)
            #.drop("game_idx")

    )
    
    merge_insert_found(batch_data = api_data_df)
    json_schema = (

            api_data_df
            .selectExpr("schema_of_json_agg(payload) as json_schema")
            .first()["json_schema"]
    )
    missing_payloads = (

            api_data_df 
            .withColumn("parsed_json", f.from_json(f.col("payload"), json_schema))
            .filter(f.size(f.col("parsed_json.plays")) == 0)
            .drop("parsed_json")
    )
    if not missing_payloads.isEmpty():
        update_missing_games(batch_data = missing_payloads)
    
    row_cnt = len(api_data)

    api_data.clear()
    gc.collect()

    return row_cnt

def update_missing_games(batch_data, DataFrame) -> None:

    try: 
        
        batch_data.createOrReplaceTempView("pbp_data_missing_tmp")
        spark.sql(f"""
                  
                  
                with date_param as (

                    select from_utc_timestamp(current_timestamp(), '{user_region}')::date as current_run_dte
                ) 
                ,
                src as (
                    
                    ---need to grab the season associated with the game that is going to be inserted into games_missing_pbp table
                    select /*+ broadcast (p) */ distinct 
                        a.season,
                        b.request_key as game_id 
                    from nhl_data_staged.games.schedules a
                    cross join date_param p 
                    inner join pbp_data_missing_tmp b 
                        on a.game_id = b.request_key
                    where 1 = 1
                        and a.game_type in (2,3)
                        and a.game_date <= p.current_run_dte

                )
                
                merge into nhl_data_staged.games.games_missing_pbp t 
                using src s 
                    on t.season = s.season 
                    and t.game_id = s.game_id 
                
                when matched then update set 

                    last_attempt_dte = from_utc_timestamp(current_timestamp(), '{user_region}')::date,
                    next_retry_dte = date_add(from_utc_timestamp(current_timestamp(), '{user_region}')::date, 15),
                    attempt_count = t.attempt_count + 1,
                    update_dte = current_timestamp()
                
                when not matched then insert (

                    season, 
                    game_id, 
                    last_attempt_dte,
                    next_retry_dte,
                    attempt_count,
                    insert_dte,
                    update_dte
                )

                values (

                    s.season, 
                    s.game_id, 
                    from_utc_timestamp(current_timestamp(), '{user_region}')::date,
                    date_add(from_utc_timestamp(current_timestamp(), '{user_region}')::date, 15),
                    1,
                    current_timestamp(),
                    null 
                )
                
                ;
                  
        
        """)
        spark.catalog.dropTempView("pbp_data_missing_tmp")
        print("Batch successfully inserted into nhl_data_staged.ops.pbp_missing_shift table")
        print("=" * 100)
    except Exception as e: 
        print(f"Error occured during insert into nhl_data_staged.ops.pbp_missing_shift table: {e}")

def merge_insert_found(batch_data: DataFrame) -> None:

    try: 
        batch_data.createOrReplaceTempView("pbp_data_tmp")
        spark.sql("""
                
                with src as (

                    select 
                        s.*,
                        ---regex checks to see if plays is empty list or empty dict 
                        regexp_like(s.payload, '"plays"\\s*:\\s*\\[\\s*\\]') as empty_condition_one,
                        regexp_like(s.payload, '"plays"\\s*:\\s*\\[\\s*\\{\\s*\\}\\s*\\]') as empty_condition_two
                    from pbp_data_tmp s
                    where 1 = 1
                        and s.http_status = 200


                )
                
                merge into nhl_data_raw.games.pbp_data t 
                using src s
                    on t.request_key = s.request_key
                when matched and 
                    t.payload <> s.payload 
                    and t.http_status = 200
                    ---setting hard rule so that it doesn't override data from a previous scrape with a blank payload 
                    and s.payload is not null
                    and s.empty_condition_one = false 
                    and s.empty_condition_two = false
 

                then update set 
                    payload = s.payload,
                    update_ts_utc = current_timestamp()
                    
                when not matched then insert (

                    endpoint, 
                    request_key, 
                    http_status, 
                    payload,
                    api_url,
                    ingest_ts_utc,
                    update_ts_utc
                    
                )
                values (

                    s.endpoint, 
                    s.request_key, 
                    s.http_status, 
                    s.payload,
                    s.api_url, 
                    current_timestamp(),
                    null
                )
                
        """)
        spark.catalog.dropTempView("pbp_data_tmp")
        print("Batch successfully inserted into nhl_data_raw.games.pbp_data table")
        print("=" * 100)
    except Exception as e: 
        print(f"Error occured during insert into nhl_data_raw.games.pbp_data table: {e}")

kickoff = not get_games(spark, table_name = "nhl_data_raw.games.pbp_data").isEmpty()
if kickoff:
    print(f"Starting batch scrape process...")
    print("=" * 50)
    batch_size = 500
    max_loops = 75
    api_data, missing_games = [], [] 
    seen_keys = set()
    n = 0
    memory_limit_pct = 80
    rows_written_total = 0
    rate_limiter = RateLim(
                        rps = 2.0,
                        min_rps = 1.0,
                        max_rps = 5.0,
                        step_up = 0.5,
                        step_down = 1.0,
                        eval_every = 50,
                        window_size = 50,
                        max_error_rate = 0.05,
                        max_429_rate = 0.02
        )
    rows_written_total = 0 
    pbp_schema = spark.sql("""select schema_of_json_agg(payload) as json_schema from nhl_data_raw.games.pbp_data where payload is not null and http_status = 200""").first()["json_schema"]
    while True: 

        if n > max_loops: 
            break 
        games = find_games(limit_n = batch_size, raw_schema = pbp_schema)
        if games.isEmpty():
            print(f"No eligible games found, skipping scrape...")
            break 
        buckets = {row["which_game"] for row in games.select("which_game").distinct().collect()}
        final_pass = buckets.issubset({"in play", "last_two", "ended today", "loaded but missing data"})
    
        pbp_data_urls = games.select("api_url").collect()
        total_games = len(pbp_data_urls)
        n_batches = math.ceil(total_games / batch_size)

        for batch_num, batch_rows in enumerate(chunk_list(pbp_data_urls, batch_size), start = 1):
            print(f"Starting batch {n} of {max_loops}...")
            urls = [row["api_url"] for row in batch_rows]
            batch_results = scrape_batch(urls = urls, endpoint = "pbp_data")
            
            for row in batch_results:
                
                request_key = row["request_key"]
                if request_key not in seen_keys:
                    api_data.append(row)
                    seen_keys.add(request_key)

                if row["http_status"] != 200:
                    missing_games.append(row)

                if memory_check(api_data = api_data, memory_limit_pct = memory_limit_pct):
                    print(f"Memory threshold hit at {driver_mem_pct()}%, clearing memory")
                    rows_written_total += flush_api_data(api_data = api_data)
            if api_data:
                rows_written_total += flush_api_data(api_data = api_data)
                seen_keys.clear()
        n += 1

        if final_pass: 
            break 
    print("=" * 50)
    print(f"Done, total rows written = {rows_written_total:,}") if rows_written_total > 0 else print(f"Done")

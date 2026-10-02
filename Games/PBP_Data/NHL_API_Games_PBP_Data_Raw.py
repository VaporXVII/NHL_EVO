import sys
from pathlib import Path

if "__file__" in globals():
    script_dir = Path(__file__).resolve().parent
else:
    script_dir = Path.cwd()

project_root = script_dir.parents[1]
sys.path.insert(0, str(project_root))

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

def find_games(limit_n: int | None = None, raw_schema: str = None) -> DataFrame: 

            #SQL below is used as part of batch processing. Since pbp data is the second largest data set from the NHL API, 
            #attempting to collect data for all games, without doing batch processing, can cause the Serverless compute cluster to run out of memory
    limit_clause = f"limit {limit_n}" if limit_n is not None else ""
    return spark.sql(f"""
                        
            
        with date_param as (

            select 
                from_utc_timestamp(current_timestamp(), '{user_region}')::date as current_run_dte,
                from_utc_timestamp(current_timestamp(), '{user_region}')::timestamp as current_run_time


        )
        ,
        cold_start as (

            select /*+ broadcast (p) */
                (count(*) = 0) as cold_start_ind
            from nhl_evo_raw.games.pbp_raw_data a 
            cross join date_param p 
            where 1 = 1
                and a.payload is not null 
                and a.http_status = 200 
                and from_utc_timestamp(a.ingest_ts_utc, '{user_region}') <= p.current_run_dte 
        )
        ,
        games as (

            select /*+ broadcast */
                a.season,
                a.game_date, 
                a.game_id,
                a.start_time_utc
            from nhl_data_staged.games.schedules a  
            cross join date_param p
            where 1 = 1
                and a.game_type in (1,2,3)
                and lower(a.home_road) = 'home'
                and a.game_date <= p.current_run_dte 

        )
        ,
        games_missing as (

            select /*+ broadcast (p), broadcast (b) */
                a.season, 
                a.game_id,
                b.next_retry_dte
            from games a  
            cross join date_param p
            inner join nhl_data_staged.ops.games_missing_pbp b 
                on a.season = b.season 
                and a.game_id = b.game_id 
            where 1 = 1
                and a.game_date < p.current_run_dte - interval 2 days

        )
        ,
        games_missing_retry as (

            select /*+ broadcast (p) */
                a.*
            from games_missing a
            cross join date_param p
            where 1 = 1
                and a.next_retry_dte = p.current_run_dte 
        )
        ,
        already_loaded as (

            select /*+ broadcast (p), broadcast (b) */
                a.request_key as game_id
            from nhl_evo_raw.games.pbp_raw_data a  
            cross join date_param p 
            left anti join games_missing b  
                on a.request_key = b.game_id 
            where 1 = 1
                ---ensure that game is not in the two day lookback window 
                and from_utc_timestamp(a.ingest_ts_utc, '{user_region}') < p.current_run_time - interval 2 days 

        )
        ,
        games_ended_today as (

            select /*+ broadcast (p) */  
                a.season, 
                a.game_id 
            from games a  
            cross join date_param p 
            left semi join nhl_evo_staged.games.pbp_data c 
                on a.season = c.season 
                and a.game_id = c.game_id 
                and lower(c.event_type) = 'game-end'
            where 1 = 1
                and a.game_date = p.current_run_dte

        )
        ,
        latest_two_day_retry as (

            select /*+ broadcast (p) */
                a.game_id,
                (max(from_utc_timestamp(b.insert_dte, '{user_region}')::date) = max(p.current_run_dte))::boolean as latest_retry_today_ind
            from games a  
            cross join date_param p
            inner join nhl_evo_staged.games.pbp_data b 
                on a.season = b.season
                and a.game_id = b.game_id
            where 1 = 1
                and a.game_date <> p.current_run_dte
                and a.game_date >= p.current_run_dte - interval 2 days
                and b.period = 1
                and b.event_type in ('game-start', 'faceoff') 
            group by all 

        )
        , 
        game_status as (

        select /*+ broadcast (b), broadcast (c), broadcast (d), broadcast (e), broadcast (f), broadcast (cs), broadcast (p) */
            a.*, 
            cs.cold_start_ind,
            ---cold start takes precedent over all others 
            case when cs.cold_start_ind = true then 'cold start'
                ---check to see if game is eligible for missing retry (defined as last_attempt_dte + 15 days)
                when d.game_id is not null then 'missing retry'
                ---check to see if game is in missing list entirely 
                when c.game_id is not null then 'missing pbp data'
                ---check to see if game was played before the 2 day lookback window (valid because if it's missing pbp data then it won't be in the missing_pbp_data table)
                when a.game_date < p.current_run_dte - interval 2 days and b.game_id is not null then 'already loaded' 
                ---check to see if game was played within the last two days to capture most relevant record 
                when a.game_date <> p.current_run_dte and a.game_date >= p.current_run_dte - interval 2 days and f.latest_retry_today_ind = false then 'last two'
                ---check to see if game has ended today, will pause scrape until next day 
                when e.game_id is not null then 'ended today' 
                ---check to see if game is in play today 
                when a.game_date = current_date() and p.current_run_time >= timestampadd(minute, 15, from_utc_timestamp(a.start_time_utc, '{user_region}')) then 'in play'
                ---check to see if game is in play today but not yet started
                when a.game_date = current_date() and from_utc_timestamp(p.current_run_time, '{user_region}') < from_utc_timestamp(a.start_time_utc, '{user_region}') then 'not started'
                else 'unknown'
                end as which_game 
        from games a   
        left join already_loaded b  
            on a.game_id = b.game_id
        left join games_missing c
            on a.season = c.season
            and a.game_id = c.game_id 
        left join games_missing_retry d  
            on a.season = d.season 
            and a.game_id = d.game_id 
        left join games_ended_today e
            on a.game_id = e.game_id 
        left join latest_two_day_retry f 
            on a.game_id = f.game_id 
        cross join cold_start cs 
        cross join date_param p 

        )

        select distinct
            a.*, 
            date_format(a.start_time_utc, 'hh:mm a') as game_start_time_cst,
            concat('https://api-web.nhle.com/v1/gamecenter/', a.game_id, '/play-by-play') as api_url
        from game_status a
        where 1 = 1
            and lower(a.which_game) not in ('not started', 'already loaded', 'missing pbp data', 'unknown')
        order by a.game_date desc, game_start_time_cst, a.game_id
        {limit_clause}
                                
""")

def update_missing_games(batch_data: DataFrame) -> None:

    try: 
        
        batch_data.createOrReplaceTempView("pbp_data_missing_tmp")
        spark.sql(f"""
                  
                  
                with date_param as (

                    select from_utc_timestamp(current_timestamp(), '{user_region}')::date as current_run_dte
                ) 
                ,
                src as (
                    
                    ---need to grab the season associated with the game that is going to be inserted into games_missing_pbp table
                    select /*+ broadcast (p), broadcast (b) */ distinct 
                        a.season,
                        b.request_key as game_id 
                    from nhl_data_staged.games.schedules a
                    cross join date_param p 
                    inner join pbp_data_missing_tmp b 
                        on a.game_id = b.request_key
                    where 1 = 1
                        and a.game_type in (1,2,3)
                        and a.game_date <= p.current_run_dte

                )
                
                merge into nhl_data_staged.ops.games_missing_pbp t 
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
        print("Batch successfully inserted into nhl_data_staged.ops.games_missing_pbp table")
        print("=" * 100)
    except Exception as e: 
        print(f"Error occured during insert into nhl_data_staged.ops.games_missing_pbp table: {e}")

def merge_insert_found(batch_data: DataFrame) -> None:

    try: 
        batch_data.createOrReplaceTempView("pbp_data_tmp")
        spark.sql("""
                
                with src as (

                    select 
                        s.*
                    from pbp_data_tmp s
                    where 1 = 1
                        and s.http_status = 200


                )
            
                
                merge into nhl_data_raw.games.pbp_data t 
                using src s
                    on t.request_key = s.request_key
                when matched and (

                    t.payload <> s.payload 
                    and t.http_status = 200
                    ---setting hard rule so that it doesn't override data from a previous scrape with a blank payload 
                    and s.payload is not null
                    and get_json_object(s.payload,'$.plays') <> '[]'
 
                )
                
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

def flush_api_data(api_data: list) -> int:

    if not api_data:
        return 0 
    
    else: 
        api_data_df = spark.createDataFrame(api_data)
        
        #section below eliminated as result of NHL_EVO streaming tables 
        #================================================================================================================================================
        #take sample of payload schemas
        #since batches are limited to 100 games regardless of number of games in play that day, using schema_of_json_agg to determine what pbp schema is
        # json_schema = (
                
                
        #         api_data_df
        #         .selectExpr("schema_of_json_agg(payload) as json_schema")
        #         .first()["json_schema"]
        # )
        
        #add column that represents the sample schema found above
        # api_data_df = (

        #         api_data_df 
        #         .withColumn("parsed_json", f.from_json(f.col("payload"), json_schema))
        # )

        #filter down to payloads that are not empty
        # non_empty_payloads = (

        #         api_data_df 
        #         .filter(f.size(f.col("parsed_json.plays")) > 0)
        #         .drop("parsed_json")
        # )

        #filter down to paylaods that are empty
        # empty_payloads = (

        #         api_data_df 
        #         .filter(f.size(f.col("parsed_json.plays")) == 0)
        #         .drop("parsed_json")
        # )

        # if not empty_payloads.isEmpty():
        #         update_missing_games(batch_data = empty_payloads)

        # if not non_empty_payloads.isEmpty():
        #         merge_insert_found(batch_data = non_empty_paylods)
        
        #================================================================================================================================================
        if not api_data_df.isEmpty():
                merge_insert_found(batch_data = api_data_df)
        
        row_cnt = len(api_data)

        api_data.clear()
        gc.collect()

    return row_cnt

kickoff = not get_games(spark, table_name = "nhl_data_raw.games.pbp_data").isEmpty()
if kickoff: 
    print("Starting batch scrape process...")
    print("=" * 50)
    batch_size = 100
    eligible_count = find_games(limit_n = 10_000_000).count()
    # One additional loop confirms that no eligible games remain.
    max_loops = math.ceil(eligible_count / batch_size) + 1
    api_data = []
    seen_keys = set()
    previous_game_ids = None
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

    # pbp_schema = spark.sql(f"""
                             
    #     select 
    #         schema_of_json_agg(payload) as json_schema
    #     from nhl_data_raw.games.pbp_data
    #     where 1 = 1
    #         and http_status = 200
    #         and payload is not null
    #         ---only consider schemas that are within the last year vs all time
    #         and from_utc_timestamp(ingest_ts_utc, '{user_region}')::date >= date_sub(current_date(), 365)
            
    # """).first()["json_schema"]

    while n < max_loops:

        games = find_games(limit_n = batch_size)

        # Collect the small batch once instead of running count(), isEmpty(), and multiple collect() operations.
        game_rows = (
            
            games
            .select(
            "game_id",
            "which_game",
            "api_url"
            )

        ).collect()

        game_count = len(game_rows)
        if game_count == 0:
            print("No eligible games found, skipping scrape...")
            break
        buckets = {row["which_game"] for row in game_rows}
        current_game_ids = {row["game_id"] for row in game_rows}

        # Prevent the same batch from being scraped repeatedly when find_games() fails to make progress.
        if current_game_ids == previous_game_ids:
            raise RuntimeError("No progress detected: find_games returned the same batch twice")
            
        previous_game_ids = current_game_ids

        final_pass = ("cold start" not in buckets and "missing pbp data" not in buckets and game_count < batch_size)
        pbp_data_urls = [row["api_url"] for row in game_rows]
        print(f"Starting batch {n + 1} of {max_loops} " f"({game_count:,} games)...")

        for urls in chunk_list(pbp_data_urls, batch_size):

            batch_results = scrape_batch(urls = list(urls), endpoint = "pbp", push_to_s3 = True)
            for row in batch_results:

                request_key = row["request_key"]

                if request_key not in seen_keys:
                    api_data.append(row)
                    seen_keys.add(request_key)

                if memory_check(api_data = api_data, memory_limit_pct = memory_limit_pct):
                    print(f"Memory threshold hit at " f"{driver_mem_pct()}%, clearing memory")
                    rows_written_total += flush_api_data(api_data = api_data)

            if api_data:
               rows_written_total += flush_api_data(api_data = api_data)

            seen_keys.clear()

        n += 1

        if final_pass:
            break

    print("=" * 50)
    print(f"Done, total rows written = {rows_written_total:,}" if rows_written_total > 0 else "Done")
else: 
    print("=" * 50)
    print(f"No games in play today or within the last two days, skipping...")

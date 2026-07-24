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

    #SQL below is used as part of batch processing. Since shift data is considerably larger than any other data from the NHL API, 
    #attempting to collect data for all games, without doing batch processing, can cause the Serverless compute cluster to run out of memory
    return spark.sql(f"""
                

                    with date_param as (

                        select 
                            from_utc_timestamp(current_timestamp(), '{user_region}')::date as current_run_dte,
                            current_timestamp() as current_run_time
             


                    ) 
                    , 
                    cold_start_check as (

                        ---check to see if any rows have been inserted into nhl_data_staged.games.shift_data table, if none exist then set cold_start_ind = true
                        ---using where to filter table down to avoid excess scanning
                        select distinct 
                            request_key as game_id
                        from nhl_data_raw.games.shift_data 
                        
                    )
                    ,
                    games as (
                        
                        ---pull list of all games and various information, limiting to games that occured in 20102011 season or later
                        ---since NHL didn't start collecting shift data until then 
                        select /*+ broadcast (p), broadcast (c), broadcast (b) */ 
                            a.season,
                            a.game_id,
                            a.game_date,
                            a.start_time_utc,
                            ---check to see if the game has been loaded into the bronze layer (i.e. is a cold start needed)
                            ---accounts for if cold start takes place on date where games are in play and whether or not the game is in play yet
                            ---using 25 minute delay from start time because shift data takes a little while longer to populate compared to PBP data 
                            (c.game_id is null
                                and (
                                a.game_date < p.current_run_dte
                                or 
                                (a.game_date = p.current_run_dte and from_utc_timestamp(p.current_run_time, '{user_region}') >= from_utc_timestamp(a.start_time_utc, '{user_region}') + interval 25 minutes)
                                )
                            )::boolean as cold_start_ind,
                            ---check to see if current timestamp is at least 15 minutes after the game's scheduled start time
                            (a.game_date = p.current_run_dte and from_utc_timestamp(p.current_run_time, '{user_region}') >= from_utc_timestamp(a.start_time_utc, '{user_region}') + interval 25 minutes)::boolean as game_in_play_ind,
                            ---check to see if the game was played in the prior two days
                            (a.game_date between date_sub(p.current_run_dte, 2) and date_sub(p.current_run_dte, 1))::boolean as game_prior_two_ind,
                            ---check to see if the game is part of the games_missing_shift table and is eligible for retry on the current date
                            (b.game_id is not null)::boolean as missing_game_ind,
                            b.next_retry_dte
                        from nhl_data_staged.games.schedules a
                        cross join date_param p
                        left join cold_start_check c 
                            on a.game_id = c.game_id
                        left join nhl_data_staged.ops.games_missing_shift b 
                            on a.season = b.season 
                            and a.game_id = b.game_id 
                            and p.current_run_dte >= b.next_retry_dte
                        where 1 = 1
                            and a.season >= 20102011
                            and a.game_type in (2,3)
                            and lower(a.home_road) = 'home'
                            and a.game_date <= p.current_run_dte

                    )
                    , 
                    pbp_game_status as (

                        ---check to see what the status of the game is based on the play by play data (most reliable method)
                        ---further research found that some games in the 20092010 season didn't include a 'game-end' event_type
                        ---therefore setting those games as having ended manually
                        select
                            game_id,
                            game_date,
                            coalesce(
                                    max(1) filter (where lower(event_type) = 'game-end'), 
                                    0
                                    ) as game_ended_ind
                        from nhl_data_staged.games.pbp_data
                        where 1 = 1
                            and season >= 20102011
                            and period >= 3
                        group by 
                            game_id,
                            game_date

                    )
                    ,
                    games_ended_today as (

                        ---check to see which games from the games list that were scheduled for the current date have ended
                        select /*+ broadcast(b), broadcast(p) */ 
                            a.season,
                            a.game_id,
                            a.game_date,
                            a.start_time_utc
                        from games a  
                        inner join pbp_game_status b
                            on a.game_id = b.game_id
                            and a.game_date = b.game_date 
                            and b.game_ended_ind = 1
                        cross join date_param p
                        where 1 = 1
                            and a.game_date = p.current_run_dte
                            and a.cold_start_ind = false 
                            and a.missing_game_ind = false 
                            
                    )
                    ,
                    games_in_play as (

                        ---check to see which games from the games list that were scheduled for the current date are in play
                        ---based on the game start time + 15 minute window (NHL games typically don't drop the puck until about 15 minutes after)
                        select /*+ broadcast (b) */ 
                            a.season,
                            a.game_id,
                            a.game_date,
                            a.start_time_utc 
                        from games a 
                        left anti join games_ended_today b 
                            on a.season = b.season 
                            and a.game_id = b.game_id 
                            and a.game_date = b.game_date 
                        where 1 = 1
                            and a.cold_start_ind = false 
                            and a.missing_game_ind = false 
                            and a.game_in_play_ind = true 
                            
                    )
                    ,
                    games_prior_two as (

                        select
                            a.season,
                            a.game_id,
                            a.game_date,
                            a.start_time_utc
                        from games a 
                        where 1 = 1
                            and a.cold_start_ind = false 
                            and a.missing_game_ind = false
                            and a.game_prior_two_ind = true  
                            
                    )
                    ,
                    final_games as (


                        select 
                            "in play" as which_game,
                            season,
                            game_id,
                            game_date,
                            start_time_utc
                        from games_in_play 
                        union all 
                        select 
                            "ended today" as which_game,
                            season,
                            game_id,
                            game_date,
                            start_time_utc
                        from games_ended_today 
                        union all 
                        select 
                            "last two" as which_game,
                            season,
                            game_id,
                            game_date,
                            start_time_utc
                        from games_prior_two 
                        union all 
                        select 
                            "missing shift data" as which_game,
                            season,
                            game_id,
                            game_date,
                            start_time_utc
                        from games a 
                        where 1 = 1 
                            and cold_start_ind = false 
                            and missing_game_ind = true 
                        union all 
                        select /*+ broadcast (p) */
                            "cold start" as which_game,
                            season,
                            game_id,
                            game_date,
                            start_time_utc
                        from games a  
                        cross join date_param p
                        where 1 = 1
                            and cold_start_ind = true 
                            and (
                                a.game_date < p.current_run_dte
                                or a.game_in_play_ind = true
                            )

                    )
                    select 
                        
                        a.which_game,
                        a.game_id,
                        a.game_date,
                        a.start_time_utc,
                        date_format(from_utc_timestamp(start_time_utc, '{user_region}'), 'hh:mm a') as game_start_time_cst,
                        concat('https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId=', game_id) as api_url
                    from final_games a 
                    order by a.game_date, a.game_id
                    limit {limit_n}
                  
    """)

def update_missing_games(batch_data: DataFrame) -> None:

    try: 
        batch_data.createOrReplaceTempView("shift_data_missing_tmp")
        spark.sql(f"""
                  
                  
                with date_param as (

                    select from_utc_timestamp(current_timestamp(), '{user_region}')::date as current_run_dte
                ) 
                ,
                src as (
                    
                    ---need to grab the season associated with the game that is going to be inserted into games_missing_shift table
                    select /*+ broadcast (p), broadcast (b) */ distinct 
                        a.season,
                        b.request_key as game_id 
                    from nhl_data_staged.games.schedules a
                    cross join date_param p 
                    inner join shift_data_missing_tmp b 
                        on a.game_id = b.request_key
                    where 1 = 1
                        and a.game_type in (2,3)
                        and a.game_date <= p.current_run_dte

                )
                
                merge into nhl_data_staged.ops.games_missing_shift t 
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
        spark.catalog.dropTempView("shift_data_missing_tmp")
        print("Batch successfully inserted into nhl_data_staged.ops.games_missing_shift table")
        print("=" * 100)
    except Exception as e: 
        print(f"Error occured during insert into nhl_data_staged.ops.games_missing_shift table: {e}")

def merge_insert_found(batch_data: DataFrame) -> None:

    try: 
        batch_data.createOrReplaceTempView("shift_data_tmp")
        spark.sql("""
                
                with src as (

                    select 
                        s.*,
                        from_json(s.payload, 'STRUCT<data: ARRAY<STRING>, total: INT>') as payload_json
                    from shift_data_tmp s
                    where 1 = 1
                        and s.http_status = 200


                )
                
                merge into nhl_data_raw.games.shift_data t 
                using src s
                    on t.request_key = s.request_key
                when matched and (

                    t.payload <> s.payload 
                    and t.http_status = 200 
                    ---shift data api endpoint can be very inconsistent, sometimes data will be there, sometimes not
                    ---setting hard rule so that it doesn't override good data from a previous scrape with an empty payload 
                    and s.payload_json.total is not null 
                    and s.payload_json.total > 0
                    and size(s.payload_json.data) > 0
                
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
        spark.catalog.dropTempView("shift_data_tmp")
        print("Batch successfully inserted into nhl_data_raw.games.shift_data table")
        print("=" * 100)
    except Exception as e: 
        print(f"Error occured during insert into nhl_data_raw.games.shift_data table: {e}")

def flush_api_data(api_data: list) -> int:

    if not api_data:
        return 0 
    
    else: 
        api_data_df = spark.createDataFrame(api_data)

        #take sample of payload schemas
        json_schema = (

                api_data_df
                .sample(fraction = 0.07, seed = 17)
                .selectExpr("schema_of_json_agg(payload) as json_schema")
                .first()["json_schema"]
        )
        
        #add column that represents the sample schema found above 
        api_data_df = (

                api_data_df 
                .withColumn("parsed_json", f.from_json(f.col("payload"), json_schema))
        )

        #filter down to payloads that are not empty
        non_empty_payloads = (

                api_data_df 
                .filter(
                        (f.col("parsed_json.total") > 0) 
                        | (f.size(f.col("parsed_json.data")) > 0)
                )
                .drop("parsed_json") 
                
        )

        #filter down to payloads that are empty
        empty_payloads = (

                api_data_df 
                .filter(
                        (f.col("parsed_json.total") == 0) 
                        | (f.size(f.col("parsed_json.data")) == 0)
                )
                .drop("parsed_json")
        )

        if not non_empty_payloads.isEmpty():
                merge_insert_found(batch_data = non_empty_payloads)

        if not empty_payloads.isEmpty():
                update_missing_games(batch_data = empty_payloads)


 
        row_cnt = len(api_data)

        api_data.clear()
        gc.collect()

    return row_cnt

kickoff = not get_games(spark, table_name = "nhl_data_raw.games.shift_data").isEmpty()
kickoff = True
if kickoff:
    print(f"Starting batch scrape process...")
    print("=" * 50)
    batch_size = 100
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
    shift_schema = spark.sql("select schema_of_json_agg(payload) as json_schema from nhl_data_raw.games.shift_data where http_status = 200 and payload is not null").first()["json_schema"]
    while True: 
        
        if n > max_loops:
            break
        games = find_games(limit_n = batch_size, raw_schema = shift_schema)
        game_count = games.count()
        if games.isEmpty():
            print(f"No eligible games found, skipping scrape...")
            break 
        buckets = {row["which_game"] for row in games.select("which_game").distinct().collect()}
        final_pass = (
                buckets.issubset({"in play", "last two", "ended today", "missing shift data"})
                and game_count < batch_size 
        )

        shift_data_urls = games.select("api_url").collect()
        total_games = len(shift_data_urls)
        n_batches = math.ceil(total_games / batch_size)

        for batch_num, batch_rows in enumerate(chunk_list(shift_data_urls, batch_size), start = 1):
            print(f"Starting batch {n} of {max_loops}...")
            urls = [row["api_url"] for row in batch_rows]
            batch_results = scrape_batch(urls = urls, endpoint = "shift_data")
            
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
    print(f"Done, total rows written = {rows_written_total:,}") if rows_written_total > 0 else print("Done")

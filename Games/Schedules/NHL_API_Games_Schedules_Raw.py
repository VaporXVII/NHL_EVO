import sys 
username = spark.sql("select current_user()").first()[0]
sys.path.append(f"/Workspace/Users/{username}/NHL_Pipeline")

from pyspark.sql import SparkSession
from pyspark.sql import functions as f, types as t 
from delta.tables import DeltaTable
from zoneinfo import ZoneInfo
import requests, json, time, random, threading, math
import datetime as dt
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
from pipeline_funcs.api_utils import *
from pipeline_funcs.user_utc_region import region_return 
from pipeline_funcs.table_maint import run_table_maint

user_region = region_return()
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", f'{user_region}')
central_timezone = ZoneInfo(f"{user_region}")

def now_central():
    return dt.datetime.now(central_timezone)

def today_central():
    return now_central().date()

def get_dates(start_date: dt.date, end_date: dt.date) -> list:

    if start_date and end_date:
        
        return set(
            (start_date + dt.timedelta(days = i)).strftime("%Y-%m-%d")
            for i in range(0, (end_date - start_date).days + 1, 7)
        )

    return set()

def to_date(x):
    return x.date() if isinstance(x, dt.datetime) else x

schedules = spark.sql(f"""

                    select 
                        max(season)::integer as season,
                        max(pre_season_start_date)::date as ps_start_date,
                        max(regular_season_start_date)::date as rs_start_date,
                        max(regular_season_end_date)::date as rs_end_date,
                        max(game_date) filter (where game_type = 2)::date as rs_last_game_date,
                        max(playoff_end_date)::date as playoff_end_date,
                        (max(regular_season_end_date) >= date_sub(max(game_date) filter (where game_type = 2), 1))::boolean as sched_fully_loaded
                    from nhl_data_staged.games.schedules a 
                    where 1 = 1
                        and a.game_type between 1 and 3      
                        and a.game_date >= date_sub(from_utc_timestamp(current_timestamp(), '{user_region}')::date, 365)

    """)
missing_dates = spark.sql(f"""
    
                    with dates as (
                    
                        select 
                            min(request_key)::date as start_date,
                            max(request_key)::date as end_date 
                        from nhl_data_raw.games.schedules
                        where 1 = 1

                    )
                    , 
                    dates_list as (

                    ---producing a list of dates starting from the start_date to the end_date, spaced out in 7 day increments per NHL API response
                        select explode(sequence(start_date, end_date, interval 7 days)) as game_date 
                        from dates 

                    )
                    
                    
                    select 
                        a.game_date 
                    from dates_list a 
                    left anti join nhl_data_raw.games.schedules b
                        on a.game_date = b.request_key
                    left anti join nhl_data_staged.games.schedules c 
                        on a.game_date = c.game_date

    """)
ready = True
missing_ready = not missing_dates.isEmpty()

if ready: 
    #today helps get the date in 'America/Chicago' (or whichever region name the user specified in get_utc_region module) formatting so that the date is set properly
    today = today_central()
    schedule_info = (

            schedules
            .select("ps_start_date", "rs_start_date", "rs_end_date", "rs_last_game_date", "playoff_end_date", "sched_fully_loaded")
            .first()
    )
    pre_season_start_date = to_date(schedule_info["ps_start_date"])
    rs_start_date = to_date(schedule_info["rs_start_date"])
    rs_end_date = to_date(schedule_info["rs_end_date"])
    rs_last_game_date = to_date(schedule_info["rs_last_game_date"])
    playoff_end_date = to_date(schedule_info["playoff_end_date"])
    schedule_fully_loaded = schedule_info["sched_fully_loaded"]
    gap_window = 30
    #pipeline has never been ran and user runs script for the first time
    if rs_end_date is None:
        start_dt = dt.date(2008, 10, 1)

        #user runs script before the current season has ended 
        if today.month <= 6:
            end_dt = dt.date(today.year, 6, 30)
            scrape_plan = "init_scrape_within_season"
        #user runs script after the current season has ended and looking ahead to set end date 
        #using next regular season
        else: 
            end_dt = dt.date(today.year + 1, 6, 30)
            scrape_plan = "init_scrape_outside_season"

    #pipeline has been run before and is being ran again on the same day as the last game of the regular season 
    #(just in case playoff schedules have been established and loaded)
    elif (rs_start_date is not None and rs_start_date <= today <= rs_last_game_date):

        start_dt = rs_end_date
        end_dt = playoff_end_date
        scrape_plan = "sec_scrape_rs_last_game"
    
    #pipeline has been run before but is being ran before end of regular season 
    elif (rs_start_date is not None and rs_start_date <= today < rs_end_date):
        
        start_dt = None
        end_dt = None 
    
    #outlier scenario in case NHL doesn't have playoff start date set when the regular season schedule is released 
    elif (rs_start_date is not None and rs_end_date is not None and playoff_end_date is None and 1 <= today.month <= 3 and today.day in (1, 15, 28)):

        start_dt = rs_start_date
        end_dt = rs_end_date
        scrape_plan = "sec_scrape_playoff_check_within_rs"

    #pipeline has been ran before and today is on or after the end of the regular season
    elif (rs_end_date is not None and playoff_end_date is not None and rs_end_date <= today <= playoff_end_date):

        start_dt = rs_end_date 
        end_dt = playoff_end_date 
        scrape_plan = "sec_scrape_playoffs_after_rs_end"
    
    #NHL may release a handful of regular season games before releasing full regular season schedule AND prior to releasing pre-season schedule 
    elif (rs_start_date is not None and today < rs_start_date and pre_season_start_date.year < rs_start_date.year and not schedule_fully_loaded):

        start_dt = rs_start_date 
        end_dt = playoff_end_date
        scrape_plan = f"sec_scrape_after_initial_schedule_release"

    #user has run script before and there's at least a 30 day gap between the end of the prior season 
    #and the current date.
    #start date is set to the current_year/9/1 
    #end date set to current_year + 1/6/30
    elif playoff_end_date is not None and (today - playoff_end_date).days >= gap_window:
        
        #discovered that NHL releases only a handful of regular season games initially, then releases remainder of schedule afterwards
        #setting the end_dt in this instance to be mid-october that way a scrape isn't ran to capture a whole season's worth of data
        #when it's not there
        start_dt = dt.date(playoff_end_date.year, 9, 1)
        end_dt = dt.date(playoff_end_date.year, 10, 15)
        scrape_plan = f"sec_scrape_at_least_{gap_window}_days_after_playoffs_ended"

    #scraping schedules within 30 days of playoffs ending 
    elif playoff_end_date is not None and (today - playoff_end_date).days < gap_window:

        start_dt = None 
        end_dt = None 
        scrape_plan = f"sec_scrape_within_{gap_window}_days_after_playoffs"

    else: 
        start_dt = None
        end_dt = None
        scrape_plan = "issue_found"


    all_dates = get_dates(start_dt, end_dt)

scrape_ready = False
if missing_ready: 
    missing_dates = missing_dates.select("game_date").collect()
    missing_dates_set = {str(date[0]) for date in missing_dates}
else: 
    missing_dates_set = {}
all_dates_set = all_dates.union(missing_dates_set)
game_urls = sorted(set(f"https://api-web.nhle.com/v1/schedule/{d}" for d in all_dates_set))
if game_urls: 
    scrape_ready = True

insert_ready = False
if scrape_ready: 
    
    rate_limiter = RateLim(rps = 2.0)
    with ThreadPoolExecutor(max_workers = 5) as executor:
        _rate_lock = threading.Lock()
        api_data = []
        futures = [
            executor.submit(
                call_api,
                url,
                rate_limiter,
                "schedule"
            )
            for url in game_urls
        ]
        completed = 0 
        for future in concurrent.futures.as_completed(futures): 
            result = future.result()
            if result is not None:
                api_data.append(result)
                completed += 1 
                if completed % 50 == 0:
                    print(f"{completed} / {len(game_urls)} schedule urls fetched.")
        print(f"{completed} / {len(game_urls)} schedule urls fetched.")
    
    insert_ready = bool(api_data)

if insert_ready:

    schedules_schema = t.StructType([

    t.StructField("endpoint", t.StringType(), False),
    t.StructField("request_key", t.StringType(), False),
    t.StructField("http_status", t.IntegerType(), False),
    t.StructField("payload", t.StringType(), True),
    t.StructField("api_url", t.StringType(), False),   
    
    ])
    schedules_df = spark.createDataFrame(api_data, schema = schedules_schema)
    if not schedules_df.isEmpty():
        
        row_sample = max(1, math.ceil(len(api_data) * 0.25))
        schedules_df.createOrReplaceTempView("schedules_insert_tmp")
        sched_schema = spark.sql(f"""
                                
                        with sample_payloads as (

                            select *
                            from schedules_insert_tmp
                            tablesample ({row_sample} rows)
                        
                        )
                        select 
                            schema_of_json_agg(payload) as json_schema 
                        from schedules_insert_tmp 
                        where http_status = 200 and payload is not null
                        
                    """).first()["json_schema"]
        spark.sql(f"""
                      
                with src as (

                    select 
                        s.endpoint,
                        s.request_key,
                        s.http_status,
                        s.payload,
                        s.api_url,
                        '{scrape_plan}' as scrape_plan,
                        from_json(s.payload, '{sched_schema}') as payload_json
                    from schedules_insert_tmp s
                    where 1 = 1
                        and s.http_status = 200
                        and s.payload is not null 
                        and s.request_key is not null

                )

                merge into nhl_data_raw.games.schedules t 
                using src s 
                    on t.request_key = s.request_key 
                    and t.api_url = s.api_url 

                when matched and (
                    
                    t.payload <> s.payload 
                    and s.payload_json.numberOfGames > 0 
                    and size(s.payload_json.gameWeek) > 0
                )

                then update set 

                    payload = s.payload,
                    http_status = s.http_status,
                    ingest_ts_utc = current_timestamp(),
                    scrape_plan = s.scrape_plan 

                when not matched and (

                    ---ensuring that data coming through the API actually contains games and not an empty API call
                    s.payload_json.numberOfGames > 0 
                    and size(s.payload_json.gameWeek) > 0

                )
                
                then insert (

                    endpoint, 
                    http_status,
                    request_key,
                    api_url,
                    payload,
                    ingest_ts_utc,
                    scrape_plan

                )
                values (

                    s.endpoint,
                    s.http_status,
                    s.request_key,
                    s.api_url,
                    s.payload,
                    current_timestamp(), 
                    s.scrape_plan
                )
                ;


    """)

    spark.catalog.dropTempView("schedules_insert_tmp")
    print(f"Schedules data successfully loaded into nhl_data_raw.games.schedules table")
    run_table_maint(spark, "nhl_data_raw.games.schedules")
else: 
    print(f"No new data to insert into nhl_data_raw.games.schedules, skipping insert")

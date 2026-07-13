from pyspark.sql import SparkSession
from pyspark.sql import functions as f, types as t 
from delta.tables import DeltaTable
from zoneinfo import ZoneInfo
import requests, json, time, random, threading
import datetime as dt
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "America/Chicago")
central_timezone = ZoneInfo("America/Chicago")

def now_central():
    return dt.datetime.now(central_timezone)

def today_central():
    return now_central().date()
#today = today_central()

def get_dates(start_date: dt.date, end_date: dt.date) -> list:

    if start_date and end_date:
        
        return set(
            (start_date + dt.timedelta(days = i)).strftime("%Y-%m-%d")
            for i in range(0, (end_date - start_date).days + 1, 7)
        )

    return set()

def to_date(x):
    return x.date() if isinstance(x, dt.datetime) else x

def throttle(rps:float = 2.0, next_allowed: float = 0.0):

    
    with _rate_lock: 
        now = time.monotonic()
        wait = max(0.0, next_allowed - now)
        _next_allowed = max(next_allowed, now) + (1.0 / rps)
    
    if wait: 
        time.sleep(wait)

def call_api(url:str, rps:float = 2.0, max_attempts: int = 3, time_out:int = 10):

    for attempt in range(max_attempts):

        try: 
            throttle(rps = rps)
            response = requests.get(url, timeout = time_out)
            last_status = response.status_code 
            if response.status_code == 200:
                
                return {
                    "endpoint": "schedule",
                    "request_key": url.split('/')[-1],
                    "http_status": int(last_status),
                    "payload": json.dumps(response.json()),
                    "api_url": url, 
                    "scrape_plan": date_plan

                    }
            
            if response.status_code in (429, 502, 503, 504):
                time.sleep((2 ** attempt) + random.random())
                continue 

            return {
                    "endpoint": "schedule",
                    "request_key": url.split('/')[-1],
                    "http_status": int(last_status),
                    "payload": None,
                    "api_url": url, 
                    "scrape_plan": date_plan

                    }
                
        except requests.RequestException:
            time.sleep((2 ** attempt) + random.random())
           
    return None

schedules = spark.sql(f"""

                      select 
                            max(season)::integer as season,
                            max(pre_season_start_date)::date as ps_start_date,
                            max(regular_season_start_date)::date as rs_start_date,
                            max(regular_season_end_date)::date as rs_end_date,
                            max(game_date) filter (where game_type = 2)::date as rs_last_game_date,
                            max(playoff_end_date)::date as playoff_end_date
                        from nhl_data_staged.games.schedules a 
                        where 1 = 1
                            and a.game_type between 1 and 3        


                    """)
missing_dates = spark.sql("""
    
                        with dates as (
                        
                        select min(request_key)::date as start_date,
                            max(request_key)::date as end_date 
                        from nhl_data_raw.games.schedules
                        where 1 = 1

                        )
                        , 
                        dates_list as (


                        select explode(
                                    sequence(start_date, end_date, interval 7 days)
                        ) as game_date 
                        from dates 

                        )
                        
                        select 
                            a.game_date 
                        from dates_list a 
                        left anti join nhl_data_raw.games.schedules b
                            on a.game_date = b.request_key

                    """
)
ready = True
missing_ready = not missing_dates.isEmpty()

if ready: 
    today = today_central()
    schedule_info = (
    schedules
    .agg(f.max("ps_start_date").alias("ps_start_date"), 
        f.max("rs_start_date").alias("rs_start_date"),
        f.max("rs_end_date").alias("rs_end_date"),
        f.max("rs_last_game_date").alias("rs_last_game_date"),
        f.max("playoff_end_date").alias("playoff_end_date")
        )
    .first()
    )
    pre_season_start_date = to_date(schedule_info["ps_start_date"])
    rs_start_date = to_date(schedule_info["rs_start_date"])
    rs_end_date = to_date(schedule_info["rs_end_date"])
    rs_last_game_date = to_date(schedule_info["rs_last_game_date"])
    playoff_end_date = to_date(schedule_info["playoff_end_date"])
    gap_window = 30
    #pipeline has never been ran and user runs script for the first time
    if rs_end_date is None:
        start_dt = dt.date(2008, 10, 1)

        #user runs script before the current season has ended 
        if today.month <= 6:
            end_dt = dt.date(today.year, 6, 30)
            date_plan = "init_scrape_within_season"
        #user runs script after the current season has ended and looking ahead to set end date 
        #using next regular season
        else: 
            end_dt = dt.date(today.year + 1, 6, 30)
            date_plan = "init_scrape_outside_season"

    #pipeline has been run before and is being ran again on the same day as the last game of the regular season 
    #(just in case playoff schedules have been established and loaded)
    elif (rs_start_date is not None and rs_start_date <= today <= rs_last_game_date):

        print("elif1")
        start_dt = rs_end_date
        end_dt = playoff_end_date
        date_plan = "sec_scrape_rs_last_game"
    
    #pipeline has been run before but is being ran before end of regular season 
    elif (rs_start_date is not None and rs_start_date <= today < rs_end_date):
        
        print("elif2")
        start_dt = None
        end_dt = None 
    
    #outlier scenario in case NHL doesn't have playoff start date set when the regular season schedule is released 
    elif (rs_start_date is not None and rs_end_date is not None and playoff_end_date is None and 1 <= today.month <= 3 and today.day in (1, 15, 28)):

        print("elif3")
        start_dt = rs_start_date
        end_dt = rs_end_date
        date_plan = "sec_scrape_playoff_check_within_rs"

    #pipeline has been ran before and today is on or after the end of the regular season
    elif (rs_end_date is not None and playoff_end_date is not None and rs_end_date <= today <= playoff_end_date):

        print("elif4")
        start_dt = rs_end_date 
        end_dt = playoff_end_date 
        date_plan = "sec_scrape_playoffs_after_rs_end"
    
    #user has run script before and there's at least a 30 day gap between the end of the prior season 
    #and the current date.
    #start date is set to the current_year/9/1 
    #end date set to current_year + 1/6/30
    elif playoff_end_date is not None and (today - playoff_end_date).days >= gap_window:
        
        print("elif5")
        start_dt = dt.date(playoff_end_date.year, 9, 1)
        end_dt = dt.date(playoff_end_date.year + 1, 6, 30)
        date_plan = f"sec_scrape_at_least_{gap_window}_days_after_playoffs_ended"

    #scraping schedules within 30 days of playoffs ending 
    elif playoff_end_date is not None and (today - playoff_end_date).days < gap_window:

        print("elif6")
        start_dt = None 
        end_dt = None 
        date_plan = f"sec_scrape_within_{gap_window}_days_after_playoffs"

    else: 
        start_dt = None
        end_dt = None
        date_plan = "issue_found"


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
    
    with ThreadPoolExecutor(max_workers = 5) as executor:
        _rate_lock = threading.Lock()
        api_data = []
        futures = [executor.submit(call_api, url) for url in game_urls]
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
    t.StructField("scrape_plan", t.StringType(), False)
    
    ])
    schedules_df = spark.createDataFrame(api_data, schema = schedules_schema)
    if not schedules_df.isEmpty():
        
        schedules_df.createOrReplaceTempView("schedules_insert_tmp")

        spark.sql("""
                      
                with src as (

                    select 
                        s.*,
                        from_json(payload, 
                    'STRUCT<numberOfGames: INT, 
                                        gameWeek: ARRAY<STRUCT<
                                        date: STRING, 
                                        numberOfGames:INT
                                        >>
                                    >'
                                            
                                        ) as payload_json
                    from schedules_insert_tmp s
                    where 1 = 1
                        and payload is not null 
                        and request_key is not null
                    )

                merge into nhl_data_raw.games.schedules t 
                using src s 
                    on t.request_key = s.request_key 
                    and t.api_url = s.api_url 

                when matched and (
                    
                    t.payload <> s.payload 
                    and s.http_status = 200 
                    and s.payload_json.numberOfGames > 0 
                    and size(s.payload_json.gameWeek) > 0
                )

                then update set 

                    payload = s.payload,
                    http_status = s.http_status,
                    ingest_ts_utc = current_timestamp(),
                    scrape_plan = s.scrape_plan 

                when not matched and (

                    s.http_status = 200 
                    ---ensuring that data coming through the API actually contains games and not an empty API call
                    and s.payload_json.numberOfGames > 0 
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
else: 
    print(f"No new data to insert into nhl_data_raw.games.schedules, skipping insert")

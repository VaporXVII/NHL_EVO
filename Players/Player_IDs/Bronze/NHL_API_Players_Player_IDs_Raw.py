from pyspark.sql import SparkSession 
from pyspark.sql import functions as f, types as t
from pyspark.sql import DataFrame
from delta.tables import DeltaTable
from zoneinfo import ZoneInfo 

import requests, json, time, random, threading, string, datetime, concurrent.futures 
from concurrent.futures import ThreadPoolExecutor 
from urllib.parse import urlencode


spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "America/Chicago")
central_timezone = ZoneInfo("America/Chicago")

def now_central():
    return datetime.datetime.now(central_timezone)

def today_central():
    return now_central().date()
today = today_central()

def call_api(url:str, rps:float = 2.0, max_attempts: int = 3, time_out:int = 20):

    for attempt in range(max_attempts):

        try: 
            throttle(rps = rps)
            response = requests.get(url, timeout = time_out)
            payload = response.json()
            if isinstance(payload, list):
                payload = {"data": payload}
            last_status = response.status_code 
            if response.status_code == 200:
                
                return {

                    "endpoint": "player_search",
                    "request_key": url.split('/')[-1],
                    "http_status": int(last_status),
                    "payload": json.dumps(payload, ensure_ascii = False),
                    "api_url": url

                    }
            
            if response.status_code in (429, 502, 503, 504):
                time.sleep((2 ** attempt) + random.random())
                continue 

            return {

                    "endpoint": "player_search",
                    "request_key": url.split('/')[-1],
                    "http_status": int(last_status),
                    "payload": None,
                    "api_url": url

                    }
                
        except requests.RequestException:
            time.sleep((2 ** attempt) + random.random())
           
    return None

def fetch(url: str):
    r = requests.get(url, timeout = 60)
    r.raise_for_status()
    return r.status_code, r.json()

throttle_lock = threading.Lock()
next_allowed = 0.0 

def throttle(rps:float = 2.0):

    global next_allowed 
    min_interval = 1.0 / rps 

    with _rate_lock: 
        now = time.monotonic()
        if now < next_allowed: 
            sleep = next_allowed - now 
            next_allowed = next_allowed + min_interval
        else: 
            sleep = 0.0 
            next_allowed = now + min_interval 
    
    if sleep > 0: 
        time.sleep(sleep)

def insert_data(source: DataFrame, target_table: str) -> None:

    source.createOrReplaceTempView("source_tmp")
    spark.sql(f"""
                with src as (

                    select * 
                    from source_tmp 
                    where 1 = 1
                        and size(from_json(payload, 'data ARRAY<STRING>').data) > 0
                )
                merge into {target_table} t   
                using src s  
                    on t.request_key = s.request_key 
                    and t.api_url = s.api_url 
                    and from_utc_timestamp(t.ingest_ts_utc, 'America/Chicago')::date = from_utc_timestamp(current_timestamp(), 'America/Chicago')::date

                when matched and (
                    
                    t.payload <> s.payload 
                    and s.payload is not null 
                    and s.payload not in ('{{}}', '[]')

                )

                then update set 

                    http_status = s.http_status,
                    payload = s.payload,
                    ingest_ts_utc = current_timestamp()

                when not matched then insert (
                    
                    endpoint,
                    http_status,
                    request_key,
                    api_url,
                    payload,
                    ingest_ts_utc

                )

                values (

                    s.endpoint,
                    s.http_status,
                    s.request_key,
                    s.api_url,
                    s.payload,
                    current_timestamp()

                )
              
    """)
    spark.catalog.dropTempView("source_tmp")
    print(f"Player ids data successfully loaded into {target_table} table")

season_search = spark.sql(f"""

                with seasons as (
                    
                    ---pipeline runs schedules tables before any other tables, therefore coalesce(season, 19001901) is not needed
                    select 
                        min(season)::integer as start_season,
                        max(season)::integer as end_season
                    from nhl_data_staged.games.schedules
                    where 1 = 1
                        and game_type between 1 and 3

                )
                , 
                current_season_dates as (

                    select distinct 
                        season, 
                        game_date, 
                        game_type
                    from nhl_data_staged.games.schedules 
                    where 1 = 1
                        and game_date <= from_utc_timestamp(current_timestamp(), 'America/Chicago')::date
                        and game_type = 2
                    qualify season = max(season) over()
                    union all 
                    select distinct 
                        season, 
                        game_date,
                        game_type  
                    from nhl_data_staged.games.schedules 
                    where 1 = 1
                        and game_date <= from_utc_timestamp(current_timestamp(), 'America/Chicago')::date
                        and game_type = 3
                    qualify season = max(season) over ()

                )
                , 
                current_season_dates_idx as (

                    select /*+ broadcast(b) */
                        b.start_season,
                        b.end_season,
                        a.*, 
                        row_number() over (partition by a.season order by a.game_date) as date_idx 
                    from current_season_dates a
                    cross join seasons b 

                )
                ,
                last_active_season as (

                    select 
                        coalesce(max(last_active_season), 19001901)::integer as last_active_season
                    from nhl_data_staged.players.master_ids 
                    where 1 = 1
                        and last_active_season is not null 

                )
                ---run_scrape_ind checks to see if at least 15 days have lapsed per the NHL schedule
                ---using this method instead of datetime.datetime.today().day % 15 == 0 so that it tracks against the NHL schedule and not date of scrape
                select /*+ broadcast (b) */
                    a.start_season,
                    a.end_season,
                    b.last_active_season,
                    a.season,
                    a.game_date,
                    a.game_type, 
                    a.date_idx,
                    (date_idx % 15 = 0)::boolean as run_scrape_ind,
                    (a.end_season <> b.last_active_season)::boolean as new_season_ind 
                from current_season_dates_idx a 
                cross join last_active_season b 
                order by a.game_date desc 
                limit 1
                          
""")

start_season = season_search.select(f.col("start_season").alias("start")).first()["start"]
end_season = season_search.select(f.col("end_season").alias("end")).first()["end"]
player_ids_table_last_active_season = season_search.select(f.col("last_active_season").alias("las")).first()["las"]
run_scrape_ind = season_search.select(f.col("run_scrape_ind").alias("rsi")).first()["rsi"]
new_season_ind = season_search.select(f.col("new_season_ind").alias("new")).first()["new"]

#if player ids have not been scraped yet at all, even though variables are set above, setting them again to make intent clear
if player_ids_table_last_active_season == 19001901: 
    print(f"Cold start")
    start_season = start_season 
    end_season = end_season
#if user has done scrape of player ids already and the most recent end season from schedules
#doesn't match most recent end season from players table 
elif new_season_ind: 
    print(f"Cold start already performed and new season has started")
    #setting start season to be the prior season and the end season to be the current season/ upcoming season
    start_season = player_ids_table_last_active_season
#if user has done scrape of player ids already and want to do an incremental refresh to capture any new players that have come in 
#only executing this block if the regular season is currently in play 
elif end_season == player_ids_table_last_active_season and run_scrape_ind: 
    print(f"Current season in play and run_scrape_ind = true")
    end_season = player_ids_table_last_active_season
#if user has done scrape already and the both the most recent end season from schedules 
#and most last active year from players master ids table are a match
else: 
    print(f"Cold start already performed and new season has not yet started")
    start_season = None
    end_season = None

if end_season is not None: 
    
    with ThreadPoolExecutor(max_workers = 5) as executor:
        
        #https://api.nhle.com/stats/rest/en/skater/summary?isAggregate=false&isGame=false&start=0&limit=500&cayenneExp=seasonId=20242025
        primary_api_url = "https://search.d3.nhle.com/api/v1/search/player"
        params_common = {"culture": "en-us", "limit": 5000}
        letters = list(string.ascii_lowercase) + [str(d) for d in range(10)]
        prefixes = [f"{c}*" for c in letters]
        player_urls = [f"{primary_api_url}?{urlencode({**params_common, 'q': q})}" for q in prefixes]
        _rate_lock = threading.Lock()

        api_data = []
        futures = [executor.submit(call_api, url) for url in player_urls]
        completed = 0 
        for future in concurrent.futures.as_completed(futures): 
            result = future.result()
            if result is not None:
                api_data.append(result)
                completed += 1 
                if completed % 5 == 0:
                    print(f"{completed} / {len(player_urls)} schedule urls fetched.")

        print(f"Player ids by player search scraped.")
        players_schema = t.StructType([

        t.StructField("endpoint", t.StringType(), False),
        t.StructField("request_key", t.StringType(), False),
        t.StructField("http_status", t.IntegerType(), False),
        t.StructField("payload", t.StringType(), False),
        t.StructField("api_url", t.StringType(), False),   
        ])
        players_df = spark.createDataFrame(api_data, schema = players_schema)
        players_df = players_df.filter(
                                    (f.col("payload").cast(t.StringType()) != "[]") & 
                                    (f.col("payload").cast(t.StringType()) != "{}") & 
                                    (f.col("payload").cast(t.StringType()).isNotNull())
        )
        insert_ready = not players_df.isEmpty()
        if insert_ready: 
            #table below is source that feeds nhl_data_staged.players.master_ids
            insert_data(source = players_df, target_table = "nhl_data_raw.players.player_search")
       
else: 
    print(f"Current season still in play, skipping player ids scrape.")

if end_season is not None:
    
    skater_api_url = (
    f"https://api.nhle.com/stats/rest/en/skater/realtime?"
    f"limit=-1&cayenneExp=seasonId>={start_season}%20and%20seasonId<={end_season}%20and%20gameTypeId=2"
    )
    goalie_api_url = (
        f"https://api.nhle.com/stats/rest/en/goalie/summary?"
        f"limit=-1&cayenneExp=seasonId>={start_season}%20and%20seasonId<={end_season}%20and%20gameTypeId=2"
    )

    api_data = []

    status, payload = fetch(skater_api_url)
    if isinstance(payload, list):
        payload =   {"data": payload}
    api_data.append({
        
        "endpoint": "skater_summary",
        "request_key": f"{start_season}-{end_season}-2",
        "api_url": skater_api_url,
        "http_status": int(status),
        "payload": json.dumps(payload, ensure_ascii = False),
    })

    status, payload = fetch(goalie_api_url)
    api_data.append({

        "endpoint": "goalie_summary",
        "request_key": f"{start_season}-{end_season}-2",
        "api_url": goalie_api_url,
        "http_status": int(status),
        "payload": json.dumps(payload, ensure_ascii = False),
    })
    print(f"Player ids by season scraped.")
    players_season = spark.createDataFrame(api_data, schema = players_schema)
    players_season = players_season.filter(
                                        (f.col("payload").cast(t.StringType()) != "{}") &
                                        (f.col("payload").cast(t.StringType()) != "[]") & 
                                        (f.col("payload").cast(t.StringType()).isNotNull())
    )
    if not players_season.isEmpty():
        insert_data(source = players_season, target_table = "nhl_data_raw.players.player_search_season")
else: 
    print(f"Current season still in play, skipping player ids scrape.")

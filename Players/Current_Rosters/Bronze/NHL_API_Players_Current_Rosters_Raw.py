from pyspark.sql import SparkSession 
from pyspark.sql import functions as f, types as t, Window as w, DataFrame
from zoneinfo import ZoneInfo 
from delta.tables import DeltaTable
import requests, json, time, random, threading, datetime as dt
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor 

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "America/Chicago")
central_timezone = ZoneInfo("America/Chicago")

_rate_lock = threading.Lock()
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

def call_api(url:str, rps:float = 2.0, max_attempts: int = 3, time_out:int = 20):

    for attempt in range(max_attempts):

        try: 
            api_url = url.rsplit("/", 1)[0]
            game_id = int(url.rsplit("/", 1)[1].rsplit("-", 1)[0])
            team_id = int(url.rsplit("/", 1)[1].rsplit("-", 1)[1])
            team_abbrev = api_url.split("/roster/")[1].split("/current")[0]
            throttle(rps = rps)
            response = requests.get(api_url, timeout = time_out)
            payload = response.json()
            if isinstance(payload, list):
                payload = {"data": payload}
            last_status = response.status_code 
            if response.status_code == 200:
                
                return {

                    "endpoint": "player_game_roster_search",
                    "request_key": team_abbrev,
                    "team_id": team_id,
                    "game_id": game_id,
                    "http_status": int(last_status),
                    "payload": json.dumps(payload, ensure_ascii = False),
                    "api_url": api_url

                    }
            
            if response.status_code in (429, 502, 503, 504):
                time.sleep((2 ** attempt) + random.random())
                continue 

            return {
                    "endpoint": "player_game_roster_search",
                    "request_key": team_abbrev,
                    "team_id": team_id,
                    "game_id": game_id,
                    "http_status": int(last_status),
                    "payload": None,
                    "api_url": api_url

                    }
                
        except requests.RequestException:
            time.sleep((2 ** attempt) + random.random())
           
    return None

todays_teams = spark.sql("""
                        
                        select /*+ broadcast (b) */ distinct 
                            a.game_id, 
                            a.team_abbrev, 
                            a.team_id 
                        from nhl_data_staged.games.schedules a  
                        inner join nhl_data_staged.teams.details b 
                            on a.team_id = b.team_id
                        left anti join nhl_data_staged.players.player_game_rosters c 
                            on a.game_id = c.game_id
                        where 1 = 1
                            and a.game_date = from_utc_timestamp(current_timestamp(), 'America/Chicago')::date
                            and b.is_active = true
                            and a.game_type in (2,3)

    """)
ready = not bool(todays_teams.isEmpty())

insert_ready = False
if ready: 
    
    todays_games_info = (
                todays_teams 
                .select(f.col("team_abbrev"), f.col("game_id"))
                .distinct()
    )
    todays_teams_dict = {row["team_abbrev"]: str(row["game_id"]) + '-' + str(row["team_id"]) for row in todays_teams.collect()}
    team_api_urls = [f"https://api-web.nhle.com/v1/roster/{team}/current/{extra_info}" for team, extra_info in todays_teams_dict.items()]
    with ThreadPoolExecutor(max_workers = 5) as executor:

        api_data = []
        futures = [executor.submit(call_api, url) for url in team_api_urls]
        completed = 0 
        for future in concurrent.futures.as_completed(futures): 
            result = future.result()
            if result is not None:
                api_data.append(result)
                completed += 1 
                if completed % 5 == 0:
                    print(f"{completed} / {len(team_api_urls)} schedule urls fetched.")

    
    pgr_schema = t.StructType([

    t.StructField("endpoint", t.StringType(), False),
    t.StructField("request_key", t.StringType(), False),
    t.StructField("team_id", t.IntegerType(), False),
    t.StructField("game_id", t.LongType(), False),
    t.StructField("http_status", t.IntegerType(), False),
    t.StructField("payload", t.StringType(), False),
    t.StructField("api_url", t.StringType(), False),   
    
    ])
    pgr_df = spark.createDataFrame(api_data, schema = pgr_schema)
    pgr_df = pgr_df.filter(f.col("request_key").isNotNull())
    insert_ready = not bool(pgr_df.isEmpty())

if insert_ready: 

    pgr_df.createOrReplaceTempView("pgr_df_tmp")
    spark.sql("""
              
              
        merge into nhl_data_raw.players.player_game_rosters t  
        using pgr_df_tmp s 
            on t.team_id = s.team_id 
            and t.game_id = s.game_id           
            and from_utc_timestamp(t.ingest_ts_utc, 'America/Chicago')::date between 
                date_sub(from_utc_timestamp(current_timestamp(), 'America/Chicago')::date, 2) 
                and 
                from_utc_timestamp(current_timestamp(), 'America/Chicago')::date
        
        when matched and (

            not (t.http_status <=> s.http_status)
            or (
                not (t.payload <=> s.payload)
                and s.payload is not null 
                and s.payload not in ("[]", "{}")
                and s.http_status = 200
            )
        )

        then update set 

            http_status = s.http_status,
            payload = s.payload,
            ingest_ts_utc = current_timestamp()

        when not matched then insert (


            endpoint,
            http_status,
            request_key,
            team_id,
            game_id, 
            api_url,
            payload,
            ingest_ts_utc
            
        )

        values (

            s.endpoint, 
            s.http_status,
            s.request_key,
            s.team_id,
            s.game_id,
            s.api_url,
            s.payload,
            current_timestamp()
        )
              
    """)
    spark.catalog.dropTempView("pgr_df_tmp")
    print(f"Player ids data successfully loaded into nhl_data_raw.players.player_game_rosters table")
else:
    print("No new data to load into nhl_data_raw.players.player_game_rosters table")

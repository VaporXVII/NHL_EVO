from pyspark.sql import SparkSession 
from pyspark.sql import functions as f, types as t, Window as w, DataFrame
from delta.tables import DeltaTable
from zoneinfo import ZoneInfo 
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import requests, certifi, json, time, random, threading, datetime as dt
from itertools import product

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "America/Chicago")

_rate_lock = threading.Lock()
_next_allowed = 0.0
def throttle(rps: float = 2.0) -> None:

    global _next_allowed

    if rps <= 0:
        raise ValueError("rps must be greater than 0.")

    with _rate_lock:
        now = time.monotonic()
        wait = max(0.0, _next_allowed - now)

        _next_allowed = max(
            _next_allowed,
            now
        ) + (1.0 / rps)

    if wait > 0:
        time.sleep(wait)

def get_dates(topic: str) -> DataFrame: 

    topics_dict = {"powerplay": "pp_stats", "penaltykill": "pk_stats", "powerplaytime": "pp_toi", "penaltykilltime": "pk_toi"}
    topic_table = f"nhl_data_staged.games.{topics_dict[topic]}"


    return spark.sql(f"""
                    
            with game_dates as (


                select 
                    ---schedules table is filled first so game dates should never be null, avoiding using coalesce
                    min(game_date) as start_date,
                    max(game_date) as end_date,
                    max(from_utc_timestamp(current_timestamp(), 'America/Chicago'))::date as todays_date
                from nhl_data_staged.games.schedules 
                where 1 = 1
                    and game_date < date_sub(from_utc_timestamp(current_timestamp(), 'America/Chicago'), 2)::date 
                    and game_type in (2,3)

                    
                )                 
            ,
            date_list as (


                select 
                    explode(sequence(start_date::date, end_date::date, interval 1 day)) as game_date
                from game_dates 

            )
            ,
            games_loaded as (

                
                select /*+ broadcast (c) */  
                    game_date,
                    game_id,
                    team_id
                from {topic_table}
                cross join game_dates c
                where 1 = 1
                    and game_date < date_sub(todays_date, 2) 


            )

            select /*+ broadcast (c) */
                a.game_type, 
                min(concat("https://api.nhle.com/stats/rest/en/team/", '{topic}')) as topic_url,
                min(a.game_date) as start_date, 
                max(a.game_date) as end_date
            from nhl_data_staged.games.schedules a   
            left anti join games_loaded b  
                on a.game_id = b.game_id
                and a.game_date = b.game_date
                and a.team_id = b.team_id
            cross join game_dates c 
            where 1 = 1
                and a.game_date < date_sub(c.todays_date, 2)
                and a.game_type = 2
            group by 
                a.game_type
            union all 
            select /*+ broadcast (c) */ 
                a.game_type,
                min(concat("https://api.nhle.com/stats/rest/en/team/", '{topic}')) as topic_url,
                min(a.game_date) as start_date,
                max(a.game_date) as end_date
            from nhl_data_staged.games.schedules a 
            left anti join games_loaded b 
                on a.game_id = b.game_id
                and a.game_date = b.game_date 
                and a.team_id = b.team_id
            cross join game_dates c   
            where 1 = 1
                and a.game_date < date_sub(c.todays_date, 2)
                and a.game_type = 3
            group by 
                a.game_type

            """)

def set_params(param_type: str, cayenne_exp: str) -> dict[str]:

    if "stats" in param_type.lower():
        params = {
            "isAggregate": "false",
            "isGame": "true",
            "start": 0,
            "limit": 100,
            "sort": json.dumps([
                {"property": "gameDate", "direction": "ASC"}
            ]),
            "factCayenneExp": "gamesPlayed>=1",
            "cayenneExp": cayenne_exp
        }

    else:
        params = {
            "isAggregate": "false",
            "isGame": "true",
            "sort": json.dumps([
                {"property": "gameDate", "direction": "ASC"},
                {"property": "teamId", "direction": "ASC"}
            ]),
            "start": 0,
            "limit": 100,
            "cayenneExp": cayenne_exp
        }

    return params

def initialize(topic: str, topic_type: str, game_type: int) -> DataFrame:
    date_ranges = (
        get_dates(topic)
        .filter(f.col("game_type") == game_type)
        .groupBy("game_type", "topic_url")
        .agg(
            f.min("start_date").alias("start_date"),
            f.max("end_date").alias("end_date")
        )
        .collect()
    )
    api_data = []
    if date_ranges: 
        date_range = date_ranges[0]

        cayenne_exp = (
            f'gameTypeId={date_range["game_type"]} '
            f'and gameDate >= "{date_range["start_date"]}" '
            f'and gameDate <= "{date_range["end_date"]}"'
        )

        params = set_params(param_type = topic_type, cayenne_exp = cayenne_exp)
        api_data = call_api(date_range["topic_url"], params_dict = params, game_type = game_type)

    return api_data if api_data else []

def call_api(url: str, game_type: int, params_dict: dict, rps: float = 2.0, max_attempts: int = 3) -> list[dict]:

    updated_params = params_dict.copy()

    api_data = []
    scrape_type = url.rsplit("/", 1)[-1]

    rows_scraped = 0
    start = int(updated_params.get("start", 0))
    limit = int(updated_params.get("limit", 100))

    print(f"Starting {scrape_type.capitalize()} scrape for game_type {game_type}...")

    while True:
        updated_params["start"] = start

        response = None

        for attempt in range(max_attempts):
            try:
                throttle(rps = rps)

                response = requests.get(
                    url,
                    params = updated_params,
                    timeout = 10,
                    verify = certifi.where()
                )

                if response.status_code == 200:
                    break

                if response.status_code in {429, 502, 503, 504}:
                    if attempt < max_attempts - 1:
                        wait_seconds = (2 ** attempt) + random.random()
                        time.sleep(wait_seconds)
                        continue

                response.raise_for_status()

            except requests.RequestException:
                if attempt < max_attempts - 1:
                    wait_seconds = (2 ** attempt) + random.random()
                    time.sleep(wait_seconds)
                    continue

                raise

        if response is None:
            raise RuntimeError("Request failed and no response was returned.")

        if response.status_code != 200:
            response.raise_for_status()

        data = response.json()
        chunk = data.get("data", [])

        if not chunk:
            print(
                f"{scrape_type.capitalize()} scrape done. "
                f"Total rows scraped: {rows_scraped}"
            )
            break

        api_data.append({
            "endpoint": scrape_type,
            "request_key": json.dumps(
                updated_params,
                ensure_ascii = False
            ),
            "http_status": response.status_code,
            "payload": json.dumps(data, ensure_ascii = False),
            "api_url": url
        })

        rows_scraped += len(chunk)

        if rows_scraped % 500 < limit:
            print(
                f"Scraped {rows_scraped} rows "
                f"(start={start})"
            )

        start += limit

    return api_data

def merge_insert(api_data: DataFrame, topic: str, game_type: int) -> None: 

    if api_data: 
        api_data.createOrReplaceTempView("special_teams_insert_tmp")
        spark.sql("""
                        
            -- Match rows for the same request and endpoint, but only if:
            --   1. the existing record was ingested today or yesterday (allows refreshes), or
            --   2. the previous API request failed (HTTP status <> 200) so it can be retried/replaced.
            merge into nhl_data_raw.games.special_teams t 
            using special_teams_insert_tmp s  
                on t.request_key = s.request_key
                and t.api_url = s.api_url 
                and t.http_status = 200
                and (
                    from_utc_timestamp(t.ingest_ts_utc, 'America/Chicago')::date between 
                    date_sub(from_utc_timestamp(current_timestamp(), 'America/Chicago')::date, 2) 
                    and 
                    from_utc_timestamp(current_timestamp(), 'America/Chicago')::date
            
                )

            when matched and (

                t.payload <> s.payload 
                and s.payload is not null 
                and s.payload not in ('[]', '{}')
                and get_json_object(s.payload, "$.total")::integer > 0
                and s.http_status = 200

            )

            then update set 
            
                payload = s.payload,
                endpoint = s.endpoint, 
                http_status = s.http_status,
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

        spark.catalog.dropTempView("special_teams_insert_tmp")
        print(f"{topic.capitalize()} game_type {game_type} Special Teams data successfully loaded into nhl_data_raw.games.special_teams table")
    else: 
        print(f"No new data to insert into nhl_data_raw.games.special_teams, skipping insert")

def wrangle() -> None:

    topics = ["powerplay", "penaltykill", "powerplaytime", "penaltykilltime"]
    game_types = [2, 3]
    topic_types = {"powerplay": "stats", "penaltykill": "stats", "powerplaytime": "toi", "penaltykilltime": "toi"}
    for topic, game_type in product(topics, game_types):

        api_data = initialize(topic = topic, topic_type = topic_types[topic], game_type = game_type)
        if not api_data: 
            print(f"No API data returned for {topic} game_type {game_type}")
            continue 
        batch_df = (

                spark
                .createDataFrame(api_data, schema = special_teams_schema)
                .withColumn(
                "total",
                f.coalesce(
                    f.get_json_object(
                        f.col("payload"),
                        "$.total"
                    ).cast("int"),
                    f.lit(0)
                )
            )
            .filter(f.col("total") > 0)
            .drop("total")
        )
        merge_insert(api_data = batch_df, topic = topic, game_type = game_type)

special_teams_schema = t.StructType([

    t.StructField("endpoint", t.StringType(), False),
    t.StructField("request_key", t.StringType(), False),
    t.StructField("http_status", t.IntegerType(), True),
    t.StructField("payload", t.StringType(), True),
    t.StructField("api_url", t.StringType(), False)


])

games = spark.sql("""
                         
                         
            with parameters as (

                select
                    from_utc_timestamp(current_timestamp(), 'America/Chicago')::date as run_date

            )
            ,
            game_dates as (

                select
                    p.run_date,
                    max(s.game_date) filter (where s.game_date < p.run_date) as latest_game_date,
                    max(s.game_date) filter (where s.game_date between date_sub(p.run_date, 2) and date_sub(p.run_date, 1)) as recent_game_date
                from nhl_data_staged.games.schedules s
                cross join parameters p
                group by p.run_date

            )
            ,
            special_teams_counts as (

                select count(*) as row_count
                from nhl_data_staged.games.pp_stats
                union all
                select count(*) as row_count
                from nhl_data_staged.games.pk_stats
                union all
                select count(*) as row_count
                from nhl_data_staged.games.pp_toi
                union all
                select count(*) as row_count
                from nhl_data_staged.games.pk_toi

            )
            ,
            special_teams as (

                select
                    min(row_count) = 0 as cold_start_needed_ind
                from special_teams_counts

            )
            ,
            ready as (

            select
                (s.cold_start_needed_ind or g.recent_game_date is not null) as scrape_ready_ind
            from game_dates g
            cross join special_teams s

            )

            select scrape_ready_ind
            from ready
                         
                        
    """)
ready = games.first()["scrape_ready_ind"]
if ready: 
    wrangle()

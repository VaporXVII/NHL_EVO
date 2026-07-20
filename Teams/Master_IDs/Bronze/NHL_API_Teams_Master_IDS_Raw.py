import sys 
username = spark.sql("select current_user()").first()[0]
sys.path.append(f"/Workspace/Users/{username}/NHL_Pipeline")

from pyspark.sql import SparkSession 
from pyspark.sql import functions as f, types as t, Window as w, DataFrame
from delta.tables import DeltaTable
from zoneinfo import ZoneInfo 
import datetime as dt, re, requests, json, certifi
from pipeline_funcs.user_utc_region import region_return 

user_region = region_return()
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", f"{user_region}")
central_timezone = ZoneInfo(f"{user_region}")

def now_central():
    return dt.datetime.now(central_timezone)

def today_central():
    return now_central().date()
today = today_central()

season_check_df = spark.sql(f"""
                            
                            
                select 
                    (
                    select max(season) 
                    from nhl_data_staged.games.schedules 
                    where 1 = 1
                        and from_utc_timestamp(current_timestamp(), '{user_region}')::date >= from_utc_timestamp(insert_dte, '{user_region}')::date
                    )::integer as sched_current_season,
                    (
                    select 
                        coalesce(max(season), 19001901) 
                    from nhl_data_staged.teams.master_ids
                    )::integer as team_ids_season,
                    not (sched_current_season = team_ids_season)::boolean as new_season_started_ind
                            
    """)

current_season_check = season_check_df.first()["new_season_started_ind"]
if current_season_check:

    url = f"https://api.nhle.com/stats/rest/en/team"
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(url, headers = headers, verify = certifi.where())
    body = None
    try: 
        body = r.json()
    except Exception: 
        pass

    if isinstance(body, dict):
        body = [body]
    
    if isinstance(body, list) or len(body) > 0:

        payload = json.dumps(body, separators = (",", ":"), ensure_ascii = False)

        api_data = [

            {

            "endpoint": "teams", 
            "request_key": None, 
            "http_status": int(r.status_code), 
            "payload": payload,
            "api_url": url

            }
        ]

        teams_schema = t.StructType([

            t.StructField("endpoint", t.StringType(), False), 
            t.StructField("request_key", t.StringType(), True), 
            t.StructField("http_status", t.IntegerType(), False), 
            t.StructField("payload", t.StringType(), False), 
            t.StructField("api_url", t.StringType(), False)

        ])
        api_data = spark.createDataFrame(api_data, schema = teams_schema)
        api_data = (
                season_check_df
                .select("sched_current_season")
                .withColumnRenamed("sched_current_season", "season")
                .crossJoin(api_data)
        )

        if not api_data.isEmpty():
            
            api_data.createOrReplaceTempView("teams_api_tmp")
            json_schema = spark.sql("select schema_of_json_agg(payload) as json_schema from teams_api_tmp where http_status = 200 and payload is not null").first()["json_schema"]
            spark.sql(f"""
                      
                    with raw as (

                        select 
                            season,
                            endpoint,
                            request_key,
                            http_status,
                            payload,
                            api_url,
                            explode(from_json(payload, '{json_schema}')) as json_payload
                        from teams_api_tmp
                        where 1 = 1
                            and payload is not null 
                            and payload not in ('[]', '{{}}')
                            and http_status = 200

                    )
                    ,
                    src as (
                        
                        ---below safeguards empty payloads from entering
                        select  
                            season,
                            endpoint,
                            request_key,
                            http_status,
                            payload,
                            api_url
                        from raw
                        where 1 = 1
                            and json_payload.total::integer > 0 
                            and size(json_payload.data) > 0

                    )

                    merge into nhl_data_raw.teams.master_ids t 
                    using src s 
                        on t.season = s.season 
                    
                    when matched and (

                        (
                        from_utc_timestamp(t.ingest_ts_utc, '{user_region}')::date <> from_utc_timestamp(current_timestamp(), '{user_region}')::date
                        or t.http_status <> s.http_status 
                        or t.payload <> s.payload
                        )

                    )

                    then update set 

                        http_status = s.http_status,
                        payload = s.payload,
                        ingest_ts_utc = current_timestamp()

                    when not matched then insert (

                        season,
                        endpoint,
                        http_status,
                        request_key,
                        api_url,
                        payload,
                        ingest_ts_utc
                    
                    )

                    values (

                        s.season,
                        s.endpoint,
                        s.http_status,
                        s.request_key,
                        s.api_url,
                        s.payload,
                        current_timestamp()

                    )  
            """)
            spark.catalog.dropTempView("teams_api_tmp")
            print(f"Team master ids ingested into nhl_data_raw.teams.master_ids table")
        else: 
            print(f"Current season still in play, no new data to ingest")
else: 
    print("Current season still in play, no new data to ingest")

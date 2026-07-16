import sys 
username = spark.sql("select current_user()").first()[0]
sys.path.append(f"/Workspace/Users/{username}/NHL_Pipeline")

from pyspark.sql import SparkSession, functions as f, types as t
import requests 
from pipeline_funcs.user_utc_region import region_return 
from pipeline_funcs.api_utils import scrape_batch

user_region = region_return()
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", f"{user_region}")

insert_ready = False
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
                        coalesce(max(current_season), 19001901) 
                    from nhl_data_staged.teams.details
                    )::integer as team_ids_season,
                    not (sched_current_season = team_ids_season)::boolean as new_season_started_ind
                            
    """)
ready = season_check_df.select("new_season_started_ind").first()[0]

if ready: 
    current_season = season_check_df.select("sched_current_season").first()[0]
    teams_hist_url = f"https://api.nhle.com/stats/rest/en/franchise?sort=fullName&include=lastSeason.id&include=firstSeason.id"
    teams_hist_list = scrape_batch(urls = [teams_hist_url], endpoint = "team_details")
    teams_hist_schema = t.StructType([

                    t.StructField("endpoint", t.StringType(), False),
                    t.StructField("http_status", t.IntegerType(), False),
                    t.StructField("request_key", t.StringType(), True), 
                    t.StructField("api_url", t.StringType(), False),
                    t.StructField("payload", t.StringType(), True)
                    
                    
    ])
    teams_hist = spark.createDataFrame(teams_hist_list, teams_hist_schema)
    insert_ready = not teams_hist.isEmpty()

if insert_ready:
    
    teams_hist.createOrReplaceTempView("team_details_tmp")
    spark.sql(f"""

                with src as (
        
                    select
                        '{current_season}' as season, 
                        a.endpoint, 
                        a.http_status,
                        a.request_key,
                        a.api_url, 
                        a.payload
                    from team_details_tmp a 

                )

                merge into nhl_data_raw.teams.details t 
                using src s 
                    on t.season = s.season 
                
                when matched and (

                        from_utc_timestamp(t.ingest_ts_utc, '{user_region}') <> from_utc_timestamp(current_timestamp(), '{user_region}')
                        and s.http_status = 200 
                        and t.payload <> s.payload 
                )
            
                then update set 

                        api_url = s.api_url,
                        endpoint = s.endpoint,
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
    spark.catalog.dropTempView("team_details_tmp")
    print(f"Team master ids ingested into nhl_data_raw.teams.details table")
else: 
    print(f"Current season still in play, no new data to ingest")

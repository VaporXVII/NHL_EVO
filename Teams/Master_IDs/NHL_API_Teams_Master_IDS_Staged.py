import sys 
username = spark.sql("select current_user()").first()[0]
sys.path.append(f"/Workspace/Users/{username}/NHL_Pipeline")

from pyspark.sql import SparkSession
from pyspark.sql import functions as f, types as t, DataFrame
from delta.tables import DeltaTable
from zoneinfo import ZoneInfo
import datetime as dt, re, requests, json
from pipeline_funcs.schema_utils import convert_case, apply_schema, build_fields, get_schema
from pipeline_funcs.user_utc_region import region_return 
from pipeline_funcs.table_maint import run_table_maint

user_region = region_return()
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", f"{user_region}")
central_timezone = ZoneInfo(f"{user_region}")

season_check_df = spark.sql(f"""
                            
                            
                select 
                    (
                    select 
                        max(season) 
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

field_mapping = {


            "franchiseId": {
                
                            "target": "franchise_id", 
                            "type": "integer"
            }, 
            "fullName": {

                            "target": "team_name", 
                            "type": "string", 
                            "trim": True
            },
            "id": {

                            "target": "team_id", 
                            "type": "integer", 

            }, 
            "leagueId": {

                            "target": "league_id", 
                            "type": "integer"
            },
            "rawTricode": {

                            "target": "team_abbrev", 
                            "type": "string", 
                            "upper": True, 
                            "trim": True
            }
}

insert_ready = False
current_season_check = season_check_df.first()["new_season_started_ind"]
if current_season_check:
  py_source = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")[-1]
  master_ids_raw = spark.sql("""
                              
                    select 
                      season,
                      payload
                    from nhl_data_raw.teams.master_ids 
                    where 1 = 1 
                    qualify ingest_ts_utc = max(ingest_ts_utc) over ()
                      
      """)
  master_ids_schema = get_schema(master_ids_raw)
  master_ids = (

          master_ids_raw
          .alias("a")
          .withColumn("json", f.from_json(f.col("payload"), master_ids_schema))
          .withColumn("data", f.explode("json"))
          .withColumn("extract", f.explode("data.data"))
          .select("season", "extract.*")
    
  )
  add_fields_expr = [build_fields(src_col, rule, master_ids.columns) for src_col, rule in field_mapping.items()]
  master_ids_schema = t.StructType([

    t.StructField("season", t.IntegerType(), False), 
    t.StructField("franchise_id", t.IntegerType(), True), 
    t.StructField("team_name", t.StringType(), False), 
    t.StructField("team_id", t.IntegerType(), False), 
    t.StructField("league_id", t.IntegerType(), True),
    t.StructField("team_abbrev", t.StringType(), False),
    t.StructField("py_source", t.StringType(), False)
    
  ])

  master_ids_df = (

    master_ids
    .select("season", *add_fields_expr)
    .withColumn("py_source", f.lit(py_source))
    .transform(apply_schema, master_ids_schema)

  )
  insert_ready = not master_ids_df.isEmpty()

if insert_ready: 

    master_ids_df.createOrReplaceTempView("team_master_ids_tmp")
    spark.sql(f"""
              
              merge into nhl_data_staged.teams.master_ids t  
              using team_master_ids_tmp s 
                on t.team_id = s.team_id
              
              when matched and (

                t.season <> s.season 
                or coalesce(t.franchise_id, 999) <> coalesce(s.franchise_id, 999) 
                or t.team_name <> s.team_name
                or t.league_id <> s.league_id
                or t.team_abbrev <> s.team_abbrev

              )
              
              then update set 

                season = s.season,
                franchise_id = s.franchise_id,
                team_name = s.team_name,
                league_id = s.league_id,
                team_abbrev = s.team_abbrev,
                update_dte = current_timestamp(),
                py_source = s.py_source
              
              when not matched then insert (

                season,
                franchise_id,
                team_name,
                team_id,
                league_id,
                team_abbrev,
                insert_dte,
                update_dte,
                py_source

              )
              
              values (

                s.season,
                s.franchise_id,
                s.team_name,
                s.team_id,
                s.league_id,
                s.team_abbrev,
                current_timestamp(),
                null,
                s.py_source
              )
    """)
    spark.catalog.dropTempView("team_master_ids_tmp")
    print(f"Schedules data successfully loaded into nhl_data_staged.teams.master_ids table")
    run_table_maint(spark, "nhl_data_staged.teams.master_ids")
    run_table_maint(spark, "nhl_data.teams.master_ids")

else:   
    print(f"No new data to insert into nhl_data_staged.teams.master_ids, skipping insert")

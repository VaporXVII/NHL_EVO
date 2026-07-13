import sys 
username = spark.sql("select current_user()").first()[0]
sys.path.append(f"/Workspace/Users/{username}/NHL_Pipeline")

from pyspark.sql import SparkSession 
from pyspark.sql import functions as f, types as t
from pyspark.sql import DataFrame
from delta.tables import DeltaTable
from zoneinfo import ZoneInfo 
import datetime as dt
import re
from pipeline_funcs.schema_utils import convert_case, apply_schema

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "America/Chicago")
central_timezone = ZoneInfo("America/Chicago")

insert_ready = False
sched_silver = spark.sql("""
                         
            with season_info as (

                select 
                    max(season)::integer as current_season 
                from nhl_data_staged.games.schedules
                
            )
            ,
            teams_info as (

                select distinct 
                season,
                team_id, 
                team_abbrev,
                team_name,
                team_city, 
                game_date
            from nhl_data_staged.games.schedules a 
            
            )

            select /*+ broadcast(b) */
                a.season,
                a.team_id,
                a.team_abbrev,
                a.team_name,
                a.team_city,
                a.game_date,
                b.current_season
            from teams_info a 
            cross join season_info b 
                        
    """)

if not sched_silver.isEmpty():
    
    py_source = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")[-1]
    teams = (
        
        sched_silver 
        .select("team_id", "team_abbrev", "team_name", "team_city", "current_season")
        .dropDuplicates(["team_id"])


    )

    teams_all = (

        teams 
        .select("team_id", "team_abbrev", "team_name", "current_season")
        .dropDuplicates(["team_id"])
    )

    teams_current = (

                    sched_silver 
                    .alias("a")
                    .join(

                            sched_silver 
                            .select("season")
                            .agg(f.max("season").alias("max_season"))
                            .alias("b")
                    , how = "inner", on = f.col("a.season") == f.col("b.max_season")
                    )
                    .select("a.team_id")
                    .withColumn("is_active", f.lit(True).cast(t.BooleanType()))
                    .dropDuplicates(["team_id"])

    )

    teams_flagged = (

        teams
        .alias("t")
        .join(f.broadcast(teams_current).alias("tc"), how = "left", on = "team_id")
        .withColumn("is_active", f.coalesce(f.col("tc.is_active"), f.lit(False)))
        .select("t.*", "is_active")
    )

    team_active_dates = (

        sched_silver 
        .groupBy("team_id")
        .agg(f.min("game_date").alias("active_from"), 
            f.max("game_date").alias("active_to"))
        
    )

    teams_final = (

        teams_flagged
        .alias("tf")
        .join(f.broadcast(team_active_dates).alias("td"), how = "left", on = "team_id")
        .withColumn("active_from", f.col("td.active_from"))
        .withColumn("active_to", 
                    f.when(f.col("tf.is_active") == True, f.lit(None).cast(t.DateType()))
                    .otherwise(f.col("td.active_to"))
                    )
        .withColumn("py_source", f.lit(py_source))
        .select(
            "team_id", 
            "team_abbrev", 
            "team_name", 
            "team_city",
            "is_active",
            "active_from", 
            "active_to",
            "current_season",
            "py_source"
            ) 
        .withColumn("team_abbrev", f.upper(f.trim(f.col("team_abbrev"))))
        .withColumn("team_name", f.trim(f.col("team_name")))
        .withColumn("team_city", f.trim(f.col("team_city")))
    )

    teams_schema = t.StructType([

        t.StructField("team_id", t.IntegerType(), False),
        t.StructField("team_abbrev", t.StringType(), False), 
        t.StructField("team_name", t.StringType(), False), 
        t.StructField("team_city", t.StringType(), True),
        t.StructField("is_active", t.BooleanType(), False), 
        t.StructField("active_from", t.DateType(), False), 
        t.StructField("active_to", t.DateType(), True), 
        t.StructField("current_season", t.IntegerType(), False),
        t.StructField("py_source", t.StringType(), False)
    ])


    teams_final = (
                    teams_final
                    .transform(apply_schema, teams_schema)
                    .filter(
                            (f.col("team_id") > 0) | 
                            (f.lower(f.col("team_abbrev")) != "tbd") | 
                            (f.lower(f.col("team_name")) != "tbd") 
                            )
                        )

    insert_ready = True

if insert_ready: 
    
    teams_final.createOrReplaceTempView("teams_final_tmp")
    spark.sql(f"""
              
              
            merge into nhl_data_staged.teams.details t 
            using teams_final_tmp s 
                on t.team_id = s.team_id

            when matched and (

                t.team_abbrev <> s.team_abbrev
                or t.team_name <> s.team_name
                or t.team_city <> s.team_city
                or t.is_active <> s.is_active 
                or t.active_from <> s.active_from 
                or not (t.active_to <=> s.active_to)
                or t.current_season <> s.current_season
                
            )

            then update set 

                team_abbrev = s.team_abbrev,
                team_name = s.team_name,
                team_city = s.team_city,
                active_from = least(t.active_from, s.active_from)::date, 
                active_to = case when s.is_active then null else s.active_to_end,
                is_active = s.is_active,
                current_season = s.current_season,
                update_dte = current_timestamp(),
                py_source = s.py_source

            
            when not matched then insert (

                team_id, 
                team_abbrev,
                team_name,
                team_city,
                is_active,
                active_from,
                active_to,
                current_season,
                insert_dte,
                update_dte,
                py_source

            )
              
            values (

                s.team_id,
                s.team_abbrev,
                s.team_name,
                s.team_city,
                s.is_active,
                s.active_from,
                s.active_to,
                s.current_season,
                current_timestamp(),
                null,
                s.py_source

            )
              
    """)
    spark.catalog.dropTempView("teams_final_tmp")
    print(f"Teams data successfully loaded into nhl_data_staged.teams.details table")
else: 
  print(f"No new data to insert into nhl_data_staged.teams.details, skipping insert")

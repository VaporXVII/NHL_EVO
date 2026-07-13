import sys 
username = spark.sql("select current_user()").first()[0]
sys.path.append(f"/Workspace/Users/{username}/NHL_Pipeline")

from pyspark.sql import SparkSession 
from pyspark.sql import functions as f, types as t, Window as w
from delta.tables import DeltaTable
import datetime as dt, re
from zoneinfo import ZoneInfo 
from pipeline_funcs.schema_utils import apply_schema

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "America/Chicago")
central_timezone = ZoneInfo("America/Chicago")

sched_staged = spark.sql("""
          
                    select 
                        a.*
                    from nhl_data_staged.games.schedules a 
                    where 1 = 1
                    ---limit to only games that are in play on the current date 
                    ---or games that were added to the schedules staged area on the 
                    ---current date, same with update date
                        and a.game_date = from_utc_timestamp(current_timestamp(), 'America/Chicago')::date
                        or (
                            a.insert_dte::date >= date_sub(current_date(), 1) 
                            or 
                            a.update_dte::date >= date_sub(current_date(), 1)
                            )
                        
                    
                    """)
ready = not sched_staged.isEmpty()

insert_ready = False
if ready: 
    sched_df = (
        
        sched_staged 
        .filter(f.col("game_date").isNotNull())
        .withColumn(
            "start_time_utc_ts",
            f.coalesce(
                f.to_timestamp("start_time_utc", "yyyy-MM-dd'T'HH:mm:ssX"),
                f.to_timestamp("start_time_utc", "yyyy-MM-dd'T'HH:mm:ss.SSSX"),
                f.to_timestamp("start_time_utc")
            )
        )
        .withColumn("start_utc_epoch", f.col("start_time_utc_ts").cast("long"))

        
        .withColumn("eastern_utc_offset", f.regexp_replace("eastern_utc_offset", r"^([+-]\d{2}):(\d{2})$", r"$1:$2:00"))
        .withColumn("venue_utc_offset",   f.regexp_replace("venue_utc_offset",   r"^([+-]\d{2}):(\d{2})$", r"$1:$2:00"))

       
        .withColumn("e_sign", f.when(f.substring("eastern_utc_offset", 1, 1) == "-", f.lit(-1)).otherwise(f.lit(1)))
        .withColumn("v_sign", f.when(f.substring("venue_utc_offset",   1, 1) == "-", f.lit(-1)).otherwise(f.lit(1)))

        .withColumn("e_h", f.substring("eastern_utc_offset", 2, 2).cast("int"))
        .withColumn("e_m", f.substring("eastern_utc_offset", 5, 2).cast("int"))
        .withColumn("e_s", f.substring("eastern_utc_offset", 8, 2).cast("int"))

        .withColumn("v_h", f.substring("venue_utc_offset", 2, 2).cast("int"))
        .withColumn("v_m", f.substring("venue_utc_offset", 5, 2).cast("int"))
        .withColumn("v_s", f.substring("venue_utc_offset", 8, 2).cast("int"))

        .withColumn("eastern_offset_sec", f.col("e_sign") * (f.col("e_h") * 3600 + f.col("e_m") * 60 + f.col("e_s")))
        .withColumn("venue_offset_sec",   f.col("v_sign") * (f.col("v_h") * 3600 + f.col("v_m") * 60 + f.col("v_s")))
        .withColumn("central_offset_sec", f.col("eastern_offset_sec") - f.lit(3600))  # eastern - 1 hour

        
        .withColumn("start_venue", f.from_unixtime(f.col("start_utc_epoch") + f.col("venue_offset_sec")).cast("timestamp"))
        .withColumn("game_date",   f.coalesce(f.to_date("start_venue"), f.col("game_date")))
        #.withColumn("start_time",  f.date_format(f.from_unixtime(f.col("start_utc_epoch") + f.col("central_offset_sec")).cast("timestamp"), "HH:mm:ss"))
        .withColumn("start_time", f.date_format(f.col("start_time_utc_ts"), "HH:mm:ss"))
        #.withColumn("season", f.lit(f.substring(f.col("game_id").cast("string"), 1, 4).cast("int")))
        
        .select("season", "game_id", "game_date", "game_type", "start_time", "team_abbrev", "team_id", "home_road")
        .withColumn("team_abbrev", f.upper(f.col("team_abbrev")))
        .withColumn("home_road", f.trim(f.col("home_road")))
        
    )
    sched_schema = t.StructType([

        t.StructField("season", t.IntegerType(), False), 
        t.StructField("game_id", t.LongType(), False), 
        t.StructField("game_date", t.DateType(), False), 
        t.StructField("game_type", t.IntegerType(), False), 
        t.StructField("start_time", t.StringType(), True), 
        t.StructField("team_abbrev", t.StringType(), False), 
        t.StructField("team_id", t.IntegerType(), False), 
        t.StructField("home_road", t.StringType(), False),

    ])

    sched_df = apply_schema(sched_df, sched_schema)
    insert_ready = not sched_df.isEmpty()

if insert_ready: 

    sched_df.createOrReplaceTempView("schedules_insert_tmp")
    spark.sql("""

        merge into nhl_data.games.schedules t 
        using schedules_insert_tmp s 
            on t.season = s.season 
            and t.game_id = s.game_id
            and t.team_id = s.team_id
            and s.team_id > 0 
            and s.team_id is not null 
            and t.game_date between 
                date_sub(from_utc_timestamp(current_timestamp(), 'America/Chicago')::date, 2) 
                and 
                from_utc_timestamp(current_timestamp(), 'America/Chicago')::date
        
        when matched and (

            t.game_date <> s.game_date 
            or t.game_type <> s.game_type 
            or not (t.start_time <=> s.start_time) 
            or not (t.home_road <=> s.home_road)

        )

        then update set 

            game_date = s.game_date,
            start_time = s.start_time,
            game_type = s.game_type,
            home_road = s.home_road, 
            update_dte = current_timestamp()

        when not matched then insert (

            season,
            game_id,
            game_date,
            game_type,
            start_time,
            team_abbrev,
            team_id,
            home_road,
            insert_dte,
            update_dte

        )
        values (

            s.season,
            s.game_id,
            s.game_date,
            s.game_type,
            s.start_time,
            s.team_abbrev,
            s.team_id,
            s.home_road,
            current_timestamp(),
            null

        )

        ;
    """)
    spark.catalog.dropTempView("schedules_insert_tmp")
    print(f"Schedules data successfully loaded into nhl_data.games.schedules table")
else: 
    print(f"No new data to insert into nhl_data.games.schedules, skipping insert")

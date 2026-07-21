import sys 
username = spark.sql("select current_user()").first()[0]
sys.path.append(f"/Workspace/Users/{username}/NHL_Pipeline")

from pyspark.sql import SparkSession 
from pyspark.sql import functions as f, types as t, Window as w, DataFrame
from delta.tables import DeltaTable
from zoneinfo import ZoneInfo 
import datetime as dt, re, json, datetime
from pipeline_funcs.schema_utils import convert_case, build_fields, apply_schema, get_schema
from pipeline_funcs.user_utc_region import region_return
from pipeline_funcs.table_maint import run_table_maint

user_region = region_return()
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", f"{user_region}")
central_timezone = ZoneInfo(f"{user_region}")

def wrangle_data(df: DataFrame, pov: str) -> DataFrame:

    pov = pov.lower()
    d = (

        df
        .select(*base_fields, f.col(f"{pov}Team.*"))
        .select("*", f.col("commonName.*"))
        .withColumnRenamed("id", "team_id")
        .withColumnRenamed("default", "team_name")
        .drop("fr", f"{pov}SplitSquad")
        .select("*", f.col("placeName.*"))
        .withColumnRenamed("default", "team_city")
        .withColumnRenamed("abbrev", "team_abbrev")
        .withColumn("team_name", f.concat_ws(' ', f.col("team_city"), f.col("team_name")))
        .withColumn("team_name", f.regexp_replace(f.col("team_name"), r"\b(.+?)\s+(?=\1\b)", ""))
        .withColumn("home_road", f.lit(f"{pov.upper()}"))
        .select(*final_fields, "team_city")
        .filter(
            
            (f.col("game_date").isNotNull()) & 
            (f.col("game_type").isNotNull())
            
            )
    )


    return d

def clean_data(df: DataFrame) -> DataFrame:

    d = (

        df
        .withColumn("team_abbrev", f.upper(f.trim(f.col("team_abbrev"))))
        .withColumn("team_name", f.trim(f.col("team_name")))
        .withColumn("py_source", f.lit(py_source))
    
    )

    d = (

        d
        .toDF(*[convert_case(c) for c in d.columns])
        .withColumn("team_abbrev", f.upper(f.col("team_abbrev")))
        .withColumn("unused_structs", f.lit(" | ".join(compare)))
        .withColumn("num_unused_structs", f.lit(len(compare)))
    )

    return d

sched_raw = spark.sql(f"""
                      
                      with date_param as (

                        select from_utc_timestamp(current_timestamp(), '{user_region}')::date as current_run_date
                        
                      )
                      
                      select /*+ broadcast (p) */
                        request_key, 
                        payload
                      from nhl_data_raw.games.schedules a
                      cross join date_param p 
                      where 1 = 1
                        ---below pulls in game on the current date or games that were ingested within 
                        ---the last 7 days (this is for playoff games since the NHL will publish the series schedule for the first 6 games 
                        ---but then the series ends before the game is scheduled))
                        and http_status = 200
                        and (
                            from_utc_timestamp(ingest_ts_utc, '{user_region}')::date = current_run_date
                            or 
                            from_utc_timestamp(ingest_ts_utc, '{user_region}')::date between date_sub(current_run_date, 7) and current_run_date
                            )
                      
    """)
ready = not sched_raw.isEmpty()

field_mapping = {

        "season": {
                    "target": "season", 
                    "type": "integer"
        },
        "id":   {
                    "target": "game_id", 
                    "type": "bigint"
        },
        "date": {
                    "target": "game_date", 
                    "type": "date"
        },
        "gameType": {
                    "target": "game_type", 
                    "type": "integer"
        },
        "startTimeUTC": {

                    "target": "startTimeUTC", 
                    "type": "string"
        },
        "easternUTCOffset": {
                    "target": "easternUTCOffset",
                    "type": "string"
        },
        "venueUTCOffset": {
                    "target": "venueUTCOffset", 
                    "type": "string"
        },
        "venueTimezone": {
                    "target": "venueTimezone", 
                    "type": "string"
        },
        "regularSeasonStartDate": {

                    "target": "regularSeasonStartDate", 
                    "type": "date"
        }, 
        "regularSeasonEndDate": {
                    "target": "regularSeasonEndDate",
                    "type": "date"
        },
        "preSeasonStartDate": {
                    "target": "preSeasonStartDate", 
                    "type": "date"
        },
        "playoffEndDate": {
                    "target": "playoffEndDate", 
                    "type": "date"
        },
        "neutralSite": {
                    "target": "neutralSite", 
                    "type": "boolean"
        }
}

insert_ready = False
update_ready = True
if ready: 
    
    py_source = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")[-1]
    sched_raw_schema = get_schema(sched_raw)
    base_fields = [

        "season", "game_id", "game_date", "game_type", "startTimeUTC", "easternUTCOffset", "venueUTCOffset", "venueTimezone",
        "regularSeasonStartDate", "regularSeasonEndDate", "preSeasonStartDate", "playoffEndDate", "neutralSite"
    ]

    final_fields = base_fields + ["team_abbrev", "team_id", "team_name", "home_road"]

    sched_tmp = (

        sched_raw
        .withColumn("json", f.from_json(f.col("payload"), sched_raw_schema))
        .transform(lambda df: df.select("json.*", *[x for x in df.columns if x in base_fields]))
        .transform(lambda df: df.select(*[x for x in df.columns if x in base_fields], f.explode_outer("gameWeek").alias("gw")))
        .transform(lambda df: df.select(*[x for x in df.columns if x in base_fields], "gw.*"))
        .withColumn("game_raw", f.explode_outer("games"))
    )
    add_fields_expr = [build_fields(src_col, rule, sched_tmp.columns) for src_col, rule in field_mapping.items()]
    sched_df1 = (

        sched_tmp
        .select(*[x for x in sched_tmp.columns if x != "game_raw"], "game_raw.*")
        .transform(lambda df: df.select(*[build_fields(src_col, rule, df.columns) for src_col, rule in field_mapping.items()], "awayTeam", "homeTeam"))
    )

    nested_fields = {
            
            field.name: [subfield.name for subfield in field.dataType.fields if isinstance(subfield.dataType, t.StructType)]
            for field in sched_df1.schema.fields
            if isinstance(field.dataType, t.StructType)
        }
    unused_fields = list(nested_fields.values())
    compare = unused_fields[0] if len(unused_fields) > 0 else None

    sched_schema = t.StructType([

            t.StructField("season", t.IntegerType(), False),
            t.StructField("pre_season_start_date", t.DateType(), True),
            t.StructField("regular_season_start_date", t.DateType(), True), 
            t.StructField("regular_season_end_date", t.DateType(), True),
            t.StructField("playoff_end_date", t.DateType(), True),
            t.StructField("game_id", t.LongType(), False), 
            t.StructField("game_date", t.DateType(), False), 
            t.StructField("game_type", t.IntegerType(), False), 
            t.StructField("start_time_utc", t.StringType(), True), 
            t.StructField("eastern_utc_offset", t.StringType(), True), 
            t.StructField("venue_utc_offset", t.StringType(), True), 
            t.StructField("venue_timezone", t.StringType(), True), 
            t.StructField("team_abbrev", t.StringType(), False), 
            t.StructField("team_id", t.IntegerType(), False), 
            t.StructField("team_name", t.StringType(), True), 
            t.StructField("home_road", t.StringType(), False), 
            t.StructField("team_city", t.StringType(), True),
            t.StructField("neutral_site", t.BooleanType(), True), 
            t.StructField("py_source", t.StringType(), False), 
            t.StructField("unused_structs", t.StringType(), True), 
            t.StructField("num_unused_structs", t.IntegerType(), True)

    ])

    away_teams = sched_df1.transform(wrangle_data, "away")
    home_teams = sched_df1.transform(wrangle_data, "home")
    sched_silver = (
                    away_teams
                    .unionByName(home_teams)
                    .transform(clean_data)
                    .select(

                            "season", 
                            "game_id", 
                            "game_date",
                            "game_type", 
                            "start_time_utc",
                            "eastern_utc_offset", 
                            "venue_utc_offset", 
                            "venue_timezone", 
                            "pre_season_start_date", 
                            "regular_season_start_date", 
                            "regular_season_end_date", 
                            "playoff_end_date",
                            "neutral_site",
                            "team_id", 
                            f.upper(f.trim(f.col("team_abbrev"))).alias("team_abbrev"), 
                            f.trim("team_name").alias("team_name"), 
                            f.upper(f.col("home_road")).alias("home_road"), 
                            f.trim("team_city").alias("team_city"), 
                            "unused_structs", 
                            "num_unused_structs", 
                            "py_source"
                    )
                    .transform(apply_schema, sched_schema)
                    .filter(
                        (f.col("game_id").isNotNull()) & 
                        (f.lower(f.col("team_abbrev")) != "tbd")

                    )
                    .dropDuplicates(["season", "game_id", "game_date", "home_road"])
                    )
    insert_ready = not sched_silver.isEmpty()

if insert_ready: 

    sched_silver.createOrReplaceTempView("schedules_insert_tmp")
    spark.sql(f"""
            
            merge into nhl_data_staged.games.schedules t 
            using schedules_insert_tmp s 
                on t.season = s.season 
                and t.game_id = s.game_id
                and t.team_id = s.team_id 
                and t.home_road = s.home_road 
                ---not merging on game date in case of a rare circumstance where a game date gets changed, but do want to limit target table to games within the past week
                and (
                    (
                        t.game_date between 
                        date_sub(from_utc_timestamp(current_timestamp(), '{user_region}')::date, 7) 
                        and 
                        from_utc_timestamp(current_timestamp(), '{user_region}')::date
                    )
                    or 
                    (
                        from_utc_timestamp(t.insert_dte, '{user_region}')::date between 
                        date_sub(from_utc_timestamp(current_timestamp(), '{user_region}')::date, 7)
                        and 
                        from_utc_timestamp(current_timestamp(), '{user_region}')::date
                    )
                )
            when matched and (

                    t.game_date <> s.game_date 
                    or t.game_type <> s.game_type
                    or coalesce(t.pre_season_start_date, '1900-01-01'::date) <> coalesce(s.pre_season_start_date, t.pre_season_start_date, '1900-01-01'::date)
                    or coalesce(t.playoff_end_date, '1900-01-01'::date) <> coalesce(s.playoff_end_date, t.playoff_end_date, '1900-01-01'::date)
                    or coalesce(t.start_time_utc, '0') <> coalesce(s.start_time_utc, t.start_time_utc, '0')
                    or coalesce(t.eastern_utc_offset, '0') <> coalesce(s.eastern_utc_offset, t.eastern_utc_offset, '0')
                    or coalesce(t.venue_utc_offset, '0') <> coalesce(s.venue_utc_offset, t.venue_utc_offset, '0')
                    or coalesce(t.venue_timezone, 'NONE') <> coalesce(s.venue_timezone, t.venue_timezone, 'NONE')


                
                )    
                    
            then update set 

                pre_season_start_date = s.pre_season_start_date,
                regular_season_start_date = s.regular_season_start_date,
                regular_season_end_date = s.regular_season_end_date,
                playoff_end_date = s.playoff_end_date,
                game_date = s.game_date,
                start_time_utc = s.start_time_utc,
                eastern_utc_offset = s.eastern_utc_offset,
                venue_utc_offset = s.venue_utc_offset,
                game_type = s.game_type, 
                unused_structs = s.unused_structs,          
                num_unused_structs = s.num_unused_structs,
                update_dte = current_timestamp(),
                py_source = s.py_source
                
            when not matched then insert (

                season,
                pre_season_start_date,
                regular_season_start_date,
                regular_season_end_date,
                playoff_end_date,
                game_id,
                game_date,
                game_type,
                start_time_utc,
                eastern_utc_offset,
                venue_utc_offset,
                venue_timezone,
                team_abbrev,
                team_id,
                team_name,
                home_road,
                team_city,
                neutral_site,
                insert_dte,
                update_dte,
                py_source,
                unused_structs,
                num_unused_structs

            )

            values (

                s.season,
                s.pre_season_start_date,
                s.regular_season_start_date,
                s.regular_season_end_date,
                s.playoff_end_date,
                s.game_id,
                s.game_date,
                s.game_type,
                s.start_time_utc,
                s.eastern_utc_offset,
                s.venue_utc_offset,
                s.venue_timezone,
                s.team_abbrev,
                s.team_id,
                s.team_name,
                s.home_road,
                s.team_city,
                s.neutral_site,
                current_timestamp(),
                null,
                s.py_source,
                s.unused_structs,
                s.num_unused_structs

            )
        ---below handles instances where a playoff game gets scheduled but the series ends before that game is played, therefore making it no longer necessary
        when not matched by source 
            and t.game_date >= current_date()
            and t.game_type = 3
        then delete 
        ;
            
    """)
    #spark.catalog.dropTempView("schedules_insert_tmp")
    print(f"Schedules data successfully loaded into nhl_data_staged.games.schedules table")
    run_table_maint(spark, "nhl_data_staged.games.schedules")
else: 
    print(f"No new data to insert into nhl_data_staged.games.schedules, skipping insert")

if update_ready: 

    #logic below goes in and flags games from the prior day as no longer being in play
    #this can be done in this script because schedules scrapes are only ran once a day 
    spark.sql(f"""
              
            update nhl_data_staged.games.pbp_data 
            set game_in_play = false,
                update_dte = current_timestamp()
            where 1 = 1
                and game_in_play = true 
                and game_date = date_sub(from_utc_timestamp(current_timestamp(), '{user_region}')::date, 1) 
            
    """)
    
    print(f"Games in play field successfully updated in nhl_data_staged.games.pbp_data table")

    spark.sql(f"""
              
            update nhl_data_staged.games.shift_data 
            set game_in_play = false,
                update_dte = current_timestamp()
            where 1 = 1
                and game_in_play = true 
                and game_date = date_sub(from_utc_timestamp(current_timestamp(), '{user_region}')::date, 1)
              
    """)
    
    spark.catalog.dropTempView("schedules_insert_tmp")
    print(f"Games in play field successfully updated in nhl_data_staged.games.shift_data table")

else: 
    print(f"No new data to update, skipping update")

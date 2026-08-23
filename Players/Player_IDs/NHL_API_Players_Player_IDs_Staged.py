import sys 
username = spark.sql("select current_user()").first()[0]
sys.path.append(f"/Workspace/Users/{username}/NHL_Pipeline")

from pyspark.sql import SparkSession 
from pyspark.sql import functions as f, types as t, window as w, DataFrame
from delta.tables import DeltaTable
from zoneinfo import ZoneInfo 
import datetime, re
from pipeline_funcs.schema_utils import convert_case, apply_schema, build_fields, get_schema
from pipeline_funcs.user_utc_region import region_return
from pipeline_funcs.table_maint import run_table_maint

user_region = region_return()
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", f"{user_region}")
central_timezone = ZoneInfo(f"{user_region}")

def mget(map_col, keys_):
    
    # returns first non-null value among the provided map keys
    return f.coalesce(*[f.col(map_col).getItem(k) for k in keys_])

def explode_data(df):

    schema = t.StructType([
                        t.StructField("data", 
                                      t.ArrayType(t.MapType(t.StringType(), t.StringType())), 
                                      True),
                        ])

    flat = (
        df
        .withColumn("j", f.from_json(f.col("payload"), schema))
        .withColumn("rec", f.explode_outer(f.col("j.data")))
    )

    keys = (
        flat
        .select(f.explode(f.map_keys("rec")).alias("k"))
        .distinct()
        .collect()
    )
    keys = [r["k"] for r in keys]

    wide = flat.select(
        *[f.col("rec").getItem(k).alias(k) for k in keys]
    )

    return wide

field_mapping = {

            "player_id": {

                    "target": "player_id", 
                    "type": "integer"
            },

           "name": {
                    "target": "player_name", 
                    "type": "string", 
                    "trim": True
           },

           "position_code": {
               
                    "target": "player_pos", 
                    "type": "string", 
                    "upper": True, 
                    "trim": True
           },

           "team_id": {
               
                    "target": "team_id", 
                    "type": "integer"
           },

           "team_abbrev": {
               
                    "target": "team_abbrev", 
                    "type": "string", 
                    "upper": True,
                    "trim": True
           },

           "last_team_id": {
               
                    "target": "team_id_prev_team", 
                    "type": "integer", 
                  
           },

           "last_team_abbrev": {
               
                    "target": "team_abbrev_prev_team", 
                    "type": "string", 
                    "upper": True,
                    "trim": True
           },

           "last_season_id": {
               
                    "target": "last_active_season", 
                    "type": "integer", 
           },

           "sweater_number": {
               
                    "target": "jersey_num", 
                    "type": "integer",
           },

           "active": {
               
                    "target": "is_active", 
                    "type": "boolean"
           }
}

keep_fields = ['player_id', 'player_name', 'player_pos', 'player_pos_cat', 'team_id', 'team_abbrev', 'team_id_prev_team', 'team_abbrev_prev_team', 'last_active_season', 'jersey_num', 'shoots_catches', 'is_active']
payload_type = t.ArrayType(t.MapType(t.StringType(), t.StringType()))

players_raw = spark.sql(f"""
                        
                        select 
                            request_key,
                            payload, 
                            ingest_ts_utc
                        from nhl_data_raw.players.player_search
                        where 1 = 1
                            and from_utc_timestamp(ingest_ts_utc, '{user_region}')::date >= from_utc_timestamp(current_timestamp(), '{user_region}')::date
                            and payload is not null 
                            and payload not in ('[]', '{{}}')
                            and size(from_json(payload, 'data ARRAY<STRING>').data) > 0
                        qualify row_number() over (partition by request_key order by ingest_ts_utc desc) = 1
    """)
                        
skaters_raw = spark.sql(f"""
                        
                        select 
                            request_key,
                            payload, 
                            ingest_ts_utc
                        from nhl_data_raw.players.player_search_season 
                        where 1 = 1 
                            and from_utc_timestamp(ingest_ts_utc, '{user_region}')::date >= from_utc_timestamp(current_timestamp(), '{user_region}')::date
                            and endpoint = 'skater_summary'
                            and payload is not null 
                            and payload not in ('[]', '{{}}')    
                            and size(from_json(payload, 'data ARRAY<STRING>').data) > 0 
                        qualify row_number() over (partition by endpoint, request_key order by ingest_ts_utc desc) = 1

    """)
                        
goalies_raw = spark.sql(f"""
                        
                        select 
                            request_key, 
                            payload, 
                            ingest_ts_utc
                        from nhl_data_raw.players.player_search_season 
                        where 1 = 1
                            and from_utc_timestamp(ingest_ts_utc, '{user_region}')::date >= from_utc_timestamp(current_timestamp(), '{user_region}')::date
                            and endpoint = 'goalie_summary'
                            and payload is not null 
                            and payload not in ('[]', '{{}}')
                            and size(from_json(payload, 'data ARRAY<STRING>').data) > 0
                        qualify row_number() over (partition by endpoint, request_key order by ingest_ts_utc desc) = 1

    """)

player_schema = t.StructType([

    t.StructField("playerId", t.StringType()),
    t.StructField("name", t.StringType()),
    t.StructField("positionCode", t.StringType()),
    t.StructField("teamId", t.StringType()),
    t.StructField("teamAbbrev", t.StringType()),
    t.StructField("lastTeamId", t.StringType()),
    t.StructField("lastTeamAbbrev", t.StringType()),
    t.StructField("lastSeasonId", t.StringType()),
    t.StructField("sweaterNumber", t.IntegerType()),
    t.StructField("active", t.BooleanType()),
    t.StructField("height", t.StringType()),
    t.StructField("heightInInches", t.IntegerType()),
    t.StructField("heightInCentimeters", t.IntegerType()),
    t.StructField("weightInPounds", t.IntegerType()),
    t.StructField("weightInKilograms", t.IntegerType()),
    t.StructField("birthCity", t.StringType()),
    t.StructField("birthStateProvince", t.StringType()),
    t.StructField("birthCountry", t.StringType())
    
])
ready = not players_raw.isEmpty()

keep_going = False 
if ready: 
    #player_schema = t.StructType([t.StructField("data", t.ArrayType(player_schema)])
    player_schema = get_schema(players_raw)

    players_data = (
        players_raw
        .withColumn("json_data", f.from_json(f.col("payload"), player_schema))
        .withColumn("player_data", f.explode("json_data.data"))
        .select("player_data.*")
        .transform(lambda df: df.toDF(*[convert_case(c) for c in df.columns]))
        
    )
    add_fields_expr = [build_fields(src_col, rule, players_data.columns) for src_col, rule in field_mapping.items()]

    required = ["player_id", "player_name", "team_id", "team_abbrev", "last_active_season", "is_active"]
    players_silver = (

        players_data
        .select(*add_fields_expr)
        .withColumn(
            "player_pos_cat",
            f.when(f.lower(f.col("player_pos")).isin(["l", "r", "c"]), f.lit("F"))
            .when(f.lower(f.col("player_pos")) == "d", f.lit("D"))
            .when(f.lower(f.col("player_pos")) == "g", f.lit("G"))
        )
        .withColumn("shoots_catches", f.lit(None))
        .select(*keep_fields)
        .withColumn("required_field_empty_rate",
                            f.round(
                                (
                                    sum(f.col(c).isNull().cast("int") for c in required) / f.lit(len(required))
                                ),
                                2
                            )
        )
        .dropDuplicates(["player_id"])
    )
    keep_going = not players_silver.isEmpty()

insert_ready = False
if keep_going: 
    py_source = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")[-1]
    shoots_lookup = (


                explode_data(skaters_raw)
                .transform(lambda c: c.toDF(*[convert_case(c) for c in c.columns]))
                .select("player_id", "shoots_catches")
                .unionByName(

                    explode_data(goalies_raw)
                    .transform(lambda c: c.toDF(*[convert_case(c) for c in c.columns]))
                    .select("player_id", "shoots_catches")
                        
                )   
                .withColumnRenamed("shoots_catches", "shoots_catches_lu")
                .dropDuplicates(['player_id'])

    )
    player_id_window = w.Window.partitionBy("player_id").orderBy(f.col("is_active").desc(), f.col("required_field_empty_rate").asc())

    players_silver = (

                players_silver 
                .join(f.broadcast(shoots_lookup), how = "left", on = "player_id")
                .withColumn("shoots_catches", f.coalesce("shoots_catches_lu", "shoots_catches"))
                .select(*keep_fields, "required_field_empty_rate")
                .filter(f.col("player_id").isNotNull())
                .drop("player_pos_cat")
                .withColumn("row_num", f.row_number().over(player_id_window))
                .withColumn("py_source", f.lit(py_source))
                .filter(f.col("row_num") == 1)

    )

    players_silver_schema = t.StructType([


        t.StructField("player_id", t.LongType(), False),
        t.StructField("player_name", t.StringType(), False), 
        t.StructField("player_pos", t.StringType(), True), 
        t.StructField("team_id", t.IntegerType(), True),
        t.StructField("team_abbrev", t.StringType(), True), 
        t.StructField("team_id_prev_team", t.IntegerType(), True), 
        t.StructField("team_abbrev_prev_team", t.StringType(), True), 
        t.StructField("last_active_season", t.IntegerType(), True), 
        t.StructField("jersey_num", t.IntegerType(), True), 
        t.StructField("shoots_catches", t.StringType(), True), 
        t.StructField("is_active", t.BooleanType(), True),        
        t.StructField("required_field_empty_rate", t.DecimalType(4,2), False),
        t.StructField("py_source", t.StringType(), False)
    
    ])
    players_silver = apply_schema(players_silver, players_silver_schema)
    insert_ready = not players_silver.isEmpty()
    missing_player_ids = ( 
                        players_silver 
                        .filter(f.col("player_id").isNull())
                        .limit(1)
                        .count()
            )
    if missing_player_ids > 0: 
        raise Exception("Missing player ids detected in players_silver source")
    dupes = ( 
            
            players_silver 
            .groupBy("player_id")
            .count() 
            .filter(f.col("count") > 1)
            .limit(1) 
            .count()
    )
    if dupes > 0: 
        raise Exception("Duplicate player ids detected in players_silver source")

if insert_ready: 

    players_silver.createOrReplaceTempView("players_tmp")
    spark.sql(f"""
                
            with player_data as (

                select 
                    a.player_id,
                    a.player_name,
                    a.player_pos,
                    coalesce(a.team_id, b.team_id) as team_id,
                    coalesce(a.team_abbrev, b.team_abbrev) as team_abbrev,
                    a.team_id_prev_team,
                    a.team_abbrev_prev_team,
                    a.last_active_season,
                    a.jersey_num,
                    a.shoots_catches,
                    a.is_active,
                    a.required_field_empty_rate
                from players_tmp a   
                left join nhl_data_staged.players.master_ids b  
                    on a.player_id = b.player_id 

            )
            , 
            src as (

                select 
                    player_id, 
                    player_name, 
                    player_pos, 
                    team_id, 
                    team_abbrev,
                    case when team_id = team_id_prev_team then null
                        when is_active = false then coalesce(team_id, team_id_prev_team)
                        else team_id_prev_team 
                        end as team_id_prev_team,
                    case when team_id = team_id_prev_team then null
                        when is_active = false then coalesce(team_abbrev, team_abbrev_prev_team)
                        else team_abbrev_prev_team
                        end as team_abbrev_prev_team,
                    last_active_season,
                    jersey_num,
                    shoots_catches,
                    is_active,
                    required_field_empty_rate
                from player_data 

            )
        
            merge into nhl_data_staged.players.master_ids t 
            using src s 
                on t.player_id = s.player_id

            when matched and (

                not (t.player_name <=> coalesce(s.player_name, t.player_name))
                or not (t.team_id <=> coalesce(s.team_id, t.team_id))
                or not (t.jersey_num <=> s.jersey_num) 
                or not (t.shoots_catches <=> s.shoots_catches)
                or not (t.player_pos <=> coalesce(s.player_pos, t.player_pos))
                or coalesce(t.is_active, false) <> coalesce(s.is_active, false)

            )
                    
            then update set 

                player_name = coalesce(s.player_name, t.player_name),
                team_id = coalesce(s.team_id, t.team_id),
                team_abbrev = coalesce(s.team_abbrev, t.team_abbrev),
                ---purpose is to move the target team_id to the team_id_prev_team spot if the player has moved to a new team
                team_id_prev_team = case 
                                        ---if the target source team_id is the same as the target team_id and team_id_prev_team then set to null  
                                        when coalesce(t.team_id, s.team_id) = s.team_id and t.team_id_prev_team = s.team_id then null 
                                        ---if the target team_id is null then use the first non-null of target/source team_id_prev_team
                                        when t.team_id is null then coalesce(t.team_id_prev_team, s.team_id_prev_team) 
                                        ---if the target team_id is not equal to the source team_id then move the target team_id to the target team_id_prev_team spot
                                        when not (t.team_id <=> coalesce(s.team_id, t.team_id)) then t.team_id
                                        ---if the target team_id_prev_team is null then leave it null 
                                        when t.team_id_prev_team is null then null
                                        ---if the target team id equals source team_id then use the target team_id_prev_team 
                                        when t.team_id <=> coalesce(s.team_id, t.team_id) then t.team_id_prev_team
                                        else t.team_id 
                                        end, 
                team_abbrev_prev_team = case 
                                        when coalesce(t.team_id, s.team_id) = s.team_id and t.team_id_prev_team = s.team_id then null 
                                        when t.team_id is null then coalesce(t.team_abbrev_prev_team, s.team_abbrev_prev_team)
                                        when not (t.team_id <=> coalesce(s.team_id, t.team_id)) then t.team_abbrev
                                        when t.team_id_prev_team is null then null
                                        when t.team_id <=> coalesce(s.team_id, t.team_id) then t.team_abbrev_prev_team
                                        else t.team_abbrev
                                        end, 
                last_active_season = case when t.last_active_season is null and s.last_active_season is null then null 
                                        else greatest(
                                                coalesce(s.last_active_season, 19001901),
                                                coalesce(t.last_active_season, 19001901)
                                                )
                                        end,
                jersey_num = coalesce(s.jersey_num, t.jersey_num),
                player_pos = coalesce(s.player_pos, t.player_pos),
                is_active = coalesce(s.is_active, t.is_active),
                update_dte = current_timestamp(),
                required_field_empty_rate = s.required_field_empty_rate,
                py_source = '{py_source}',
                failed_condition = nullif(
                                            concat_ws(
                                                ', '
                                                , filter(
                                                    array(
                                                            case when not (t.player_name <=> coalesce(s.player_name, t.player_name)) then 'player_name' end,
                                                            case when not (t.team_id <=> coalesce(s.team_id, t.team_id)) then 'team_id' end,
                                                            case when not (t.jersey_num <=> s.jersey_num) then 'jersey_num' end,
                                                            case when not (t.player_pos <=> s.player_pos) then 'player_pos' end,
                                                            case when coalesce(t.is_active, false) <> coalesce(s.is_active, false) then 'is_active' end,
                                                            case when t.required_field_empty_rate <> s.required_field_empty_rate then 'required_field_empty_rate' end
                                                    )
                                                    , x -> x is not null
                                                )
                                            )
                                            , ''
                                        )


            when not matched then insert (

                player_id,
                player_name,
                player_pos,
                team_id,
                team_abbrev,
                team_id_prev_team,
                team_abbrev_prev_team,
                last_active_season,
                jersey_num,
                shoots_catches,
                is_active,
                insert_dte,
                update_dte,
                required_field_empty_rate,
                py_source,
                failed_condition

            )

            values (

                s.player_id,
                s.player_name,
                s.player_pos,
                s.team_id,
                s.team_abbrev,
                s.team_id_prev_team,
                s.team_abbrev_prev_team,
                s.last_active_season,
                s.jersey_num,
                s.shoots_catches,
                s.is_active,
                current_timestamp(),
                null,
                s.required_field_empty_rate,
                '{py_source}',
                null
            )
                    
    """)
    spark.catalog.dropTempView("players_tmp")
    print("Player ids successfully loaded into nhl_data_staged.players.master_ids table")
    run_table_maint(spark, "nhl_data_staged.players.master_ids")
    run_table_maint(spark, "nhl_data.players.master_ids")
else: 
    print("No new batch of player ids found, skipping insert")

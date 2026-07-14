import sys 
username = spark.sql("select current_user()").first()[0]
sys.path.append(f"/Workspace/Users/{username}/NHL_Pipeline")

from pyspark.sql import SparkSession
from pyspark.sql import functions as f, types as t, Window as w, DataFrame
from delta.tables import DeltaTable
from zoneinfo import ZoneInfo
from functools import reduce
import re, json, time, datetime
from pipeline_funcs.games import get_games
from pipeline_funcs.schema_utils import convert_case, build_fields, apply_schema, get_schema

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "America/Chicago")

def flatten_df(df):
    
    while True:
        # get struct + array columns
        complex_fields = [(field.name, field.dataType)
                          for field in df.schema.fields
                          if isinstance(field.dataType, (t.ArrayType, t.StructType))]

        if not complex_fields:
            break

        col_name, dtype = complex_fields[0]

        # explode arrays
        if isinstance(dtype, t.ArrayType):
            df = df.withColumn(col_name, f.explode_outer(col_name))

        # expand structs
        elif isinstance(dtype, t.StructType):
            expanded = [f.col(f"{col_name}.{c.name}").alias(f"{col_name}_{c.name}")
                        for c in dtype.fields]
            df = df.select("*", *expanded).drop(col_name)

    return df

kickoff = not get_games(spark, "nhl_data_staged.games.pbp_data").isEmpty()
if kickoff: 
    #section below handles retries for games where there was missing play by play data
    #the retry logic relies on an index created based on game_date in the schedules table rather than relying on the current_date() function
    #because we want to retry every 15 days per the NHL schedule 
    run_missing = spark.sql(f"""
    
        with season_param as (
            
            ---if table hasn't been populated, use 19001901 for season to indicate a cold start is needed 
            select 
                coalesce(max(season), 19001901) as pbp_table_season
            from nhl_data_staged.games.pbp_data 
            where 1 = 1
                and game_date <= from_utc_timestamp(current_timestamp(), 'America/Chicago')::date

        )      
        ,
        current_season_dates as (

                select /*+ broadcast (p) */ distinct 
                    a.season, 
                    a.game_date,
                    (p.pbp_table_season = 19001901)::boolean as cold_start_ind
                from nhl_data_staged.games.schedules 
                cross join season_param p
                where 1 = 1
                    and a.game_date <= from_utc_timestamp(current_timestamp(), 'America/Chicago')::date
                qualify a.season = max(a.season) over()
        )
        , 
        current_season_dates_idx as (

            select 
                a.*, 
                row_number() over (partition by a.season order by a.game_date) as date_idx 
            from current_season_dates a
            
        )

        select 
            a.*, 
            (a.date_idx % 15 = 0 or cold_start_ind = true)::boolean as run_missing_ind
        from current_season_dates_idx a
        order by a.game_date desc 
        limit 1 
    
    """)
    run_missing_ind = run_missing.select(f.col("run_missing_ind").alias("rmi")).first()["rmi"]
    pbp_schema = spark.sql("""
                           
                    select schema_of_json_agg(payload) as json_schema 
                    from nhl_data_raw.games.pbp_data 
                    where 1 = 1
                        and payload is not null 
                        and http_status = 200 
                           
    """).first()["json_schema"]
    if run_missing_ind:
        
        spark.sql(f"""
                  
                  
                with staged as (
                    
                    select distinct 
                        a.season, 
                        a.game_date,
                        a.game_id,
                        a.start_time_utc,
                        from_json(payload, '{pbp_schema}') as payload_json
                    from nhl_data_staged.games.schedules a  
                    left join nhl_data_raw.games.pbp_data b 
                        on a.game_id = b.request_key 
                    where 1 = 1
                        ---want to avoid games that are in play today in the vent that the game hasn't started yet or the data feed is slightly delayed
                        ---to avoid current day games getting added to the games_missing_pbp table
                        and a.game_date < from_utc_timestamp(current_timestamp(), 'America/Chicago')::date
                        and a.game_type in (2,3)

                )
                , 
                src as (
                    
                    select * 
                    from staged 
                    where 1 = 1
                        and payload_json is not null 
                        and size(payload_json.plays) = 0 

                )
                
                
                merge into nhl_data_staged.ops.games_missing_pbp t 
                using src s 
                    on t.season = s.season
                    and t.game_id = s.game_id

                when matched and (
                    
                    ---using condition check below to ensure that only one retry happens per day
                    t.last_attempt_dte <> from_utc_timestamp(current_timestamp(), 'America/Chicago')::date

                )
                then update set 

                    last_attempt_dte = from_utc_timestamp(current_timestamp(), 'America/Chicago')::date,
                    next_retry_dte = date_add(from_utc_timestamp(current_timestamp(), 'America/Chicago')::date, 15),
                    attempt_count = t.attempt_count + 1,
                    update_dte = current_timestamp()

                when not matched then insert (

                    season, 
                    game_id, 
                    last_attempt_dte,
                    next_retry_dte,
                    attempt_count,
                    insert_dte,
                    update_dte

                )
                  
                values (

                    s.season,
                    s.game_id,
                    from_utc_timestamp(current_timestamp(), 'America/Chicago')::date,
                    date_add(from_utc_timestamp(current_timestamp(), 'America/Chicago')::date, 15),
                    1,
                    current_timestamp(),
                    null
                )
                  
                when not matched by source then delete;
                  
        """)
        print(f"Data successfully inserted/update into nhl_data_staged.ops.games_missing_pbp table")
    else: 
        print(f"Skipping insert since current season game date is not eligble")
else: 
    print(f"No new data found, skipping insert")

if kickoff: 
    games = spark.sql("""
                                        
                        ---pull in all games up until the current date (inclusive)
                        with date_param as (

                            select from_utc_timestamp(current_timestamp(), 'America/Chicago')::date as current_run_date

                        ) 
                        , 
                        games as (

                            select /*+ broadcast (p) */ distinct
                                a.season,
                                a.game_id,
                                a.game_date,
                                a.start_time_utc
                            from nhl_data_staged.games.schedules a
                            cross join date_param p 
                            where 1 = 1
                                and a.game_type in (2, 3)
                                and a.game_date <= p.current_run_date


                        )
                        ,
                        ---pull in games that have ended today since we want to continue to scrape them to ensure data is as accurate a
                        ---and as up to date as possible 
                        games_ended_today as (

                            select /*+ broadcast(b), broadcast(p) */ distinct
                                a.season,
                                a.game_id,
                                a.game_date,
                                b.start_time_utc
                            from nhl_data_staged.games.pbp_data a 
                            inner join games b
                                on a.season = b.season
                                and a.game_id = b.game_id
                                and a.game_date = b.game_date
                            cross join date_param p
                            where 1 = 1
                                and a.game_date = p.current_run_date
                                and a.event_type = 'game-end'


                        )
                        ,
                        ---pull in games that are in play today and where the current timestamp is >= the start time + 15 minutes
                        ---since NHL games typically don't start for at least 15 minutes after start time
                        games_in_play as (

                            select /*+ broadcast (p) */ distinct
                                    a.season,
                                    a.game_id,
                                    a.game_date,
                                    a.start_time_utc
                            from games a
                            left anti join games_ended_today b 
                                on a.season = b.season
                                and a.game_id = b.game_id
                                and a.game_date = b.game_date 
                            cross join date_param p 
                            where 1 = 1
                                and a.game_date = p.current_run_date
                                and from_utc_timestamp(current_timestamp(), 'America/Chicago') >= from_utc_timestamp(a.start_time_utc, 'America/Chicago') + interval 15 minutes

                        )
                        ,
                        ---pull in games that were played in the prior two days to scrape again and ensure data accuracy 
                        games_prior_two as (

                            select /*+ broadcast (p) */
                                a.season,
                                a.game_id,
                                a.game_date,
                                a.start_time_utc
                            from games a 
                            cross join date_param p
                            where 1 = 1
                                and a.game_date between 
                                    date_sub(p.current_run_date, 2) 
                                    and 
                                    date_sub(p.current_run_date, 1)
                                

                        )
                        ,
                        ---pull in games that have been loaded into the pbp table already 
                        games_loaded as (

                            select distinct
                                game_id
                            from nhl_data_staged.games.pbp_data
                            where 1 = 1
                                and game_id is not null

                        )
                        ,
                        ---pull in games that have ended and occured before current date
                        games_ended as (

                            select /*+ broadcast (p) */ distinct
                                a.game_id
                            from nhl_data_staged.games.pbp_data a 
                            cross join date_param p
                            where 1 = 1
                                and a.game_id is not null
                                and a.game_date < p.current_run_date
                                and a.event_type = 'game-end'

                        )
                        ,
                        ---pull in games that didn't have any pbp data in the API
                        games_missing_from_api as (

                            select 
                                game_id
                            from nhl_data_staged.ops.games_missing_pbp

                        )
                        ,
                        /* historical games never loaded */
                        games_not_loaded as (

                            select /*+ broadcast (p) */
                                a.season,
                                a.game_id,
                                a.game_date,
                                a.start_time_utc
                            from games a
                            left anti join games_loaded b
                                on a.game_id = b.game_id
                            cross join date_param p 
                            where 1 = 1
                                and a.game_date < p.current_run_date

                        )
                        ,
                        final_games as (

                            select 
                                "in play" as which_game,
                                season, 
                                game_id, 
                                game_date, 
                                start_time_utc
                            from games_in_play 
                            union 
                            select 
                                "ended today" as which_game,
                                season, 
                                game_id, 
                                game_date, 
                                start_time_utc
                            from games_ended_today 
                            union 
                            select 
                                "not loaded" as which_game,
                                season, 
                                game_id, 
                                game_date, 
                                start_time_utc
                            from games_not_loaded a 
                            left anti join games_prior_two b 
                                on a.game_id = b.game_id 
                            left anti join games_missing_from_api c 
                                on a.game_id = c.game_id
                            union 
                            select 
                                "last two" as which_game,
                                season, 
                                game_id, 
                                game_date,
                                start_time_utc
                            from games_prior_two

                        )

                        select 
                            date_format(a.start_time_utc, 'hh:mm a') as game_start_time_cst,
                            a.*, 
                            b.request_key,
                            b.payload
                        from final_games a 
                        inner join nhl_data_raw.games.pbp_data b 
                            on a.game_id = b.request_key
                        where 1 = 1
                            and b.http_status = 200
                            and b.payload is not null 
                            and b.payload not in ('[]', '{}')
                        ---using below as a safeguard against dupe rows that may have snuck into raw table since this stream refreshes every 20-30 minutes
                        ---don't want to use where ingest_ts_utc::date = current_date() since these needs to work with a cold start
                        qualify row_number() over (partition by b.request_key order by b.ingest_ts_utc desc) = 1    
                        ;
                    
        """)
    teams = spark.sql("""
                    
                    select 
                        team_id as event_team_id,
                        team_abbrev as event_team_abbrev
                    from nhl_data_staged.teams.master_ids    

        """)

    players = spark.sql("""
                        
                        select 
                            player_id, 
                            player_name, 
                            player_pos,
                            shoots_catches, 
                            team_id, 
                            team_id_prev_team, 
                            team_abbrev, 
                            team_abbrev_prev_team, 
                            last_active_season
                        from nhl_data_staged.players.master_ids a 
                        
                        
        """)
    current_rosters = spark.sql("""
                                
                        with date_param as (

                            select from_utc_timestamp(current_timestamp(), 'America/Chicago')::date as current_run_date


                        )
                            
                        select /*+ broadcast (p) */ distinct 
                            a.season, 
                            a.game_id, 
                            b.game_date, 
                            a.player_id, 
                            a.jersey_num, 
                            a.team_id, 
                            a.headshot, 
                            a.player_name
                        from nhl_data_staged.players.player_game_rosters a 
                        inner join nhl_data_staged.games.schedules b 
                            on a.season = b.season 
                            and a.game_id = b.game_id
                            and a.game_date = b.game_date
                            and a.team_id = b.team_id
                        cross join date_param p
                        where 1 = 1
                            and a.insert_dte::date between 
                                date_sub(p.current_run_date, 1) 
                                and 
                                p.current_run_date
                                
                                
        """)

    ready = not games.isEmpty()

field_mapping = { 

        "plays_period_descriptor_number":           {"period": "tinyint"},

        "plays_period_descriptor_period_type":      {"target": "period_type",
                                                     "type": "string",
                                                     "upper": True
                                                     
                                                     },
        "clock_in_intermission":                    {"intermission_active": "boolean"},
        "clock_running":                            {"game_in_play": "boolean"},
        "plays_event_id":                           {"event_id": "integer"},
        "plays_type_code":                          {"event_type_code": "integer"},

        "plays_type_desc_key":                      {
                                                "target": "event_type", 
                                                "type": "string",
                                                "lower": True
                                                     },

        "plays_situation_code":                     {"situation_code": "varchar(5)"},
        "plays_details_zone_code":                  {
                                                "target": "zone_code",
                                                "type": "string",
                                                "lower": True
                                                     },
        "plays_sort_order":                         {"event_idx": "integer"},
        "plays_time_in_period":                     {"time_in_period": "varchar(4)"},
        "plays_time_remaining":                     {"time_remaining": "varchar(4)"},

        "plays_home_team_defending_side":           {
                                                    "target": "home_team_defending_side",
                                                     "type": "string", 
                                                     "lower": True,
                                                     "trim": True
                                                     },
        "plays_details_event_owner_team_id":        {"event_team_id": "integer"},
        "plays_details_away_score":                 {"away_score": "tinyint"}, 
        "plays_details_away_sog":                   {"away_sog": "integer"}, 
        "plays_details_home_score":                 {"home_score": "tinyint"}, 
        "plays_details_home_sog":                   {"home_sog": "integer"},
        "plays_details_scoring_player_id":          {"scoring_player_id": "bigint"}, 
        "plays_details_scoring_player_total":       {"scoring_player_total": "tinyint"}, 
        "plays_details_assist1_player_id":          {"assist1_player_id": "bigint"}, 
        "plays_details_assist1_player_total":       {"assist1_player_total": "tinyint"}, 
        "plays_details_assist2_player_id":          {"assist2_player_id": "bigint"}, 
        "plays_details_assist2_player_total":       {"assist2_player_total": "tinyint"}, 
        "plays_details_x_coord":                    {"x_coord": "integer"}, 
        "plays_details_y_coord":                    {"y_coord": "integer"}, 
        "plays_details_blocking_player_id":         {"blocking_player_id": "bigint"}, 
        "plays_details_committed_by_player_id":     {"penalty_committed_by_player_id": "bigint"},

        "plays_details_desc_key":                   {
                                                "target": "penalty_desc", 
                                                "type": "string",
                                                "lower": True,
                                                "trim": True
                                                     },

        "plays_details_type_code":                  {
                                                "target": "penalty_type_desc",
                                                "type": "string",
                                                "lower": True
                                                     },

        "plays_details_drawn_by_player_id":          {"penalty_drawn_by_player_id": "bigint"},
        "plays_details_duration":                    {"penalty_duration": "tinyint"},
        "plays_details_hittee_player_id":            {"hit_taken_by_player_id": "bigint"}, 
        "plays_details_hitting_player_id":           {"hit_given_by_player_id": "bigint"},
        "plays_details_player_id":                   {"player_id": "bigint"}, 
        "plays_details_reason":                      {
                                                "target": "missed_shot_desc",
                                                "type": "string", 
                                                "lower": True,
                                                "trim": True
                                                         
                                                         },

        "plays_details_secondary_reason":               {
                                                        
                                                "target": "play_stopped_reason",
                                                "type": "string",
                                                "lower": True,
                                                "trim": True
                                                         
                                                         
                                                         }, 
        "plays_details_shooting_player_id":             {"shooting_player_id": "bigint"},
        "plays_details_shot_type":                      {
                
                                                "target": "shot_type",
                                                "type": "string",
                                                "lower": True,
                                                "trim": True
                                                
                                                        },
        "plays_details_goalie_in_net_id":               {"goalie_in_net_id": "bigint"},
        "plays_details_winning_player_id":              {"faceoff_winning_player_id": "bigint"},
        "plays_details_losing_player_id":               {"faceoff_losing_player_id": "bigint"},
        "plays_period_descriptor_max_regulation_periods": {"max_regulation_periods": "tinyint"},



}

quarantine_rules = {

     "missing event type": f.col("event_type").isNull(),
     "missing time in period": f.col("time_in_period").isNull(),
     "missing time remaining": f.col("time_remaining").isNull(),
     "missing situation code": (
                    f.col("event_type").isin("shot-on-goal", "missed-shot", "blocked-shot", "goal", "penalty") 
                    & f.col("situation_code").isNull()
                    )
}

quarantine_condition = reduce(lambda x, y: x | y, quarantine_rules.values())
quarantine_reason = f.array_remove(
               f.array(*[f.when(condition, reason) for reason, condition in quarantine_rules.items()]), None
               )

pbp_insert_ready = False
quarantine_insert_ready = False
rosters_insert_ready = False
if ready: 

        try: 
                py_source = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")[-1]
                pbp_raw_schema = get_schema(games)
                pbp_raw = (
                        games 
                        .orderBy(f.col("request_key").desc())
                        .withColumn("json", f.from_json("payload", pbp_raw_schema))
                        .select(
                        f.col("request_key").alias("game_id"),
                        f.col("json.gameDate").alias("gameDate").cast("date").alias("game_date"),
                        f.concat(
                        f.substring(f.col("request_key").cast("string"), 1, 4), 
                        f.substring(f.col("request_key").cast("string"), 1, 4).cast("integer") + f.lit(1)
                        )
                        .cast("string")
                        .cast("integer")
                        .alias("season"),
                        f.explode_outer(f.col("json.plays")).alias("plays"),
                        f.col("json.clock").alias("clock"),
                        f.col("json.rosterSpots").alias("rosters"),
                        )
                        .withColumn("py_source", f.lit(py_source))
                        .transform(lambda d: flatten_df(d))
                        .transform(lambda c: c.toDF(*[convert_case(c) for c in c.columns]))
                )
                add_fields_expr = [build_fields(src_col, rule, pbp_raw.columns) for src_col, rule in field_mapping.items()]
                pbp_staged = (
                                pbp_raw
                                .select(
                                
                                "season",
                                "game_id",
                                "game_date",
                                "plays_sort_order",
                                *add_fields_expr,
                                "py_source"

                                )
                                
                                .filter(f.col("period").isNotNull())
                                .distinct()
                                .orderBy(f.col("season"), f.col("game_id"), f.col("plays_sort_order"))
                                .withColumn("game_type", f.lit(f.substring(f.col("game_id").cast("string"), 6, 1).cast("tinyint")))
                                .withColumn("time_in_period_seconds",
                                                        f.when(
                                                        f.col("time_in_period").isNotNull(),
                                                        f.split(f.col("time_in_period"), ":")[0].cast("int") * 60
                                                        + f.split(f.col("time_in_period"), ":")[1].cast("int")
                                                        )
                                        )

                                # period length
                                .withColumn("period_length_seconds",
                                        f.when(f.col("period") <= 3, 1200)
                                        .when((f.col("period") >= 4) & (f.col("game_type") == 2), 300)
                                        .when((f.col("period") >= 4) & (f.col("game_type") == 3), 1200)
                                )

                                # elapsed seconds within period
                                .withColumn("time_elapsed_seconds",
                                        f.when(
                                        f.col("time_in_period_seconds").isNotNull(),
                                        f.col("period_length_seconds") - f.col("time_in_period_seconds")
                                        )
                                )

                                # seconds from all prior periods
                                .withColumn("prior_period_seconds",
                                        f.when(f.col("period") == 1, 0)
                                        .when(f.col("period") == 2, 1200)
                                        .when(f.col("period") == 3, 2400)
                                        .when((f.col("period") == 4) & (f.col("game_type") == 2), 3600)
                                        .when((f.col("period") == 4) & (f.col("game_type") == 3), 3600)
                                        .when((f.col("period") >= 5) & (f.col("game_type") == 2), 3900)
                                        .when((f.col("period") >= 5) & (f.col("game_type") == 3), 3600 + ((f.col("period") - 4) * 1200))
                                )
                                # continuous game clock
                                .withColumn("game_seconds",
                                        f.when(
                                        f.col("period") <= 3,
                                        ((f.col("period") - 1) * 1200 + f.col("time_in_period_seconds"))
                                        )
                                        .when(
                                        (f.col("period") == 4) & (f.col("game_type") == 2),  # regular season OT (5 min)
                                        (3 * 1200 + f.col("time_in_period_seconds"))
                                        )
                                        .when(
                                        (f.col("period") == 5) & (f.col("game_type") == 2), 
                                        ((3 * (20 * 60) + (5 * 60)))
                                        )
                                        .when(
                                        (f.col("period") >= 4) & (f.col("game_type") == 3),  # playoffs OT (20 min each)
                                        (3 * 1200 + (f.col("period") - 4) * 1200 + f.col("time_in_period_seconds"))
                                        )
                                )
                                .withColumn("game_seconds", f.col("game_seconds").cast("int"))
                                .withColumn("event_idx", f.row_number().over(w.partitionBy("game_id").orderBy("game_seconds", "plays_sort_order")) - 1)
                                .withColumn("game_in_play", 
                                            
                                                f.when(f.col("game_date") < f.date_sub(f.to_date(
                                                                                f.from_utc_timestamp(f.current_timestamp(), "America/Chicago")
                                                                                ), 2), f.lit(False))
                                            .otherwise(f.lit(True))
                                            
                                )
                                .drop("game_type", "time_in_period_seconds", "period_length_seconds", "time_elapsed_seconds", "prior_period_seconds", "plays_sort_order")
                                .alias("p")
                                .join(f.broadcast(teams).alias("t"), how = "left", on = f.col("p.event_team_id") == f.col("t.event_team_id"))
                                .select("p.*", "t.event_team_abbrev")
                                .alias("pbp")
                                .join(
                                        f.broadcast(
                                        pbp_raw
                                        .filter(
                                                       
                                                ((f.lower(f.col("plays_type_desc_key")) == "game-end"))
                                        )
                                        .select(

                                                f.col("season").alias("raw_season"),
                                                f.col("game_id").alias("raw_game_id")
                                        )
                                        .distinct()
                                        )
                                        .alias("raw"), 
                                        how = "left", 
                                        on = (
                                        
                                        (f.col("pbp.season") == f.col("raw.raw_season")) & 
                                        (f.col("pbp.game_id") == f.col("raw.raw_game_id")) 
                                        
                                        )
                                )
                                .withColumn("game_in_play",
                                        f.when(
                                                f.col("pbp.season") <= 2009,
                                                f.lit(False)
                                        )
                                        .when(
                                                (f.col("raw.raw_game_id").isNotNull()) & (f.col("game_date") == f.to_date(f.from_utc_timestamp(f.current_timestamp(), 'America/Chicago'))),
                                                f.lit(False)
                                        )
                                        .when(

                                                f.col("raw.raw_game_id").isNotNull(), 
                                                f.lit(False)
                                        )
                                        .when(
                                                (f.col("pbp.game_date") == f.date_sub(f.to_date(f.from_utc_timestamp(f.current_timestamp(), 'America/Chicago')), 1)) &
                                                (f.col("raw.raw_game_id").isNull()),
                                                f.lit(True)
                                        )
                                        .otherwise(f.lit(True))
                                        )
                                .withColumn("event_type",
                                            
                                        f.when(f.trim(f.col("event_type")) == "", f.lit(None))
                                        .otherwise(f.col("event_type"))

                                )
                                .withColumn("time_in_period",
                                            
                                        f.when(f.trim(f.col("time_in_period")) == "", f.lit(None))
                                        .otherwise(f.col("time_in_period"))
                                        .cast("string")
                                )
                                .withColumn("time_remaining", 
                                            
                                        f.when(f.trim(f.col("time_remaining")) == "", f.lit(None))
                                        .otherwise(f.col("time_remaining"))
                                        .cast("string")
                                            
                                )
                                .withColumn("situation_code", 
                                            
                                        f.when((f.col("period") == 1) & (f.col("event_type") == "period-start"), '1551')
                                        .otherwise(f.col("situation_code"))
                                        .cast("string")

                                )
                
                )
                pbp_schema = t.StructType([

                        t.StructField("season", t.IntegerType(), False),
                        t.StructField("game_id", t.LongType(), False),
                        t.StructField("game_date", t.DateType(), False),
                        t.StructField("period", t.ByteType(), False),
                        t.StructField("period_type", t.StringType(), True),
                        t.StructField("home_team_defending_side", t.StringType(), True),
                        t.StructField("time_in_period", t.StringType(), True),
                        t.StructField("time_remaining", t.StringType(), True),
                        t.StructField("game_seconds", t.IntegerType(), True),
                        t.StructField("situation_code", t.StringType(), True),
                        t.StructField("zone_code", t.StringType(), True),
                        t.StructField("event_id", t.IntegerType(), True),
                        t.StructField("event_idx", t.IntegerType(), True),
                        t.StructField("event_type", t.StringType(), True),
                        t.StructField("event_type_code", t.IntegerType(), True),
                        t.StructField("event_team_id", t.IntegerType(), True),
                        t.StructField("event_team_abbrev", t.StringType(), True),
                        t.StructField("player_id", t.LongType(), True),
                        t.StructField("x_coord", t.IntegerType(), True),
                        t.StructField("y_coord", t.IntegerType(), True),
                        t.StructField("goalie_in_net_id", t.LongType(), True),
                        t.StructField("shooting_player_id", t.LongType(), True),
                        t.StructField("shot_type", t.StringType(), True),
                        t.StructField("missed_shot_desc", t.StringType(), True),
                        t.StructField("away_sog", t.IntegerType(), True),
                        t.StructField("home_sog", t.IntegerType(), True),
                        t.StructField("away_score", t.ByteType(), True),
                        t.StructField("home_score", t.ByteType(), True),
                        t.StructField("scoring_player_id", t.LongType(), True),
                        t.StructField("scoring_player_total", t.ByteType(), True),
                        t.StructField("assist1_player_id", t.LongType(), True),
                        t.StructField("assist1_player_total", t.ByteType(), True),
                        t.StructField("assist2_player_id", t.LongType(), True),
                        t.StructField("assist2_player_total", t.ByteType(), True),
                        t.StructField("blocking_player_id", t.LongType(), True),
                        t.StructField("penalty_desc", t.StringType(), True),
                        t.StructField("penalty_type_desc", t.StringType(), True),
                        t.StructField("penalty_duration", t.IntegerType(), True),
                        t.StructField("penalty_committed_by_player_id", t.LongType(), True),
                        t.StructField("penalty_drawn_by_player_id", t.LongType(), True),
                        t.StructField("hit_given_by_player_id", t.LongType(), True),
                        t.StructField("hit_taken_by_player_id", t.LongType(), True),
                        t.StructField("faceoff_winning_player_id", t.LongType(), True),
                        t.StructField("faceoff_losing_player_id", t.LongType(), True),
                        t.StructField("max_regulation_periods", t.ByteType(), True),
                        t.StructField("play_stopped_reason", t.StringType(), True),
                        t.StructField("intermission_active", t.BooleanType(), False),
                        t.StructField("game_in_play", t.BooleanType(), False),
                        t.StructField("py_source", t.StringType(), False), 

                ])
                pbp_data = pbp_staged.transform(apply_schema, pbp_schema)
                quarantine = (
                        
                        pbp_data
                        .filter(quarantine_condition)
                        .withColumn("quarantine_reason", quarantine_reason)
                )
                pbp_data = (

                        pbp_data 
                        .filter(
                                (f.col("event_type").isNotNull())
                                & (f.col("time_in_period").isNotNull())
                                & (f.col("time_remaining").isNotNull())
                        )
                )
                #true if df is not empty, false, if it is
                pbp_insert_ready = not pbp_data.isEmpty()
                quarantine_insert_ready = not quarantine.isEmpty()
        except Exception as e: 
                print(f"Error constructing play by play dataframe: {e}")

if pbp_insert_ready: 

    pbp_data.createOrReplaceTempView("pbp_insert_tmp")
    spark.sql(f"""
              
              
        merge into nhl_data_staged.games.pbp_data t 
        using pbp_insert_tmp s
            on t.season = s.season
            and t.game_id = s.game_id
            and t.game_date = s.game_date 
            and t.event_idx = s.event_idx 
            and t.game_date between 
                date_sub(from_utc_timestamp(current_timestamp(), 'America/Chicago')::date, 2) 
                and 
                from_utc_timestamp(current_timestamp(), 'America/Chicago')::date

        when matched and (

            t.active_row = true 
            and 
            (
                not (t.game_seconds <=> s.game_seconds) 
                or not (t.situation_code <=> s.situation_code)
                or not (t.zone_code <=> s.zone_code)
                or not (t.event_type <=> s.event_type)  
                or not (t.event_id <=> s.event_id)  
                or not (t.home_team_defending_side <=> s.home_team_defending_side)
                or not (t.event_team_id <=> s.event_team_id)
                or not (t.scoring_player_id <=> s.scoring_player_id)
                or not (t.shooting_player_id <=> s.shooting_player_id)
                or not (t.assist1_player_id <=> s.assist1_player_id)
                or not (t.assist2_player_id <=> s.assist2_player_id)
                or not (t.blocking_player_id <=> s.blocking_player_id)

            )
        )

        then update set 
            game_date = s.game_date,
            period = s.period,
            period_type = s.period_type,
            home_team_defending_side = s.home_team_defending_side,
            time_in_period = s.time_in_period,
            time_remaining = s.time_remaining,
            game_seconds = s.game_seconds,
            situation_code = s.situation_code,
            zone_code = s.zone_code,
            event_idx = s.event_idx,
            event_type = s.event_type,
            event_type_code = s.event_type_code,
            event_team_id = s.event_team_id,
            event_team_abbrev = s.event_team_abbrev,
            player_id = s.player_id,
            x_coord = s.x_coord,
            y_coord = s.y_coord,
            goalie_in_net_id = s.goalie_in_net_id,
            shooting_player_id = s.shooting_player_id,
            shot_type = s.shot_type,
            missed_shot_desc = s.missed_shot_desc,
            away_sog = s.away_sog,
            home_sog = s.home_sog,
            away_score = s.away_score,
            home_score = s.home_score,
            scoring_player_id = s.scoring_player_id,
            scoring_player_total = s.scoring_player_total,
            assist1_player_id = s.assist1_player_id,
            assist1_player_total = s.assist1_player_total,
            assist2_player_id = s.assist2_player_id,
            assist2_player_total = s.assist2_player_total,
            blocking_player_id = s.blocking_player_id,
            penalty_desc = s.penalty_desc,
            penalty_type_desc = s.penalty_type_desc,
            penalty_duration = s.penalty_duration,
            penalty_committed_by_player_id = s.penalty_committed_by_player_id,
            penalty_drawn_by_player_id = s.penalty_drawn_by_player_id,
            hit_given_by_player_id = s.hit_given_by_player_id,
            hit_taken_by_player_id = s.hit_taken_by_player_id,
            faceoff_winning_player_id = s.faceoff_winning_player_id,
            faceoff_losing_player_id = s.faceoff_losing_player_id,
            max_regulation_periods = s.max_regulation_periods,
            play_stopped_reason = s.play_stopped_reason,
            intermission_active = s.intermission_active,
            game_in_play = s.game_in_play,
            update_dte = current_timestamp(),
            py_source = s.py_source,
            active_row = true,
            logic_block = "match one",
            failed_condition = nullif(
                                concat_ws(
                                    ', '
                                    , filter(
                                        array(
                                            case when not (t.event_type <=> s.event_type) then 'event_type' end,
                                            case when not (t.game_seconds <=> s.game_seconds) then 'game_seconds' end,
                                            case when not (t.situation_code <=> s.situation_code) then 'situation_code' end,
                                            case when not (t.zone_code <=> s.zone_code) then 'zone_code' end,
                                            case when not (t.event_id <=> s.event_id) then 'event_id' end,
                                            case when not (t.home_team_defending_side <=> s.home_team_defending_side) then 'home_team_defending_side' end,
                                            case when not (t.event_team_id <=> s.event_team_id) then 'event_team_id' end,
                                            case when not (t.scoring_player_id <=> s.scoring_player_id) then 'scoring_player_id' end,
                                            case when not (t.shooting_player_id <=> s.shooting_player_id) then 'shooting_player_id' end,
                                            case when not (t.assist1_player_id <=> s.assist1_player_id) then 'assist1_player_id' end,
                                            case when not (t.assist2_player_id <=> s.assist2_player_id) then 'assist2_player_id' end,
                                            case when not (t.blocking_player_id <=> s.blocking_player_id) then 'blocking_player_id' end
                                        )
                                        , x -> x is not null
                                    )
                                )
                                , ''
                            )

        when matched and 
            t.game_date >= date_sub(from_utc_timestamp(current_timestamp(), 'America/Chicago')::date, 2)
            and not (t.game_in_play <=> s.game_in_play)
        then update set 
            game_in_play = s.game_in_play,
            update_dte = current_timestamp(),
            logic_block = "match two",
            py_source = s.py_source

        when not matched by target then insert (

                season,
                game_id,
                game_date,
                period,
                period_type,
                home_team_defending_side,
                time_in_period,
                time_remaining,
                game_seconds,
                situation_code,
                zone_code,
                event_id,
                event_idx,
                event_type,
                event_type_code,
                event_team_id,
                event_team_abbrev,
                player_id,
                x_coord,
                y_coord,
                goalie_in_net_id,
                shooting_player_id,
                shot_type,
                missed_shot_desc,
                away_sog,
                home_sog,
                away_score,
                home_score,
                scoring_player_id,
                scoring_player_total,
                assist1_player_id,
                assist1_player_total,
                assist2_player_id,
                assist2_player_total,
                blocking_player_id,
                penalty_desc,
                penalty_type_desc,
                penalty_duration,
                penalty_committed_by_player_id,
                penalty_drawn_by_player_id,
                hit_given_by_player_id,
                hit_taken_by_player_id,
                faceoff_winning_player_id,
                faceoff_losing_player_id,
                max_regulation_periods,
                play_stopped_reason,
                intermission_active,
                game_in_play,
                insert_dte,
                update_dte,
                py_source,
                active_row,
                logic_block,
                failed_condition

        ) 
        values (

                s.season,
                s.game_id,
                s.game_date,
                s.period,
                s.period_type,
                s.home_team_defending_side,
                s.time_in_period,
                s.time_remaining,
                s.game_seconds,
                s.situation_code,
                s.zone_code,
                s.event_id,
                s.event_idx,
                s.event_type,
                s.event_type_code,
                s.event_team_id,
                s.event_team_abbrev,
                s.player_id,
                s.x_coord,
                s.y_coord,
                s.goalie_in_net_id,
                s.shooting_player_id,
                s.shot_type,
                s.missed_shot_desc,
                s.away_sog,
                s.home_sog,
                s.away_score,
                s.home_score,
                s.scoring_player_id,
                s.scoring_player_total,
                s.assist1_player_id,
                s.assist1_player_total,
                s.assist2_player_id,
                s.assist2_player_total,
                s.blocking_player_id,
                s.penalty_desc,
                s.penalty_type_desc,
                s.penalty_duration,
                s.penalty_committed_by_player_id,
                s.penalty_drawn_by_player_id,
                s.hit_given_by_player_id,
                s.hit_taken_by_player_id,
                s.faceoff_winning_player_id,
                s.faceoff_losing_player_id,
                s.max_regulation_periods,
                s.play_stopped_reason,
                s.intermission_active,
                s.game_in_play,
                current_timestamp(),
                null,
                s.py_source,
                true,
                "insert",
                null
        )
        ---below looks to see if an event came through an initial scrape that no longer appears in the following scrapes
        ---if it isn't then it gets flagged as not being active (looks back two days to see if pbp data from the past two days still exists or if it was updated)
        when not matched by source 
            and t.game_in_play = true
            and t.active_row = true
            and game_date = from_utc_timestamp(current_timestamp(), 'America/Chicago')::date
            --and game_date between date_sub(current_date(), 2) and current_date()
            then update set 
                update_dte = current_timestamp(),
                active_row = false,
                logic_block = "no match update one",
                py_source = '{py_source}',
                failed_condition = nullif(
                                    concat_ws(
                                        ', '
                                        , filter(
                                            array(
                                                case when t.game_in_play = true then 'missing_from_source_one_game_in_play' end
                                                , case when t.active_row = true then 'missing_from_source_one_active_row' end
                                            )
                                            , x -> x is not null
                                        )
                                    )
                                    , ''
                                )
        when not matched by source 
            and t.game_in_play = false 
            and t.active_row = true
            and t.game_date between date_sub(from_utc_timestamp(current_timestamp(), 'America/Chicago')::date, 2) and date_sub(from_utc_timestamp(current_timestamp(), 'America/Chicago')::date, 1)
            then update set 
                update_dte = current_timestamp(),
                py_source = '{py_source}',
                active_row = false,
                logic_block = "no match update two",
                failed_condition = nullif(
                        concat_ws(
                            ', '
                            , filter(
                                array(
                                    case when t.game_in_play = false then 'missing_from_source_two_game_not_in_play' end
                                    , case when t.active_row = true then 'missing_from_source_two_active_row' end
                                )
                                , x -> x is not null
                            )
                        )
                        , ''
                    )
                                    
          ;
              
    """)
    spark.catalog.dropTempView("pbp_insert_tmp")
    print(f"Play by Play data successfully loaded into nhl_data_staged.games.pbp_data table")
    if datetime.datetime.today().day % 5 == 1:
        spark.sql("""analyze table nhl_data_staged.games.pbp_data compute statistics;""")
        spark.sql("""optimize nhl_data_staged.games.pbp_data;""")
        spark.sql("""vacuum nhl_data_staged.games.pbp_data;""")
        spark.sql("""analyze table nhl_data.games.pbp_data compute statistics;""")
        spark.sql("""optimize nhl_data.games.pbp_data;""")
        spark.sql("""vacuum nhl_data.games.pbp_data;""")
        
else: 
    print(f"No new data to insert into nhl_data_staged.games.pbp_data, skipping insert")

if ready: 

        rosters_schema = t.StructType([

                t.StructField("season", t.IntegerType(), False),
                t.StructField("game_id", t.LongType(), False),
                t.StructField("game_date", t.DateType(), False),
                t.StructField("player_id", t.LongType(), False),
                t.StructField("player_name", t.StringType(), False), 
                t.StructField("player_pos", t.StringType(), False),
                t.StructField("jersey_num", t.IntegerType(), True),
                t.StructField("shoots_catches", t.StringType(), True),
                t.StructField("team_id", t.IntegerType(), False),
                t.StructField("team_id_prev_team", t.IntegerType(), False),
                t.StructField("team_abbrev", t.StringType(), False),
                t.StructField("last_active_season", t.LongType(), False),
                t.StructField("headshot", t.StringType(), True)

        ])
        rosters_field_mapping = {
                                "rosters_player_id": "player_id", 
                                "rosters_sweater_number": "jersey_num", 
                                "rosters_team_id": "team_id", 
                                "rosters_first_name_default": "first_name", 
                                "rosters_last_name_default": "last_name", 
                                "rosters_headshot": "headshot"
        
                                }
        try: 
                rosters = (
                        pbp_raw                        
                        .select(
                                "season", "game_id", "game_date", *[x for x in pbp_raw.columns if "rosters" in x]
                        )
                        .distinct()
                        .withColumnsRenamed(rosters_field_mapping)
                        .select(
                                
                                "season", "game_id", "game_date", 
                                f.col("player_id").cast("bigint"), 
                                f.col("jersey_num").cast("integer"), 
                                f.col("team_id").cast("integer"), 
                                f.col("headshot").cast("string"),
                                f.concat_ws(" ", f.trim(f.col("first_name")), f.trim(f.col("last_name"))).cast("string").alias("player_name")
                                
                        )
                        .union(current_rosters)
                
                )
                unused_fields = [x for x in pbp_raw.columns if "rosters" in x and x not in rosters_field_mapping.keys()]
                rosters_ready = (
                        
                        rosters 
                        .filter(f.col("player_id").isNotNull())
                        .alias("r")
                        .join(f.broadcast(players).alias("p"), how = "inner", on = f.col("r.player_id") == f.col("p.player_id"))

                        .select(
                                
                                "r.player_id", "r.season", "r.game_id", "r.game_date", "r.jersey_num", "p.player_name", "p.player_pos", "p.shoots_catches", "r.team_id", 
                                f.when(
                                        f.col("r.team_id") != f.col("p.team_id"), f.lit(f.col("r.team_id").cast("integer"))
                                        )
                                .otherwise(f.col("p.team_id_prev_team").cast("integer")).alias("team_id_prev_team"),      
                                f.concat(f.col("r.season").cast("string"), f.col("r.season") + 1).cast("integer").alias("last_active_season"),
                                "r.headshot"

                                )   
                        .alias("r2")
                        .join(f.broadcast(teams).alias("t"), how = "inner", on = f.col("r2.team_id") == f.col("t.event_team_id"))
                        .select("r2.*", f.col("event_team_abbrev").alias("team_abbrev"))
                        .transform(lambda d: apply_schema(d, rosters_schema))
                        .withColumn("py_source", f.lit(py_source))
                        .withColumn("unused_structs", f.lit(unused_fields))
                        .withColumn("num_unused_structs", f.lit(len(unused_fields)))
                        .alias("pgr")
                        .join(
                                f.broadcast(
                                pbp_data 
                                .select(
                                        f.col("game_id").alias("pbp_game_id"), 
                                        f.col("game_in_play")
                                )
                                .distinct()
                                )
                                .alias("pbp"),
                                how = "left", 
                                on = (f.col("pgr.game_id") == (f.col("pbp.pbp_game_id")))
                                

                        )
                        #catch all to remove dupes 
                        .withColumn("row_idx", f.row_number().over(w.partitionBy("season", "game_id", "player_id").orderBy("game_date")))
                        .filter(f.col("row_idx") == 1)
                        .drop("pbp_game_id", "row_idx")
                )
                        
                rosters_insert_ready = not rosters_ready.isEmpty()
        except Exception as e: 
                print(e)

if rosters_insert_ready: 

    rosters_ready.createOrReplaceTempView("rosters_ready_tmp")
    spark.sql(f"""
              
        
            merge into nhl_data_staged.players.player_game_rosters t 
            using rosters_ready_tmp s 
                on t.season = s.season
                and t.game_id = s.game_id
                and t.game_date = s.game_date
                and t.player_id = s.player_id
                and t.team_id = s.team_id
                and t.game_date between 
                    date_sub(from_utc_timestamp(current_timestamp(), 'America/Chicago')::date, 2) 
                    and 
                    from_utc_timestamp(current_timestamp(), 'America/Chicago')::date
                

            when matched and (
                
                (    
                    t.player_name <> s.player_name 
                    or not (t.player_pos <=> s.player_pos) 
                    or not (t.jersey_num <=> s.jersey_num)
                    or t.game_in_play <> s.game_in_play
                )
            
            )
            
            then update set 

                season = s.season,
                player_name = s.player_name, 
                player_pos = s.player_pos, 
                jersey_num = s.jersey_num, 
                shoots_catches = coalesce(s.shoots_catches, t.shoots_catches), 
                is_active = true, 
                game_in_play = s.game_in_play,
                update_dte = current_timestamp(),
                py_source = s.py_source

            when not matched by target then insert (

                season,
                game_id,
                game_date,
                team_id,
                team_abbrev,
                player_id,
                player_name,
                player_pos,
                jersey_num,
                shoots_catches,
                birth_dte,
                birth_country,
                height_inches,
                weight_lbs,
                is_active,
                headshot,
                game_in_play,
                insert_dte,
                update_dte,
                unused_structs,
                num_unused_structs,
                py_source

            )
            values (

                s.season,
                s.game_id,
                s.game_date,
                s.team_id,
                s.team_abbrev,
                s.player_id,
                s.player_name,
                s.player_pos,
                s.jersey_num,
                s.shoots_catches,
                '1900-01-01'::date,
                null,
                null,
                null,
                true,
                s.headshot,
                s.game_in_play,
                current_timestamp(),
                null,
                s.unused_structs,
                s.num_unused_structs,
                s.py_source
            )
            ---below handles if a player was loaded in the scarpe done earlier in the day (rosters_current_staged)
            ---but isn't listed on the roster log that comes from the pbp api source 
            ---if that game is in play but the player isnt' there anymore then set them to inactive. only doing this on cames that are in play 
            ---because finished games on the same day can see all their rows flipped to not being active 
            when not matched by source 
                and t.game_in_play = true 
                and t.is_active = true 
                then update set 
                    t.update_dte = current_timestamp(),
                    t.is_active = false,
                    t.py_source = '{py_source}'

    """)
    spark.catalog.dropTempView("rosters_ready_tmp")
    print(f"New players data successfully loaded into nhl_data_staged.players.player_game_rosters table")
else: 
    print(f"No new data to insert into nhl_data_staged.players.player_game_rosters, skipping insert")

if quarantine_insert_ready: 

    quarantine.createOrReplaceTempView("pbp_quarantine_tmp")
    spark.sql("""
                
        merge into nhl_data_staged.quarantine.pbp_data t 
        using pbp_quarantine_tmp s 
            on t.season = s.season
            and t.game_id = s.game_id 
            and t.game_date = s.game_date
            and t.game_date = s.game_date 
            and t.event_idx = s.event_idx 
        
        when not matched then insert (

            season,
            game_id,
            game_date,
            period,
            period_type,
            home_team_defending_side,
            time_in_period,
            time_remaining,
            game_seconds,
            situation_code,
            zone_code,
            event_id,
            event_idx,
            event_type,
            event_type_code,
            event_team_id,
            event_team_abbrev,
            player_id,
            x_coord,
            y_coord,
            goalie_in_net_id,
            shooting_player_id,
            shot_type,
            missed_shot_desc,
            away_sog,
            home_sog,
            away_score,
            home_score,
            scoring_player_id,
            scoring_player_total,
            assist1_player_id,
            assist1_player_total,
            assist2_player_id,
            assist2_player_total,
            blocking_player_id,
            penalty_desc,
            penalty_type_desc,
            penalty_duration,
            penalty_committed_by_player_id,
            penalty_drawn_by_player_id,
            hit_given_by_player_id,
            hit_taken_by_player_id,
            faceoff_winning_player_id,
            faceoff_losing_player_id,
            max_regulation_periods,
            play_stopped_reason,
            intermission_active,
            game_in_play,
            insert_dte,
            update_dte,
            py_source,
            active_row,
            quarantine_reason
            
            )  
        values (

            s.season,
            s.game_id,
            s.game_date,
            s.period,
            s.period_type,
            s.home_team_defending_side,
            s.time_in_period,
            s.time_remaining,
            s.game_seconds,
            s.situation_code,
            s.zone_code,
            s.event_id,
            s.event_idx,
            s.event_type,
            s.event_type_code,
            s.event_team_id,
            s.event_team_abbrev,
            s.player_id,
            s.x_coord,
            s.y_coord,
            s.goalie_in_net_id,
            s.shooting_player_id,
            s.shot_type,
            s.missed_shot_desc,
            s.away_sog,
            s.home_sog,
            s.away_score,
            s.home_score,
            s.scoring_player_id,
            s.scoring_player_total,
            s.assist1_player_id,
            s.assist1_player_total,
            s.assist2_player_id,
            s.assist2_player_total,
            s.blocking_player_id,
            s.penalty_desc,
            s.penalty_type_desc,
            s.penalty_duration,
            s.penalty_committed_by_player_id,
            s.penalty_drawn_by_player_id,
            s.hit_given_by_player_id,
            s.hit_taken_by_player_id,
            s.faceoff_winning_player_id,
            s.faceoff_losing_player_id,
            s.max_regulation_periods,
            s.play_stopped_reason,
            s.intermission_active,
            s.game_in_play,
            current_timestamp(),
            null,
            s.py_source,
            false,
            s.quarantine_reason

        )
                  
    """)
    spark.catalog.dropTempView("pbp_quarantine_tmp")
    print(f"Shift data successfully loaded into nhl_data_staged.quarantine.pbp_data table")
else: 
    print(f"No new data to insert into nhl_data_staged.quarantine.pbp_data, skipping insert")

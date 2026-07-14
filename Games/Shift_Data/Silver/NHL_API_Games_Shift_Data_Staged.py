import sys
username = spark.sql("select current_user()").first()[0]
sys.path.append(f"/Workspace/Users/{username}/NHL_Pipeline")

from pyspark.sql import SparkSession 
from pyspark.sql import functions as f, types as t, Window as w, DataFrame
from delta.tables import DeltaTable
from zoneinfo import ZoneInfo 
from functools import reduce 
import json, re, datetime
from pipeline_funcs.games import get_games
from pipeline_funcs.schema_utils import convert_case, build_fields, apply_schema, get_schema

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "America/Chicago")

kickoff = not get_games(spark, "nhl_data_staged.games.shift_data").isEmpty()
if kickoff: 
    #section below handles retries for games where there was missing shift data
    #the retry logic relies on an index created based on game_date in the schedules table rather than relying on the current_date() function
    #because we want to retry every 15 days per the NHL schedule 
    run_missing = spark.sql(f"""
    
        with season_param as (

            ---if table hasn't been populated, use 19001901 for season to indicate a cold start is needed
            select 
                coalesce(max(season), 19001901) as shift_table_season
            from nhl_data_staged.games.shift_data 
            where 1 = 1
                and game_date <= from_utc_timestamp(current_timestamp(), 'America/Chicago')::date

        ) 
        ,
        current_season_dates as (

                select /*+ broadcast (p) */ distinct 
                    a.season, 
                    a.game_date,
                    (p.shift_table_season = 19001901) as cold_start_ind
                from nhl_data_staged.games.schedules a 
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
    shift_schema = spark.sql("""
                             
                        select schema_of_json_agg(payload) as json_schema
                        from nhl_data_raw.games.shift_data 
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
                        from_json(b.payload, '{shift_schema}') as payload_json
                    from nhl_data_staged.games.schedules a 
                    left join nhl_data_raw.games.shift_data b 
                        on a.game_id = b.request_key 
                    where 1 = 1
                        ---want to avoid games that are in play today in the vent that the game hasn't started yet or the data feed is slightly delayed
                        ---to avoid current day games getting added to the games_missing_shift table
                        ---the shift data endpoint has longer delay than the pbp endpoint does and can be notorious for having data disappear out of nowhere
                        ---for unknown reasons
                        and a.game_date < from_utc_timestamp(current_timestamp(), 'America/Chicago')::date
                        and a.game_type in (2,3)
                        and a.season >= 20102011

                )
                ,
                src as (

                    select * 
                    from staged 
                    where 1 = 1
                        ----check to see if the payload was empty, in the shift data payload there is a key called total which contains the # of values inside the shift payload
                        ----if it's 0 then the payload json was empty
                        and payload_json is not null 
                        and payload_json.total = 0 
                        and size(payload_json.data) = 0 
                        ---ensuring that we aren't looking at games that are being played on the current date 
                      
                )

                merge into nhl_data_staged.ops.games_missing_shift t 
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
        print(f"Data successfully inserted/update into nhl_data_staged.ops.games_missing_shift table")
    else: 
        print(f"Skipping insert since current season game date is not eligble")
else: 
    print(f"No new data found, skipping insert")

if kickoff: 
    games = spark.sql("""
                    
                with date_param as (
                    ---start with getting current date of script being ran
                    select from_utc_timestamp(current_timestamp(), 'America/Chicago')::date as current_run_date

                )
                , 
                pbp_game_status as (
                    ---check to see what the status of the game is based on the play by play data (most reliable method)
                    select
                        game_id
                        , max(coalesce(game_in_play, false)) as game_in_play
                        , coalesce(max(1) filter (where event_type = 'game-end'), 0) as has_game_end
                    from nhl_data_staged.games.pbp_data
                    group by game_id

                )
                , 
                games as (
                ---pull list of all games and various information 
                    select distinct
                        a.season
                        , a.game_id
                        , a.game_date
                        , a.start_time_utc
                        , coalesce(b.game_in_play, false) as game_in_play
                        , coalesce(b.has_game_end, 0) as has_game_end
                    from nhl_data_staged.games.schedules a
                    left join pbp_game_status b
                        on a.game_id = b.game_id
                    cross join date_param p
                    where 1 = 1
                        and a.season >= 20102011
                        and a.game_type in (2,3)
                        and a.game_date <= p.current_run_date

                )
                ,
                raw_totals as (
                    ---check to see what the total is in the payload that sits in the raw table (which contains data scraped from the API)
                    select 
                        g.season,
                        a.request_key,
                        g.game_date,
                        g.start_time_utc,
                        g.game_in_play,
                        get_json_object(a.payload, "$.total") as total_rows
                    from nhl_data_raw.games.shift_data a 
                    inner join games g 
                        on a.request_key = g.game_id 
                    
                )
                , 
                quarantined_rows as (
                    ---check to see how many rows were inserted into the quarantined data table for each game
                    select 
                        a.game_id,
                        count(*) as total_rows
                    from nhl_data_staged.quarantine.shift_data a 
                    group by 
                        a.game_id 
                )
                ,
                games_ended_today as (
                    ---check to see which games have ended already that are in play today 
                    ---since the play by play table is populated before the shift table is, we want to make sure that if a game 
                    ---that was in play today has ended before all shift data could be loaded that we continue to load that data 
                    ---(i.e. game ended 15 minutes prior but the remaining shift data for that game hasn't been inserted into staging tables)
                    select 
                        a.*, 
                        "ended today" as which_game
                    from games a
                    where 1 = 1
                        and game_date = from_utc_timestamp(current_timestamp(), 'America/Chicago')::date
                        and has_game_end = 1
                )
                , 
                games_missing_from_api as (
                ---check to see if game doesn't have any shift data found in API (this happens more frequently as shift data API endpoint seems to be somewhat unrealiable)
                    select 
                        game_id
                    from nhl_data_staged.ops.games_missing_shift

                )
                , 
                games_in_play_today as (
                
                ---check to see which games are in play today that started at least 15 minutes before the time of the scrape (NHL games always have a lag in start time so don't scrape the API unless the game has truly started)
                    select distinct
                        a.season
                        , a.game_id
                        , a.game_date
                        , a.start_time_utc
                        , a.game_in_play
                        , 'in play' as which_game
                    from games a
                    cross join date_param p
                    where 1 = 1
                        and a.game_date = p.current_run_date
                        and from_utc_timestamp(current_timestamp(), 'America/Chicago') >= from_utc_timestamp(a.start_time_utc, 'America/Chicago') + interval 15 minutes
                        and a.game_in_play = true

                )
                , 
                games_loaded as (
                ---check totals for games that have already been loaded, this will be used later to compare to make sure that a secondary scrape of that game doesn't need to be done
                    select
                        game_id,
                        count(*) as total_rows
                    from nhl_data_staged.games.shift_data
                    where 1 = 1
                        and game_id is not null
                        and season >= 20102011
                    group by 
                        game_id
                )
                , 
                games_not_loaded as (
                ---check to see which games have not yet been loaded that aren't part of the games that have already been loaded 
                    select
                        a.season
                        , a.game_id
                        , a.game_date
                        , a.start_time_utc
                        , a.game_in_play
                        , 'not loaded' as which_game
                    from games a
                    left anti join games_loaded b
                        on a.game_id = b.game_id
                    where 1 = 1

                )
                , 
                games_prior_two as (
                ---setting up catch all the continue scraping data for games that have been played in the last 2 days (to ensure data accuracy)
                    select
                        a.season
                        , a.game_id
                        , a.game_date
                        , a.start_time_utc
                        , a.game_in_play
                        , 'last two' as which_game
                    from games a
                    cross join date_param p
                    where 1 = 1
                        and a.game_date between date_sub(p.current_run_date, 2) and date_sub(p.current_run_date, 1)

                )
                , 
                games_partially_loaded as (
                    ---check to see which games were loaded but where the totals in the staging table don't match the total that is listed in the API payload json 
                    select 
                        a.season, 
                        a.game_date,
                        a.request_key as game_id,
                        a.start_time_utc,
                        "partially loaded" as which_game,
                        a.game_in_play,
                        a.total_rows as raw_rows, 
                        coalesce(b.total_rows, 0) as staged_rows, 
                        coalesce(c.total_rows, 0) as quarantined_rows
                    from raw_totals a
                    left join games_loaded b 
                        on a.request_key = b.game_id
                    left join quarantined_rows c 
                        on a.request_key = c.game_id 

                )
                , 
                final_games as (
                ---create final list of games that need to be scraped, works during first run of pipeline and then after
                select 
                    season,
                    game_id,
                    game_date,
                    start_time_utc,
                    which_game,
                    game_in_play
                from games_partially_loaded a
                left anti join nhl_data_staged.quarantine.shift_data b 
                    on a.game_id = b.game_id
                where 1 = 1
                    and raw_rows <> (staged_rows + quarantined_rows)
                union all 
                select 
                    season,
                    game_id,
                    game_date,
                    start_time_utc,
                    which_game,
                    game_in_play
                from games_ended_today
                union 
                select 
                    season, 
                    game_id, 
                    game_date, 
                    start_time_utc, 
                    which_game,
                    game_in_play
                from games_in_play_today
                union
                select 
                    season, 
                    game_id, 
                    game_date, 
                    start_time_utc, 
                    which_game,
                    game_in_play
                from games_not_loaded
                union
                select 
                    season, 
                    game_id, 
                    game_date, 
                    start_time_utc, 
                    which_game,
                    game_in_play
                from games_prior_two

                )

                , 
                final_games_clean as (
                ---pull out games from final games that don't have data showing in the API, don't need to continue to scrape them 
                select
                    a.season
                    , a.game_id
                    , a.game_date
                    , a.start_time_utc
                    , a.game_in_play
                    , a.which_game
                from final_games a
                left anti join games_missing_from_api b
                    on a.game_id = b.game_id

                )

                , 
                raw_data as (

                select
                    request_key,
                    payload,
                    ingest_ts_utc,
                    from_json(payload, 'struct<data: array<string>, total: int>') as payload_json,
                    get_json_object(payload, "$.total") as payload_total_rows
                from nhl_data_raw.games.shift_data a  
                left anti join quarantined_rows b 
                    on a.request_key = b.game_id
                where 1 = 1
                    and http_status = 200
                    and substring(request_key, 1, 4)::int >= 2010

                )



                select
                    date_format(from_utc_timestamp(a.start_time_utc, 'America/Chicago'), 'hh:mm a') as game_start_time_cst
                    , a.*
                    , b.payload
                from final_games_clean a
                inner join raw_data b
                    on a.game_id = b.request_key
                where 1 = 1
                    and b.payload is not null
                    ---and b.payload_json is not null
                    and b.payload_total_rows > 0 
                    ---and b.payload_json.data is not null
                    and size(b.payload_json.data) > 0
                qualify row_number() over (partition by b.request_key order by b.ingest_ts_utc desc) = 1
                order by a.game_date desc;


    """)
    ready = not games.isEmpty()

field_mapping = { 

        "game_id": {"game_id": "bigint"},
        "team_id": {"team_id": "integer"},
        "team_abbrev": {"team_abbrev": "string"},
        "team_name": {"team_name": "string"},
        "player_id": {"player_id": "bigint"},
        "first_name": {"target": "first_name", "type": "string", "trim": True},
        "last_name": {"target": "last_name", "type": "string", "trim": True},
        "id": {"shift_id": "bigint"},
        "period": {"period": "integer"},
        "start_time": {"start_time": "varchar(5)"},
        "end_time": {"end_time": "varchar(5)"},
        "duration": {"duration": "varchar(5)"},
        "shift_number": {"shift_number": "integer"},
        "detail_code": {"detail_code": "integer"},
        "event_description": {"target": "event_description", "type": "string", "upper": True},
        "event_details": {"target": "event_details", "type": "string", "upper": True, "trim": True}, 
        "event_number": {"event_number": "integer"},
        "hex_value": {"hex_value": "string"},
        "type_code": {"type_code": "integer"}


}

quarantine_rules = {

        "missing player id": f.col("player_id").isNull(),
        "missing start time": f.col("start_time").isNull(),
        "missing end time": f.col("end_time").isNull()
}

quarantine_condition = reduce(lambda x, y: x | y, quarantine_rules.values())
quarantine_reason = f.array_remove(
               f.array(*[f.when(condition, reason) for reason, condition in quarantine_rules.items()]), None
               )

insert_ready = False
shift_insert_ready = False
rosters_ready_insert = False
quarantine_insert_ready = False
if ready: 
    py_source = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")[-1]
    players = spark.sql("""
                     
                    select 
                        player_id, 
                        player_name,
                        player_pos,
                        shoots_catches
                    from nhl_data_staged.players.master_ids a 
                    where 1 = 1
                     
                     """)
    shift_schema = get_schema(games)
    shifts_raw = (

        games
        .orderBy(f.col("game_date").desc())
        .withColumn("json", f.from_json("payload", shift_schema))
        .select("game_date", "game_in_play", "json.*")
        .select("game_date", "game_in_play", f.explode(f.col("data")).alias("data"))
        .select("game_date", "game_in_play", "data.*")
        .transform(lambda c: c.toDF(*[convert_case(c) for c in c.columns]))
    )
    add_fields_expr = [build_fields(src_col, rule, shifts_raw.columns) for src_col, rule in field_mapping.items()]
    shifts_data = (

        shifts_raw
        .select(
            "game_date",
            "game_in_play",
            *add_fields_expr,

        )
        .withColumn("season", 
                        (f.concat(
                        f.substring(f.col("game_id").cast("string"), 1, 4), 
                        f.substring(f.col("game_id").cast("string"), 1, 4).cast("integer") + f.lit(1)).cast("string")
                        ).cast("integer")

        )
        .withColumn("start_time", 
                    f.when(f.trim(f.col("start_time")) == "", f.lit(None)).otherwise(f.col("start_time")).cast("string")           

        )
        .withColumn("end_time", 
                    f.when(f.trim(f.col("end_time")) == "", f.lit(None)).otherwise(f.col("end_time")).cast("string")           

        )
        .withColumn("py_source", f.lit(py_source))
        .alias("s")
        .join(f.broadcast(players).alias("p"), how = "left", on = (f.col("s.player_id") == f.col("p.player_id")))
        .select(
            "season", "game_id", "game_date", "team_id", "team_abbrev", "team_name", 
            "s.player_id", "first_name", "last_name", "p.player_name", "p.player_pos", "shift_id", 
            "period", "start_time", "end_time", "duration", "shift_number", 
            "detail_code", "event_description", "event_details", "event_number", "game_in_play", 
            "hex_value", "type_code", "py_source", "p.shoots_catches"

        )

    )

    shifts_schema = t.StructType([

            t.StructField("season", t.IntegerType(), False),
            t.StructField("game_id", t.LongType(), False),
            t.StructField("game_date", t.DateType(), False),
            t.StructField("team_id", t.IntegerType(), False),
            t.StructField("team_abbrev", t.StringType(), True),
            t.StructField("team_name", t.StringType(), True),
            t.StructField("player_id", t.LongType(), False),
            t.StructField("first_name", t.StringType(), True),
            t.StructField("last_name", t.StringType(), True),
            t.StructField("player_name", t.StringType(), False),
            t.StructField("player_pos", t.StringType(), True),
            t.StructField("shift_id", t.IntegerType(), False),
            t.StructField("period", t.ByteType(), False),
            t.StructField("start_time", t.StringType(), False),
            t.StructField("end_time", t.StringType(), False),
            t.StructField("duration", t.StringType(), False),
            t.StructField("shift_number", t.LongType(), False),
            t.StructField("detail_code", t.IntegerType(), True),
            t.StructField("event_description", t.StringType(), True),
            t.StructField("event_details", t.StringType(), True), 
            t.StructField("event_number", t.IntegerType(), True),
            t.StructField("hex_value", t.StringType(), True),
            t.StructField("type_code", t.IntegerType(), True),
            t.StructField("game_in_play", t.BooleanType(), False),
            t.StructField("py_source", t.StringType(), False)


    ])

    rosters_data = (

            shifts_data 
            .select("season", "game_id", "team_id", "team_abbrev", "player_id", "player_name", "player_pos", 
                    f.lit(None).alias("jersey_num"), 
                    "shoots_catches", 
                    f.lit(None).alias("birth_dte"),
                    f.lit(None).alias("height_inches"),
                    f.lit(None).alias("weight_lbs"),
                    "game_in_play", 
                    "py_source"
                    
                    )
            .dropDuplicates(["season", "player_id", "game_id"])
        
    )
    
    
    quarantine_rows = (
                        shifts_data
                        .filter(quarantine_condition)
                        .withColumn("quarantine_reason", quarantine_reason)
                                
                    )
    
    shifts_data = (
                    shifts_data
                    .drop("shoots_catches")
                    .transform(apply_schema, shifts_schema)
                    .filter(
                            (f.col("player_id").isNotNull())
                            & (f.col("start_time").isNotNull())
                            & (f.col("end_time").isNotNull())
                            
                            )

    )
    shift_insert_ready = not shifts_data.isEmpty()
    rosters_ready_insert = not rosters_data.isEmpty()
    quarantine_insert_ready = not quarantine_rows.isEmpty()

if shift_insert_ready:

    shifts_data.createOrReplaceTempView("shift_insert_tmp")
    spark.sql(f"""
              
            with src as (

                select 
                    s.season, 
                    s.game_id,
                    s.game_date,
                    t.team_id, 
                    t.team_abbrev,
                    t.team_name,
                    s.player_id,
                    s.first_name, 
                    s.last_name,
                    s.player_name,
                    s.shift_id,
                    s.period,
                    s.start_time,
                    s.end_time,
                    s.duration,
                    s.shift_number,
                    s.detail_code,
                    s.event_description,
                    s.event_details,
                    s.event_number,
                    s.hex_value,
                    s.type_code,
                    s.game_in_play,
                    s.py_source
                from shift_insert_tmp s 
                inner join nhl_data_staged.teams.master_ids t 
                    on s.team_id = t.team_id 
                where 1 = 1
            )
            
            merge into nhl_data_staged.games.shift_data t 
            using src s 
                on t.game_id = s.game_id 
                and t.game_date = s.game_date 
                and t.team_id = s.team_id
                and t.shift_id = s.shift_id 
                and t.game_date between 
                    date_sub(from_utc_timestamp(current_timestamp(), 'America/Chicago')::date, 2) 
                    and 
                    from_utc_timestamp(current_timestamp(), 'America/Chicago')::date
        
            when matched and (

                t.active_row = true
                and (

                    not (t.game_date <=> s.game_date)
                    or not (t.player_name <=> s.player_name)
                    or not (t.player_id <=> s.player_id)
                    or not (s.shift_number <=> s.shift_number)
                    or not (t.start_time <=> s.start_time)
                    or not (t.end_time <=> s.end_time)
                    or not (t.duration <=> s.duration) 
                    or not (t.event_details <=> s.event_details)
                    
                    
                    )
            
            )

            then update set 

                game_date = s.game_date,
                player_id = s.player_id,
                first_name = s.first_name, 
                last_name = s.last_name,
                player_name = s.player_name,
                period = s.period,
                start_time = s.start_time,
                end_time = s.end_time,
                duration = s.duration,
                shift_number = s.shift_number,
                detail_code = s.detail_code,
                event_description = s.event_description,
                event_details = s.event_details,
                event_number = s.event_number,
                type_code = s.type_code,
                game_in_play = s.game_in_play,
                update_dte = current_timestamp(),
                py_source = s.py_source,
                active_row = true,
                logic_block = "match one"

            when matched and 
                t.game_date between date_sub(from_utc_timestamp(current_timestamp(), 'America/Chicago')::date, 2) and from_utc_timestamp(current_timestamp(), 'America/Chicago')::date
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
                team_id, 
                team_abbrev,
                team_name,
                player_id,
                first_name, 
                last_name,
                player_name,
                shift_id,
                period,
                start_time,
                end_time,
                duration,
                shift_number,
                detail_code,
                event_description,
                event_details,
                event_number,
                hex_value,
                type_code,
                game_in_play,
                insert_dte,
                update_dte,
                py_source,
                active_row,
                logic_block
            )
            values (

                s.season,
                s.game_id,
                s.game_date,
                s.team_id,
                s.team_abbrev,
                s.team_name,
                s.player_id,
                s.first_name,
                s.last_name,
                s.player_name,
                s.shift_id,
                s.period,
                s.start_time,
                s.end_time,
                s.duration,
                s.shift_number,
                s.detail_code,
                s.event_description,
                s.event_details,
                s.event_number,
                s.hex_value,
                s.type_code,
                s.game_in_play,
                current_timestamp(),
                null,
                s.py_source,
                true,
                "insert"
            )

            when not matched by source 
                and t.game_in_play = true
                and t.active_row = true
                ---below ensures that only rows that are on current date are being looked at for updates that are missing in source
                and t.game_date = from_utc_timestamp(current_timestamp(), 'America/Chicago')::date

            then update set 

                t.update_dte = current_timestamp(),
                t.py_source = '{py_source}',
                t.active_row = false,
                t.logic_block = "no match update one"

            when not matched by source 
                and t.game_in_play = false
                and t.active_row = true
                and t.game_date between date_sub(from_utc_timestamp(current_timestamp(), 'America/Chicago')::date, 2) and date_sub(from_utc_timestamp(current_timestamp(), 'America/Chicago')::date, 1)

            then update set 

                t.update_dte = current_timestamp(),
                t.py_source = '{py_source}',
                t.active_row = false,
                t.logic_block = "no match update two"
            
              
    """)
    spark.catalog.dropTempView("shift_insert_tmp")
    print(f"Shift data successfully loaded into nhl_data_staged.games.shift_data table")
    if datetime.datetime.today().day % 5 == 1:
        spark.sql("""analyze table nhl_data_staged.games.shift_data compute statistics;""")
        spark.sql("""optimize nhl_data_staged.games.shift_data;""")
        spark.sql("""vacuum nhl_data_staged.games.shift_data;""")
        spark.sql("""analyze table nhl_data.games.shift_data compute statistics;""")
        spark.sql("""optimize nhl_data.games.shift_data;""")
        spark.sql("""vacuum nhl_data.games.shift_data;""")
else: 
    print(f"No new data to insert into nhl_data_staged.games.shift_data, skipping insert")

if rosters_ready_insert: 

    rosters_data.createOrReplaceTempView("rosters_ready_tmp")
    spark.sql(f"""
            
            with src as (

                select * 
                from rosters_ready_tmp
                where 1 = 1
                    and player_id is not null 

            )
            merge into nhl_data_staged.players.player_game_rosters t 
            using src s 
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
                null,
                s.shoots_catches,
                '1900-01-01'::date,
                null,
                null,
                null,
                true,
                null,
                s.game_in_play,
                current_timestamp(),
                null,
                null,
                null,
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
                    t.py_source = '{py_source}',
                    t.is_active = false

    """)
    spark.catalog.dropTempView("rosters_ready_tmp")
    print(f"Shift data successfully loaded into nhl_data_staged.players.player_game_rosters table")
else: 
    print(f"No new data to insert into nhl_data_staged.players.player_game_rosters, skipping insert")

if quarantine_insert_ready:

    quarantine_rows.createOrReplaceTempView("shift_quarantine_tmp")
    spark.sql("""
                
        merge into nhl_data_staged.quarantine.shift_data t 
        using shift_quarantine_tmp s 
            on t.season = s.season 
            and t.game_id = s.game_id
            and t.game_date = s.game_date
            and t.shift_id = s.shift_id


        when not matched by target then insert (

            season, 
            game_id,
            game_date,
            team_id, 
            team_abbrev,
            team_name,
            player_id,
            first_name, 
            last_name,
            player_name,
            shift_id,
            period,
            start_time,
            end_time,
            duration,
            shift_number,
            detail_code,
            event_description,
            event_details,
            event_number,
            hex_value,
            type_code,
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
            s.team_id,
            s.team_abbrev,
            s.team_name,
            s.player_id,
            s.first_name,
            s.last_name,
            s.player_name,
            s.shift_id,
            s.period,
            s.start_time,
            s.end_time,
            s.duration,
            s.shift_number,
            s.detail_code,
            s.event_description,
            s.event_details,
            s.event_number,
            s.hex_value,
            s.type_code,
            s.game_in_play,
            current_timestamp(),
            null,
            s.py_source,
            false,
            s.quarantine_reason
        )
                
    """)
    spark.catalog.dropTempView("shift_quarantine_tmp")
    print(f"Shift data successfully loaded into nhl_data_staged.quarantine.shift_data table")
else: 
    print(f"No new data to insert into nhl_data_staged.quarantine.shift_data, skipping insert")

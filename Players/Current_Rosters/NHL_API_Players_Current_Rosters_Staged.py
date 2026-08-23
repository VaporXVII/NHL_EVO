import sys 
username = spark.sql("select current_user()").first()[0]
sys.path.append(f"/Workspace/Users/{username}/NHL_Pipeline")

from pyspark.sql import SparkSession 
from pyspark.sql import functions as f, types as t, Window as w, DataFrame
from delta.tables import DeltaTable
from zoneinfo import ZoneInfo 
import datetime, re
from pipeline_funcs.schema_utils import convert_case, apply_schema, get_schema
from pipeline_funcs.user_utc_region import region_return
from pipeline_funcs.table_maint import run_table_maint

user_region = region_return()
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", f"{user_region}")
central_timezone = ZoneInfo(f"{user_region}")

def wrangle_data(df: DataFrame, pos: str) -> DataFrame:

    d = (

        df 
        .select("game_id", "request_key", "team_id", "json.*")
        .withColumn(pos[0], f.explode(f.col(pos)))
        .select("game_id", "request_key", "team_id", f"{pos[0]}.*")
        .select("*", "firstName.*")
        .withColumnsRenamed({"request_key": "team_abbrev", "default": "first_name"})
        .select("*", "lastName.*")
        .withColumnsRenamed(
                        {
                        "default": "last_name", 
                        "id": "player_id",
                        "positionCode": "player_pos", 
                        "shootsCatches": "shoots_catches", 
                        "sweaterNumber": "jersey_num", 
                        "birthCountry": "birth_country", 
                        "birthDate": "birth_dte", 
                        "heightInInches": "height_inches",
                        "weightInPounds": "weight_lbs"
                         })
        .withColumn("player_name", f.concat_ws(" ", f.trim(f.col("first_name")), f.trim(f.col("last_name"))))

    )
    d = d.drop("firstName", "lastName", "first_name", "last_name")
    base_fields = ["game_id", "team_id", "team_abbrev", "player_id", "player_name", "player_pos", "jersey_num", "shoots_catches", "birth_dte", "birth_country", "height_inches", "weight_lbs", "headshot"]
    keep_fields = base_fields + [x for x in d.columns if x.startswith("first_name_") or x.startswith("last_name_")]
    unused = [x for x in d.columns if x not in keep_fields]
    d = (
        d 
        .withColumn("unused_structs", f.lit(" | ".join(unused)))
        .withColumn("num_unused_structs", f.lit(len(unused)))
        .select(*keep_fields, "unused_structs", "num_unused_structs")
    )
 
 
    return d

def clean_data(df: DataFrame) -> DataFrame: 

    d = (

        df
        .select(*[f.trim(f.col(c)).alias(c) if isinstance(df.schema[c].dataType, t.StringType) else f.col(c) for c in df.columns])
        .select(*[f.col(c).cast(t.DateType()).alias(c) if c.endswith("_dte") else f.col(c) for c in df.columns])
        .withColumn("jersey_num", f.col("jersey_num").cast(t.IntegerType()))
        .withColumn("height_inches", f.col("height_inches").cast(t.IntegerType()))
        .withColumn("weight_lbs", f.col("weight_lbs").cast(t.IntegerType()))
        .withColumn("player_name", f.trim(f.col("player_name")))
        .withColumn("team_abbrev", f.upper(f.trim(f.col("team_abbrev"))))
        .withColumn("player_pos", f.upper(f.trim(f.col("player_pos"))))
        .withColumn("shoots_catches", f.upper(f.trim(f.col("shoots_catches"))))

    )

    return d

pgr_schema = t.StructType([

    t.StructField("season", t.IntegerType(), False),
    t.StructField("game_id", t.LongType(), False), 
    t.StructField("team_id", t.IntegerType(), False),
    t.StructField("team_abbrev", t.StringType(), False),
    t.StructField("player_id", t.LongType(), False),
    t.StructField("player_name", t.StringType(), False),
    t.StructField("player_pos", t.StringType(), True), 
    t.StructField("jersey_num", t.IntegerType(), True), 
    t.StructField("shoots_catches", t.StringType(), True), 
    t.StructField("birth_dte", t.DateType(), True), 
    t.StructField("birth_country", t.StringType(), True), 
    t.StructField("height_inches", t.IntegerType(), True), 
    t.StructField("weight_lbs", t.IntegerType(), True), 
    t.StructField("is_active", t.BooleanType(), False),
    t.StructField("py_source", t.StringType(), False), 
    t.StructField("headshot", t.StringType(), True), 
    t.StructField("unused_structs", t.StringType(), True),
    t.StructField("num_unused_structs", t.IntegerType(), True)

])

players_master = spark.sql("""
                           
                    select 
                        player_id as player_id_nhl_api, 
                        player_name as player_name_nhl_api
                    from nhl_data_staged.players.master_ids
                    where 1 = 1

    """)

pgr_raw = spark.sql(f"""
                    
                    select 
                        request_key,
                        team_id, 
                        game_id, 
                        payload
                    from nhl_data_raw.players.player_game_rosters a 
                    where 1 = 1
                        and payload is not null 
                        and payload not in ('[]', '{}')
                        and ingest_ts_utc::date = from_utc_timestamp(current_timestamp(), '{user_region}')::date
                    --qualify ingest_ts_utc = max(ingest_ts_utc) over ()
                        
                    
    """)

game_tracker = spark.sql(f"""
                         
                    with date_param as (


                        select from_utc_timestamp(current_timestamp(), '{user_region}')::date as current_run_date

                    ) 
                    ,
                    finished_games as (

                        select /*+ broadcast (p) */ distinct 
                            a.game_id
                        from nhl_data_staged.games.pbp_data a 
                        cross join date_param p 
                        where 1 = 1
                            and a.game_date <= p.current_run_date
                            and a.event_type = 'game-end'
                    ) 

                    select /*+ broadcast (b), broadcast (p) */ distinct 
                        a.game_id, 
                        a.game_date, 
                        a.team_abbrev, 
                        case when a.season <= 20092010 then false 
                            when b.game_id is not null then false 
                            when a.game_date = date_sub(p.current_run_date, 2) then false 
                            when a.game_date = date_sub(p.current_run_date, 1) and b.game_id is null then true 
                            else true 
                            end as game_in_play   
                    from nhl_data_staged.games.schedules a 
                    left join finished_games b  
                        on a.game_id = b.game_id 
                    cross join date_param p 
                    where 1 = 1
                        and a.game_type in (2,3)
                        ---limiting to only games that have been finished and were played up until the current date
                        and a.game_date <= p.current_run_date
        
    """)

pbp_prior_games = spark.sql(f"""
                            
                        ---logic below is used to pull player ids from the pbp data for all games including the previous day to check 
                        ---if there were any players that may have been missed by current rosters staged logic or weren't the rosters source that 
                        ---comes from the pbp api
                        with player_stack_past as (

                            select distinct 
                                a.season, 
                                a.game_id, 
                                a.game_date, 
                                b.player_id,
                                b.event_team_id as team_id,
                                b.event_team_abbrev as team_abbrev, 
                                a.py_source
                            from nhl_data_staged.games.pbp_data a
                            join lateral stack(
                                12,
                                player_id, event_team_id, event_team_abbrev, 'primary_player',
                                shooting_player_id, event_team_id, event_team_abbrev, 'shooter',
                                scoring_player_id, event_team_id, event_team_abbrev, 'scorer',
                                assist1_player_id, event_team_id, event_team_abbrev, 'assist_primary',
                                assist2_player_id, event_team_id, event_team_abbrev, 'assist_secondary',
                                goalie_in_net_id, event_team_id, event_team_abbrev, 'goalie',
                                penalty_committed_by_player_id, event_team_id, event_team_abbrev, 'penalty_committer',
                                penalty_drawn_by_player_id, event_team_id, event_team_abbrev, 'penalty_drawn',
                                hit_given_by_player_id, event_team_id, event_team_abbrev, 'hitter',
                                hit_taken_by_player_id, event_team_id, event_team_abbrev, 'hit_receiver',
                                faceoff_winning_player_id, event_team_id, event_team_abbrev, 'faceoff_winner',
                                faceoff_losing_player_id, event_team_id, event_team_abbrev, 'faceoff_loser'
                                ) as b(player_id, event_team_id, event_team_abbrev, player_role)
                            ---allows user to get all back on the first run of the pipeline, then moving forward skips them 
                            left anti join nhl_data_staged.players.player_game_rosters c 
                                on a.season = c.season
                                and a.game_id = c.game_id
                                and a.game_date = c.game_date 
                                and b.player_id = c.player_id
                            where 1 = 1
                                and a.game_date < from_utc_timestamp(current_timestamp(), '{user_region}')::date
                                and a.event_team_id is not null
                                and a.event_type not in ('period-start', 'period-end', 'stoppage', 'game-start', 'game-end')
                                and b.player_id is not null 
                                and b.player_role not in ('faceoff_loser', 'hit_receiver') 
                        )
                        ,
                        player_stack_current as (
                        
                            select distinct 
                                a.season, 
                                a.game_id, 
                                a.game_date, 
                                b.player_id,
                                b.event_team_id as team_id,
                                b.event_team_abbrev as team_abbrev, 
                                a.py_source
                            from nhl_data_staged.games.pbp_data a
                            join lateral stack(
                                12,
                                player_id, event_team_id, event_team_abbrev, 'primary_player',
                                shooting_player_id, event_team_id, event_team_abbrev, 'shooter',
                                scoring_player_id, event_team_id, event_team_abbrev, 'scorer',
                                assist1_player_id, event_team_id, event_team_abbrev, 'assist_primary',
                                assist2_player_id, event_team_id, event_team_abbrev, 'assist_secondary',
                                goalie_in_net_id, event_team_id, event_team_abbrev, 'goalie',
                                penalty_committed_by_player_id, event_team_id, event_team_abbrev, 'penalty_committer',
                                penalty_drawn_by_player_id, event_team_id, event_team_abbrev, 'penalty_drawn',
                                hit_given_by_player_id, event_team_id, event_team_abbrev, 'hitter',
                                hit_taken_by_player_id, event_team_id, event_team_abbrev, 'hit_receiver',
                                faceoff_winning_player_id, event_team_id, event_team_abbrev, 'faceoff_winner',
                                faceoff_losing_player_id, event_team_id, event_team_abbrev, 'faceoff_loser'
                                ) as b(player_id, event_team_id, event_team_abbrev, player_role)
                            ---below is meant to remove players that have already been placed into player game rosters table for the game 
                            ---in question (i.e. Connor McDavid in game id 123 is already in the player game rosters table, leave him out)
                            left anti join nhl_data_staged.players.player_game_rosters c 
                                on a.season = c.season 
                                and a.game_id = c.game_id
                                and a.game_date = c.game_date 
                                and b.player_id = c.player_id
                            where 1 = 1
                                and a.game_date = from_utc_timestamp(current_timestamp(), '{user_region}')::date 
                                and a.event_team_id is not null
                                and a.event_type not in ('period-start', 'period-end', 'stoppage', 'game-start', 'game-end')
                                and b.player_id is not null 
                                and b.player_role not in ('faceoff_loser', 'hit_receiver') 

                        )
                
                        select /* broadcast (pm) */
                            p.*, 
                            pm.player_pos, 
                            pm.jersey_num::integer as jersey_num,
                            pm.shoots_catches,
                            pm.player_name
                        from player_stack_past p 
                        left join nhl_data_staged.players.master_ids pm 
                            on p.player_id = pm.player_id
                        where 1 = 1
                            and pm.player_name is not null
                        union all
                        select /* broadcast (pm) */
                            c.*,
                            pm.player_pos, 
                            pm.jersey_num::integer as jersey_num,
                            pm.shoots_catches,
                            pm.player_name
                        from player_stack_current c
                        left join nhl_data_staged.players.master_ids pm 
                            on c.player_id = pm.player_id 
                        where 1 = 1
                            and pm.player_name is not null
                        
                            
    """)

ready = not pgr_raw.isEmpty()
pbp_players_ready = not pbp_prior_games.isEmpty()

insert_ready = False 
if ready: 
    
    py_source = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")[-1]
    pgr_raw_schema = (

        pgr_raw 
        .select("team_id", f.schema_of_json(f.col("payload")).alias("schema"))
        .limit(1)
        .collect()[0]["schema"]
    )
    pgr_raw_schema = get_schema(pgr_raw)
    pgr = (

        pgr_raw 
        .withColumn("json", f.from_json(f.col('payload'), pgr_raw_schema))

    )
    if not pgr.isEmpty():
        defense = pgr.transform(wrangle_data, "defensemen").transform(clean_data)
        forwards = pgr.transform(wrangle_data, "forwards").transform(clean_data)
        goalies = pgr.transform(wrangle_data, "goalies").transform(clean_data)
        pgr_df = apply_schema(
            
                defense 
                .unionByName(forwards)
                .unionByName(goalies)
                .filter(f.col("player_id").isNotNull())
                .dropDuplicates(["game_id", "player_id"])
                .withColumn("season", 
                        (f.concat(
                        f.substring(f.col("game_id").cast("string"), 1, 4), 
                        f.substring(f.col("game_id").cast("string"), 1, 4).cast("integer") + f.lit(1)).cast("string")
                        ).cast("integer")

                )
                #setting is_active to True since the player is on the roster as of the date of the scrape
                .withColumn("is_active", f.lit(True))
                .withColumn("py_source", f.lit(py_source))

            , pgr_schema)
        
        player_id_window = w.partitionBy("player_id", "game_id").orderBy(f.col("is_active").desc())
        master_pgr_merged = (

        players_master 
        .alias("pm")
        .join(pgr_df.alias("pgr"), how = "inner", on = f.col("pm.player_id_nhl_api") == f.col("pgr.player_id"))
        .withColumn("player_name", f.col("pm.player_name_nhl_api"))
        .drop("player_id", "player_name_nhl_api")
        .withColumnsRenamed({"player_id_nhl_api": "player_id"})
        .withColumn("last_active_season", f.concat(

                                                f.substring(f.col("game_id").cast(t.StringType()), 1, 4), 
                                                (f.substring(f.col("game_id").cast(t.StringType()), 1, 4).cast(t.IntegerType()) + 1).cast(t.StringType())
                                    )
                )
        .withColumn("row_num", f.row_number().over(player_id_window))
        .filter(f.col("row_num") == 1)
        .alias("t")
        .join(
                f.broadcast(game_tracker 
                .select(f.col("game_id").alias("game_tracker_game_id"), f.col("game_in_play"))
                )
                .alias("g"),
                how = "left", 
                on = (f.col("t.game_id") == f.col("g.game_tracker_game_id"))

        )
        .withColumn("game_in_play", f.coalesce("g.game_in_play", f.lit(False)))
        .drop("game_tracker_game_id")
        )
        pbp_prior = (

            pbp_prior_games
            .alias("pbp")
            .join(
            (
                master_pgr_merged 
                .select(
                        f.col("player_id").alias("mgr_player_id"),
                        "birth_dte",
                        "birth_country", 
                        "height_inches",
                        "weight_lbs", 
                        "headshot",
                        f.col("jersey_num").alias("mgr_jersey_num"),
                        "last_active_season",
                        "row_num"
                )
                .alias("m")
                ), how = "left", on = (f.col("pbp.player_id") == f.col("m.mgr_player_id"))
            )
            .drop("mgr_player_id")
            .select( 
                    "season", "game_id", "team_id", "team_abbrev", "player_id", "player_name", "player_pos", 
                    f.col("mgr_jersey_num").alias("jersey_num"),
                    "pbp.shoots_catches", 
                    "m.birth_dte",
                    "m.birth_country",
                    "m.height_inches", 
                    "m.weight_lbs",    
                    f.lit(False).alias("is_active"), 
                    "pbp.py_source",
                    "m.headshot", 
                    f.lit('[]').alias("unused_structs"),
                    f.lit(0).alias("num_unused_structs"),
                    "last_active_season", 
                    "row_num",
                    f.lit(False).alias("game_in_play"), 

            )
        )
        master_pgr_merged = (

            master_pgr_merged
            .filter(f.col("player_id").isNotNull())
            .unionByName(
                pbp_prior.filter(f.col("player_id").isNotNull())
            )
            .dropDuplicates(["player_id", "season", "game_id"])
           

        )
        insert_ready = not master_pgr_merged.isEmpty()

if insert_ready: 

    master_pgr_merged.createOrReplaceTempView("player_game_rosters_tmp")
    spark.sql(f"""
              
        with src as (

            select 
                s.game_date,
                p.*
            from player_game_rosters_tmp p 
            inner join nhl_data_staged.games.schedules s 
                on p.season = s.season 
                and p.game_id = s.game_id 
                and p.team_id = s.team_id 

        )
              
        merge into nhl_data_staged.players.player_game_rosters t 
        using src s
            on t.season = s.season 
            and t.game_id = s.game_id 
            and t.game_date = s.game_date
            and t.player_id = s.player_id
            and t.game_date between 
                date_sub(from_utc_timestamp(current_timestamp(), '{user_region}')::date, 2) 
                and 
                from_utc_timestamp(current_timestamp(), '{user_region}')::date 
            

        when matched and (

            not (lower(trim(t.player_name)) <=> lower(trim(s.player_name)))
            or not (t.team_abbrev <=> s.team_abbrev)
            or not (t.team_id <=> s.team_id) 
            or not (t.jersey_num <=> s.jersey_num) 
            or not (t.shoots_catches <=> s.shoots_catches)
            or t.game_in_play <> s.game_in_play 

        )

        then update set 

            player_name = s.player_name,
            team_id = s.team_id,
            team_abbrev = s.team_abbrev,
            jersey_num = s.jersey_num,
            shoots_catches = s.shoots_catches,
            game_in_play = s.game_in_play,
            is_active = true,
            update_dte = current_timestamp(),
            py_source = s.py_source

        when not matched then insert (

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
            coalesce(s.birth_dte, to_date('1900-01-01')),
            s.birth_country,
            s.height_inches,
            s.weight_lbs,
            true,
            s.headshot,
            s.game_in_play,
            current_timestamp(),
            null,
            s.unused_structs,
            s.num_unused_structs,
            s.py_source
        )
        ;
                    
    """)
    print(f"Team rosters data successfully loaded into nhl_data_staged.players.player_game_rosters table")
else: 
    print(f"No new data to insert into nhl_data_staged.players.player_game_rosters table.")
if datetime.datetime.today().day % 5 == 0:
    run_table_maint(spark, "nhl_data_staged.players.player_game_rosters")
    run_table_maint(spark, "nhl_data.players.player_game_rosters")

if insert_ready: 

    spark.sql(f"""
              
            with last_loaded as (

                select 
                coalesce(max(season), 19001901) as latest_season,
                coalesce(max(game_date), '1900-01-01') as latest_game_date
            from nhl_data_staged.games.schedules 
            where 1 = 1
                and game_type in (2,3)
                and from_utc_timestamp(current_timestamp(), '{user_region}')::date >= game_date

            )
            , 
            players_prepped as (

                select 
                    a.player_id, 
                    b.player_name,
                    coalesce(a.player_pos, 'u') as player_pos, 
                    a.team_id,
                    a.team_abbrev,
                    b.team_id_prev_team,
                    b.team_abbrev_prev_team,
                    a.season as last_active_season,
                    coalesce(a.jersey_num, 999) as jersey_num,
                    coalesce(a.shoots_catches, 'u') as shoots_catches
                from nhl_data_staged.players.player_game_rosters a 
                inner join nhl_data_staged.players.master_ids b 
                    on a.player_id = b.player_id
                cross join last_loaded c  
                where 1 = 1
                    and (a.player_id is not null and a.team_id is not null and a.player_name is not null)
                    and a.season >= c.latest_season - 20000
                    and a.game_date >= c.latest_game_date
                qualify row_number() over (partition by a.player_id order by a.season desc, a.game_date desc) = 1

            )
            ,
            src as (

                select 
                    p.*,
                    (
                    (
                    if(nullif(player_pos, 'u') is null, 1, 0) + 
                    if(team_id_prev_team is null, 1, 0) + 
                    if(team_abbrev_prev_team is null, 1, 0) + 
                    if(nullif(jersey_num, 999) is null, 1, 0) + 
                    if(nullif(shoots_catches, 'u') is null, 1, 0) 
                    ) / 16
                    )::decimal(4,2) as required_field_empty_rate
                from players_prepped p

            )

            merge into nhl_data_staged.players.master_ids t 
            using src s 
                on t.player_id = s.player_id 

            when matched and (

                not (coalesce(t.team_id, 0) <=> s.team_id) 
                or (not (coalesce(t.jersey_num, 999) <=> s.jersey_num) and s.jersey_num <> 999)
                or (not (coalesce(t.shoots_catches, 'u') <=> s.shoots_catches) and s.shoots_catches <> 'u')
                or (not (coalesce(t.player_pos, 'u') <=> s.player_pos) and s.player_pos <> 'u')

            )

            then update set 

                player_pos = s.player_pos,
                team_id = s.team_id,
                team_abbrev = s.team_abbrev,
                team_id_prev_team = case when not (coalesce(t.team_id, 0) <=> s.team_id) then t.team_id else t.team_id_prev_team end,
                team_abbrev_prev_team = case when not (coalesce(t.team_id, 0) <=> s.team_id) then t.team_abbrev else t.team_abbrev_prev_team end,
                last_active_season = greatest(t.last_active_season, s.last_active_season),
                shoots_catches = s.shoots_catches,
                jersey_num = s.jersey_num,
                update_dte = current_timestamp(),
                py_source = '{py_source}',
                failed_condition = nullif(
                                    concat_ws(
                                        ', '
                                        , filter(
                                            array(
                                                
                                                case when not (coalesce(t.team_id, 0) <=> s.team_id) then 'team_id' end,
                                                case when (not (coalesce(t.jersey_num, 999) <=> s.jersey_num) and s.jersey_num <> 999) then 'jersey_num' end,
                                                case when (not (coalesce(t.shoots_catches, 'u') <=> s.shoots_catches) and s.shoots_catches <> 'u') then 'shoots_catches' end,
                                                case when (not (coalesce(t.player_pos, 'u') <=> s.player_pos) and s.player_pos <> 'u') then 'player_pos' end
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
                nullif(s.player_pos, 'u'),
                s.team_id,
                s.team_abbrev,
                null,
                null,
                s.last_active_season,
                nullif(s.jersey_num, 999),
                nullif(s.shoots_catches, 'u'),
                true,
                current_timestamp(),
                null,
                s.required_field_empty_rate,
                '{py_source}',
                null

            )
            ;
                        
    """)
    print(f"Player data successfully inserted/updated into nhl_data_staged.players.master_ids table")
else: 
    print(f"No new data to insert/updated into nhl_data_staged.players.master_ids table.")

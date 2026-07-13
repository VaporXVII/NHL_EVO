import sys
username = spark.sql("select current_user()").first()[0]
sys.path.append(f"/Workspace/Users/{username}/NHL_Pipeline")

from pyspark.sql import SparkSession 
from pyspark.sql import functions as f, types as t, window as w, DataFrame
from delta.tables import DeltaTable
from zoneinfo import ZoneInfo 
import datetime, re
from pipeline_funcs.schema_utils import convert_case, apply_schema, unpack_data

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "America/Chicago")

spec_teams_raw = spark.sql(r"""
                           
                         with date_param as (

                              select from_utc_timestamp(current_timestamp(), 'America/Chicago')::date as current_run_date 
                         )
                         , 
                         raw_array as (

                              select /*+ broadcast (p) */
                                   a.endpoint,
                                   a.request_key,
                                   a.payload,
                                   ---extracting game_ids from payload to cross check if they are already in the staging table
                                   regexp_extract_all(payload, '"gameId"\\s*:\\s*([0-9]+)', 1) as game_id
                              from nhl_data_raw.games.special_teams a 
                              cross join date_param p 
                              where 1 = 1 
                                   and endpoint ilike '%penalty%'
                                   and endpoint ilike '%time%'
                                   and http_status = 200 
                                   and payload is not null 
                                   and payload not in ('[]', '{}')
                                   and get_json_object(payload, "$.total")::integer > 0
                                   and from_utc_timestamp(ingest_ts_utc, 'America/Chicago')::date between date_sub(current_run_date, 2) and current_run_date
                         )
                         , 
                         raw_data as (

                              select distinct
                                   a.endpoint, 
                                   a.request_key, 
                                   a.payload, 
                                   raw_game_id::bigint as game_id
                              from raw_array a 
                              lateral view explode(a.game_id) g as raw_game_id

                         )
                         select /*+ broadcast (p) */
                              a.* 
                         from raw_data a  
                         cross join date_param p
                         ---below removes games that are already in raw that had a game date more than two days ago 
                         ---this is to enable a two day lookback window in the event that NHL updates special teams data after the game has ended
                         left anti join nhl_data_staged.games.pk_toi b 
                              on a.game_id = b.game_id
                              and b.game_date < date_sub(current_run_date, 2)
                              

     """)

insert_ready = False
if not spec_teams_raw.isEmpty():

    py_source = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")[-1]
    spec_teams = unpack_data(spec_teams_raw)
    spec_teams = (
        
        spec_teams
        .toDF(*[convert_case(c) for c in spec_teams.columns])
        .withColumnsRenamed({"team_full_name": "team_name", "opponent_team_abbrev": "opp_team_abbrev"})
        .withColumn("team_name", f.trim((f.col("team_name"))))
        .withColumn("opp_team_abbrev", f.trim(f.upper(f.col("opp_team_abbrev"))))
        .withColumn("season", 
                        (f.concat(
                        f.substring(f.col("game_id").cast("string"), 1, 4), 
                        f.substring(f.col("game_id").cast("string"), 1, 4).cast("integer") + f.lit(1)).cast("string")
                        ).cast("integer")

        )
        .withColumn("home_road", f.trim(f.upper(f.col("home_road"))))
        .withColumn("py_source", f.lit(py_source))
        .distinct()
    )
    insert_ready = not spec_teams.isEmpty()

if insert_ready: 
    spec_teams.createOrReplaceTempView("pk_toi_staged_tmp")
    spark.sql("""
              
        with params as (

            select from_utc_timestamp(current_timestamp(), 'America/Chicago')::date as current_run_date

        )
        , 
        src as (

            select /*+ broadcast (p) */
                a.*
            from pk_toi_staged_tmp a 
            cross join params p 
            left anti join nhl_data_staged.games.pk_toi b 
                on a.season = b.season
                and a.game_id = b.game_id
                and a.game_date = b.game_date 
                and a.team_id = b.team_id 
                and b.game_date < date_sub(p.current_run_date, 2)

        )

        merge into nhl_data_staged.games.pk_toi t 
        using src s 
            on t.season = s.season 
            and t.game_id = s.game_id 
            and t.game_date = s.game_date 
            and t.team_id = s.team_id 
            and (
                t.game_date between
                    date_sub(
                        from_utc_timestamp(current_timestamp(), 'America/Chicago')::date,
                        2
                    )
                    and from_utc_timestamp(current_timestamp(), 'America/Chicago')::date
            )     
              
              
        when matched and (

            t.home_road <> s.home_road
            or t.opp_team_abbrev <> s.opp_team_abbrev
            or coalesce(t.time_on_ice_shorthanded, 0) <> coalesce(s.time_on_ice_shorthanded, 0)
            or coalesce(t.times_shorthanded, 0) <> coalesce(s.times_shorthanded, 0)
            or coalesce(t.shorthanded_goals_against, 0) <> coalesce(s.shorthanded_goals_against, 0)

        )
              
        then update set 
        
            team_name = s.team_name,
            home_road = s.home_road,
            opp_team_abbrev = s.opp_team_abbrev,
            goals_against3v4 = s.goals_against3v4,
            goals_against3v5 = s.goals_against3v5,
            goals_against4v5 = s.goals_against4v5,
            overall_penalty_kill_pct = s.overall_penalty_kill_pct,
            penalty_kill_pct3v4 = s.penalty_kill_pct3v4,
            penalty_kill_pct3v5 = s.penalty_kill_pct3v5,
            penalty_kill_pct4v5 = s.penalty_kill_pct4v5,
            point_pct = s.point_pct,
            shorthanded_goals_against = s.shorthanded_goals_against,
            time_on_ice3v4 = s.time_on_ice3v4,
            time_on_ice3v5 = s.time_on_ice3v5,
            time_on_ice4v5 = s.time_on_ice4v5,
            time_on_ice_shorthanded = s.time_on_ice_shorthanded,
            times_shorthanded = s.times_shorthanded,
            times_shorthanded3v4 = s.times_shorthanded3v4,
            times_shorthanded3v5 = s.times_shorthanded3v5,
            times_shorthanded4v5 = s.times_shorthanded4v5,
            update_dte = current_timestamp(),
            py_source = s.py_source

        when not matched then insert (

            season,
            game_id,
            game_date,
            team_id,
            team_name,
            home_road,
            opp_team_abbrev,
            goals_against3v4,
            goals_against3v5,
            goals_against4v5,
            overall_penalty_kill_pct,
            penalty_kill_pct3v4,
            penalty_kill_pct3v5,
            penalty_kill_pct4v5,
            point_pct,
            shorthanded_goals_against,
            time_on_ice3v4,
            time_on_ice3v5,
            time_on_ice4v5,
            time_on_ice_shorthanded,
            times_shorthanded,
            times_shorthanded3v4,
            times_shorthanded3v5,
            times_shorthanded4v5,
            insert_dte,
            update_dte,
            py_source

        )

        values (

            s.season,
            s.game_id,
            s.game_date,
            s.team_id,
            s.team_name,
            s.home_road,
            s.opp_team_abbrev,
            s.goals_against3v4,
            s.goals_against3v5,
            s.goals_against4v5,
            s.overall_penalty_kill_pct,
            s.penalty_kill_pct3v4,
            s.penalty_kill_pct3v5,
            s.penalty_kill_pct4v5,
            s.point_pct,
            s.shorthanded_goals_against,
            s.time_on_ice3v4,
            s.time_on_ice3v5,
            s.time_on_ice4v5,
            s.time_on_ice_shorthanded,
            s.times_shorthanded,
            s.times_shorthanded3v4,
            s.times_shorthanded3v5,
            s.times_shorthanded4v5,
            current_timestamp(),
            null,
            s.py_source

        )

    """)
    spark.catalog.dropTempView("pk_toi_staged_tmp")
    print(f"Schedules data successfully loaded into nhl_data_staged.games.pk_toi table")
else: 
    print(f"No new data to insert into nhl_data_staged.games.pk_toi, skipping insert")
if datetime.datetime.today().day % 5 == 0:
    spark.sql("analyze table nhl_data_staged.games.pk_toi compute statistics;")

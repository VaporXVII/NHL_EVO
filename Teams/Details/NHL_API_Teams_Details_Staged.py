import sys 
username = spark.sql("select current_user()").first()[0]
sys.path.append(f"/Workspace/Users/{username}/NHL_Pipeline")

from pyspark.sql import SparkSession 
from pyspark.sql import functions as f, types as t
from pyspark.sql import DataFrame
from delta.tables import DeltaTable
from zoneinfo import ZoneInfo 
import datetime as dt
import re, math
from pipeline_funcs.schema_utils import convert_case, apply_schema
from pipeline_funcs.user_utc_region import region_return
from pipeline_funcs.table_maint import run_table_maint


user_region = region_return()
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", f"{user_region}")
central_timezone = ZoneInfo(f"{user_region}")

insert_ready = False
sched_silver = spark.sql(f"""
                         
            with season_info as (

                select 
                    max(season)::integer as current_season 
                from nhl_data_staged.games.schedules
                where 1 = 1
                    and from_utc_timestamp(current_timestamp(), '{user_region}')::date >= from_utc_timestamp(insert_dte, '{user_region}')::date
                
            )
            ,
            teams_info as (

                select distinct 
                    a.team_id, 
                    a.team_abbrev,
                    a.team_name,
                    a.team_city, 
                    min(a.pre_season_start_date) as active_from,
                    max(a.playoff_end_date) active_to,
                    max(a.season) as last_active_season
                from nhl_data_staged.games.schedules a
                group by 
                    a.team_id,
                    a.team_abbrev,
                    a.team_name,
                    a.team_city
            
            
            )

            select /*+ broadcast(b), broadcast (c) */
                a.team_id,
                a.team_abbrev,
                a.team_name,
                a.team_city,
                a.active_from,
                a.active_to,
                a.last_active_season,
                b.current_season,
                c.franchise_id
            from teams_info a 
            cross join season_info b 
            inner join nhl_data_staged.teams.master_ids c 
                on a.team_id = c.team_id
                        
    """)

if not sched_silver.isEmpty():
    
    py_source = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")[-1]
    team_details_raw = spark.sql(f"""
    
                    select 
                        season, 
                        payload 
                    from nhl_data_raw.teams.details 
                    where 1 = 1
                        and from_utc_timestamp(ingest_ts_utc, '{user_region}')::date >= from_utc_timestamp(current_timestamp(), '{user_region}')::date
                    qualify from_utc_timestamp(ingest_ts_utc, '{user_region}')::date = max(from_utc_timestamp(ingest_ts_utc, '{user_region}')::date) over () 
    
    
    """)
    team_details_schema = spark.sql("""

                    with most_recent as (

                        select 
                            payload 
                        from nhl_data_raw.teams.details
                        where 1 = 1
                            and from_utc_timestamp(current_timestamp(), '{user_region}')::date >= from_utc_timestamp(ingest_ts_utc, '{user_region}')::date
                        qualify from_utc_timestamp(ingest_ts_utc, '{user_region}')::date = max(from_utc_timestamp(ingest_ts_utc, '{user_region}')::date) over ()
                        
                    )

                    select 
                        schema_of_json_agg(payload) as json_schema 
                    from nhl_data_raw.teams.details
                    where 1 = 1


    """).first()["json_schema"]
    base_fields = ["season", "total"]
    team_details = (
        
        team_details_raw 
        .withColumn("payload_json", f.from_json(f.col("payload"), team_details_schema))
        .select("season", 
        
            f.explode("payload_json.data").alias("data"),
            f.col("payload_json.total").alias("total")
        )
        .select(*base_fields, "data.*")
        .select(*base_fields, f.col("fullName").alias("team_name"), "firstSeason.*", f.col("id").alias("franchise_id"), "lastSeason")
        .select(*base_fields, "team_name", "franchise_id", f.col("id").alias("first_season"), "lastSeason.*")
        .select(*base_fields, "team_name", "franchise_id", "first_season", f.col("id").alias("last_active_season"))
        
        
    )
    
    sched_silver.createOrReplaceTempView("sched_silver_tmp")
    team_details.createOrReplaceTempView("team_details_tmp")
    team_details_master = spark.sql(f"""

                with team_bounds as (

                        select 
                            a.*
                            ---need franchise ID to join on team_details table 
                        from sched_silver_tmp a
                        where 1 = 1

                    )
                    ,
                    team_details_info as (

                    ---teams that have a record of playing in a game that occurred during or after 2008 season
                        select 
                            a.team_id, 
                            a.team_abbrev, 
                            a.team_name,
                            a.team_city,
                            a.franchise_id,
                            ---team details NHL API endpoint only returns Original 6 and Expansion teams, but will overwrite teams that have played in one city
                            ---the moved to another (i.e. Winnipeg Jets overwrite Atlanta Thrashers) therefore the logic below is designed to 
                            ---determine whether or not a team is truly active
                            case when b.last_active_season is not null then false 
                                when extract(year from a.active_to::date)::integer < substring(a.current_season::string, 1, 4)::integer and b.last_active_season is null then false 
                                else true 
                                end as is_active,
                            a.active_from,
                            a.active_to,
                            case when extract(year from a.active_to::date)::integer < substring(a.current_season::string, 1, 4)::integer
                                then concat((extract(year from a.active_to::date)::integer - 1)::string, extract(year from a.active_to::date)::integer)
                                else a.current_season 
                                end as last_active_season
                        from team_bounds a   
                        left join team_details_tmp b 
                            on a.franchise_id = b.franchise_id 
                        union all 
                        ---teams that are considered historial which have no record of playing in a game after 2008 season (e.g. Quebec Nordiques, Hartford Whalers)
                        select 
                            a.team_id,
                            a.team_abbrev,
                            a.team_name,
                            null::string as team_city,
                            a.franchise_id,
                            false,
                            null::date as active_from,
                            null::date as active_to,
                            null::integer as last_active_season
                        from nhl_data_staged.teams.master_ids a 
                        left anti join team_bounds b 
                            on a.team_id = b.team_id 
        

                    ),
                    team_details_master as (

                        select 
                            a.team_id,
                            a.team_abbrev,
                            a.team_name,
                            a.team_city,
                            a.is_active,
                            a.active_from,
                            a.active_to,
                            case when a.last_active_season is not null then a.last_active_season
                                when a.is_active = false and b.last_active_season is null then a.last_active_season
                                else b.last_active_season
                                end as last_active_season
                        from team_details_info a 
                        left join team_details_tmp b 
                            on a.franchise_id = b.franchise_id 
                            and b.last_active_season is not null 
                    )

                    select 
                        team_id,
                        team_abbrev,
                        team_name,
                        team_city,
                        is_active,
                        active_from,
                        active_to,
                        last_active_season,
                        '{py_source}' as py_source
                    from team_details_master

    """)
    insert_ready = not team_details_master.isEmpty()

if insert_ready: 
    
    team_details_master.createOrReplaceTempView("teams_final_tmp")
    spark.sql(f"""
              
              
            merge into nhl_data_staged.teams.details t 
            using teams_final_tmp s 
                on t.team_id = s.team_id

            when matched and (

                t.team_abbrev <> s.team_abbrev
                or t.team_name <> s.team_name
                or coalesce(t.team_city, '__NULL__') <> coalesce(s.team_city, t.team_city, '__NULL__')
                or t.is_active <> s.is_active 
                or coalesce(t.active_from, '1900-01-01'::date) <> coalesce(s.active_from, t.active_from, '1900-01-01'::date)
                or coalesce(t.active_to, '1900-01-01'::date) <> coalesce(s.active_to, t.active_to, '1900-01-01'::date)
                or coalesce(t.last_active_season, 0) <> coalesce(s.last_active_season, t.last_active_season, 0)
                
            )

            then update set 

                team_abbrev = s.team_abbrev,
                team_name = s.team_name,
                team_city = s.team_city,
                active_from = s.active_from, 
                active_to = s.active_to,
                is_active = s.is_active,
                last_active_season = s.last_active_season,
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
                last_active_season,
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
                s.last_active_season,
                current_timestamp(),
                null,
                s.py_source

            )
              
    """)
    spark.catalog.dropTempView("sched_silver_tmp")
    spark.catalog.dropTempView("team_details_tmp")
    spark.catalog.dropTempView("teams_final_tmp")
    print(f"Teams data successfully loaded into nhl_data_staged.teams.details table")
    run_table_maint(spark, "nhl_data_staged.teams.details")
    run_table_maint(spark, "nhl_data.teams.details")
else: 
  print(f"No new data to insert into nhl_data_staged.teams.details, skipping insert")

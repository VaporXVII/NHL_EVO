#purpose of this notebook is to check and see if at least 30 days has passed since the season ended, based on the playoff_end_date that is stored in the NHL API
#if so, the we want to check and see if the new schedule information has been released by the NHL so those games, and any updated team information, can be loaded 
import sys 
username = spark.sql("select current_user()").first()[0]
sys.path.append(f"/Workspace/Users/{username}/NHL_Pipeline")

from pipeline_funcs.user_utc_region import region_return 
user_region = region_return()

season_ended_time_lapsed = spark.sql(f"""
              
            with date_param as (

                select from_utc_timestamp(current_timestamp(), '{user_region}')::date as current_run_dte
            ) 
            ,
            latest_season as (

                select /*+ broadcast (p) */ 
                    coalesce(max(a.season), 19001901) as season
                from nhl_data_staged.games.schedules a 
                cross join date_param p 
                where 1 = 1
                    and a.game_date <= p.current_run_dte 

            ) 
            , 
            end_dates as (

                select /*+ broadcast (b), broadcast (p) */
                    coalesce(max(a.playoff_end_date), '1900-01-01')::date as playoff_end_date          
                from nhl_data_staged.games.schedules a
                cross join date_param p  
                inner join latest_season b 
                    on a.season = b.season
                where 1 = 1
                    and a.game_date <= p.current_run_dte

            
            )

            select /*+ broadcast (p) */
                a.playoff_end_date, 
                (datediff(p.current_run_dte, a.playoff_end_date) >= 30)::boolean as time_lapsed_ind 
            from end_dates a 
            cross join date_param p

    """).first()

dbutils.jobs.taskValues.set(

    key = "season_ended_time_lapsed", 
    value = season_ended_time_lapsed["time_lapsed_ind"]
)

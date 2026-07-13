#purpose of this notebook is to check and see if at least 30 days has passed since the season ended, based on the playoff_end_date that is stored in the NHL API
#if so, the we want to check and see if the new schedule information has been released by the NHL so those games, and any updated team information, can be loaded 
season_ended_time_lapsed = (


    spark.sql(f"""
              
            with latest_season as (

                select 
                    coalesce(max(season), 19001901) as season
                from nhl_data_staged.games.schedules 
            ) 
            , 
            end_dates as (

                select 
                    coalesce(max(playoff_end_date), '1900-01-01')::date as playoff_end_date          
                from nhl_data_staged.games.schedules a  
                inner join latest_season b 
                    on a.season = b.season

            
            )

            select 
                playoff_end_date, 
                (date_diff(from_utc_timestamp(current_timestamp(), 'America/Chicago')::date, playoff_end_date) >= 30)::boolean as time_lapsed_ind 
            from end_dates 

        """)
    .first()
)

dbutils.jobs.taskValues.set(

    key = "season_ended_time_lapsed", 
    value = season_ended_time_lapsed["time_lapsed_ind"]
)

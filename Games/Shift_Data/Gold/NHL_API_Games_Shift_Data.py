import sys
username = spark.sql("select current_user()").first()[0]
sys.path.append(f"/Workspace/Users/{username}/NHL_Pipeline")

from pyspark.sql import SparkSession
from pipeline_funcs.user_utc_region import region_return 

user_region = region_return()
spark.sql(f"""
        
    with src as (

        select 
            season,
            game_id,
            game_date,
            team_id,
            team_abbrev,
            player_id,
            player_name,
            shift_id,
            period,
            start_time,
            end_time,
            coalesce(duration, '00:00') as duration,
            shift_number,
            detail_code,
            event_description,
            event_Details,
            event_number,
            type_code,
            game_in_play
        from nhl_data_staged.games.shift_data a
        where 1 = 1
            ---only grab rows that are active from staging table
            and active_row = true 
            ---below allows records that have been inserted or updated to be eligible for MERGE/INSERT/UPDATE
            and (
                a.game_date >= date_sub(from_utc_timestamp(current_timestamp(), '{user_region}')::date, 2) 
                or 
                a.insert_dte::date >= date_sub(from_utc_timestamp(current_timestamp(), '{user_region}')::date, 2)
                )
    )


    merge into nhl_data.games.shift_data t 
    using src s 
        on t.season = s.season 
        and t.game_id = s.game_id
        and t.game_date = s.game_date 
        and t.team_id = s.team_id 
        and t.player_id = s.player_id 
        and t.shift_id = s.shift_id 
        and t.game_date between 
            date_sub(from_utc_timestamp(current_timestamp(), '{user_region}')::date, 2)
            and 
            from_utc_timestamp(current_timestamp(), '{user_region}')::date 

    when matched and (

        t.period <> s.period 
        or t.player_name <> s.player_name 
        or t.start_time <> s.start_time 
        or t.end_time <> s.end_time 
        or t.duration <> s.duration 
        or t.shift_number <> s.shift_number 
        or t.game_in_play <> s.game_in_play 
        
        )

    then update set 

        period = s.period,
        start_time = s.start_time, 
        end_time = s.end_time, 
        duration = s.duration, 
        shift_number = s.shift_number, 
        player_name = s.player_name, 
        detail_code = s.detail_code,
        event_description = s.event_description, 
        event_details = s.event_details,
        event_number = s.event_number, 
        type_code = s.type_code,
        game_in_play = s.game_in_play,
        update_dte = current_timestamp()


    when not matched then insert (

        season,
        game_id,
        game_date,
        team_id,
        team_abbrev,
        player_id,
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
        type_code,
        game_in_play,
        insert_dte,
        update_dte

    )
    values (

        s.season,
        s.game_id,
        s.game_date,
        s.team_id,
        s.team_abbrev,
        s.player_id,
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
        s.type_code,
        s.game_in_play,
        current_timestamp(),
        null 

    )
    ;
""")

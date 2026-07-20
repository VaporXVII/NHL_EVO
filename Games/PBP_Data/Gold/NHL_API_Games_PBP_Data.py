import sys
username = spark.sql("select current_user()").first()[0]
sys.path.append(f"/Workspace/Users/{username}/NHL_Pipeline")

from pyspark.sql import SparkSession
from pipeline_funcs.user_utc_region import region_return

user_region = region_return()
spark.sql(f"""
          
    with players as (

    select 
        player_id,
        player_name
    from nhl_data_staged.players.master_ids 

    )
    ,
    game_data as (

    select 
        coalesce(player_id, scoring_player_id, shooting_player_id, blocking_player_id, faceoff_winning_player_id, penalty_committed_by_player_id, hit_given_by_player_id) as first_player_id,
        coalesce(assist1_player_id, faceoff_losing_player_id, penalty_drawn_by_player_id, hit_taken_by_player_id, goalie_in_net_id) as second_player_id,
        coalesce(assist2_player_id) as third_player_id,
        a.season,
        a.game_id, 
        a.game_date,
        a.period, 
        a.time_in_period,
        a.time_remaining,
        a.game_seconds,
        a.situation_code,
        a.zone_code,
        a.event_type,
        a.event_idx,
        a.event_id,
        a.event_team_id, 
        a.event_team_abbrev, 
        a.x_coord, 
        a.y_coord,
        a.shot_type,
        a.missed_shot_desc, 
        a.penalty_desc, 
        a.penalty_type_desc, 
        a.penalty_duration,
        a.play_stopped_reason,
        a.intermission_active, 
        a.game_in_play
    from nhl_data_staged.games.pbp_data a  
    where 1 = 1
        ---only grab rows that are active from staging table
        and a.active_row = true 
        ---below allows records that have been inserted or updated to be eligible for MERGE/INSERT/UPDATE
        and (
        a.game_date >= date_sub(from_utc_timestamp(current_timestamp(), '{user_region}')::date, 2) 
        or 
        a.insert_dte::date >= date_sub(from_utc_timestamp(current_timestamp(), '{user_region}')::date, 2)
        )


    )
    , 
    src as (

    select 
        a.season, 
        a.game_id, 
        a.game_date,
        a.period,
        a.time_in_period,
        a.time_remaining,
        a.game_seconds,
        a.situation_code,
        a.zone_code,
        a.event_type,
        a.event_idx,
        a.event_id,
        a.event_team_id, 
        a.event_team_abbrev,
        a.first_player_id as event_p1_player_id, 
        p.player_name as event_p1_player_name, 
        a.second_player_id as event_p2_player_id, 
        p2.player_name as event_p2_player_name,
        a.third_player_id as event_p3_player_id, 
        p3.player_name as event_p3_player_name,
        a.x_coord,
        a.y_coord,
        a.shot_type,
        a.missed_shot_desc,
        a.penalty_desc,
        a.penalty_type_desc,
        a.penalty_duration,
        a.play_stopped_reason,
        a.intermission_active,
        a.game_in_play
    from game_data a  
    left join players p 
        on a.first_player_id = p.player_id
    left join players p2 
        on a.second_player_id = p2.player_id
    left join players p3 
        on a.third_player_id = p3.player_id
    where 1 = 1

    )

    merge into nhl_data.games.pbp_data t 
    using src s
    on t.season = s.season
    and t.game_id = s.game_id
    and t.game_date = s.game_date
    and t.event_idx = s.event_idx 
    and t.game_date between 
        date_sub(from_utc_timestamp(current_timestamp(), '{user_region}')::date, 2) 
        and 
        from_utc_timestamp(current_timestamp(), '{user_region}')::date

    when matched and (
    
        not (t.period <=> s.period)
        or not (t.game_seconds <=> s.game_seconds)
        or not (t.event_type <=> s.event_type) 
        or not (t.event_id <=> s.event_id)
        or not (t.event_team_id <=> s.event_team_id)
        or not (t.situation_code <=> s.situation_code)
        or not (t.event_p1_player_id <=> s.event_p1_player_id)
        or not (t.event_p2_player_id <=> s.event_p2_player_id)
        or not (t.event_p3_player_id <=> s.event_p3_player_id)
        or not (t.shot_type <=> s.shot_type) 
        or not (t.penalty_desc <=> s.penalty_desc)
        
    )

    then update set 

    game_date = s.game_date,
    event_type = s.event_type,
    period = s.period,
    game_seconds = s.game_seconds,
    situation_code = s.situation_code,
    zone_code = s.zone_code,
    time_in_period = s.time_in_period,
    time_remaining = s.time_remaining,
    event_idx = s.event_idx,
    event_team_id = s.event_team_id,
    event_team_abbrev = s.event_team_abbrev,
    event_p1_player_id = s.event_p1_player_id,
    event_p1_player_name = s.event_p1_player_name,
    event_p2_player_id = s.event_p2_player_id,
    event_p2_player_name = s.event_p2_player_name,
    event_p3_player_id = s.event_p3_player_id,
    event_p3_player_name = s.event_p3_player_name,
    x_coord = s.x_coord,
    y_coord = s.y_coord,
    shot_type = s.shot_type,
    missed_shot_desc = s.missed_shot_desc,
    penalty_desc = s.penalty_desc,
    penalty_type_desc = s.penalty_type_desc,
    penalty_duration = s.penalty_duration,
    play_stopped_reason = s.play_stopped_reason,
    intermission_active = s.intermission_active,
    game_in_play = s.game_in_play,
    update_dte = current_timestamp()


    when matched and 
            t.game_date between date_sub(from_utc_timestamp(current_timestamp(), '{user_region}')::date, 2) and from_utc_timestamp(current_timestamp(), '{user_region}')::date
            and not (t.game_in_play <=> s.game_in_play)
    then update set 
        game_in_play = s.game_in_play,
        update_dte = current_timestamp()

    
    when not matched then insert (

    season,
    game_id,
    game_date,
    period,
    time_in_period,
    time_remaining,
    game_seconds,
    situation_code,
    zone_code,
    event_type,
    event_idx,
    event_id,
    event_team_id,
    event_team_abbrev,
    event_p1_player_id,
    event_p1_player_name,
    event_p2_player_id,
    event_p2_player_name,
    event_p3_player_id,
    event_p3_player_name,
    x_coord,
    y_coord,
    shot_type,
    missed_shot_desc,
    penalty_desc,
    penalty_type_desc,
    penalty_duration,
    play_stopped_reason,
    intermission_active,
    game_in_play,
    insert_dte,
    update_dte

    )
    values (

    s.season,
    s.game_id,
    s.game_date,
    s.period,
    s.time_in_period,
    s.time_remaining,
    s.game_seconds,
    s.situation_code,
    s.zone_code,
    s.event_type,
    s.event_idx,
    s.event_id,
    s.event_team_id,
    s.event_team_abbrev,
    s.event_p1_player_id,
    s.event_p1_player_name,
    s.event_p2_player_id,
    s.event_p2_player_name,
    s.event_p3_player_id,
    s.event_p3_player_name,
    s.x_coord,
    s.y_coord,
    s.shot_type,
    s.missed_shot_desc,
    s.penalty_desc,
    s.penalty_type_desc,
    s.penalty_duration,
    s.play_stopped_reason,
    s.intermission_active,
    s.game_in_play,
    current_timestamp(),
    null

    )
    ;

""")

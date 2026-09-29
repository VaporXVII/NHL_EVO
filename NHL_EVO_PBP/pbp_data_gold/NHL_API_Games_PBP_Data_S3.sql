create or replace view nhl_evo.games.pbp_data 
comment "NHL EVO gold layer for play by play data" as (


    with players as (

        select 
            player_id,
            player_name
        from nhl_data_staged.players.master_ids 


    )
    , 
    game_data as (



        select 
            coalesce(a.player_id, a.scoring_player_id, a.shooting_player_id, a.blocking_player_id, a.faceoff_winning_player_id, a.penalty_committed_by_player_id, a.hit_given_by_player_id) as first_player_id,
            coalesce(a.assist1_player_id, a.faceoff_losing_player_id, a.penalty_drawn_by_player_id, a.hit_taken_by_player_id, a.goalie_in_net_id) as second_player_id,
            coalesce(a.assist2_player_id) as third_player_id,
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
            a.game_in_play,
            a.scrape_ts_utc,
            a.insert_dte
        from nhl_evo_staged.games.pbp_data a
  
    )

        select /*+ broadcast (p), broadcast (p2), broadcast (p3) */
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
            a.game_in_play,
            a.scrape_ts_utc,
            a.insert_dte 
        from game_data a  
        left join players p 
            on a.first_player_id = p.player_id
        left join players p2 
            on a.second_player_id = p2.player_id
        left join players p3 
            on a.third_player_id = p3.player_id


)
;
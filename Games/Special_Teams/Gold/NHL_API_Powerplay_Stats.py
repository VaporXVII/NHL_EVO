import sys 
username = spark.sql("select current_user()").first()[0]
sys.path.append(f"/Workspace/Users/{username}/NHL_Pipeline")

from pipeline_funcs.user_utc_region import region_return 
user_region = region_return()

spark.sql(f"""
          
    with date_param as (

    select from_utc_timestamp(current_timestamp(), '{user_region}')::date as current_run_date

    ) 
    ,
    last_loaded as (

    select 
        coalesce(max(game_date), '1900-01-01') as last_game_date
    from nhl_data.games.pp_stats 

    )
    , 
    src as (

    select /*+ broadcast (b), broadcast (d), broadcast (p) */
        b.team_abbrev,
        a.*
    from nhl_data_staged.games.pp_stats a
    inner join nhl_data_staged.teams.master_ids b 
        on a.team_id = b.team_id
    cross join last_loaded d
    cross join date_param p 
    where 1 = 1
        and (
        game_date between least(d.last_game_date, date_sub(p.current_run_date, 2)) and p.current_run_date
        or 
        from_utc_timestamp(a.insert_dte, '{user_region}')::date between date_sub(p.current_run_date, 2) and p.current_run_date
        or 
        from_utc_timestamp(a.update_dte, '{user_region}')::date between date_sub(p.current_run_date, 2) and p.current_run_date
        )
    )


    merge into nhl_data.games.pp_stats t 
    using src as s
        on t.season = s.season 
        and t.game_id = s.game_id 
        and t.game_date = s.game_date 
        and t.team_id = s.team_id


    when matched and (

        t.home_road <> s.home_road 
        or t.opp_team_abbrev <> s.opp_team_abbrev
        or coalesce(t.pp_opportunities, 0) <> coalesce(s.pp_opportunities, 0)
        or coalesce(t.power_play_goals_for, 0) <> coalesce(s.power_play_goals_for, 0) 
        or coalesce(t.pp_time_on_ice_per_game, 0) <> coalesce(s.pp_time_on_ice_per_game, 0) 

    )
    then update set 

        team_abbrev = s.team_abbrev, 
        game_date = s.game_date, 
        home_road = s.home_road, 
        opp_team_abbrev = s.opp_team_abbrev, 
        power_play_goals_for = s.power_play_goals_for, 
        power_play_pct = s.power_play_pct, 
        power_play_net_pct = s.power_play_net_pct, 
        pp_net_goals = s.pp_net_goals, 
        pp_opportunities = s.pp_opportunities, 
        pp_time_on_ice_per_game = s.pp_time_on_ice_per_game,
        sh_goals_against = s.sh_goals_against, 
        update_dte = current_timestamp()


    when not matched then insert (

            season, 
            team_id, 
            team_abbrev, 
            game_id, 
            game_date,
            home_road, 
            opp_team_abbrev, 
            power_play_goals_for, 
            power_play_pct, 
            power_play_net_pct, 
            pp_net_goals, 
            pp_opportunities, 
            pp_time_on_ice_per_game,
            sh_goals_against, 
            insert_dte, 
            update_dte
            
    )

    values ( 

        s.season, 
        s.team_id, 
        s.team_abbrev, 
        s.game_id, 
        s.game_date,
        s.home_road, 
        s.opp_team_abbrev, 
        s.power_play_goals_for, 
        s.power_play_pct, 
        s.power_play_net_pct,
        s.pp_net_goals,
        s.pp_opportunities,
        s.pp_time_on_ice_per_game,
        s.sh_goals_against,
        current_timestamp(),
        null
    )
    ;

""")

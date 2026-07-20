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
        from nhl_data.games.pk_toi 
    ) 
    , 
    src as (

    select /*+ broadcast (b), broadcast (d), broadcast (p) */
        b.team_abbrev,
        a.*
    from nhl_data_staged.games.pk_toi a
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


    merge into nhl_data.games.pk_toi t 
    using src as s
        on t.season = s.season 
        and t.game_id = s.game_id 
        and t.game_date = s.game_date
        and t.team_id = s.team_id

    when matched and (

        t.home_road <> s.home_road
        or t.opp_team_abbrev <> s.opp_team_abbrev
        or coalesce(t.time_on_ice_shorthanded, 0) <> coalesce(s.time_on_ice_shorthanded, 0)
        or coalesce(t.times_shorthanded, 0) <> coalesce(s.times_shorthanded, 0)
        or coalesce(t.shorthanded_goals_against, 0) <> coalesce(s.shorthanded_goals_against, 0)



    )
    then update set 

        team_abbrev = s.team_abbrev, 
        game_date = s.game_date, 
        home_road = s.home_road, 
        opp_team_abbrev = s.opp_team_abbrev, 
        goals_against3v4 = s.goals_against3v4,
        goals_against3v5 = s.goals_against3v5,
        goals_against4v5 = s.goals_against4v5,
        overall_penalty_kill_pct = s.overall_penalty_kill_pct,
        penalty_kill_pct3v4 = s.penalty_kill_pct3v4,
        penalty_kill_pct3v5 = s.penalty_kill_pct3v5,
        penalty_kill_pct4v5 = s.penalty_kill_pct4v5,
        shorthanded_goals_against = s.shorthanded_goals_against,
        time_on_ice3v4 = s.time_on_ice3v4,
        time_on_ice3v5 = s.time_on_ice3v5,
        time_on_ice4v5 = s.time_on_ice4v5,
        time_on_ice_shorthanded = s.time_on_ice_shorthanded,
        times_shorthanded = s.times_shorthanded,
        times_shorthanded3v4 = s.times_shorthanded3v4,
        times_shorthanded3v5 = s.times_shorthanded3v5,
        times_shorthanded4v5 = s.times_shorthanded4v5,
        update_dte = current_timestamp()


    when not matched then insert (

        season, 
        team_id, 
        team_abbrev, 
        game_id, 
        game_date,
        home_road, 
        opp_team_abbrev, 
        goals_against3v4,
        goals_against3v5,
        goals_against4v5,
        overall_penalty_kill_pct,
        penalty_kill_pct3v4,
        penalty_kill_pct3v5,
        penalty_kill_pct4v5,
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
        update_dte
        
        
    )

    values ( 

        s.season ,
        s.team_id,
        s.team_abbrev,
        s.game_id,
        s.game_date,
        s.home_road,
        s.opp_team_abbrev,
        s.goals_against3v4,
        s.goals_against3v5,
        s.goals_against4v5,
        s.overall_penalty_kill_pct,
        s.penalty_kill_pct3v4,
        s.penalty_kill_pct3v5,
        s.penalty_kill_pct4v5,
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
        null

    )
    ;

""")

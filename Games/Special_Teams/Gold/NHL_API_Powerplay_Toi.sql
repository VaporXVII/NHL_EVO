with date_param as (

  select from_utc_timestamp(current_timestamp(), 'America/Chicago')::date as current_run_date

) 
,
last_loaded as (

  select 
    coalesce(max(game_date), '1900-01-01') as last_game_date
  from nhl_data.games.pp_toi 

)
,
src as (

  select /*+ broadcast (b), broadcast (d), broadcast (p) */
    b.team_abbrev,
    a.*
  from nhl_data_staged.games.pp_toi a
  inner join nhl_data_staged.teams.master_ids b 
    on a.team_id = b.team_id
  cross join last_loaded d
  cross join date_param p
  where 1 = 1
    and (
      game_date between least(d.last_game_date, date_sub(p.current_run_date, 2)) and p.current_run_date
      or 
      from_utc_timestamp(a.insert_dte, 'America/Chicago')::date between date_sub(p.current_run_date, 2) and p.current_run_date
      or 
      from_utc_timestamp(a.update_dte, 'America/Chicago')::date between date_sub(p.current_run_date, 2) and p.current_run_date
    )
)


merge into nhl_data.games.pp_toi t 
using src as s
  on t.season = s.season 
  and t.game_id = s.game_id 
  and t.game_date = s.game_date
  and t.team_id = s.team_id 
  and t.game_date between s.min_game_date and s.max_game_date


when matched and (

 t.home_road <> s.home_road 
  or t.opp_team_abbrev <> s.opp_team_abbrev
  or coalesce(t.pp_opportunities, 0) <> coalesce(s.pp_opportunities, 0)
  or coalesce(t.power_play_goals_for, 0) <> coalesce(s.power_play_goals_for, 0)
  or coalesce(t.time_on_ice_pp, 0) <> coalesce(s.time_on_ice_pp, 0)
 


)
then update set 

  team_abbrev = s.team_abbrev, 
  game_date = s.game_date, 
  home_road = s.home_road, 
  opp_team_abbrev = s.opp_team_abbrev, 
  pp_opportunities = s.pp_opportunities,
  opportunities4v3 = s.opportunities4v3,
  opportunities5v3 = s.opportunities5v3,
  opportunities5v4 = s.opportunities5v4,
  power_play_pct4v3 = s.power_play_pct4v3,
  power_play_pct5v3 = s.power_play_pct5v3,
  power_play_pct5v4 = s.power_play_pct5v4,
  power_play_goals_for = s.power_play_goals_for,
  time_on_ice4v3 = s.time_on_ice4v3,
  time_on_ice5v3 = s.time_on_ice5v3, 
  time_on_ice5v4 = s.time_on_ice5v4,
  time_on_ice_pp = s.time_on_ice_pp,
  update_dte = current_timestamp()


when not matched then insert (

    season, 
    team_id, 
    team_abbrev, 
    game_id, 
    game_date,
    home_road, 
    opp_team_abbrev, 
    pp_opportunities,
    opportunities4v3,
    opportunities5v3,
    opportunities5v4,
    power_play_pct4v3,
    power_play_pct5v3,
    power_play_pct5v4,
    power_play_goals_for,
    time_on_ice4v3,
    time_on_ice5v3,
    time_on_ice5v4,
    time_on_ice_pp,
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
    s.pp_opportunities,
    s.opportunities4v3,
    s.opportunities5v3,
    s.opportunities5v4,
    s.power_play_pct4v3,
    s.power_play_pct5v3,
    s.power_play_pct5v4,
    s.power_play_goals_for,
    s.time_on_ice4v3,
    s.time_on_ice5v3,
    s.time_on_ice5v4,
    s.time_on_ice_pp,
    current_timestamp(),
    null

)
;

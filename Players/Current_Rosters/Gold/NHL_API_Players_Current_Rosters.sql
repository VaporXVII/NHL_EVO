with date_param as (

  select from_utc_timestamp(current_timestamp(), 'America/Chicago')::date as current_run_date

) 
,
src as (

  select /*+ broadcast (p) */
    a.*
  from nhl_data_staged.players.player_game_rosters a
  cross join date_param p  
  where 1 = 1
    and (
        (a.game_date between date_sub(p.current_run_date, 1) and p.current_run_date)
        or 
        (a.insert_dte::date between date_sub(p.current_run_date, 1) and p.current_run_date)
        or 
        (a.update_dte::date between date_sub(p.current_run_date, 1) and p.current_run_date)
    )
    and a.player_id is not null


)

merge into nhl_data.players.player_game_rosters t 
using src s 
  on t.season = s.season
  and t.game_date = s.game_date 
  and t.game_id = s.game_id
  and t.player_id = s.player_id

when matched and (

    lower(trim(t.player_name)) <> lower(trim(s.player_name))
    or not (t.team_id <=> s.team_id)
    or not (t.player_pos <=> s.player_pos)
  
) 
then update set 

  player_name = s.player_name,
  team_id = s.team_id,
  player_pos = s.player_pos,
  update_dte = current_timestamp()

when not matched then insert (

  season, 
  player_id, 
  player_name, 
  game_id,
  game_date,
  team_id,
  player_pos, 
  is_active, 
  insert_dte,
  update_dte

)
values (

  s.season,
  s.player_id, 
  s.player_name, 
  s.game_id,
  s.game_date, 
  s.team_id,
  s.player_pos, 
  true, 
  current_timestamp(), 
  null

)
;

with src as (

  select *
  from nhl_data_staged.players.player_game_rosters
  where 1 = 1
    and update_dte::date = from_utc_timestamp(current_timestamp(), 'America/Chicago')::date
  )

merge into nhl_data.players.master_ids t
using src s 
  on t.player_id = s.player_id
when not matched then insert (

  player_id,
  player_name,
  player_pos, 
  shoots_catches,
  insert_dte,
  update_dte
  
)
values (

  s.player_id,
  s.player_name,
  s.player_pos,
  s.shoots_catches,
  current_timestamp(),
  null
)
;

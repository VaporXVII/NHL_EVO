with season_info as (

  ---checking to see if the scrape is done during the current season or if a new one has rolled over 
  select 
    max(season) as latest_season 
  from nhl_data_staged.games.schedules 


)
,
src as (

---joining the latest season from above with the source table to be able to tell the merge to execute only if they don't match (i.e. a new season has rolled over)
  select /*+ broadcast (b) */
    a.*,
    b.latest_season
  from nhl_data_staged.teams.details a 
  cross join season_info b 

)

merge into nhl_data.teams.details t 
using src s 
  on t.team_id = s.team_id
when matched and (

  t.team_abbrev <> s.team_abbrev
  or t.team_name <> s.team_name
  or t.is_active <> s.is_active
  or coalesce(t.active_from, '1900-01-01'::date) <> coalesce(s.active_from, t.active_from, '1900-01-01'::date)
  or coalesce(t.active_to, '1900-01-01'::date) <> coalesce(s.active_to, t.active_to, '1900-01-01'::date)
  

) 
then update set 

  team_abbrev = s.team_abbrev, 
  team_name = s.team_name, 
  is_active = s.is_active, 
  active_from = s.active_from, 
  active_to = s.active_to, 
  update_dte = current_timestamp()

  when not matched then insert (

    team_id, 
    team_abbrev, 
    team_name, 
    is_active, 
    active_from, 
    active_to, 
    insert_dte, 
    update_dte

  )

  values (

    s.team_id, 
    s.team_abbrev, 
    s.team_name, 
    s.is_active, 
    s.active_from, 
    s.active_to, 
    current_timestamp(),
    null
  
  )
  ;

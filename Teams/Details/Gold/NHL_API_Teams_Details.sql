with season_info as (

  ---checking to see if the scrape is done during the current season or if a new one has rolled over 
  select 
    max(season) as latest_season 
  from nhl_data_staged.games.schedules 

)
,
source_data as (

---joining the latest season from above with the source table to be able to tell the merge to execute only if they don't match (i.e. a new season has rolled over)
  select /*+ broadcast (b) */
    a.*,
    b.latest_season
  from nhl_data_staged.teams.details a 
  cross join season_info b 

)

merge into nhl_data.teams.details t 
using source_data s 
  on t.team_id = s.team_id
when matched and 

  ---if the current season from the team.details staging table doesn't match the latest season from the schedules table then do the update because a new season has rolled over 
  ---otherwise insert the data (assumes that user has not yet run the pipeline at all)
  s.current_season <> s.latest_season
  and (

  t.team_abbrev <> s.team_abbrev
  or t.team_name <> s.team_name
  or t.is_active <> s.is_active
  or t.active_from <> s.active_from 
  or not (t.active_to <=> s.active_to)
  

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

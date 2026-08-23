---merge statement below looks to see if the team information from the most 
---recent season matches what's already in the table then updates accordingly
with season_info as (

  ---checking to see if the scrape is done during the current season or if a new one has rolled over 
  ---not using coalesce since schedules tables are the first ones populated in the NHL_EVO, therefore season should never be null
  select 
    max(season) as latest_season 
  from nhl_data_staged.games.schedules 
  

)
,
source_data as (

  select /*+ broadcast (b) */
    a.*,
    b.latest_season
  from nhl_data_staged.teams.master_ids a 
  cross join season_info b 
  where 1 = 1
    ---NHL using team_id 70 as a placeholder for any new incoming teams, with a team_abbrev of 'TBD'. It's unknown if this team_id changes over time, so relying on
    ---the team_abbrev of 'TBD' seems to be safest. Removing this team id from the finalized table
    and a.team_abbrev <> 'TBD'

)

merge into nhl_data.teams.master_ids t 
using source_data as s
  on t.team_id = s.team_id
when matched and (

  s.season <> s.latest_season

  and (

  t.team_abbrev <> s.team_abbrev 
  or t.team_name <> s.team_name 

)

)
then update set

  team_abbrev = s.team_abbrev, 
  team_name = s.team_name, 
  update_dte = current_timestamp()

when not matched then insert (


  team_id, 
  team_abbrev, 
  team_name, 
  insert_dte,
  update_dte 

)

values ( 

  s.team_id, 
  s.team_abbrev, 
  s.team_name, 
  current_timestamp(), 
  null
  
)
;

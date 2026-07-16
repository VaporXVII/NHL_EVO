create catalog if not exists nhl_data_raw;
create catalog if not exists nhl_data_staged;
create catalog if not exists nhl_data;

--Schedules

create schema if not exists nhl_data_raw.games;
create schema if not exists nhl_data_staged.games;
create schema if not exists nhl_data.games;

create table if not exists nhl_data_raw.games.schedules (

  endpoint string not null,
  http_status integer not null,
  request_key string not null,
  api_url string not null,
  payload string,
  ingest_ts_utc timestamp not null, 
  scrape_plan string not null

)
using delta
cluster by (request_key, ingest_ts_utc)
tblproperties (delta.autoOptimize.optimizeWrite = true, delta.enableChangeDataFeed = true);

create table if not exists nhl_data_staged.games.schedules (


  season integer not null,
  pre_season_start_date date, 
  regular_season_start_date date, 
  regular_season_end_date date, 
  playoff_end_date date,
  game_id bigint not null, 
  game_date date not null, 
  game_type integer not null, 
  start_time_utc string, 
  eastern_utc_offset string, 
  venue_utc_offset string, 
  venue_timezone string, 
  team_abbrev string, 
  team_id integer, 
  team_name string, 
  home_road string not null, 
  team_city string,
  neutral_site boolean, 
  insert_dte timestamp not null, 
  update_dte timestamp, 
  py_source string not null, 
  unused_structs string, 
  num_unused_structs integer


)
using delta
cluster by (season, game_date, team_id)
tblproperties (delta.autoOptimize.optimizeWrite = true, delta.enableChangeDataFeed = true);

create table if not exists nhl_data.games.schedules (

    season integer not null, 
    game_id bigint not null, 
    game_date date not null, 
    game_type integer not null, 
    start_time string, 
    team_abbrev string not null, 
    team_id integer not null, 
    home_road string not null, 
    insert_dte timestamp not null, 
    update_dte timestamp

)
using delta 
cluster by (season, game_date, team_id)
tblproperties (delta.autoOptimize.optimizeWrite = true, delta.enableChangeDataFeed = true);

--Special Teams Staged

create table if not exists nhl_data_raw.games.special_teams (

  endpoint string not null,
  http_status integer not null,
  request_key string not null,
  api_url string not null,
  payload string,
  ingest_ts_utc timestamp not null

)
using delta 
cluster by (endpoint, ingest_ts_utc)
tblproperties (delta.autoOptimize.optimizeWrite = true, delta.enableChangeDataFeed = true);


create table if not exists nhl_data_staged.games.pp_stats (

  season integer not null, 
  game_id bigint not null, 
  game_date date not null, 
  team_id integer not null, 
  team_name string, 
  home_road string not null, 
  opp_team_abbrev string not null, 
  games_played integer, 
  wins integer, 
  losses integer, 
  ot_losses integer,
  points integer, 
  point_pct decimal(4,2),
  power_play_goals_for integer, 
  power_play_pct decimal(6,5),
  power_play_net_pct decimal(6,5),
  pp_goals_per_game integer, 
  pp_net_goals integer, 
  pp_net_goals_per_game integer,
  pp_opportunities integer, 
  pp_opportunities_per_game integer, 
  pp_time_on_ice_per_game integer, 
  sh_goals_against integer, 
  sh_goals_against_per_game integer,
  insert_dte timestamp not null, 
  update_dte timestamp,
  py_source string not null 
  

)
using delta 
cluster by (season, game_date, team_id)
tblproperties (delta.autoOptimize.optimizeWrite = true, delta.enableChangeDataFeed = true);

create table if not exists nhl_data_staged.games.pk_stats (

season integer not null, 
game_id bigint not null, 
game_date date not null, 
team_id integer not null, 
team_name string not null, 
home_road string, 
opp_team_abbrev string, 
games_played integer, 
wins integer, 
losses integer, 
ot_losses integer,
points integer, 
points_pct decimal(4,2),
penalty_kill_net_pct decimal(6,5),
penalty_kill_pct decimal(6,5),
pk_net_goals integer,
pk_net_goals_per_game integer, 
pk_time_on_ice_per_game integer, 
pp_goals_against integer,
pp_goals_against_per_game integer,
sh_goals_for integer,
sh_goals_for_per_game integer,
times_shorthanded integer,
times_shorthanded_per_game integer,
insert_dte timestamp not null,
update_dte timestamp,
py_source string not null 

)
using delta 
cluster by (season, game_date, team_id)
tblproperties (delta.autoOptimize.optimizeWrite = true, delta.enableChangeDataFeed = true);

create table if not exists nhl_data_staged.games.pk_toi ( 


season integer not null,
game_id bigint not null,
game_date date not null, 
team_id integer not null, 
team_name string not null,
home_road string, 
opp_team_abbrev string, 
goals_against3v4 integer, 
goals_against3v5 integer, 
goals_against4v5 integer, 
overall_penalty_kill_pct decimal(6,5),
penalty_kill_pct3v4 decimal(6,5), 
penalty_kill_pct3v5 decimal(6,5),
penalty_kill_pct4v5 decimal(6,5),
point_pct decimal(4,2),
shorthanded_goals_against integer, 
time_on_ice3v4 integer, 
time_on_ice3v5 integer, 
time_on_ice4v5 integer, 
time_on_ice_shorthanded integer, 
times_shorthanded integer, 
times_shorthanded3v4 integer, 
times_shorthanded3v5 integer, 
times_shorthanded4v5 integer, 
insert_dte timestamp not null,
update_dte timestamp,
py_source string not null


)
using delta 
cluster by (season, game_date, team_id)
tblproperties (delta.autoOptimize.optimizeWrite = true, delta.enableChangeDataFeed = true);

create table if not exists nhl_data_staged.games.pp_toi (


season integer not null, 
game_id bigint not null, 
game_date date not null, 
team_id integer not null, 
team_name string not null, 
home_road string, 
opp_team_abbrev string, 
games_played integer, 
goals4v3 integer,
goals5v3 integer,
goals5v4 integer, 
opportunities4v3 integer, 
opportunities5v3 integer, 
opportunities5v4 integer,
overall_power_play_pct decimal(6,5),
point_pct decimal(4,2),
power_play_goals_for integer,
power_play_pct4v3 decimal(6,5),
power_play_pct5v3 decimal(6,5),
power_play_pct5v4 decimal(6,5),
pp_opportunities integer,
time_on_ice4v3 integer,
time_on_ice5v3 integer,
time_on_ice5v4 integer,
time_on_ice_pp integer, 
insert_dte timestamp not null,
update_dte timestamp,
py_source string not null


)
using delta 
cluster by (season, game_date, team_id)
tblproperties (delta.autoOptimize.optimizeWrite = true, delta.enableChangeDataFeed = true);

--Special Teams

create table if not exists nhl_data.games.pp_stats (

    season integer not null,
    team_id integer not null,
    team_abbrev string, 
    game_id bigint not null, 
    game_date date,
    home_road string, 
    opp_team_abbrev string, 
    power_play_goals_for integer, 
    power_play_pct decimal(6,5), 
    power_play_net_pct decimal(6,5), 
    pp_net_goals integer, 
    pp_opportunities integer, 
    pp_time_on_ice_per_game integer,
    sh_goals_against integer, 
    insert_dte timestamp not null, 
    update_dte timestamp

)
using delta
cluster by (season, game_date, team_id)
tblproperties (delta.autoOptimize.optimizeWrite = true, delta.enableChangeDataFeed = true);

create table if not exists nhl_data.games.pk_stats (


    season integer not null,
    team_id integer not null,
    team_abbrev string, 
    game_id bigint not null, 
    game_date date,
    home_road string, 
    opp_team_abbrev string,  
    penalty_kill_net_pct decimal(6,5),
    penalty_kill_pct decimal(6,5),
    pk_net_goals integer,
    pk_time_on_ice_per_game integer,
    pp_goals_against integer,
    sh_goals_for integer,
    times_shorthanded integer,
    insert_dte timestamp not null,
    update_dte timestamp 

)
using delta 
cluster by (season, game_date, team_id)
tblproperties (delta.autoOptimize.optimizeWrite = true, delta.enableChangeDataFeed = true);


create table if not exists nhl_data.games.pp_toi (

    season integer not null,
    team_id integer not null,
    team_abbrev string,
    game_id bigint not null,
    game_date date,
    home_road string,
    opp_team_abbrev string,
    pp_opportunities integer,
    opportunities4v3 integer,
    opportunities5v3 integer,
    opportunities5v4 integer,
    power_play_pct4v3 decimal(6,5),
    power_play_pct5v3 decimal(6,5),
    power_play_pct5v4 decimal(6,5),
    power_play_goals_for integer, 
    time_on_ice4v3 integer,
    time_on_ice5v3 integer,
    time_on_ice5v4 integer,
    time_on_ice_pp integer,
    insert_dte timestamp not null,
    update_dte timestamp

)
using delta 
cluster by (season, game_date, team_id)
tblproperties (delta.autoOptimize.optimizeWrite = true, delta.enableChangeDataFeed = true);


create table if not exists nhl_data.games.pk_toi (


    season integer not null,
    team_id integer not null,
    team_abbrev string,
    game_id bigint not null,
    game_date date,
    home_road string,
    opp_team_abbrev string,
    goals_against3v4 integer,
    goals_against3v5 integer,
    goals_against4v5 integer,
    overall_penalty_kill_pct decimal(6,5),
    penalty_kill_pct3v4 decimal(6,5),
    penalty_kill_pct3v5 decimal(6,5),
    penalty_kill_pct4v5 decimal(6,5),
    shorthanded_goals_against integer,
    time_on_ice3v4 integer,
    time_on_ice3v5 integer,
    time_on_ice4v5 integer,
    time_on_ice_shorthanded integer,
    times_shorthanded integer,
    times_shorthanded3v4 integer,
    times_shorthanded3v5 integer,
    times_shorthanded4v5 integer,
    insert_dte timestamp not null, 
    update_dte timestamp


)
using delta 
cluster by (season, game_date, team_id)
tblproperties (delta.autoOptimize.optimizeWrite = true, delta.enableChangeDataFeed = true);

--Vegas Totals

create table if not exists nhl_data_raw.games.vegas_totals ( 


  endpoint string not null,
  http_status integer not null,
  request_key string not null,
  api_url string not null,
  payload string,
  ingest_ts_utc timestamp not null,
  update_ts_utc timestamp


)
using delta
cluster by (ingest_ts_utc, request_key)
tblproperties (delta.autoOptimize.optimizeWrite = true, delta.enableChangeDataFeed = true);


create table if not exists nhl_data_staged.games.vegas_totals (

    season_id bigint not null,
    season_type string not null,
    game_date date not null,
    game_date_time timestamp not null,
    event_id bigint not null,
    game_type integer not null,
    conference_game boolean,
    division_game boolean,
    team_name string not null,
    team_abbrev string not null,
    team_id integer not null,
    home_road string not null,
    fav_dog string,
    moneyline_open integer,
    moneyline_current integer,
    moneyline_pct integer,
    moneyline_change integer,
    over_under_open decimal(3,1),
    over_under_current decimal(3,1),
    over_under_pct integer,
    over_under_change decimal(3,1),
    implied_total_open decimal(3,1),
    implied_total_current decimal(3,1),
    neutral_site boolean,
    stadium_name string,
    playoff_ind boolean,
    number_of_bets double,
    number_of_bets_z_score double,
    insert_dte timestamp not null, 
    update_dte timestamp,
    py_source string not null

)
using delta 
cluster by (season_id, game_date, team_id)
tblproperties (delta.autoOptimize.optimizeWrite = true, delta.enableChangeDataFeed = true);


create table if not exists nhl_data.games.vegas_totals (


  game_date date not null, 
  team_id integer not null, 
  team_name string not null,
  team_abbrev string not null, 
  home_road string not null,
  moneyline integer, 
  implied_total decimal(3,1),
  fav_dog string,
  insert_dte timestamp not null,
  update_dte timestamp


)
using delta 
cluster by (game_date, team_id)
tblproperties (delta.autoOptimize.optimizeWrite = true, delta.enableChangeDataFeed = true);

--Play by Play Data

create table if not exists nhl_data_raw.games.pbp_data (

    endpoint string not null, 
    request_key bigint not null, 
    http_status int not null,
    payload string not null, 
    api_url string not null, 
    ingest_ts_utc timestamp not null,
    update_ts_utc timestamp

)
using delta 
cluster by (ingest_ts_utc)
tblproperties (delta.autoOptimize.optimizeWrite = true, delta.enableChangeDataFeed = true);


create table if not exists nhl_data_staged.games.pbp_data (

    season integer not null,
    game_id bigint not null,
    game_date date not null,
    period tinyint not null,
    period_type string,
    home_team_defending_side string,
    time_in_period varchar(5),
    time_remaining varchar(5),
    game_seconds integer,
    situation_code varchar(4),
    zone_code string,
    event_id integer,
    event_idx integer,
    event_type string,
    event_type_code integer,
    event_team_id integer,
    event_team_abbrev string,
    player_id bigint,
    x_coord integer,
    y_coord integer,
    goalie_in_net_id bigint,
    shooting_player_id bigint,
    shot_type string,
    missed_shot_desc string,
    away_sog integer,
    home_sog integer,
    away_score tinyint,
    home_score tinyint,
    scoring_player_id bigint,
    scoring_player_total tinyint,
    assist1_player_id bigint,
    assist1_player_total tinyint,
    assist2_player_id bigint,
    assist2_player_total tinyint,
    blocking_player_id bigint,
    penalty_desc string,
    penalty_type_desc string,
    penalty_duration integer,
    penalty_committed_by_player_id bigint,
    penalty_drawn_by_player_id bigint,
    hit_given_by_player_id bigint,
    hit_taken_by_player_id bigint,
    faceoff_winning_player_id bigint,
    faceoff_losing_player_id bigint,
    max_regulation_periods tinyint,
    play_stopped_reason string,
    intermission_active boolean not null, 
    game_in_play boolean not null,
    insert_dte timestamp not null,
    update_dte timestamp,
    py_source string not null, 
    active_row boolean not null,
    logic_block string not null,
    failed_condition string


)
using delta 
cluster by (season, game_date, game_id)
tblproperties (delta.autoOptimize.optimizeWrite = true, delta.enableChangeDataFeed = true, 'delta.enableDeletionVectors' = 'true');


create table if not exists nhl_data.games.pbp_data (

  season integer not null,
  game_id bigint not null,
  game_date date not null,
  period tinyint,
  time_in_period varchar(5),
  time_remaining varchar(5),
  game_seconds integer,
  situation_code varchar(4),
  zone_code string,
  event_type string,
  event_idx integer not null,
  event_id integer,
  event_team_id integer,
  event_team_abbrev string,
  event_p1_player_id bigint,
  event_p1_player_name string,
  event_p2_player_id bigint,
  event_p2_player_name string,
  event_p3_player_id bigint,
  event_p3_player_name string,
  x_coord int,
  y_coord int,
  shot_type string,
  missed_shot_desc string,
  penalty_desc string,
  penalty_type_desc string,
  penalty_duration int,
  play_stopped_reason string,
  intermission_active boolean,
  game_in_play boolean not null,
  insert_dte timestamp not null,
  update_dte timestamp 

)
using delta 
cluster by (season, game_date, game_id)
tblproperties (delta.autoOptimize.optimizeWrite = true, delta.enableChangeDataFeed = true, 'delta.enableDeletionVectors' = 'true');

--Shift Data

create table if not exists nhl_data_raw.games.shift_data (

  endpoint string not null,
  request_key bigint not null,
  http_status int not null,
  payload string, 
  api_url string not null,
  ingest_ts_utc timestamp not null,
  update_ts_utc timestamp


)
using delta 
cluster by (ingest_ts_utc)
tblproperties (delta.autoOptimize.optimizeWrite = true, delta.enableChangeDataFeed = true);

create table if not exists nhl_data_staged.games.shift_data (

    season integer not null,
    game_id bigint not null,
    game_date date not null,
    team_id integer not null,
    team_abbrev string not null,
    team_name string not null,
    player_id bigint not null,
    first_name string,
    last_name string,
    player_name string not null,
    shift_id bigint not null,
    period tinyint,
    start_time varchar(5),
    end_time varchar(5),
    duration varchar(5),
    shift_number integer,
    detail_code integer,
    event_description string,
    event_details string,
    event_number integer,
    hex_value string,
    type_code integer,
    game_in_play boolean not null,
    insert_dte timestamp not null,
    update_dte timestamp,
    py_source string not null,
    active_row boolean not null,
    logic_block string 

)
using delta
cluster by (season, game_date, game_id)
tblproperties (delta.autoOptimize.optimizeWrite = true, delta.enableChangeDataFeed = true, 'delta.enableDeletionVectors' = 'true');

create table if not exists nhl_data.games.shift_data (

    season integer not null,
    game_id bigint not null,
    game_date date not null,
    team_id integer not null,
    team_abbrev string not null,
    player_id bigint not null,
    player_name string,
    shift_id bigint not null,
    period tinyint not null,
    start_time varchar(5) not null,
    end_time varchar(5) not null,
    duration varchar(5) not null,
    shift_number integer not null,
    detail_code integer, 
    event_description string, 
    event_details string, 
    event_number integer, 
    type_code integer, 
    game_in_play boolean not null,
    insert_dte timestamp not null, 
    update_dte timestamp

)
using delta 
cluster by (season, game_date, game_id) 
tblproperties (delta.autoOptimize.optimizeWrite = true, delta.enableChangeDataFeed = true, 'delta.enableDeletionVectors' = 'true');

--Teams

create schema if not exists nhl_data_raw.teams;
create schema if not exists nhl_data_staged.teams;
create schema if not exists nhl_data.teams;

create table if not exists nhl_data_raw.teams.master_ids (

  season integer not null,
  endpoint string not null, 
  http_status integer not null, 
  request_key string, 
  api_url string not null, 
  payload string, 
  ingest_ts_utc timestamp not null

)
using delta
tblproperties (delta.enableChangeDataFeed = true);


create table if not exists nhl_data_staged.teams.master_ids (

  season integer not null, 
  franchise_id integer, 
  team_name string not null, 
  team_id integer not null, 
  league_id integer, 
  team_abbrev string not null, 
  insert_dte timestamp not null, 
  update_dte timestamp,
  py_source string not null

)
using delta 
cluster by (season, team_id)
tblproperties (delta.enableChangeDataFeed = true);


create table if not exists nhl_data.teams.master_ids (

  team_id integer not null,
  team_abbrev string not null, 
  team_name string not null,
  insert_dte timestamp not null,
  update_dte timestamp

)
using delta
cluster by (team_id)
tblproperties (delta.enableChangeDataFeed = true);


create table if not exists nhl_data_raw.teams.details (

    season integer not null,
    endpoint string not null,
    http_status integer not null,
    request_key string,
    api_url string not null,
    payload string,
    ingest_ts_utc timestamp not null
    
)
using delta 
tblproperties (delta.enableChangeDataFeed = true);


create table if not exists nhl_data_staged.teams.details (


      team_id integer not null, 
      team_abbrev string not null, 
      team_name string not null, 
      team_city string, 
      is_active boolean not null, 
      active_from date,
      active_to date, 
      last_active_season integer,
      insert_dte timestamp not null, 
      update_dte timestamp, 
      py_source string not null


)
using delta 
cluster by (team_id, team_abbrev)
tblproperties (delta.enableChangeDataFeed = true);


create table if not exists nhl_data.teams.details (

  team_id integer not null, 
  team_abbrev string not null, 
  team_name string not null,
  is_active boolean not null, 
  active_from date not null, 
  active_to date,
  insert_dte timestamp not null, 
  update_dte timestamp

)
using delta
cluster by (team_id)
tblproperties (delta.enableChangeDataFeed = true);


create table if not exists nhl_data.teams.names (

  latest_season integer not null,
  team_id integer not null, 
  team_name string not null, 
  category string not null, 
  website string not null, 
  is_active boolean not null, 
  active_from date not null, 
  active_to date, 
  insert_dte timestamp not null,
  update_dte timestamp,
  py_source string not null
  
)
using delta
tblproperties (delta.enableChangeDataFeed = true);

create table if not exists nhl_data_raw.teams.team_lines (

  game_date date not null,
  endpoint string not null, 
  http_status integer not null, 
  request_key string, 
  api_url string not null, 
  payload string, 
  ingest_ts_utc timestamp not null

)
using delta
cluster by (game_date, endpoint)
tblproperties (delta.autoOptimize.optimizeWrite = true, delta.enableChangeDataFeed = true);


create table if not exists nhl_data_staged.teams.team_lines ( 

  season integer not null, 
  team_name string not null, 
  team_abbrev string not null,
  team_id integer not null, 
  game_date date not null,
  game_id bigint not null, 
  player_name_dfo string not null, 
  player_name_nhl_api string not null, 
  player_id bigint not null, 
  player_pos string,
  player_pos_cat string,
  team_line string not null, 
  team_line_idx integer not null,
  is_active boolean not null,
  first_name string not null,
  last_name string not null,
  player_key string not null,
  join_key string not null,
  lines_source_url string,
  dfo_update_dte timestamp,
  strength_identifier string,
  strength_name string,
  game_time_dec_ind boolean,
  injury_status string,
  position_name string,
  line_identifier string,
  line_name string,
  player_id_dfo integer,
  insert_dte timestamp not null,
  update_dte timestamp,
  py_source string not null



)
using delta 
cluster by (season, game_date, team_id)
tblproperties (delta.autoOptimize.optimizeWrite = true, delta.enableChangeDataFeed = true);


create table if not exists nhl_data.teams.team_lines (

    season integer not null, 
    team_id integer not null,
    team_abbrev string not null,
    game_date date not null,
    game_id bigint not null,
    player_name string not null, 
    player_id bigint not null,
    player_pos string,
    player_pos_cat string,
    team_line string not null,
    team_line_idx integer not null,
    insert_dte timestamp not null,
    update_dte timestamp


)
using delta 
cluster by (season, game_date, team_id)
tblproperties (delta.autoOptimize.optimizeWrite = true, delta.enableChangeDataFeed = true);

--Players

create schema if not exists nhl_data_raw.players;
create schema if not exists nhl_data_staged.players; 
create schema if not exists nhl_data.players;


create table if not exists nhl_data_raw.players.player_search (

    endpoint string not null,
    http_status integer not null,
    request_key string not null,
    api_url string not null,
    payload string,
    ingest_ts_utc timestamp not null
    
)
using delta;

create table if not exists nhl_data_raw.players.player_search_season (

  endpoint string not null, 
  http_status integer not null, 
  request_key string not null, 
  api_url string not null, 
  payload string, 
  ingest_ts_utc timestamp not null
  
)
using delta;


create table if not exists nhl_data_staged.players.master_ids (


        player_id bigint not null, 
        player_name string not null, 
        player_pos string, 
        team_id integer, 
        team_abbrev string, 
        team_id_prev_team integer, 
        team_abbrev_prev_team string, 
        last_active_season integer, 
        jersey_num integer, 
        shoots_catches string, 
        is_active boolean not null, 
        insert_dte timestamp not null, 
        update_dte timestamp,
        required_field_empty_rate decimal(4,2) not null,
        py_source string not null,
        failed_condition string

)

using delta 
cluster by (last_active_season, player_id)
tblproperties (delta.autoOptimize.optimizeWrite = true, delta.enableChangeDataFeed = true, 'delta.enableDeletionVectors' = 'true');


create table if not exists nhl_data.players.master_ids (

  player_id bigint not null, 
  player_name string not null, 
  player_pos string, 
  shoots_catches string, 
  insert_dte timestamp not null, 
  update_dte timestamp

)
using delta
cluster by (player_id)
tblproperties (delta.enableChangeDataFeed = true);


create table if not exists nhl_data_raw.players.player_game_rosters (

    endpoint string not null, 
    http_status integer not null, 
    request_key string not null, 
    team_id integer not null,
    game_id bigint not null,
    api_url string not null,
    payload string, 
    ingest_ts_utc timestamp not null
)
using delta 
cluster by (game_id, request_key);


create table if not exists nhl_data_staged.players.player_game_rosters (

      season integer not null,
      game_id bigint not null, 
      game_date date not null,
      team_id integer not null,
      team_abbrev string not null, 
      player_id bigint not null, 
      player_name string not null,
      player_pos string, 
      jersey_num integer, 
      shoots_catches string, 
      birth_dte date, 
      birth_country string, 
      height_inches integer, 
      weight_lbs integer, 
      is_active boolean not null,
      headshot string, 
      game_in_play boolean not null, 
      insert_dte timestamp not null, 
      update_dte timestamp,
      unused_structs string, 
      num_unused_structs integer, 
      py_source string not null

)

using delta 
cluster by (season, game_date, player_id)
tblproperties (delta.enableChangeDataFeed = true);


create table if not exists nhl_data.players.player_game_rosters (

  season integer not null , 
  player_id bigint not null, 
  player_name string not null,
  game_id bigint not null, 
  game_date date not null,
  team_id integer not null, 
  player_pos string, 
  is_active boolean not null, 
  insert_dte timestamp not null,
  update_dte timestamp
  
)
using delta 
cluster by (season, game_date, player_id)
tblproperties (delta.enableChangeDataFeed = true, 'delta.enableDeletionVectors' = 'true');

--Ops

create table if not exists nhl_data_staged.ops.schema_audit (

    table_name string not null,
    endpoint string,
    schema_json string,
    schema_hash string,
    schema_drift_ind boolean,
    last_check_dte date,
    insert_dte timestamp not null,
    update_dte timestamp

)
tblproperties (delta.enableChangeDataFeed = true)
;

create table if not exists nhl_data_staged.ops.games_missing_pbp (

    season int not null,
    game_id bigint not null,
    last_attempt_dte date not null,
    next_retry_dte date not null,
    attempt_count integer not null,
    insert_dte timestamp not null,
    update_dte timestamp


)
using delta
tblproperties ('delta.columnMapping.mode' = 'true')
;

create table if not exists nhl_data_staged.ops.games_missing_shift (

  season int not null,
  game_id bigint not null,
  last_attempt_dte date not null,
  next_retry_dte date not null,
  attempt_count integer not null,
  insert_dte timestamp not null,
  update_dte timestamp

)
using delta
tblproperties ('delta.columnMapping.mode' = 'true')
;

--PBP Shift Data

create table if not exists nhl_data_staged.games.pbp_shift_data (


    season integer not null,
    game_date date not null,
    game_id bigint not null,
    period tinyint not null,
    pbp_game_seconds integer, 
    time_in_period varchar(5),
    time_remaining varchar(5),
    event_idx integer not null,
    event_id integer,
    situation_code varchar(4),
    event_type string,
    zone_code string,
    x_coord integer,
    y_coord integer,
    shot_type string,
    missed_shot_desc string, 
    event_team_id integer,
    event_team_abbrev string,
    event_player_id integer,
    shift_team_id integer,
    shift_team_abbrev string,
    shift_player_id integer,
    shift_player_name string,
    shift_player_pos string,
    shift_number integer,
    shift_id integer,
    shift_idx integer,
    shift_start_game_seconds integer,
    shift_end_game_seconds integer,
    start_time string,
    end_time string,
    duration string,
    event_description string,
    event_details string,
    insert_dte timestamp not null,
    update_dte timestamp



)
cluster by (season, game_date, game_id)
tblproperties (delta.enableChangeDataFeed = true, delta.autoOptimize.optimizeWrite = true, 'delta.columnMapping.mode' = 'name', 'delta.logRetentionDuration' = 'interval 30 days',
'delta.deletedFileRetentionDuration' = 'interval 30 days', 'delta.enableDeletionVectors' = 'true');

create table if not exists nhl_data.games.pbp_shift_data (

    season integer not null,
    game_date date not null,
    game_id bigint not null,
    period tinyint,
    pbp_game_seconds integer not null,
    time_in_period varchar(5),
    time_remaining varchar(5),
    event_idx integer,
    event_id integer,
    situation_code varchar(4),
    event_type string,
    zone_code string,
    x_coord integer,
    y_coord integer, 
    shot_type string,
    missed_shot_desc string,
    event_team_id integer,
    event_team_abbrev string,
    event_player_id integer,
    shift_team_id integer,
    shift_team_abbrev string,
    shift_player_id integer,
    shift_player_name string,
    shift_number integer,
    shift_id integer,
    shift_idx integer,
    shift_start_game_seconds integer,
    shift_end_game_seconds integer,
    start_time string,
    end_time string,
    duration string,
    event_description string,
    event_details string,
    team_line integer,
    pp_unit integer,
    pk_unit integer,
    insert_dte timestamp not null,
    update_dte timestamp,
    py_source string not null,
    failed_condition string

)
cluster by (season, game_date, game_id)
tblproperties (delta.enableChangeDataFeed = true, delta.autoOptimize.optimizeWrite = true, 'delta.columnMapping.mode' = 'name', 'delta.logRetentionDuration' = 'interval 30 days', 
'delta.deletedFileRetentionDuration' = 'interval 30 days', 'delta.enableDeletionVectors' = 'true');

--Quarantine

create schema if not exists nhl_data_staged.quarantine;

create table if not exists nhl_data_staged.quarantine.pbp_data (

    season integer not null, 
    game_id bigint not null, 
    game_date date not null, 
    period tinyint, 
    period_type string, 
    home_team_defending_side string, 
    time_in_period varchar(5), 
    time_remaining varchar(5), 
    game_seconds integer, 
    situation_code varchar(4), 
    zone_code string, 
    event_id integer, 
    event_idx integer, 
    event_type string, 
    event_type_code integer, 
    event_team_id integer, 
    event_team_abbrev string, 
    player_id bigint, 
    x_coord integer, 
    y_coord integer, 
    goalie_in_net_id bigint, 
    shooting_player_id bigint, 
    shot_type string, 
    missed_shot_desc string, 
    away_sog integer, 
    home_sog integer, 
    away_score tinyint, 
    home_score tinyint, 
    scoring_player_id bigint, 
    scoring_player_total tinyint, 
    assist1_player_id bigint, 
    assist1_player_total tinyint, 
    assist2_player_id bigint, 
    assist2_player_total tinyint, 
    blocking_player_id bigint, 
    penalty_desc string, 
    penalty_type_desc string, 
    penalty_duration integer, 
    penalty_committed_by_player_id bigint, 
    penalty_drawn_by_player_id bigint, 
    hit_given_by_player_id bigint, 
    hit_taken_by_player_id bigint, 
    faceoff_winning_player_id bigint, 
    faceoff_losing_player_id bigint, 
    max_regulation_periods tinyint, 
    play_stopped_reason string, 
    intermission_active boolean, 
    game_in_play boolean, 
    insert_dte timestamp not null, 
    update_dte timestamp, 
    py_source string not null, 
    active_row boolean not null,
    quarantine_reason string not null

)
cluster by (season, game_date, game_id)
tblproperties (delta.enableChangeDataFeed = true);

create table if not exists nhl_data_staged.quarantine.shift_data (

    season integer not null,
    game_id bigint not null,
    game_date date not null,
    team_id integer,
    team_abbrev string, 
    team_name string,
    player_id integer,
    first_name string,
    last_name string,
    player_name string,
    shift_id bigint,
    period tinyint,
    start_time varchar(5),
    end_time varchar(5),
    duration varchar(5),
    shift_number integer,
    detail_code integer,
    event_description string,
    event_details string,
    event_number integer,
    hex_value string,
    type_code integer,
    game_in_play boolean,
    insert_dte timestamp not null,
    update_dte timestamp,
    py_source string not null,
    active_row boolean not null,
    quarantine_reason string not null


)
cluster by (season, game_date, game_id)
tblproperties (delta.enableChangeDataFeed = true)
;

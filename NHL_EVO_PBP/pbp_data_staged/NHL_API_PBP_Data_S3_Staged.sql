create temporary view pbp_details_cleaned_tmp as (

    with team_info as (

        select 
            team_id, 
            team_abbrev
        from nhl_data_staged.teams.master_ids 
        where 1 = 1
            and team_id <> 99
            and franchise_id is not null 

    )

    select /*+ broadcast (b) */
        cast(a.season as integer) as season,
        cast(a.game_id as bigint) as game_id,
        cast(a.gameDate as date) as game_date,
        cast(a.number as tinyint) as period,
        a.periodType as period_type,
        trim(a.homeTeamDefendingSide) as home_team_defending_side,
        cast(nullif(a.timeInPeriod, '') as string) as time_in_period,
        cast(nullif(a.timeRemaining, '') as string) as time_remaining,
        (
            (a.number - 1) * 1200
            + cast(split(a.timeInPeriod, ':')[0] as integer) * 60
            + cast(split(a.timeInPeriod, ':')[1] as integer)
        ) as game_seconds,
        cast(
            if(a.number = 1 and lower(a.typeDescKey) = 'period-start', '1551', a.situationCode)
            as string
        ) as situation_code,
        trim(a.zoneCode) as zone_code,
        cast(a.sortOrder as integer) as event_idx,
        a.eventId as event_id,
        trim(a.typeDescKey) as event_type,
        a.event_type_cde,
        cast(a.eventOwnerTeamId as integer) as event_team_id,
        b.team_abbrev as event_team_abbrev,
        -- trim(
        --     case when a.eventOwnerTeamId = b.awayTeam.id then b.awayTeam.abbrev
        --         when a.eventOwnerTeamId = b.homeTeam.id then b.homeTeam.abbrev
        --         else null
        --         end
        -- ) as event_team_abbrev,
        cast(a.playerId as integer) as player_id,
        a.xCoord as x_coord,
        a.yCoord as y_coord,
        cast(a.goalieInNetId as integer) as goalie_in_net_id,
        cast(a.shootingPlayerId as integer) as shooting_player_id,
        trim(a.shotType) as shot_type,
        trim(a.reason) as missed_shot_desc,
        cast(a.awaySOG as smallint) as away_sog,
        cast(a.homeSOG as smallint) as home_sog,
        cast(a.awayScore as tinyint) as away_score,
        cast(a.homeScore as tinyint) as home_score,
        cast(a.scoringPlayerId as integer) as scoring_player_id,
        cast(a.scoringPlayerTotal as integer) as scoring_player_total,
        cast(a.assist1PlayerId as integer) as assist1_player_id,
        cast(a.assist1PlayerTotal as integer) as assist1_player_total,
        cast(a.assist2PlayerId as integer) as assist2_player_id,
        cast(a.assist2PlayerTotal as integer) as assist2_player_total,
        cast(a.blockingPlayerId as integer) as blocking_player_id,
        trim(a.descKey) as penalty_desc,
        trim(a.typeCode) as penalty_type_desc,
        a.duration as penalty_duration,
        cast(a.committedByPlayerId as integer) as penalty_committed_by_player_id,
        cast(a.drawnByPlayerId as integer) as penalty_drawn_by_player_id,
        cast(a.hittingPlayerId as integer) as hit_given_by_player_id,
        cast(a.hitteePlayerId as integer) as hit_taken_by_player_id,
        cast(a.winningPlayerId as integer) as faceoff_winning_player_id,
        cast(a.losingPlayerId as integer) as faceoff_losing_player_id,
        a.maxRegulationPeriods as max_regulation_periods,
        a.secondaryReason as play_stopped_reason,
        cast(a.inIntermission as boolean) as intermission_active,
        --b.clock.inIntermission as intermission_active,
        cast(if(a.season <= 20092010, false, a.running) as boolean) as game_in_play,
        --cast(if(a.season <= 20092010, false, b.clock.running) as boolean) as game_in_play,
        a.scrape_ts_utc,
        a.s3_ingest_ts_utc,
        a.ingest_ts_utc,
        current_timestamp() as insert_dte,
        cast('${silver_py_source}' as string) as py_source
    from stream ${bronze_catalog}.${api_schema}.pbp_play_details 
        ---set watermark delay of 3 days as safety net since we scrape games that occured within the last two days to 
        ---ensure data is up to date and accurate
        watermark scrape_ts_utc delay of interval '3' days a
    left join team_info b 
        on a.eventOwnerTeamId = b.team_id
    
)
;

-- create or refresh streaming table ${silver_catalog}.${api_schema}.pbp_data 
-- comment 'play details from nhl_evo_staged.games.pbp_details_cleaned_tmp stream'
-- cluster by (season, game_date, game_id);


-- create flow pbp_details_flow as 
-- auto cdc into ${silver_catalog}.${api_schema}.pbp_data
-- from stream(pbp_details_cleaned_tmp)
-- keys (season, game_id, event_idx)
-- sequence by scrape_ts_utc
-- stored as scd type 2
-- track history on * except (s3_ingest_ts_utc, ingest_ts_utc, insert_dte, py_source)

-- ;

create or refresh streaming table ${silver_catalog}.${api_schema}.pbp_data
comment 'latest play details from NHL API'
cluster by (season, game_date, game_id)
flow replace using (season, game_id)
sequence by scrape_ts_utc
by name
select *
from stream(pbp_details_cleaned_tmp)
;
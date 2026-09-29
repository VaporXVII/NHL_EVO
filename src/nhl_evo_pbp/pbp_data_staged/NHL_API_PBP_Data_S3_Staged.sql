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
        a.season,
        a.game_id,
        a.gameDate as game_date,
        a.number as period,
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
        a.sortOrder as event_idx,
        a.eventId as event_id,
        trim(a.typeDescKey) as event_type,
        a.event_type_cde,
        a.eventOwnerTeamId as event_team_id,
        b.team_abbrev as event_team_abbrev,
        -- trim(
        --     case when a.eventOwnerTeamId = b.awayTeam.id then b.awayTeam.abbrev
        --         when a.eventOwnerTeamId = b.homeTeam.id then b.homeTeam.abbrev
        --         else null
        --         end
        -- ) as event_team_abbrev,
        a.playerId as player_id,
        a.xCoord as x_coord,
        a.yCoord as y_coord,
        a.goalieInNetId as goalie_in_net_id,
        a.shootingPlayerId as shooting_player_id,
        trim(a.shotType) as shot_type,
        trim(a.reason) as missed_shot_desc,
        a.awaySOG as away_sog,
        a.homeSOG as home_sog,
        a.awayScore as away_score,
        a.homeScore as home_score,
        a.scoringPlayerId as scoring_player_id,
        a.scoringPlayerTotal as scoring_player_total,
        a.assist1PlayerId as assist1_player_id,
        a.assist1PlayerTotal as assist1_player_total,
        a.assist2PlayerId as assist2_player_id,
        a.assist2PlayerTotal as assist2_player_total,
        a.blockingPlayerId as blocking_player_id,
        trim(a.descKey) as penalty_desc,
        trim(a.typeCode) as penalty_type_desc,
        a.duration as penalty_duration,
        a.committedByPlayerId as penalty_committed_by_player_id,
        a.drawnByPlayerId as penalty_drawn_by_player_id,
        a.hittingPlayerId as hit_given_by_player_id,
        a.hitteePlayerId as hit_taken_by_player_id,
        a.winningPlayerId as faceoff_winning_player_id,
        a.losingPlayerId as faceoff_losing_player_id,
        a.maxRegulationPeriods as max_regulation_periods,
        a.secondaryReason as play_stopped_reason,
        a.inIntermission as intermission_active,
        --b.clock.inIntermission as intermission_active,
        cast(if(a.season <= 20092010, false, a.running) as boolean) as game_in_play,
        --cast(if(a.season <= 20092010, false, b.clock.running) as boolean) as game_in_play,
        a.scrape_ts_utc,
        a.s3_ingest_ts_utc,
        a.ingest_ts_utc,
        current_timestamp() as insert_dte,
        cast('NHL_API_PBP_S3_Staged' as string) as py_source
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
where game_date >= date_sub(current_date(), 3);
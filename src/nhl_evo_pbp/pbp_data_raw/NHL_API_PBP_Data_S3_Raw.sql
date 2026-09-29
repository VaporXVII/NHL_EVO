create or refresh streaming table ${bronze_catalog}.${api_schema}.pbp_raw_data 
comment 'response json from NHL PBP API' 
cluster by (request_key) as (

    select 
        endpoint, 
        request_key,
        http_status,
        api_url,
        payload,
        if(coalesce(backfill, True) = true, timestampadd(minute, -10, ingest_ts_utc), scrape_ts_utc) as scrape_ts_utc,
        try_cast(s3_ingest_ts_utc as timestamp) as s3_ingest_ts_utc,
        _rescued_data,
        _metadata.file_path as source_file_path,
        current_timestamp() as ingest_ts_utc,
        cast('NHL_API_PBP_S3_Raw' as string) as py_source
    from stream read_files(
        "/Volumes/nhl_evo_s3/${api_schema}/pbp_data", 
        format => "json", 
        inferColumnTypes => true,
        schemaLocation => "/Volumes/nhl_evo_s3/${api_schema}/pbp_data/_schema",
        schemaEvolutionMode => "addNewColumns",
        rescuedDataColumn => "_rescued_data",
        schemaHints => "ingest_ts_utc timestamp, scrape_ts_utc timestamp, s3_ingest_ts_utc string, backfill boolean",
        useManagedFileEvents => true

    )
    where 1 = 1
        and payload is not null

)
;

create or refresh streaming table ${bronze_catalog}.${api_schema}.pbp_game_details (

    constraint valid_season expect (season is not null) on violation drop row,
    constraint valid_game_id expect (game_id is not null) on violation drop row,
    constraint valid_game_date expect (gameDate is not null and gameDate <= current_date()) on violation drop row

)
comment 'game details from response json from NHL PBP API'
cluster by (season, gameDate, game_id) as (

    select 
        payload.season as season,
        payload.id as game_id,
        payload.gameDate as gameDate,
        payload.* except (season, id, gameDate),
        scrape_ts_utc,
        s3_ingest_ts_utc,
        ingest_ts_utc,
        current_timestamp() as insert_dte,
        py_source
    from stream ${bronze_catalog}.${api_schema}.pbp_raw_data

)
;

create or refresh streaming table ${bronze_catalog}.${api_schema}.pbp_team_details (

    constraint valid_team_id expect (team_id is not null) on violation drop row

)
comment 'team details from pbp_game_details stream'
cluster by (season, gameDate, game_id) as (

    with team_details as (

        select
            season,
            game_id,
            gameDate,
            'away'::string as home_road,
            awayTeam.id as team_id,
            awayTeam.abbrev,
            from_json(
                to_json(awayTeam.commonName),
                'struct<default:string,fr:string>'
            ) as commonName,
            awayTeam.* except (id, abbrev, commonName),
            parse_json(to_json(awayTeam)) as team_payload,
            scrape_ts_utc,
            s3_ingest_ts_utc,
            ingest_ts_utc,
            py_source
        from stream ${bronze_catalog}.${api_schema}.pbp_game_details
        union all
        select
            season,
            game_id,
            gameDate,
            'home'::string as home_road,
            homeTeam.id as team_id,
            homeTeam.abbrev,
            from_json(
                to_json(homeTeam.commonName),
                'struct<default:string,fr:string>'
            ) as commonName,
            homeTeam.* except (id, abbrev, commonName),
            parse_json(to_json(homeTeam)) as team_payload,
            scrape_ts_utc,
            s3_ingest_ts_utc,
            ingest_ts_utc,
            py_source
        from stream ${bronze_catalog}.${api_schema}.pbp_game_details

    )

    select
        * except (py_source),
        current_timestamp() as insert_dte,
        py_source
    from team_details

)
;

create or refresh streaming table ${bronze_catalog}.${api_schema}.pbp_team_rosters ( 

    constraint valid_team_id expect (teamId is not null) on violation drop row,
    constraint valid_player_id expect (playerId is not null) on violation drop row

)
comment 'team roster details from pbp_game_details stream'
cluster by (season, gameDate, teamId) as (

    with layer_one as (

        select 
            season, game_id, gameDate, explode(rosterSpots) as rosterSpots, scrape_ts_utc, s3_ingest_ts_utc, ingest_ts_utc, py_source
        from stream ${bronze_catalog}.${api_schema}.pbp_game_details

    )
    ,
    layer_two as (

        select 
            season, game_id, gameDate, rosterSpots.*, scrape_ts_utc, s3_ingest_ts_Utc, ingest_ts_utc, py_source
        from layer_one

    )
    ,
    layer_three as (

        select 
            * except (firstName, lastName, scrape_ts_utc, s3_ingest_ts_utc, ingest_ts_utc, py_source),
            firstName.default as firstName,
            lastName.default as lastName,
            scrape_ts_utc, s3_ingest_ts_utc, ingest_ts_utc, py_source 
        from layer_two 

    )

        select 
            * except (headshot, scrape_ts_utc, s3_ingest_ts_utc, ingest_ts_utc, py_source),
            headshot, 
            scrape_ts_utc,
            s3_ingest_ts_utc,
            ingest_ts_utc, 
            current_timestamp() as insert_dte,
            py_source
        from layer_three

)
;

create or refresh streaming table ${bronze_catalog}.${api_schema}.pbp_game_status (

    constraint valid_running expect (running is not null) on violation drop row
    
)
comment 'game status from pbp_game_details stream'
cluster by (season, gameDate, game_id) as (

    select 
        season,
        game_id, 
        gameDate,
        gameScheduleState,
        gameState,
        clock.*,
        scrape_ts_utc,
        s3_ingest_ts_utc,
        ingest_ts_utc,
        current_timestamp() as insert_dte,
        py_source
    from stream ${bronze_catalog}.${api_schema}.pbp_game_details

)
;

create or refresh streaming table ${bronze_catalog}.${api_schema}.pbp_play_raw_details 
comment 'raw play details from pbp_game_details stream'
cluster by (season, gameDate, game_id) as (

    with layer_one as (

        select      
            season,
            game_id,
            gameDate,
            scrape_ts_utc,
            s3_ingest_ts_utc,
            ingest_ts_utc,
            py_source,
            explode_outer(plays) as plays_payload,
            clock.inIntermission,
            clock.running
        from stream ${bronze_catalog}.${api_schema}.pbp_game_details

    )
    , 
    layer_two as (

        select 
            season, 
            game_id,
            gameDate,
            plays_payload.*,
            inIntermission,
            running,
            scrape_ts_utc, 
            s3_ingest_ts_utc, 
            ingest_ts_utc,
            py_source
        from layer_one


    )
    ,
    layer_three as (

        select 
            season, 
            game_id,
            gameDate,
            sortOrder,
            eventId,
            typeCode as event_type_cde,
            details.*,
            periodDescriptor.*,
            *
            except (season, game_id, gameDate, eventId, sortOrder, typeCode, details, periodDescriptor)
        from layer_two

    )

        select * except(py_source),
            current_timestamp() as insert_dte,
            py_source
        from layer_three 

)
;

create or refresh streaming table ${bronze_catalog}.${api_schema}.pbp_play_details (

    ---valid play must not violate any of these conditions
    constraint valid_event_idx expect (sortOrder is not null) on violation drop row,
    constraint valid_event_id expect (eventId is not null) on violation drop row,
    constraint valid_period expect (number is not null and number >= 1) on violation drop row,
    constraint valid_time_in_period expect (timeInPeriod is not null) on violation drop row,
    constraint valid_time_remaining expect (timeRemaining is not null) on violation drop row,
    constraint valid_situation_code expect (situationCode is not null and len(situationCode) = 4) on violation drop row,
    constraint valid_goal_tracking expect (not (lower(typeDescKey) = 'goal' and scoringPlayerId is null)) on violation drop row,
    constraint valid_shot_tracking expect (not (lower(typeDescKey) ilike any ('missed%', 'shot%') and shootingPlayerId is null)) on violation drop row,
    constraint valid_shot_type_tracking expect (not (lower(typeDescKey) ilike any ('missed%', 'shot%') and shotType is null)) on violation drop row,
    constraint valid_block_tracking expect (not (lower(typeDescKey) ilike 'block%' and blockingPlayerId is null)) on violation drop row,
    constraint valid_x_coord_tracking expect (
    not (
        lower(typeDescKey) ilike any ('missed%', 'block%', 'shot%', 'goal')
        and xCoord is null
    )
    ) on violation drop row,
    constraint valid_y_coord_tracking expect (
    not (
        lower(typeDescKey) ilike any ('missed%', 'block%', 'shot%', 'goal')
        and yCoord is null
    )
) on violation drop row
)
comment 'valid play details from pbp_game_details stream'
cluster by (season, gameDate, game_id) as (

    select 
        a.* except (insert_dte, py_source),
        current_timestamp() as insert_dte,
        py_source
    from stream ${bronze_catalog}.${api_schema}.pbp_play_raw_details a


)
;

create or refresh streaming table nhl_evo_quarantine.${api_schema}.pbp_play_details (

    ---invalid play must satisfy at least one of these conditions
    constraint invalid_play expect (

                sortOrder is null   
                or eventId is null 
                or number is null
                or number < 1
                or timeInPeriod is null 
                or timeRemaining is null 
                or (situationCode is null or len(situationCode) <> 4) 
                or (lower(typeDescKey) = 'goal' and scoringPlayerId is null) 
                or (lower(typeDescKey) ilike any ('missed%', 'shot%') and shootingPlayerId is null)
                or (lower(typeDescKey) ilike any ('missed%', 'shot%') and shotType is null)
                or (lower(typeDescKey) ilike 'block%' and blockingPlayerId is null)
                or (lower(typeDescKey) ilike any ('missed%', 'block%', 'shot%', 'goal') and xCoord is null)
                or (lower(typeDescKey) ilike any ('missed%', 'block%', 'shot%', 'goal') and yCoord is null)


    ) on violation drop row 
)
comment 'quarantined play details from pbp_game_details stream'
cluster by (season, gameDate, game_id) as (

    select 
        a.* except (insert_dte, py_source),
        case when sortOrder is null then 'sortOrder'
            when eventId is null then 'eventId'
            when (`number` is null or `number` < 1) then 'number' 
            when timeInPeriod is null then 'timeInPeriod'
            when timeRemaining is null then 'timeRemaining'
            when (situationCode is null or len(situationCode) <> 4) then 'situationCode'
            when (lower(typeDescKey) = 'goal' and scoringPlayerId is null) then 'scoringPlayerId'
            when (lower(typeDescKey) ilike any ('missed%', 'shot%') and shootingPlayerId is null) then 'shootingPlayerId'
            when (lower(typeDescKey) ilike any ('missed%', 'shot%') and shotType is null) then 'shotType'
            when (lower(typeDescKey) ilike 'block%' and blockingPlayerId is null) then 'blockingPlayerId'
            when (lower(typeDescKey) ilike any ('missed%', 'block%', 'shot%', 'goal') and (xCoord is null and yCoord is null)) then 'xCoord & yCoord'
            when (lower(typeDescKey) ilike any ('missed%', 'block%', 'shot%', 'goal') and xCoord is null) then 'xCoord'
            when (lower(typeDescKey) ilike any ('missed%', 'block%', 'shot%', 'goal') and yCoord is null) then 'yCoord'
            else 'unknown'
            end as quarantine_reason,
        current_timestamp() as insert_dte,
        py_source
    from stream ${bronze_catalog}.${api_schema}.pbp_play_raw_details a


)
;

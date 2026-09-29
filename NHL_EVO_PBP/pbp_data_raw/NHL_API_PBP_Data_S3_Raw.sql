create or refresh streaming table ${bronze_catalog}.${api_schema}.pbp_raw_data 
comment 'response json from NHL PBP API' 
cluster by (request_key) as (

    select 
        endpoint, 
        request_key,
        http_status,
        api_url,
        payload,
        ---if payload backfill key = true then rely on the legacy ingest_ts_utc - 10 minutes, otherwise use the new scrape_ts_utc 
        if(coalesce(backfill, True) = true, 
            timestampadd(minute, -10, try_cast(ingest_ts_utc as timestamp)), 
            try_cast(scrape_ts_utc as timestamp)
            ) as scrape_ts_utc,
        try_cast(s3_ingest_ts_utc as timestamp) as s3_ingest_ts_utc,
        _rescued_data,
        _metadata.file_path as source_file_path,
        current_timestamp() as ingest_ts_utc,
        cast('${bronze_py_source}' as string) as py_source
    from stream read_files(
        "${s3_ext_vol}", 
        format => "json", 
        inferColumnTypes => true,
        schemaLocation => "${s3_ext_vol}/_schema",
        schemaEvolutionMode => "addNewColumns",
        rescuedDataColumn => "_rescued_data",
        schemaHints => "ingest_ts_utc timestamp, scrape_ts_utc timestamp, s3_ingest_ts_utc string, backfill boolean",
        useManagedFileEvents => true,
        maxFilesPerTrigger => 100,
        ---games from legacy tables were all ingested as of 2026-08-19, setting cut off time  
        modifiedAfter => "2026-08-19T23:59:59.999+00:00"

    )
    where 1 = 1
        and payload is not null

)
;

create or refresh streaming table ${bronze_catalog}.${api_schema}.pbp_game_details (

    ---drop the row if the season or game_id is null, also drop the row if the game_date is greater than the current_date
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

    --enusre that team id is not null otherwise drop 
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

    --ensure both team id and player id is not null otherwise drop
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

    ---ensure that the running field from the game clock isn't null otherwise drop
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
    ---event idx can't be null 
    constraint valid_event_idx expect (sortOrder is not null) on violation drop row,
    ---event id can't be null otherwise won't know what type of event it was
    constraint valid_event_id expect (eventId is not null) on violation drop row,
    ---period must not be null and must a valid period #
    constraint valid_period expect (number is not null and number >= 1) on violation drop row,
    constraint valid_time_in_period expect (timeInPeriod is not null) on violation drop row,
    constraint valid_time_remaining expect (timeRemaining is not null) on violation drop row,
    constraint valid_situation_code expect (situationCode is not null and len(situationCode) = 4) on violation drop row,
    ---if the event is a goal but the scoring player id is null then drop 
    constraint valid_goal_tracking expect (not (lower(typeDescKey) = 'goal' and scoringPlayerId is null)) on violation drop row,
    ---if the event type is a shot on goal or missed shot but the shooting player is null then drop
    constraint valid_shot_tracking expect (not (lower(typeDescKey) ilike any ('missed%', 'shot%') and shootingPlayerId is null)) on violation drop row,
    ---if the event type is a shot on goal but the shot type is null then drop 
    constraint valid_shot_type_tracking expect (not (lower(typeDescKey) ilike any ('missed%', 'shot%') and shotType is null)) on violation drop row,
    ---if the event type is a blocked shot but blocking player id is null then drop 
    constraint valid_block_tracking expect (not (lower(typeDescKey) ilike 'block%' and blockingPlayerId is null)) on violation drop row,
    ---must have valid x_coords for any shot type 
    constraint valid_x_coord_tracking expect (
    not (
        lower(typeDescKey) ilike any ('missed%', 'block%', 'shot%', 'goal')
        and xCoord is null
    )
    ) on violation drop row,
    ---must have valid y_coords for any shot type 
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

    ---invalid play must satisfy at least one of these conditions (inverse of conditions above)
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
        ---below used to identify why a row was quarantined 
        nullif(
            concat_ws(
                ', ',
                filter(
                    array(
                        case when sortOrder is null then 'sortOrder' end,
                        case when eventId is null then 'eventId' end,
                        case when (`number` is null or `number` < 1) then 'number' end,
                        case when timeInPeriod is null then 'timeInPeriod' end,
                        case when timeRemaining is null then 'timeRemaining' end,
                        case when (situationCode is null or len(situationCode) <> 4) then 'situationCode' end,
                        case when (lower(typeDescKey) = 'goal' and scoringPlayerId is null) then 'scoringPlayerId' end,
                        case when (lower(typeDescKey) ilike any ('missed%', 'shot%') and shootingPlayerId is null) then 'shootingPlayerId' end,
                        case when (lower(typeDescKey) ilike any ('missed%', 'shot%') and shotType is null) then 'shotType' end,
                        case when (lower(typeDescKey) ilike 'block%' and blockingPlayerId is null) then 'blockingPlayerId' end,
                        case when (lower(typeDescKey) ilike any ('missed%', 'block%', 'shot%', 'goal') and xCoord is null and yCoord is not null) then 'xCoord' end,
                        case when (lower(typeDescKey) ilike any ('missed%', 'block%', 'shot%', 'goal') and xCoord is not null and yCoord is null) then 'yCoord' end
                        ),
                        x -> x is not null
                        )
                    ),
                ''
        ) as quarantine_reason,
        current_timestamp() as insert_dte,
        py_source
    from stream ${bronze_catalog}.${api_schema}.pbp_play_raw_details a


)
;

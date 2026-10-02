create or refresh streaming table ${bronze_catalog}.${api_schema}.shift_raw_data
comment 'response json from NHL Shift API'
cluster by (request_key) as (


    select 
        endpoint,
        request_key,
        http_status,
        api_url,
        payload,
        _rescued_data,
        _metadata.file_path as source_file_path,
        --if backfill is listed as null then it means it wasn't included in json payload (legacy backfill), therefore setting to true to populate a scrape_ts_utc based on ingest_ts_utc from legacy json responses
        if(coalesce(backfill, True) = true, timestampadd(minute, -10, ingest_ts_utc), scrape_ts_utc) as scrape_ts_utc,
        try_cast(s3_ingest_ts_utc as timestamp) as s3_ingest_ts_utc,
        current_timestamp() as ingest_ts_utc,
        cast('${bronze_py_source}' as string) as py_source
    from stream read_files(
        "${s3_ext_vol}", 
        format => "json",
        inferColumnTypes => true,
        schemaLocation => "${s3_ext_vol}/_schema",
        schemaEvolutionMode  => "addNewColumns",
        rescuedDataColumn => "_rescued_data",
        schemaHints => "ingest_ts_utc timestamp, scrape_ts_utc timestamp, s3_ingest_ts_utc string, backfill boolean",
        useManagedFileEvents => true
    )
    where 1 = 1
        and payload is not null 

)
;

create or refresh streaming table ${bronze_catalog}.${api_schema}.shift_game_totals (

    constraint valid_records_check expect (total_records > 0) on violation drop row

)
comment 'shift totals from response json from NHL Shift API'
cluster by (season) as (


    with shift_totals as (

        select
            try_element_at(payload.data, 1).gameId as gameId,
            payload.total as total_records,
            scrape_ts_utc,
            s3_ingest_ts_utc,
            ingest_ts_utc,
            py_source
        from stream ${bronze_catalog}.${api_schema}.shift_raw_data

)

    select
        concat(
            substring(cast(gameId as string), 1, 4),
            cast(substring(cast(gameId as string), 1, 4) as integer) + 1
        ) as season,
        * except (py_source),
        current_timestamp() as insert_dte, 
        py_source
    from shift_totals

)
;

create temporary view games_missing_shift_tmp as (

    with shift_totals as (

        select
            ---since payload will be empty, must use request_key for gameId
            request_key as gameId,
            payload.total as total_records,
            coalesce(scrape_ts_utc, timestampadd(second, -30, s3_ingest_ts_utc)) as scrape_ts_utc,
            s3_ingest_ts_utc,
            ingest_ts_utc,
            py_source
        from stream ${bronze_catalog}.${api_schema}.shift_raw_data

    )

    select
        concat(
            substring(cast(gameId as string), 1, 4),
            cast(substring(cast(gameId as string), 1, 4) as integer) + 1
        ) as season,
        * except (scrape_ts_utc, s3_ingest_ts_utc, py_source),
        current_date() as first_attempt_dte,
        date_add(current_date(), 15) as next_retry_dte,
        scrape_ts_utc,
        s3_ingest_ts_utc,
        current_timestamp() as insert_dte,
        py_source 
    from shift_totals

)
;

create flow games_missing_shift_flow 
as auto cdc into ${bronze_catalog}.ops.games_missing_shift
from stream(games_missing_shift_tmp)
keys (season, gameId)
apply as delete when total_records > 0 
sequence by scrape_ts_utc
stored as scd type 1
;


-- create or refresh streaming table ${bronze_catalog}.ops.games_missing_shift 
-- comment 'games in current stream or historical games that are missing shift data'
-- cluster by (season, gameId)
-- flow replace using (season, gameId)
-- sequence by scrape_ts_utc
-- by name 
-- select * 
-- from stream(games_missing_shift_tmp)
-- ;
 
-- create or refresh streaming table ${bronze_catalog}.ops.games_missing_shift (

--     constraint invalid_missing_games expect (total_records = 0) on violation drop row

-- )
-- comment 'games missing shift data from shift_game_totals stream' as (

--     with shift_totals as (

--     select
--         ---since payload will be empty, must use request_key for gameId
--         request_key as gameId,
--         payload.total as total_records,
--         coalesce(scrape_ts_utc, timestampadd(second, -30, s3_ingest_ts_utc)) as scrape_ts_utc,
--         s3_ingest_ts_utc,
--         ingest_ts_utc,
--         py_source
--     from stream ${bronze_catalog}.${api_schema}.shift_raw_data

--     )

--     select
--         concat(
--             substring(cast(gameId as string), 1, 4),
--             cast(substring(cast(gameId as string), 1, 4) as integer) + 1
--         ) as season,
--         * except (scrape_ts_utc, s3_ingest_ts_utc, py_source),
--         current_date() as first_attempt_dte,
--         date_add(current_date(), 15) as next_retry_dte,
--         scrape_ts_utc,
--         s3_ingest_ts_utc,
--         current_timestamp() as insert_dte,
--         py_source 
--     from shift_totals
--     where 1 = 1
--         and total_records = 0

-- )
-- ;

create or refresh streaming table ${bronze_catalog}.${api_schema}.shift_raw_details
comment 'raw unfiltered shift details from response json from NHL Shift API'
cluster by (season, gameId, teamId) as (


    select
        concat(
            substring(cast(b.gameId as string), 1, 4),
            cast(substring(cast(b.gameId as string), 1, 4) as integer) + 1
        ) as season,
        b.*,
        a.scrape_ts_utc,
        a.s3_ingest_ts_utc,
        a.ingest_ts_utc,
        current_timestamp() as insert_dte,
        a.py_source
    from stream ${bronze_catalog}.${api_schema}.shift_raw_data a
    lateral view explode(a.payload.data) shift_records as b


)
;

create or replace streaming table ${bronze_catalog}.${api_schema}.shift_details (

    ---shift must be tied to valid season  
    constraint valid_game_id expect (gameId is not null) on violation drop row,
    ---shift must be tied to valid game id 
    constraint valid_team_id expect (teamId is not null) on violation drop row,
    ---shift must have taken place in a valid period  
    constraint valid_period expect (period is not null and period >= 1) on violation drop row,
    ---shift must have valid player id  
    constraint valid_player_id expect (playerId is not null) on violation drop row,
    ---shift must have valid shift id  
    constraint valid_shift_id expect (id is not null) on violation drop row,
    ---shift must have valid start time 
    constraint valid_start_time expect (startTime is not null) on violation drop row,
    ---shift must have valid end time
    constraint valid_end_time expect (endTime is not null) on violation drop row, 
    constraint valid_duration expect (duration is not null or eventDescription is not null) on violation drop row

)
comment 'valid shift details from response json from shift_raw_details'
cluster by (season, gameId, teamId) as (

    select 
        a.* except (insert_dte, py_source),
        current_timestamp() as insert_dte,
        a.py_source
    from stream ${bronze_catalog}.${api_schema}.shift_raw_details a

)
;

create or replace streaming table nhl_evo_quarantine.${api_schema}.shift_details (

    constraint invalid_shift expect (

            gameId is null 
            or teamId is null 
            or period is null 
            or period < 1 
            or playerId is null 
            or id is null 
            or startTime is null 
            or endTime is null 
            or (duration is null and eventDescription is null) 

    ) on violation drop row

)
comment 'quarantined shift details from response json from NHL shift_raw_details'
cluster by (season) as (

    select 
        a.* except (insert_dte, py_source),
        nullif(
        concat_ws(
            ', ',
            filter(
                array(
                    case when gameId is null then 'gameId' end,
                    case when teamId is null then 'teamId' end,
                    case when (period is null or period < 1) then 'period' end,
                    case when playerId is null then 'playerId' end,
                    case when `id` is null then 'id' end,
                    case when startTime is null then 'startTime' end,
                    case when endTime is null then 'endTime' end,
                    case when (duration is null and eventDescription is null) then 'duration' end
                ),
                x -> x is not null
             )
        ),
        ''
        ) as quarantine_reason,
        current_timestamp() as insert_dte,
        a.py_source
    from stream ${bronze_catalog}.${api_schema}.shift_raw_details a 


)
;


create temporary view shift_details_cleaned_tmp as (

    with shifts as (

        select 
            season,
            gameId as game_id,
            teamId as team_id,
            upper(trim(teamAbbrev)) as team_abbrev,
            trim(teamName) as team_name,
            period,
            playerId as player_id,
            concat_ws(' ', trim(firstName), trim(lastName)) as player_name,
            trim(firstName) as first_name,
            trim(lastName) as last_name,
            id as shift_id,
            shiftNumber as shift_number,
            startTime as start_time,
            endTime as end_time,
            duration as shift_duration,
            eventNumber as event_number, 
            upper(trim(eventDescription)) as event_description,
            upper(trim(eventDetails)) as event_details,
            detailCode as detail_code,
            typeCode as type_code,
            hexValue as hex_value,
            scrape_ts_utc,
            s3_ingest_ts_utc,
            ingest_ts_utc,
            cast('${silver_py_source}' as string) as py_source
        from stream ${bronze_catalog}.${api_schema}.shift_details

    )
    ,
    schedules as (

        select distinct 
            season, 
            game_id, 
            game_date
        from nhl_data_staged.games.schedules 
        where 1 = 1
            and game_type in (2,3)
            and game_date <= current_date()
    
    )

    select /*+ broadcast (b) */
        a.season,
        a.game_id, 
        coalesce(b.game_date, '3000-12-21'::date) as game_date,
        a.* except (season, game_id, py_source),
        current_timestamp() as insert_dte,
        a.py_source
    from shifts a  
    left join schedules b 
        on a.season = b.season 
        and a.game_id = b.game_id 

)
;



---===================
--original audo cdc flow, changing to replace_using since each scrape will contain plays from prior scrapes (i.e. scrape at 6PM has shifts 1-5, scrape at 7PM has shifts 1-10). replacing using helps capture records that are still valid and those that are not
--====================
-- create or refresh streaming table ${silver_catalog}.${api_schema}.shift_data 
-- comment 'shift details from nhl_evo_staged.games.shift_details_cleaned_tmp stream'
-- cluster by (season, game_date, game_id);

-- create flow shift_details_flow as 
-- auto cdc into ${silver_catalog}.${api_schema}.shift_data
-- from stream(shift_details_cleaned_tmp)
-- keys (season, game_id, period, start_time, team_id, player_id)
-- sequence by scrape_ts_utc
-- stored as scd type 2
-- track history on * except (s3_ingest_ts_utc, ingest_ts_utc, insert_dte, py_source)

--====================
create or refresh streaming table ${silver_catalog}.${api_schema}.shift_data 
comment 'shift details from nhl_evo_staged.games.shift_details_cleaned_tmp stream'
cluster by (season, game_date, game_id)
flow replace using (season, game_id)
sequence by scrape_ts_utc
by name 
select *
from stream(shift_details_cleaned_tmp)
;

import sys 
username = spark.sql("select current_user()").first()[0]
sys.path.append(f"/Workspace/Users/{username}/NHL_Pipeline")

from pipeline_funcs.user_utc_region import region_return 
user_region = region_return()

spark.sql(f"""

    with date_param as (

    select from_utc_timestamp(current_timestamp(), '{user_region}')::date as current_run_date

    ) 
    ,
    src as (

    select /*+ broadcast (p) */
        a.*
    from nhl_data_staged.players.master_ids a
    cross join date_param p 
    where 1 = 1
        and (
        insert_dte::date between date_sub(p.current_run_date, 1) and p.current_run_date
            or 
        update_dte::date between date_sub(p.current_run_date, 1) and p.current_run_date
        )

    )


    merge into nhl_data.players.master_ids t 
    using src s    
        on t.player_id = s.player_id 
    when matched and (

        not (
            lower(trim(t.player_name)) <=> lower(trim(coalesce(s.player_name, t.player_name)))
        )
        or not (t.player_pos <=> coalesce(s.player_pos, t.player_pos))
        or not (t.shoots_catches <=> coalesce(s.shoots_catches, t.shoots_catches))


    )
    then update set 

        player_name = trim(s.player_name), 
        player_pos = upper(trim(s.player_pos)), 
        shoots_catches = upper(trim(s.shoots_catches)), 
        update_dte = current_timestamp()

    when not matched then insert (

        player_id, 
        player_name, 
        player_pos, 
        shoots_catches, 
        insert_dte, 
        update_dte

    )
    values (

        s.player_id, 
        trim(s.player_name), 
        upper(trim(s.player_pos)), 
        upper(trim(s.shoots_catches)),
        current_timestamp(), 
        null

    )
;

""")

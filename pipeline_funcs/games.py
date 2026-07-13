from pyspark.sql import SparkSession, DataFrame 
def get_games(spark: SparkSession, bronze_table: str) -> DataFrame: 

    if "raw" in bronze_table.lower():

        return spark.sql(f"""
                        
                        with cold_start as (
                            ---using as method to make sure script runs on days where games are in play but also for cold start 
                            select 
                                (count(*) = 0)::boolean as is_cold_start
                            from {bronze_table.lower()}
                        )
                        , 
                        recent_games as (

                            select distinct 
                                game_id, 
                                game_date                
                            from nhl_data_staged.games.schedules a 
                            where 1 = 1
                                and game_type in (2,3)
                                and a.game_date between 
                                    date_sub(from_utc_timestamp(current_date(), 'America/Chicago')::date, 2) 
                                        and 
                                    from_utc_timestamp(current_date(), 'America/Chicago')::date
                        )

                        select 
                            game_id, 
                            game_date
                        from recent_games 
                        union all 
                        select 
                            null as game_id, 
                            null as game_date
                        from cold_start 
                        where 1 = 1 
                            and is_cold_start = true
                        order by game_date desc, game_id
                        limit 1
                        
        """)
    else: 
        print("a non-bronze layer table was passed, please correct the bronze_table and argument and try again")
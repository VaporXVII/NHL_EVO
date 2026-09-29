from pyspark.sql import SparkSession, DataFrame 
from pyspark.errors import AnalysisException

def get_games(spark: SparkSession, table_name: str) -> DataFrame: 

    try: 
        #limiting to regular and postseason games only for team lines since source site doesn't track line combos for preseason games 
        #limiting to regular and postseason games only for shift data since NHL doesn't track shift data for preseason games
        game_type_clause = f"and a.game_type in (2,3)" if "team_lines" in table_name.lower() or "shift_data" in table_name.lower() or "vegas_totals" in table_name.lower() else f"and a.game_type in (1,2,3)"
        return spark.sql(f"""
                        
                        with cold_start as (
                            ---using as method to make sure script runs on days where games are in play but also for cold start 
                            select 
                                (count(*) = 0)::boolean as is_cold_start
                            from {table_name.lower()}
                        )
                        , 
                        recent_games as (

                            select distinct 
                                game_id, 
                                game_date                
                            from nhl_data_staged.games.schedules a 
                            where 1 = 1
                                {game_type_clause}
                                and a.game_date between 
                                    date_sub(from_utc_timestamp(current_timestamp(), 'America/Chicago')::date, 2) 
                                        and 
                                    from_utc_timestamp(current_timestamp(), 'America/Chicago')::date
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
    except AnalysisException as e:
        raise ValueError(f"Table '{table_name}' not found. Please check the table name and try again.") from e
    except Exception as e: 
        print(f"Error occured: {e}")

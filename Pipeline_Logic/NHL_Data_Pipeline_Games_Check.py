from pyspark.sql import functions as f

games_today = (
    spark.table("nhl_data_staged.games.schedules")
    .filter(
        (f.col("game_date") == f.from_utc_timestamp(f.current_timestamp(), 'America/Chicago').cast("date"))
         
        )
    .count()
)

dbutils.jobs.taskValues.set(
    key = "games_today",
    value = games_today
)

games_yesterday = (

    spark
    .table("nhl_data_staged.games.schedules")
    .filter(f.col("game_date") == f.date_sub(f.current_date(), 1))
    .count()
)

dbutils.jobs.taskValues.set(
    key = "games_yesterday",
    value = games_yesterday
)

import sys 
username = spark.sql("select current_user()").first()[0]
sys.path.append(f"/Workspace/Users/{username}/NHL_Pipeline")

from pyspark.sql import functions as f
from pipeline_funcs.user_utc_region import region_return 
user_region = region_return()


games_today = (
    spark.table("nhl_data_staged.games.schedules")
    .filter(
        (f.col("game_date") == f.from_utc_timestamp(f.current_timestamp(), f'{user_region}').cast("date"))
         
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
    .filter(

            f.col("game_date") == f.date_sub(f.from_utc_timestamp(f.current_timestamp(), f'{user_region}'), 1)
    )
    .count()
)

dbutils.jobs.taskValues.set(
    key = "games_yesterday",
    value = games_yesterday
)

tables_exist = spark.catalog.tableExists("nhl_data_raw.games.schedules")

dbutils.jobs.taskValues.set(key = "schedules_table_exists", value = 1 if tables_exist else 0)

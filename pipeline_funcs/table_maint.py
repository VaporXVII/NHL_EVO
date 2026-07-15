from pyspark.sql import SparkSession 

def run_table_maint(table_name: str) -> None:

    print("=" * 50)
    print(f"Analyzing {table_name}")
    spark.sql(f"""analyze table {table_name} compute statistics;""")
    print("=" * 50)
    spark.sql(f"""optimize {table_name};""")
    print(f"Optimizing {table_name}")
    print("=" * 50)
    print(f"Vacuuming {table_name}")
    spark.sql(f"""vacuum {table_name};""")
    print("=" * 50)
    print(f"Analyze, optimize, and vacuum completed for {table_name}")

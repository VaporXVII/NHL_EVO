from pyspark.sql import SparkSession, functions as f, Row

ready = spark.sql("""
                  

        select 
            (count(*) <> 0) as ready 
        from nhl_data_staged.games.schedules 
        where 1 = 1
            ---looking to see if any records have been inserted within the last two days, if yes then proceed, if not then bypass
            and insert_dte::date >= date_sub(from_utc_timestamp(current_timestamp(), 'America/Chicago'), 2)              
                  
    
    
    """).first()["ready"]
if ready: 

    tables = spark.sql(f"""
                        
                select 
                    concat(table_catalog, ".", table_schema, ".", table_name) as table_name
                from system.information_schema.columns
                where 1 = 1
                    and table_catalog = "nhl_data_raw"
                    and table_schema in ("games", "teams", "players")
                    and column_name = "endpoint" 
                        
                        
        """).collect()
    schemas = {}
    for table_name in tables: 
        table_schema = spark.sql(f"""
                                
                
                with endpoint_schemas as (

                    select 
                        endpoint,
                        payload
                    from {table_name['table_name']}
                    qualify row_number() over (partition by endpoint order by ingest_ts_utc desc) = 1

                    )

                    select 
                        endpoint, 
                        schema_of_json_agg(payload) as pay_schema 
                    from endpoint_schemas 
                    group by 
                        endpoint 
                                
                                    
            """)
        for row in table_schema.collect():
            schema_key = f"{table_name['table_name']}-{row['endpoint']}"
            schemas[schema_key] = row["pay_schema"]

    schemas_df = (

        spark 
        .createDataFrame(
        [(k, v) for k, v in schemas.items()],
        ["table_name", "schema_json"]
        )
        .withColumn("endpoint", f.split_part(f.col("table_name"), f.lit("-"), f.lit(2)))
        .withColumn("table_name", f.split_part(f.col("table_name"), f.lit("-"), f.lit(1)))

    )
    schemas_df.createOrReplaceTempView("schemas_tmp")
    spark.sql(f"""
                merge into nhl_data_staged.ops.schema_audit t  
                using schemas_tmp s  
                    on t.table_name = s.table_name 
                    and t.endpoint = s.endpoint

                when matched and (

                    coalesce(t.schema_json, '0') <> coalesce(s.schema_json, '0')

                )

                then update set 

                    schema_json = s.schema_json,
                    schema_hash = sha2(s.schema_json, 256),
                    schema_drift_ind = True,
                    last_check_dte = current_date(),
                    update_dte = current_timestamp()

                when not matched then insert (

                    table_name,
                    endpoint, 
                    schema_json,
                    schema_hash,
                    schema_drift_ind,
                    last_check_dte,
                    insert_dte,
                    update_dte

                )

                values (

                    s.table_name,
                    s.endpoint,
                    s.schema_json,
                    sha2(s.schema_json, 256),
                    False,
                    current_date(),
                    current_timestamp(),
                    null
                )

    """)
    print(f"Data successfully inserted/updated into nhl_data_staged.ops.schema_audit table")
else: 
    print(f"Date not eligible for schema audit check, skipping...")
from pyspark.sql import functions as f, DataFrame, Column
from typing import Any, Collection, Mapping
import re 

def convert_case(name: str) -> str:

    #Split lowercase/digit to uppercase
    s1 = re.sub(r'(?<=[a-z0-9])([A-Z])', r'_\1', name)
    
    #Split acronym -> word (UTCOffset -> UTC_Offset)
    s2 = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', s1)
    
    return s2.lower()

#takes pre-defined schema and applies it to df
def apply_schema(df, schema):

    return df.select([
        f.col(c.name).cast(c.dataType).alias(c.name)
        for c in schema.fields

    ])

#takes a dict of pre-defined fields and builds them in the df using their defined parameters
def build_fields(src_col: str, rule: Mapping[str, Any], df_fields: Collection[str]) -> Column:

    if "target" not in rule:
        target_col = list(rule.keys())[0]
        dtype = list(rule.values())[0]
        expr = f.col(src_col) if src_col in df_fields else f.lit(None)
        return expr.cast(dtype).alias(target_col)

    target_col = rule["target"]
    dtype = rule["type"]

    expr = f.col(src_col) if src_col in df_fields else f.lit(None)

    if rule.get("lower"):
        expr = f.lower(expr)
    if rule.get("upper"):
        expr = f.upper(expr)
    if rule.get("trim"):
        expr = f.trim(expr)

    return expr.cast(dtype).alias(target_col)


#function pulls in df schema
def get_schema(df: DataFrame) -> str:

    return (

        df 
        .selectExpr("schema_of_json_agg(payload) as schema")
        .first()
        ["schema"]
    )

#takes a df and returns a df with the schema applied (used in for special teams data)
def unpack_data(df: DataFrame) -> DataFrame:

    #deprecating as of 7/11
    # sched_raw_schema = (

    #     df 
    #     #.select(f.schema_of_json(f.col("payload")).alias("schema"))
    #     #changing to first()["schema"] for better readability 
    #     .first()
    #     ["schema"]

    # )

    #update as of 7/11 to do aggregate schema collection per https://docs.databricks.com/aws/en/sql/language-manual/functions/schema_of_json_agg
    sched_raw_schema = get_schema(df)

    d = (

        df 
        .withColumn("json", f.from_json(f.col("payload"), sched_raw_schema))
        .select(f.col("json.*"))
        .withColumn("api_data", f.explode(f.col("data")))
        .select("api_data.*")

    )

    return d 


def flatten_df(df):
    
    while True:
        complex_fields = [
            field
            for field in df.schema.fields
            if isinstance(field.dataType, (t.StructType, t.ArrayType))
        ]

        if not complex_fields:
            break

        field = complex_fields[0]
        field_name = field.name

        if isinstance(field.dataType, t.StructType):
            expanded_columns = [
                f.col(f"`{field_name}`.`{nested_field.name}`").alias(
                    f"{nested_field.name}"
                )
                for nested_field in field.dataType.fields
            ]

            df = df.select(
                "*",
                *expanded_columns
            ).drop(field_name)

        elif isinstance(field.dataType, t.ArrayType):
            df = df.withColumn(
                field_name,
                f.explode_outer(f.col(f"`{field_name}`"))
            )

    return df


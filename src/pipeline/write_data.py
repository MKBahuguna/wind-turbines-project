from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from .config import database_name, schema_name


def merge_delta_table(spark: SparkSession, df: DataFrame, table_name: str, keys: list):
    """
    Merges a DataFrame into a Delta table if it exists, or creates a new Delta table if it doesn't.
    """
    full_table_name = f"{database_name}.{schema_name}.{table_name}"

    if spark.catalog.tableExists(full_table_name):
        delta_table = DeltaTable.forName(spark, full_table_name)

        delta_table.alias("target").merge(
            df.alias("source"),
            " AND ".join([f"target.{key} = source.{key}" for key in keys])
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        df.write.format("delta").mode("overwrite").saveAsTable(full_table_name)

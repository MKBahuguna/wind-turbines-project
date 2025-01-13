from pyspark.sql import DataFrame
from pyspark.sql.functions import col, avg, to_date, min, max
from .write_data import merge_delta_table
from .config import statistics_table_name


def output_statistics(spark, df_clean: DataFrame) -> DataFrame:
    """
        Calculates the daily min, max, avg for power_output columns and writes to delta table.
    """
    df_daily_statistics = (df_clean
                            .withColumn("date", to_date(col("timestamp")))
                            .groupBy(col("date"), col("turbine_id"))
                            .agg(
                                min("power_output").alias("power_output_min"),
                                max("power_output").alias("power_output_max"),
                                avg("power_output").alias("power_output_avg"),
                            )
                        )

    keys = ["turbine_id", "date"]
    merge_delta_table(spark, df_daily_statistics, statistics_table_name, keys)

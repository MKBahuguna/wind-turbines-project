from pyspark.sql import DataFrame
from pyspark.sql.functions import col, mean, stddev, to_date, min, max
from src.pipeline.config import statistics_table_name

def calculate_daily_statistics(df_clean: DataFrame) -> DataFrame:
    """
        1. Calculate the daily mean, min and max for power_output column
    """
    df_daily_statistics = (df_clean
                            .withColumn("date", to_date(col("timestamp")))
                            .groupBy(col("date"), col("turbine_id"))
                            .agg(
                                mean("power_output").alias("power_output_mean"),
                                min("power_output").alias("power_output_min"),
                                max("power_output").alias("power_output_max"),
                                stddev("power_output").alias("power_output_stddev"),
                            )
                        )

    df_daily_statistics.write.format("delta").mode("overwrite").saveAsTable(statistics_table_name)

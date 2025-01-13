from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from .write_data import merge_delta_table
from .config import anomalies_table_name


def output_anomalies(spark, df_zscores: DataFrame) -> None:
    """
        Calculates anomalies which are any power_output's that are 2 standard deviations 
        from the mean of power_output from that turbine for that hour slot,
        and writes to delta table.
    """
    df_anomalies = (df_zscores
                    .filter(col("power_output_zscore") > 2)
                    .select(
                        col("timestamp"), col("turbine_id"), col("wind_speed"), col("wind_direction"), col("power_output")
                    )
                    )

    keys = ["turbine_id", "timestamp"]
    merge_delta_table(spark, df_anomalies, anomalies_table_name, keys)

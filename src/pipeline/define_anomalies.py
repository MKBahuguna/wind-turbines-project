from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from .config import anomalies_table_name


def find_and_write_anomalies(df_zscores: DataFrame) -> None:
    """
        Anomalies are any power_output's that are 2 standard deviations from the mean of power_output from that turbine for that hour slot
    """
    df_anomalies = (df_zscores
                .filter(col("power_output_zscore") > 2)
                .select(
                    col("timestamp"), col("turbine_id"), col("wind_speed"), col("wind_direction"), col("power_output")
                    )
            )

    df_anomalies.write.format("delta").mode("overwrite").saveAsTable(anomalies_table_name)

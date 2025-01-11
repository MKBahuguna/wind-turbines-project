from pyspark.sql.functions import col, mean, stddev, when, coalesce
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import explode, sequence, hour
from pyspark.sql.types import TimestampType
from .config import cleaned_table_name


def get_all_timestamps(spark: SparkSession, start_date: datetime, end_date: datetime) -> DataFrame:
    """
    Generate a list of hourly timestamps between two dates and add an hour column.
    """
    df = spark.createDataFrame(
        [(start_date, end_date)], 
        ["start_date", "end_date"]
    )

    df_timestamps = df.select(
                        explode(
                            sequence(
                                df.start_date.cast(TimestampType()),
                                df.end_date.cast(TimestampType()),
                                timedelta(hours=1)
                            )
                        ).alias("timestamp")
                    ).withColumn("hour", hour("timestamp"))
    
    return df_timestamps


def impute_missing_values_with_means(df_raw: DataFrame, df_timestamps: DataFrame) -> DataFrame:
    """
        - Get a list of turbines
        - Get a list of all the expected timeslots
        - Join expected timeslots and actual timeslots for each turbine to create missing rows
        - Calculate the hourly mean for each turbine
        - Join the hourly mean to the missing rows
        - Fill in missing values with the hourly mean using coalesce
    """
    df_turbines = df_raw.select('turbine_id').distinct()

    df_turbines_with_timestamps = df_timestamps.crossJoin(df_turbines)
    df_raw_with_missing_rows = df_turbines_with_timestamps.join(df_raw, on=["timestamp", "turbine_id"], how="left")

    df_turbine_hourly_mean = (df_raw_with_missing_rows
                                .groupBy("turbine_id", "hour")
                                .agg(
                                    mean("wind_speed").alias("wind_speed_mean"),
                                    mean("wind_direction").alias("wind_direction_mean"),
                                    mean("power_output").alias("power_output_mean"),
                                    stddev("wind_speed").alias("wind_speed_stddev"),
                                    stddev("wind_direction").alias("wind_direction_stddev"),
                                    stddev("power_output").alias("power_output_stddev"),
                                    )
                            )

    df_raw_with_hourly_means = (df_raw_with_missing_rows
                                        .join(df_turbine_hourly_mean, on=["turbine_id", "hour"], how="left"))

    df_imputed = (df_raw_with_hourly_means
                    .withColumn("wind_speed", coalesce(col("wind_speed"), col("wind_speed_mean")))
                    .withColumn("wind_direction", coalesce(col("wind_direction"), col("wind_direction_mean")))
                    .withColumn("power_output", coalesce(col("power_output"), col("power_output_mean")))
                )

    return df_imputed


def calculate_zscore(df: DataFrame) -> DataFrame:
    """
        Calculate z-scores for each column
    """
    df_zscores = (df
                    .withColumn("wind_speed_zscore", abs((col("wind_speed") - col("wind_speed_mean")) / col("wind_speed_stddev")))
                    .withColumn("wind_direction_zscore", abs((col("wind_direction") - col("wind_direction_mean")) / col("wind_direction_stddev")))
                    .withColumn("power_output_zscore", abs((col("power_output") - col("power_output_mean")) / col("power_output_stddev")))    
                )

    return df_zscores

def replace_outliers_with_means(df_zscores: DataFrame) -> DataFrame:
    """
        - Replace values with the hourly mean if the z-score is greater than 3 (3 standard deviations from mean)
    """

    df_replaced_outliers = (df_zscores
                    .withColumn("wind_speed", when(col("wind_speed_zscore") > 3, col("wind_speed_mean")).otherwise(col("wind_speed")))
                    .withColumn("wind_direction", when(col("wind_direction_zscore") > 3, col("wind_direction_mean")).otherwise(col("wind_direction")))
                    .withColumn("power_output", when(col("power_output_zscore") > 3, col("power_output_mean")).otherwise(col("power_output")))
                    .select(
                        col("timestamp"), col("turbine_id"), col("wind_speed"), col("wind_direction"), col("power_output")
                        )
                )

    return df_replaced_outliers


def pre_clean_data(spark: SparkSession, df_source: DataFrame, start_date: datetime, end_date: datetime) -> DataFrame:
    """
        - Replace any missing rows with the mean of the turbine and hour
        - Calculate zscore
        This intermediate df is needed to define anomalies in a separate module
    """
    df_timestamps = get_all_timestamps(spark, start_date, end_date)
    df_imputed = impute_missing_values_with_means(df_source, df_timestamps)
    df_zscores = calculate_zscore(df_imputed)

    return df_zscores


def clean_data(df_zscores: DataFrame) -> DataFrame:
    """
        Replace any outliers (3 standard deviations from mean) with the mean of the turbine and hour
    """
    df_cleaned = replace_outliers_with_means(df_zscores)

    df_cleaned.write.format("delta").mode("overwrite").saveAsTable(cleaned_table_name)

    return df_cleaned

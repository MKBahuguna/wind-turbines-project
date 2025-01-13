from src.pipeline.process_data import replace_outliers_with_means
from src.pipeline.write_data import merge_delta_table
from .config import cleaned_table_name
from pyspark.sql import DataFrame


def output_cleaned(spark, df_zscores: DataFrame) -> DataFrame:
    """
    Cleans the data by replacing outliers (values beyond 3 standard deviations from the mean) 
    with the mean for the corresponding turbine and hour. Saves the cleaned data to a Delta table.
    """
    df_cleaned = replace_outliers_with_means(df_zscores)

    keys = ["turbine_id", "timestamp"]
    merge_delta_table(spark, df_cleaned, cleaned_table_name, keys)

    return df_cleaned

from pyspark.sql import DataFrame
from .config import raw_data_path


def load_csv_to_df(spark) -> DataFrame:
    """
    Load all CSV files into a single DataFrame
    """
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(raw_data_path)

    return df

from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def load_data(spark, file_path, start_date, end_date) -> DataFrame:
    """
    Load all CSV files into a single DataFrame
    """
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(f"file:{file_path}") \
        .filter(col("timestamp").between(start_date, end_date))

    return df

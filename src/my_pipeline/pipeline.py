from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
import argparse
from pyspark.sql.functions import col, mean, stddev, when


def main(start_date: str, end_date: str):
    spark = (SparkSession
                .builder
                .appName("App")
                .getOrCreate())

    # Convert string arguments to datetime objects
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    # Clean the source data
    clean_data(spark, start_date, end_date)


raw_table_name = 'dmfos_trial.raw.data_group_1'
cleaned_table_name = 'dmfos_trial.raw.cleaned'


def clean_data(spark, start_date, end_date):

    raw_df = spark.table(raw_table_name).filter(col("timestamp").between(start_date, end_date))

    # Impute missing values
    imputed_df = raw_df.fillna({"power_output": 0})

    # Handle outliers (e.g., z-score > 3)
    stats = imputed_df.select(mean("power_output").alias("mean"), stddev("power_output").alias("std")).first()
    mean_value, std_value = stats["mean"], stats["std"]

    cleaned_df = imputed_df.withColumn(
        "is_outlier",
        when((col("power_output") < mean_value - 3 * std_value) | (col("power_output") > mean_value + 3 * std_value), True).otherwise(False)
    ).filter(col("is_outlier") == False)

    # Write to cleaned Delta table
    cleaned_df = cleaned_df.drop("is_outlier")
    cleaned_df.write.format("delta").mode("overwrite").saveAsTable(cleaned_table_name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the pipeline")
    parser.add_argument("--start_date", required=True, help="Start date in YYYY-MM-DD format")
    parser.add_argument("--end_date", required=True, help="End date in YYYY-MM-DD format")

    args = parser.parse_args()

    main(args.start_date, args.end_date)

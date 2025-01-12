from datetime import datetime
from pyspark.sql import SparkSession
import argparse
from .config import raw_data_path
from .load_data import load_csv_to_df
from .calculate_statistics import calculate_daily_statistics
from .clean_data import clean_data, pre_clean_data
from .define_anomalies import find_and_write_anomalies


def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Run the pipeline")
    parser.add_argument("--start_date", required=True, help="Start date in YYYY-MM-DD format")
    parser.add_argument("--end_date", required=True, help="End date in YYYY-MM-DD format")
    args = parser.parse_args()

    # Convert arguments to datetime objects
    start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d")

    # Initialize Spark session
    spark = (SparkSession
             .builder
             .appName("Wind Turbines Pipeline")
             .getOrCreate())

    df_raw = load_csv_to_df(spark, raw_data_path, start_date, end_date)
    df_zscore = pre_clean_data(spark, df_raw, start_date, end_date)

    df_cleaned = clean_data(df_zscore)

    find_and_write_anomalies(df_zscore)
    calculate_daily_statistics(df_cleaned)


if __name__ == "__main__":
    main()

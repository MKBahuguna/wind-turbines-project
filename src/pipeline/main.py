from datetime import datetime
from pyspark.sql import SparkSession
import argparse
from .config import raw_data_path
from .load_data import load_data
from .process_data import process_data
from .output_cleaned import output_cleaned
from .output_anomalies import output_anomalies
from .output_statistics import output_statistics

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

    # Load raw data in a dataframe
    df_raw = load_data(spark, raw_data_path, start_date, end_date)

    # Process data
    df_processed = process_data(spark, df_raw, start_date, end_date)

    # Write cleaned data
    df_cleaned = output_cleaned(spark, df_processed)

    # Write anomalies
    output_anomalies(spark, df_processed)

    # Write statistics
    output_statistics(spark, df_cleaned)


if __name__ == "__main__":
    main()

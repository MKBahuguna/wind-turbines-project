import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, TimestampType, IntegerType, DoubleType, StringType, FloatType
from datetime import datetime
from src.pipeline.process_data import get_all_timestamps, impute_missing_values_with_means
from src.pipeline.load_data import load_data


SPARK = SparkSession.builder.appName("test").getOrCreate()

def test_load_data():
    # Arrange
    start_date = datetime(2022, 3, 1, 0, 0, 0)
    end_date = datetime(2022, 3, 2, 0, 0, 0)
    test_data = '/Users/porshmac/Projects/Porsh/wind-turbines-project/src/tests/data/data_group_1.csv'

    # Act
    df_raw = load_data(SPARK, test_data, start_date, end_date)

    # Assert
    assert df_raw.count() > 0


def test_get_all_timestamps():
    # Arrange
    start_date = datetime(2021, 1, 1, 0, 0, 0)
    end_date = datetime(2021, 1, 2, 0, 0, 0)

    # Act
    all_timestamps = get_all_timestamps(SPARK, start_date, end_date)

    # Assert
    assert all_timestamps.count() == 24
    assert all_timestamps.collect()[0].timestamp == datetime(2021, 1, 1, 0, 0)
    assert all_timestamps.collect()[-1].timestamp == datetime(2021, 1, 1, 23, 0)


def test_impute_missing_values_with_means():
    #Arrange
    schema = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("turbine_id", IntegerType(), True),
        StructField("wind_speed", FloatType(), True),
        StructField("wind_direction", FloatType(), True),
        StructField("power_output", FloatType(), True)
    ])

    data = [
        (datetime(2022, 3, 1, 0, 0), 1, 10.0, 180.0, 100.0),
        (datetime(2022, 3, 1, 1, 0), 1, 12.0, 190.0, 110.0),
        (datetime(2022, 3, 1, 2, 0), 1, 11.0, 200.0, 120.0)
    ]

    df_source = SPARK.createDataFrame(data, schema)
    df_expected_timeslots = get_all_timestamps(SPARK, datetime(2022, 3, 1, 0, 0), datetime(2022, 3, 2, 0, 0))

    # Act
    df_result = impute_missing_values_with_means(df_source, df_expected_timeslots)

    # Assert
    assert df_result.count() == 24

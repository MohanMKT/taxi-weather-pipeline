from datetime import datetime
from zoneinfo import ZoneInfo

import pytest
from pyspark.sql import SparkSession

from taxi_weather_pipeline.data_cleaner import TaxiDataCleaner

NEW_YORK = ZoneInfo("America/New_York")


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("TaxiWeatherPipelineTests")
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )


def test_clean_removes_invalid_taxi_records(
    *,
    spark: SparkSession,
) -> None:
    # Arrange
    taxi_data = spark.createDataFrame(
        [
            (
                datetime(
                    2024,
                    1,
                    1,
                    10,
                    0,
                    tzinfo=NEW_YORK,
                ),
                datetime(
                    2024,
                    1,
                    1,
                    10,
                    30,
                    tzinfo=NEW_YORK,
                ),
                2,
                5.0,
                20.0,
            ),
            (
                datetime(
                    2024,
                    1,
                    1,
                    11,
                    0,
                    tzinfo=NEW_YORK,
                ),
                datetime(
                    2024,
                    1,
                    1,
                    10,
                    30,
                    tzinfo=NEW_YORK,
                ),
                1,
                3.0,
                15.0,
            ),
            (
                datetime(
                    2024,
                    1,
                    1,
                    12,
                    0,
                    tzinfo=NEW_YORK,
                ),
                datetime(
                    2024,
                    1,
                    1,
                    12,
                    10,
                    tzinfo=NEW_YORK,
                ),
                1,
                0.0,
                10.0,
            ),
        ],
        [
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "total_amount",
        ],
    )

    cleaner = TaxiDataCleaner()

    # Act
    cleaned_data = cleaner.clean(
        taxi_data=taxi_data,
    )

    # Assert
    assert cleaned_data.count() == 1
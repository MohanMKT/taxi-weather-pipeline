from datetime import datetime
from zoneinfo import ZoneInfo

import pytest
from pyspark.sql import SparkSession

from taxi_weather_pipeline.data_joiner import TaxiWeatherJoiner

NEW_YORK = ZoneInfo("America/New_York")


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("TaxiWeatherPipelineTests")
        .getOrCreate()
    )


def test_join_matches_taxi_trip_to_hourly_weather(
    *,
    spark: SparkSession,
) -> None:
    # Arrange
    taxi_data = spark.createDataFrame(
        [
            (
                datetime(2024, 1, 1, 10, 15, tzinfo=NEW_YORK),
                4.2,
                25.0,
            ),
        ],
        [
            "tpep_pickup_datetime",
            "trip_distance",
            "total_amount",
        ],
    )

    weather_data = spark.createDataFrame(
        [
            (
                datetime(2024, 1, 1, 10, 0, tzinfo=NEW_YORK),
                2.5,
                0.0,
                3,
            ),
        ],
        [
            "time",
            "temperature_2m",
            "precipitation",
            "weather_code",
        ],
    )

    joiner = TaxiWeatherJoiner()

    # Act
    enriched_data = joiner.join(
        taxi_data=taxi_data,
        weather_data=weather_data,
    )
    result = enriched_data.first()

    # Assert
    assert result["temperature_2m"] == 2.5
    assert result["precipitation"] == 0.0
    assert result["weather_code"] == 3
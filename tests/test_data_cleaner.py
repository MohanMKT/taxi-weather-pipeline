from datetime import datetime
from zoneinfo import ZoneInfo

from pyspark.sql import SparkSession

from taxi_weather_pipeline.data_cleaner import TaxiDataCleaner

NEW_YORK = ZoneInfo("America/New_York")


def test_clean_removes_invalid_taxi_records(
    *,
    spark: SparkSession,
) -> None:
    """Keep the valid trip and remove reversed timestamps and zero-distance trips."""
    # Arrange
    # Columns: pickup, dropoff, passenger count, distance (miles), total amount (USD).
    taxi_data = spark.createDataFrame(
        [
            # Valid trip: positive duration, distance, and amount.
            (
                datetime(2024, 1, 1, 10, 0, tzinfo=NEW_YORK),
                datetime(2024, 1, 1, 10, 30, tzinfo=NEW_YORK),
                2,
                5.0,
                20.0,
            ),
            # Invalid trip: dropoff is before pickup.
            (
                datetime(2024, 1, 1, 11, 0, tzinfo=NEW_YORK),
                datetime(2024, 1, 1, 10, 30, tzinfo=NEW_YORK),
                1,
                3.0,
                15.0,
            ),
            # Invalid trip: distance is zero.
            (
                datetime(2024, 1, 1, 12, 0, tzinfo=NEW_YORK),
                datetime(2024, 1, 1, 12, 10, tzinfo=NEW_YORK),
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
    cleaned_data = cleaner.clean(taxi_data=taxi_data)

    # Assert
    assert cleaned_data.count() == 1

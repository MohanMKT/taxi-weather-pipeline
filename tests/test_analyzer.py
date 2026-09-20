from datetime import UTC, datetime
from zoneinfo import ZoneInfo

from pyspark.sql import SparkSession

from taxi_weather_pipeline.analyzer import TaxiWeatherAnalyzer

NEW_YORK = ZoneInfo("America/New_York")


def test_summarize_by_weather_categorizes_all_branches(
    *,
    spark: SparkSession,
) -> None:
    """Group missing, zero, and positive precipitation and aggregate wet trips."""
    # Arrange
    # Columns: pickup timestamp, distance (miles), total amount (USD), precipitation (mm).
    # Zero precipitation belongs in the dry category; null weather stays unknown.
    taxi_weather_data = spark.createDataFrame(
        [
            (
                datetime(2024, 1, 1, 8, 0, tzinfo=NEW_YORK),
                5.0,
                20.0,
                None,
            ),  # precipitation is null -> "Unknown"
            (
                datetime(2024, 1, 1, 9, 0, tzinfo=NEW_YORK),
                3.0,
                15.0,
                0.0,
            ),  # precipitation == 0.0 (boundary) -> "No precipitation"
            (
                datetime(2024, 1, 1, 10, 0, tzinfo=NEW_YORK),
                4.0,
                18.0,
                0.5,
            ),  # precipitation > 0 -> "Precipitation"
            (
                datetime(2024, 1, 1, 11, 0, tzinfo=NEW_YORK),
                6.0,
                22.0,
                2.0,
            ),  # A second wet trip verifies aggregation across multiple records.
        ],
        [
            "tpep_pickup_datetime",
            "trip_distance",
            "total_amount",
            "precipitation",
        ],
    )

    analyzer = TaxiWeatherAnalyzer()

    # Act
    summary = analyzer.summarize_by_weather(
        taxi_weather_data=taxi_weather_data,
    )
    rows_by_category = {row["weather_category"]: row for row in summary.collect()}

    # Assert
    assert set(rows_by_category.keys()) == {
        "Unknown",
        "No precipitation",
        "Precipitation",
    }
    assert rows_by_category["Unknown"]["trip_count"] == 1
    assert rows_by_category["No precipitation"]["trip_count"] == 1
    assert rows_by_category["Precipitation"]["trip_count"] == 2
    assert rows_by_category["Precipitation"]["average_total_amount"] == 20.0
    assert rows_by_category["Precipitation"]["average_trip_distance"] == 5.0


def test_summarize_by_hour_uses_new_york_time_and_combines_dates(
    spark: SparkSession,
) -> None:
    """Convert UTC pickups to New York hours and combine the same hour across dates."""
    # Arrange: the only input column is tpep_pickup_datetime.
    taxi_data = spark.createDataFrame(
        [
            (datetime(2024, 1, 1, 5, 0, tzinfo=UTC),),  # Jan 1, 00:00 in New York.
            (datetime(2024, 1, 2, 5, 30, tzinfo=UTC),),  # Jan 2, 00:30 in New York.
            (datetime(2024, 1, 2, 4, 59, tzinfo=UTC),),  # Jan 1, 23:59 in New York.
            (datetime(2024, 1, 2, 23, 0, tzinfo=UTC),),  # Jan 2, 18:00 in New York.
        ],
        ["tpep_pickup_datetime"],
    )

    # Act
    summary = TaxiWeatherAnalyzer().summarize_by_hour(taxi_weather_data=taxi_data)

    # Assert: output columns are local pickup hour and total trip count.
    assert [(row.pickup_hour_of_day, row.trip_count) for row in summary.collect()] == [
        (0, 2),
        (18, 1),
        (23, 1),
    ]

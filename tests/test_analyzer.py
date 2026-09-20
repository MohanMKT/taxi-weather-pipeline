from datetime import UTC, datetime
from zoneinfo import ZoneInfo

from pyspark.sql import SparkSession

from taxi_weather_pipeline.analyzer import TaxiWeatherAnalyzer

NEW_YORK = ZoneInfo("America/New_York")


def test_summarize_by_weather_categorizes_all_branches(
    *,
    spark: SparkSession,
) -> None:
    # Arrange
    # One row per branch of the when/otherwise chain, plus the precipitation == 0.0
    # boundary case: it must NOT be classified as "Precipitation" since the
    # implementation uses a strict '> 0' comparison.
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
            ),  # a second "Precipitation" row, to check aggregation across >1 row
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
    taxi_data = spark.createDataFrame(
        [
            (datetime(2024, 1, 1, 5, 0, tzinfo=UTC),),
            (datetime(2024, 1, 2, 5, 30, tzinfo=UTC),),
            (datetime(2024, 1, 2, 4, 59, tzinfo=UTC),),
            (datetime(2024, 1, 2, 23, 0, tzinfo=UTC),),
        ],
        ["tpep_pickup_datetime"],
    )
    summary = TaxiWeatherAnalyzer().summarize_by_hour(taxi_weather_data=taxi_data)
    assert [(row.pickup_hour_of_day, row.trip_count) for row in summary.collect()] == [
        (0, 2),
        (18, 1),
        (23, 1),
    ]

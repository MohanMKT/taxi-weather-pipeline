from datetime import datetime
from zoneinfo import ZoneInfo

import pytest
from pyspark.sql import SparkSession

from taxi_weather_pipeline.data_joiner import TaxiWeatherJoiner

NEW_YORK = ZoneInfo("America/New_York")


def test_join_matches_taxi_trip_to_hourly_weather(
    *,
    spark: SparkSession,
) -> None:
    """A pickup at 10:15 receives the observation for 10:00."""
    # Arrange
    # Taxi columns: pickup timestamp, distance (miles), total amount (USD).
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

    # Weather columns: observation time, temperature (°C), precipitation (mm), code.
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


def test_join_preserves_trips_without_matching_weather(
    *,
    spark: SparkSession,
) -> None:
    """A left join keeps both matched and unmatched trips without duplicating them."""
    # Arrange
    # Taxi columns: trip ID, pickup timestamp. Weather is available only at 10:00.
    taxi_data = spark.createDataFrame(
        [
            ("matched", datetime(2024, 1, 1, 10, 15, tzinfo=NEW_YORK)),
            ("unmatched", datetime(2024, 1, 1, 11, 15, tzinfo=NEW_YORK)),
        ],
        ["trip_id", "tpep_pickup_datetime"],
    )

    # Weather columns: observation time, temperature (°C), precipitation (mm), code.
    weather_data = spark.createDataFrame(
        [(datetime(2024, 1, 1, 10, 0, tzinfo=NEW_YORK), 2.5, 0.0, 3)],
        ["time", "temperature_2m", "precipitation", "weather_code"],
    )

    # Act
    rows = (
        TaxiWeatherJoiner()
        .join(
            taxi_data=taxi_data,
            weather_data=weather_data,
        )
        .collect()
    )
    rows_by_trip = {row["trip_id"]: row for row in rows}

    # Assert
    assert len(rows) == 2
    assert set(rows_by_trip) == {"matched", "unmatched"}
    assert rows_by_trip["matched"]["temperature_2m"] == 2.5
    for column in ("temperature_2m", "precipitation", "weather_code"):
        assert rows_by_trip["unmatched"][column] is None


@pytest.mark.parametrize(
    "second_temperature",
    [2.5, 8.0],
    ids=["identical_observations", "conflicting_observations"],
)
def test_join_rejects_duplicate_weather_timestamps(
    *,
    spark: SparkSession,
    second_temperature: float,
) -> None:
    """Reject repeated weather hours regardless of whether readings agree."""
    # Arrange: both identical and conflicting observations must be rejected.
    # The taxi input contains only the pickup timestamp needed by the join.
    taxi_data = spark.createDataFrame(
        [(datetime(2024, 1, 1, 10, 15, tzinfo=NEW_YORK),)],
        ["tpep_pickup_datetime"],
    )

    # Weather columns: observation time, temperature (°C).
    weather_data = spark.createDataFrame(
        [
            (datetime(2024, 1, 1, 10, 0, tzinfo=NEW_YORK), 2.5),
            (datetime(2024, 1, 1, 10, 0, tzinfo=NEW_YORK), second_temperature),
        ],
        ["time", "temperature_2m"],
    )

    # Act / Assert
    with pytest.raises(ValueError, match="Weather timestamps must be unique"):
        TaxiWeatherJoiner().join(
            taxi_data=taxi_data,
            weather_data=weather_data,
        )

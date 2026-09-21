from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from pyspark.sql import SparkSession

from taxi_weather_pipeline.data_cleaner import WeatherDataCleaner
from taxi_weather_pipeline.data_joiner import TaxiWeatherJoiner
from taxi_weather_pipeline.weather_reader import WeatherReader

NEW_YORK = ZoneInfo("America/New_York")


def test_join_matches_taxi_trip_to_hourly_weather(
    *,
    spark: SparkSession,
) -> None:
    """Adjacent pickup hours use current temperature and next-hour precipitation."""
    # Arrange
    # Taxi columns: pickup timestamp, distance (miles), total amount (USD).
    taxi_data = spark.createDataFrame(
        [
            (
                datetime(2024, 1, 1, 9, 0, tzinfo=NEW_YORK),
                4.2,
                25.0,
            ),
            (datetime(2024, 1, 1, 10, 59, 59, tzinfo=NEW_YORK), 3.0, 20.0),
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
            (datetime(2024, 1, 1, 9, 0, tzinfo=NEW_YORK), 2.5, 1.0, 3),
            (datetime(2024, 1, 1, 10, 0, tzinfo=NEW_YORK), 4.5, 2.0, 61),
            (datetime(2024, 1, 1, 11, 0, tzinfo=NEW_YORK), 6.5, 3.0, 63),
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
    rows = enriched_data.orderBy("pickup_hour").collect()

    # Assert
    assert len(rows) == 2
    assert [row.temperature_2m for row in rows] == [2.5, 4.5]
    assert [row.precipitation for row in rows] == [2.0, 3.0]
    assert [row.weather_code for row in rows] == [3, 61]
    assert all(row.precipitation_interval_start == row.pickup_hour for row in rows)


def test_join_preserves_trips_without_matching_weather(
    *,
    spark: SparkSession,
) -> None:
    """A left join keeps both matched and unmatched trips without duplicating them."""
    # Arrange
    # Taxi columns: trip ID, pickup timestamp. Weather is available only at 10:00.
    taxi_data = spark.createDataFrame(
        [
            ("precipitation_only", datetime(2024, 1, 1, 9, 15, tzinfo=NEW_YORK)),
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
    assert len(rows) == 3
    assert set(rows_by_trip) == {"precipitation_only", "matched", "unmatched"}
    assert rows_by_trip["precipitation_only"]["temperature_2m"] is None
    assert rows_by_trip["precipitation_only"]["weather_code"] is None
    assert rows_by_trip["precipitation_only"]["precipitation"] == 0.0
    assert rows_by_trip["matched"]["temperature_2m"] == 2.5
    assert rows_by_trip["matched"]["precipitation"] is None
    for column in ("temperature_2m", "precipitation", "weather_code"):
        assert rows_by_trip["unmatched"][column] is None


@pytest.mark.parametrize("include_midnight_observation", [True, False])
def test_final_january_pickup_uses_february_precipitation(
    spark: SparkSession,
    tmp_path: Path,
    include_midnight_observation: bool,
) -> None:
    """Use February midnight in New York, or retain null if it is absent."""
    path = tmp_path / "weather.csv"
    # With UTC-4 source labels, February 1 at 01:00 is midnight in New York.
    content = (
        "latitude,longitude,elevation,utc_offset_seconds,timezone,timezone_abbreviation\n"
        "40.7,-74.0,32.0,-14400,America/New_York,GMT-4\n\n"
        + WeatherReader.HEADER + "\n"
        "2024-02-01T00:00,2.5,3,1.0\n"
    )
    if include_midnight_observation:
        content += "2024-02-01T01:00,4.5,61,7.0\n"
    path.write_text(content)
    weather_data = WeatherDataCleaner().clean(
        weather_data=WeatherReader(spark=spark).read(path=str(path))
    ).select("time", "temperature_2m", "precipitation", "weather_code")
    taxi_data = spark.createDataFrame(
        [(datetime(2024, 1, 31, 23, 59, 59, tzinfo=NEW_YORK),)],
        ["tpep_pickup_datetime"],
    )

    rows = TaxiWeatherJoiner().join(
        taxi_data=taxi_data, weather_data=weather_data
    ).collect()

    assert len(rows) == 1
    assert rows[0].temperature_2m == 2.5
    assert rows[0].weather_code == 3
    if include_midnight_observation:
        assert rows[0].precipitation == 7.0
        assert rows[0].precipitation_interval_start == rows[0].pickup_hour
    else:
        assert rows[0].precipitation is None
        assert rows[0].precipitation_interval_start is None


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

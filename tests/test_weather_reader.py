from datetime import UTC, datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from taxi_weather_pipeline.data_cleaner import WeatherDataCleaner
from taxi_weather_pipeline.weather_reader import WeatherReader

HEADER = "time,temperature_2m (°C),weather_code (wmo code),precipitation (mm)"


@pytest.mark.parametrize(
    ("session_timezone", "expected_local_time"),
    [
        ("America/New_York", "2023-12-31 23:00:00"),
        ("UTC", "2024-01-01 04:00:00"),
    ],
)
def test_reader_and_cleaner_honor_source_offset(
    spark: SparkSession,
    tmp_path: Path,
    session_timezone: str,
    expected_local_time: str,
) -> None:
    """UTC-4 metadata determines the instant even when New York uses UTC-5."""
    path = tmp_path / "weather.csv"
    path.write_text(
        "latitude,longitude,elevation,utc_offset_seconds,timezone,timezone_abbreviation\n"
        "40.7,-74.0,32.0,-14400,America/New_York,GMT-4\n\n" + HEADER + "\n"
        "2024-01-01T00:00,1.9,3,0.5\n"
    )
    previous = spark.conf.get("spark.sql.session.timeZone")
    assert previous is not None
    spark.conf.set("spark.sql.session.timeZone", session_timezone)
    try:
        data = WeatherReader(spark=spark).read(path=str(path))
        row = (
            WeatherDataCleaner()
            .validate(weather_data=data)
            .select(
                "raw_time",
                "utc_offset_seconds",
                "precipitation",
                "rejection_reasons",
                F.col("time").cast("long").alias("epoch_seconds"),
                F.date_format("time", "yyyy-MM-dd HH:mm:ss").alias("local_time"),
            )
            .first()
        )

        assert row is not None
        assert row.utc_offset_seconds == -14400
        assert row.raw_time == "2024-01-01T00:00"
        assert row.epoch_seconds == int(
            datetime(2024, 1, 1, 4, tzinfo=UTC).timestamp()
        )
        assert row.local_time == expected_local_time
        assert row.precipitation == 0.5
        assert row.rejection_reasons == []
    finally:
        spark.conf.set("spark.sql.session.timeZone", previous)


def test_reader_skips_metadata_and_preserves_bad_rows_for_quarantine(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    """Skip the metadata while retaining malformed observations for validation."""
    # Arrange: hourly columns are time, temperature (°C), weather code, precipitation (mm).
    # The first two observations are valid; the rest have a bad date, number, or field count.
    path = tmp_path / "weather.csv"
    path.write_text(
        "latitude,longitude,elevation,utc_offset_seconds,timezone,timezone_abbreviation\n"
        "40.7,-74.0,32.0,-18000,America/New_York,EST\n\n" + HEADER + "\n"
        "2024-01-01T10:00,-2.5,3,0.0\n"
        "2024-01-01T11:00:00,1.0,61,0.5\n"
        "not-a-date,2.0,3,0.0\n"
        "2024-01-01T12:00,broken,3,0.0\n"
        "2024-01-01T13:00,2.0,3,0.0,extra\n"
    )

    # Act
    data = WeatherReader(spark=spark).read(path=str(path))
    rows = WeatherDataCleaner().validate(weather_data=data).collect()
    invalid = {
        row.raw_weather_row: row.rejection_reasons
        for row in rows
        if row.rejection_reasons
    }

    # Assert: bad observations remain available with their raw CSV text and reasons.
    assert data.count() == 5
    assert len([row for row in rows if not row.rejection_reasons]) == 2
    assert "invalid_weather_timestamp" in invalid["not-a-date,2.0,3,0.0"]
    assert "malformed_weather_row" in invalid["2024-01-01T12:00,broken,3,0.0"]
    assert "malformed_weather_row" in invalid["2024-01-01T13:00,2.0,3,0.0,extra"]


@pytest.mark.parametrize(
    "header",
    [
        "time,temperature_2m (°C),precipitation (mm),weather_code (wmo code)",
        "time,temperature_2m (°F),weather_code (wmo code),precipitation (mm)",
        "time,temperature_2m (°C),precipitation (mm)",
        "unrecognized header",
        HEADER + "\n" + HEADER,
    ],
    ids=[
        "reordered_columns",
        "wrong_units",
        "missing_column",
        "missing_header",
        "duplicate_header",
    ],
)
def test_reader_rejects_unexpected_or_duplicate_headers(
    spark: SparkSession,
    tmp_path: Path,
    header: str,
) -> None:
    """Fail before parsing when the weather CSV violates the declared column contract."""
    # Arrange: the observation columns follow the expected header order.
    path = tmp_path / "weather.csv"
    path.write_text(header + "\n2024-01-01T10:00,2.5,3,0.0\n")

    # Act / Assert
    with pytest.raises(ValueError, match="Expected exactly one weather CSV header"):
        WeatherReader(spark=spark).read(path=str(path))

from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from taxi_weather_pipeline.data_cleaner import WeatherDataCleaner
from taxi_weather_pipeline.weather_reader import WeatherReader

HEADER = "time,temperature_2m (°C),weather_code (wmo code),precipitation (mm)"


def test_reader_skips_metadata_and_preserves_bad_rows_for_quarantine(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
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
    data = WeatherReader(spark=spark).read(path=str(path))
    assert data.count() == 5
    rows = WeatherDataCleaner().validate(weather_data=data).collect()
    assert len([row for row in rows if not row.rejection_reasons]) == 2
    invalid = {
        row.raw_weather_row: row.rejection_reasons
        for row in rows
        if row.rejection_reasons
    }
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
)
def test_reader_rejects_unexpected_or_duplicate_headers(
    spark: SparkSession,
    tmp_path: Path,
    header: str,
) -> None:
    path = tmp_path / "weather.csv"
    path.write_text(header + "\n2024-01-01T10:00,2.5,3,0.0\n")
    with pytest.raises(ValueError, match="Expected exactly one weather CSV header"):
        WeatherReader(spark=spark).read(path=str(path))

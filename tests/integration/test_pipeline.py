from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampNTZType

from taxi_weather_pipeline.analyzer import TaxiWeatherAnalyzer
from taxi_weather_pipeline.data_cleaner import TaxiDataCleaner, WeatherDataCleaner
from taxi_weather_pipeline.data_joiner import TaxiWeatherJoiner
from taxi_weather_pipeline.main import run_pipeline
from taxi_weather_pipeline.taxi_reader import TaxiReader
from taxi_weather_pipeline.weather_reader import WeatherReader


@pytest.fixture
def local_inputs(spark: SparkSession, tmp_path: Path) -> tuple[Path, Path]:
    taxi_path = tmp_path / "data/raw/taxi/yellow_tripdata_2024-01.parquet"
    taxi = (
        spark.createDataFrame(
            [
                ("wet", "2024-01-01 09:15:00", "2024-01-01 09:45:00", 1.0, 4.0, 20.0),
                ("dry", "2024-01-31 23:30:00", "2024-02-01 00:00:00", 2.0, 6.0, 30.0),
                ("bad", "2024-01-01 09:15:00", "2024-01-01 09:45:00", 1.0, 0.0, 10.0),
            ],
            "id string, tpep_pickup_datetime string, tpep_dropoff_datetime string, "
            "passenger_count double, trip_distance double, total_amount double",
        )
        .withColumn(
            "tpep_pickup_datetime", F.col("tpep_pickup_datetime").cast("timestamp_ntz")
        )
        .withColumn(
            "tpep_dropoff_datetime",
            F.col("tpep_dropoff_datetime").cast("timestamp_ntz"),
        )
    )
    taxi.write.parquet(str(taxi_path))
    weather_path = tmp_path / "data/raw/weather/weather_january_2024.csv"
    weather_path.parent.mkdir(parents=True)
    weather_path.write_text(
        "latitude,longitude,elevation,utc_offset_seconds,timezone,timezone_abbreviation\n"
        "40.7,-74.0,32.0,-14400,America/New_York,GMT-4\n\n"
        + WeatherReader.HEADER
        + "\n"
        "2024-01-01T10:00,2.5,3,0.7\n"
        "2024-01-01T11:00,8.0,61,1.5\n"
        "2024-02-01T00:00,-2.3,3,0.8\n"
        "2024-02-01T01:00,-2.4,3,0.0\n"
        "invalid,2.5,3,0.0\n"
    )
    return taxi_path, weather_path


def test_local_files_through_analysis(
    spark: SparkSession, local_inputs: tuple[Path, Path]
) -> None:
    """Preserve Parquet wall-clock pickups through offset-aware weather enrichment."""
    # Arrange: local_inputs supplies temporary Parquet and Open-Meteo CSV files.
    taxi_path, weather_path = local_inputs
    taxi_reader = TaxiReader(spark=spark)
    taxi_cleaner = TaxiDataCleaner()
    weather_reader = WeatherReader(spark=spark)
    weather_cleaner = WeatherDataCleaner()
    joiner = TaxiWeatherJoiner()
    analyzer = TaxiWeatherAnalyzer()

    # Act
    raw_taxi = taxi_reader.read(path=str(taxi_path))
    validated_taxi = taxi_cleaner.validate(taxi_data=raw_taxi)
    accepted_taxi = validated_taxi.filter(F.size("rejection_reasons") == 0)
    rejected_taxi = validated_taxi.filter(F.size("rejection_reasons") > 0)

    raw_weather = weather_reader.read(path=str(weather_path))
    validated_weather = weather_cleaner.validate(weather_data=raw_weather)
    accepted_weather = validated_weather.filter(F.size("rejection_reasons") == 0)
    rejected_weather = validated_weather.filter(F.size("rejection_reasons") > 0)

    enriched = joiner.join(
        taxi_data=accepted_taxi,
        weather_data=accepted_weather.select(
            "time", "temperature_2m", "precipitation", "weather_code"
        ),
    )
    summary = analyzer.summarize_by_weather(taxi_weather_data=enriched)

    raw_taxi_count = raw_taxi.count()
    accepted_taxi_count = accepted_taxi.count()
    rejected_taxi_count = rejected_taxi.count()
    raw_weather_count = raw_weather.count()
    accepted_weather_count = accepted_weather.count()
    rejected_weather_count = rejected_weather.count()
    rows = enriched.collect()
    categories = {row.weather_category: row for row in summary.collect()}

    # Assert
    assert isinstance(
        raw_taxi.schema["tpep_pickup_datetime"].dataType, TimestampNTZType
    )
    assert raw_taxi_count == 3
    assert accepted_taxi_count == 2
    assert rejected_taxi_count == 1
    assert accepted_taxi_count + rejected_taxi_count == raw_taxi_count
    assert raw_weather_count == 5
    assert accepted_weather_count == 4
    assert rejected_weather_count == 1
    assert accepted_weather_count + rejected_weather_count == raw_weather_count
    assert len(rows) == 2
    assert {row.id: (row.temperature_2m, row.precipitation) for row in rows} == {
        "wet": (2.5, 1.5),
        "dry": (-2.3, 0.0),
    }
    assert sum(row.trip_count for row in categories.values()) == len(rows)
    assert categories["Precipitation"].asDict() == {
        "weather_category": "Precipitation",
        "trip_count": 1,
        "average_total_amount": 20.0,
        "average_trip_distance": 4.0,
    }


def test_pipeline_releases_caches_on_calendar_failure(
    spark: SparkSession,
    local_inputs: tuple[Path, Path],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A real incomplete-input failure releases caches and leaves the caller's session usable."""
    # Arrange
    monkeypatch.chdir(tmp_path)
    taxi_path, weather_path = local_inputs
    taxi = TaxiReader(spark=spark).read(path=str(taxi_path))
    weather = WeatherReader(spark=spark).read(path=str(weather_path))
    # Spark resolves relative paths in the JVM, independently of Python's cwd.
    monkeypatch.setattr(TaxiReader, "read", lambda self, *, path: taxi)
    monkeypatch.setattr(WeatherReader, "read", lambda self, *, path: weather)
    cached: list[DataFrame] = []
    original_cache = DataFrame.cache

    def record_cache(data: DataFrame) -> DataFrame:
        result = original_cache(data)
        cached.append(result)
        return result

    monkeypatch.setattr(DataFrame, "cache", record_cache)

    # Act / Assert
    with pytest.raises(ValueError, match="Missing weather coverage"):
        run_pipeline(spark=spark)
    session_check_count = spark.range(1).count()

    # Assert
    assert len(cached) == 2
    assert all(not data.is_cached for data in cached)
    assert session_check_count == 1

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from taxi_weather_pipeline.data_cleaner import WeatherDataCleaner


@pytest.fixture
def weather_calendar(spark: SparkSession) -> DataFrame:
    """Match the extended source's normalized range, including February midnight."""
    return spark.range(768).selectExpr(
        "timestamp '2023-12-31 23:00:00' + id * INTERVAL 1 HOUR AS time"
    )


def test_complete_weather_calendar(weather_calendar: DataFrame) -> None:
    # Arrange
    cleaner = WeatherDataCleaner()

    # Act / Assert: complete coverage must not raise an exception.
    cleaner.validate_calendar(weather_data=weather_calendar)


@pytest.mark.parametrize(
    ("missing_time", "missing_observations", "missing_precipitation"),
    [
        ("2024-01-01 00:00:00", ["2024-01-01 00:00:00"], []),
        ("2024-01-15 12:00:00", ["2024-01-15 12:00:00"], ["2024-01-15 11:00:00"]),
        ("2024-02-01 00:00:00", [], ["2024-01-31 23:00:00"]),
    ],
)
def test_missing_required_hours_are_reported(
    weather_calendar: DataFrame,
    missing_time: str,
    missing_observations: list[str],
    missing_precipitation: list[str],
) -> None:
    # Arrange
    incomplete = weather_calendar.filter(
        F.col("time") != F.lit(missing_time).cast("timestamp")
    )
    cleaner = WeatherDataCleaner()

    # Act / Assert
    with pytest.raises(ValueError, match="Missing weather coverage") as error:
        cleaner.validate_calendar(weather_data=incomplete)

    # Assert
    assert str(error.value) == (
        "Missing weather coverage for January pickup hours: "
        f"instantaneous={missing_observations}; precipitation={missing_precipitation}"
    )


def test_empty_weather_calendar_fails_clearly(weather_calendar: DataFrame) -> None:
    # Arrange
    empty_weather = weather_calendar.limit(0)
    cleaner = WeatherDataCleaner()

    # Act / Assert
    with pytest.raises(ValueError, match="Cleaned weather input is empty"):
        cleaner.validate_calendar(weather_data=empty_weather)

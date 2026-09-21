from typing import Any

import pytest

from taxi_weather_pipeline.data_cleaner import TaxiDataCleaner


@pytest.mark.parametrize("field", ["start_date", "end_date"])
@pytest.mark.parametrize("value", ["invalid", "2024-02-30", "20240101", "", None])
def test_invalid_dates_fail_at_construction(field: str, value: Any) -> None:
    """Reject invalid bounds before a Spark session or validation job is needed."""
    # Arrange
    options: dict[str, Any] = {field: value}

    # Act / Assert
    with pytest.raises(ValueError, match=f"^{field} must be a valid date"):
        TaxiDataCleaner(**options)


@pytest.mark.parametrize("end_date", ["2024-01-01", "2023-12-31"])
def test_analysis_period_must_be_increasing(end_date: str) -> None:
    # Arrange
    start_date = "2024-01-01"

    # Act / Assert
    with pytest.raises(ValueError, match="start_date must be before end_date"):
        TaxiDataCleaner(start_date=start_date, end_date=end_date)


@pytest.mark.parametrize(
    "field", ["max_distance_miles", "max_duration_hours", "max_speed_mph"]
)
@pytest.mark.parametrize(
    "value", [0, -1, float("nan"), float("inf"), -float("inf"), None, "bad"]
)
def test_invalid_limits_fail_at_construction(field: str, value: Any) -> None:
    # Arrange
    options: dict[str, Any] = {field: value}

    # Act / Assert
    with pytest.raises(
        ValueError, match=f"^{field} must be finite and greater than zero"
    ):
        TaxiDataCleaner(**options)


def test_valid_leap_date_and_positive_fractional_limits() -> None:
    # Arrange
    options: dict[str, Any] = {
        "start_date": "2024-02-29",
        "end_date": "2024-03-01",
        "max_distance_miles": 0.5,
        "max_duration_hours": 0.25,
        "max_speed_mph": 10,
    }

    # Act
    cleaner = TaxiDataCleaner(**options)

    # Assert
    assert (cleaner.start_date, cleaner.end_date) == ("2024-02-29", "2024-03-01")
    assert (
        cleaner.max_distance_miles,
        cleaner.max_duration_hours,
        cleaner.max_speed_mph,
    ) == (
        0.5,
        0.25,
        10,
    )

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from taxi_weather_pipeline.data_cleaner import TaxiDataCleaner, WeatherDataCleaner

# All taxi tuples follow this order; id identifies the scenario in assertions.
TAXI_SCHEMA = (
    "id string, "
    "tpep_pickup_datetime string, "
    "tpep_dropoff_datetime string, "
    "passenger_count double, "
    "trip_distance double, "
    "total_amount double"
)


def test_taxi_validation_preserves_rows_and_identifies_reasons(
    spark: SparkSession,
) -> None:
    """Retain all source rows and attach the expected reason to each invalid case."""
    # Arrange: most cases vary one field of an otherwise valid one-hour trip.
    pickup = "2024-01-01 10:00:00"
    dropoff = "2024-01-01 11:00:00"

    # Columns: case ID, pickup, dropoff, passengers, distance (miles), amount (USD).
    records = [
        ("valid", pickup, dropoff, 1.0, 10.0, 20.0),
        ("optional_passenger", pickup, dropoff, None, 10.0, 20.0),
        # Date boundaries and invalid timestamps.
        ("before_month", "2023-12-31 23:59:59", "2024-01-01 00:10:00", 1.0, 1.0, 20.0),
        ("after_month", "2024-02-01 00:00:00", "2024-02-01 00:10:00", 1.0, 1.0, 20.0),
        ("bad_pickup", "2024-01-32 10:00:00", dropoff, 1.0, 10.0, 20.0),
        ("null_pickup", None, dropoff, 1.0, 10.0, 20.0),
        ("bad_dropoff", pickup, "invalid", 1.0, 10.0, 20.0),
        # Implausible duration, distance, or average speed.
        ("zero_duration", pickup, pickup, 1.0, 10.0, 20.0),
        ("negative_duration", dropoff, pickup, 1.0, 10.0, 20.0),
        ("long_duration", pickup, "2024-01-02 11:00:00", 1.0, 10.0, 20.0),
        ("long_distance", pickup, "2024-01-01 13:00:00", 1.0, 201.0, 20.0),
        ("fast", pickup, dropoff, 1.0, 101.0, 20.0),
        # Non-finite values and invalid numeric signs.
        ("nan_distance", pickup, dropoff, 1.0, float("nan"), 20.0),
        ("infinite_distance", pickup, dropoff, 1.0, float("inf"), 20.0),
        ("zero_distance", pickup, dropoff, 1.0, 0.0, 20.0),
        ("nan_amount", pickup, dropoff, 1.0, 10.0, float("nan")),
        ("infinite_amount", pickup, dropoff, 1.0, 10.0, float("inf")),
        ("negative_amount", pickup, dropoff, 1.0, 10.0, -1.0),
        ("negative_passenger", pickup, dropoff, -1.0, 10.0, 20.0),
        ("nan_passenger", pickup, dropoff, float("nan"), 10.0, 20.0),
    ]
    taxi_data = spark.createDataFrame(records, TAXI_SCHEMA)
    cleaner = TaxiDataCleaner()

    # Act
    validated = cleaner.validate(taxi_data=taxi_data)
    rows = {row.id: row for row in validated.collect()}

    # Assert: validation labels rejected rows instead of losing them.
    assert len(rows) == len(records)
    assert {key for key, row in rows.items() if not row.rejection_reasons} == {
        "valid",
        "optional_passenger",
    }

    expected = {
        "before_month": "pickup_outside_analysis_period",
        "after_month": "pickup_outside_analysis_period",
        "bad_pickup": "invalid_pickup_timestamp",
        "null_pickup": "invalid_pickup_timestamp",
        "bad_dropoff": "invalid_dropoff_timestamp",
        "zero_duration": "non_positive_duration",
        "negative_duration": "non_positive_duration",
        "long_duration": "excessive_duration",
        "long_distance": "excessive_distance",
        "fast": "excessive_speed",
        "nan_distance": "invalid_trip_distance",
        "infinite_distance": "invalid_trip_distance",
        "zero_distance": "invalid_trip_distance",
        "nan_amount": "invalid_total_amount",
        "infinite_amount": "invalid_total_amount",
        "negative_amount": "invalid_total_amount",
        "negative_passenger": "invalid_passenger_count",
        "nan_passenger": "invalid_passenger_count",
    }

    for row_id, reason in expected.items():
        assert reason in rows[row_id].rejection_reasons

    # The original invalid value must remain available for quarantine review.
    assert rows["bad_pickup"].raw_pickup_datetime == "2024-01-32 10:00:00"
    assert rows["bad_pickup"].tpep_pickup_datetime is None


def test_taxi_boundaries_and_month_crossing_dropoff(spark: SparkSession) -> None:
    """Include January boundary pickups and trips exactly at the configured limits."""
    # Arrange
    # Columns: case ID, pickup, dropoff, passengers, distance (miles), amount (USD).
    records = [
        ("month_start", "2024-01-01T00:00", "2024-01-01T00:30", None, 1.0, 0.0),
        # A February dropoff is allowed when pickup is still in January.
        ("month_end", "2024-01-31T23:59:59", "2024-02-01T00:01:00", 1.0, 0.5, 10.0),
        # 200 miles in two hours reaches both the distance and speed limits.
        (
            "distance_speed_limit",
            "2024-01-02 00:00:00",
            "2024-01-02 02:00:00",
            1.0,
            200.0,
            100.0,
        ),
        # Exactly 24 hours is valid; only longer durations are rejected.
        (
            "duration_limit",
            "2024-01-02 00:00:00",
            "2024-01-03 00:00:00",
            1.0,
            100.0,
            100.0,
        ),
    ]

    taxi_data = spark.createDataFrame(records, TAXI_SCHEMA)
    cleaner = TaxiDataCleaner()

    # Act
    cleaned = cleaner.clean(taxi_data=taxi_data)
    cleaned_ids = {row.id for row in cleaned.collect()}
    hours = {
        row.id: row.hour
        for row in cleaned.select(
            "id", F.hour("tpep_pickup_datetime").alias("hour")
        ).collect()
    }

    # Assert
    assert cleaned_ids == {row[0] for row in records}
    assert hours["month_start"] == 0
    assert hours["month_end"] == 23


def test_taxi_analysis_rules_are_configurable(spark: SparkSession) -> None:
    """Apply custom dates and limits instead of the January case-study defaults."""
    # Arrange: a 300-mile, 25-hour February trip fails the defaults.
    # Columns: case ID, pickup, dropoff, passengers, distance (miles), amount (USD).
    data = spark.createDataFrame(
        [
            (
                "february",
                "2024-02-01 00:00:00",
                "2024-02-02 01:00:00",
                1.0,
                300.0,
                100.0,
            ),
        ],
        TAXI_SCHEMA,
    )

    cleaner = TaxiDataCleaner(
        start_date="2024-02-01",
        end_date="2024-03-01",
        max_distance_miles=500,
        max_duration_hours=48,
        max_speed_mph=150,
    )
    default_cleaner = TaxiDataCleaner()

    # Act
    custom_count = cleaner.clean(taxi_data=data).count()
    default_count = default_cleaner.clean(taxi_data=data).count()

    # Assert: the same record passes only with the expanded settings.
    assert custom_count == 1
    assert default_count == 0


@pytest.mark.parametrize(
    "ansi",
    ["true", "false"],
    ids=["ansi_enabled", "ansi_disabled"],
)
def test_weather_timestamps_are_consistent_and_invalid_values_are_rejected(
    spark: SparkSession,
    ansi: str,
) -> None:
    """Normalize supported formats and reject bad dates under either ANSI setting."""
    # Restore this setting because the Spark session is shared by all tests.
    previous = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", ansi)

    try:
        # Arrange: the first three values describe the same valid local hour.
        values = [
            "2024-01-01T10:00",
            "2024-01-01T10:00:00",
            "2024-01-01 10:00:00.000000",
            "2024-02-30T10:00",
            "not-a-date",
            None,
            "2024-01-01T10:00:00junk",
        ]

        # Columns: local timestamp, temperature (°C), precipitation (mm).
        data = spark.createDataFrame(
            [(value, -2.5, 0.0) for value in values],
            "time string, temperature_2m double, precipitation double",
        )

        # Act
        rows = (
            WeatherDataCleaner()
            .validate(weather_data=data)
            .select(
                "raw_time",
                "rejection_reasons",
                F.date_format("time", "yyyy-MM-dd HH:mm:ss").alias("time"),
            )
            .collect()
        )

        # Assert: valid formats normalize identically; remaining inputs are rejected.
        assert [row.time for row in rows[:3]] == ["2024-01-01 10:00:00"] * 3
        assert all(row.rejection_reasons == [] for row in rows[:3])
        assert all(
            "invalid_weather_timestamp" in row.rejection_reasons for row in rows[3:]
        )
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous)


def test_weather_rejects_nonfinite_values_negative_precipitation_and_partial_hours(
    spark: SparkSession,
) -> None:
    """Keep valid cold weather while rejecting bad numbers and non-hourly times."""
    # Arrange
    # Columns: case ID, local timestamp, temperature (°C), precipitation (mm).
    data = spark.createDataFrame(
        [
            ("valid_cold", "2024-01-01T10:00", -10.0, 0.0),
            ("nan_temp", "2024-01-01T11:00", float("nan"), 0.0),
            ("infinite_temp", "2024-01-01T12:00", float("inf"), 0.0),
            ("nan_precip", "2024-01-01T13:00", 2.0, float("nan")),
            ("infinite_precip", "2024-01-01T14:00", 2.0, float("inf")),
            ("negative_precip", "2024-01-01T15:00", 2.0, -1.0),
            ("partial_hour", "2024-01-01T15:30", 2.0, 0.0),
            ("null_temp", "2024-01-01T16:00", None, 0.0),
            ("null_precip", "2024-01-01T17:00", 2.0, None),
        ],
        "id string, time string, temperature_2m double, precipitation double",
    )
    cleaner = WeatherDataCleaner()

    # Act
    cleaned_ids = [row.id for row in cleaner.clean(weather_data=data).collect()]
    rows = {row.id: row for row in cleaner.validate(weather_data=data).collect()}

    # Assert: negative temperatures are valid, unlike negative precipitation.
    assert cleaned_ids == ["valid_cold"]
    assert rows["negative_precip"].rejection_reasons == ["invalid_precipitation"]
    assert rows["nan_temp"].rejection_reasons == ["invalid_temperature"]
    assert rows["partial_hour"].rejection_reasons == ["weather_not_on_hour"]

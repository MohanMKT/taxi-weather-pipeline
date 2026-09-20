import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from taxi_weather_pipeline.data_cleaner import TaxiDataCleaner, WeatherDataCleaner

TAXI_SCHEMA = (
    "id string, tpep_pickup_datetime string, tpep_dropoff_datetime string, "
    "passenger_count double, trip_distance double, total_amount double"
)


def test_taxi_validation_preserves_rows_and_identifies_reasons(
    spark: SparkSession,
) -> None:
    base = ("2024-01-01 10:00:00", "2024-01-01 11:00:00", 1.0, 10.0, 20.0)
    records = [
        ("valid", *base),
        ("optional_passenger", *base[:2], None, 10.0, 20.0),
        ("before_month", "2023-12-31 23:59:59", "2024-01-01 00:10:00", 1.0, 1.0, 20.0),
        ("after_month", "2024-02-01 00:00:00", "2024-02-01 00:10:00", 1.0, 1.0, 20.0),
        ("bad_pickup", "2024-01-32 10:00:00", base[1], 1.0, 10.0, 20.0),
        ("null_pickup", None, base[1], 1.0, 10.0, 20.0),
        ("bad_dropoff", base[0], "invalid", 1.0, 10.0, 20.0),
        ("zero_duration", base[0], base[0], 1.0, 10.0, 20.0),
        ("negative_duration", base[1], base[0], 1.0, 10.0, 20.0),
        ("long_duration", base[0], "2024-01-02 11:00:00", 1.0, 10.0, 20.0),
        ("long_distance", base[0], "2024-01-01 13:00:00", 1.0, 201.0, 20.0),
        ("fast", *base[:2], 1.0, 101.0, 20.0),
        ("nan_distance", *base[:2], 1.0, float("nan"), 20.0),
        ("infinite_distance", *base[:2], 1.0, float("inf"), 20.0),
        ("zero_distance", *base[:2], 1.0, 0.0, 20.0),
        ("nan_amount", *base[:2], 1.0, 10.0, float("nan")),
        ("infinite_amount", *base[:2], 1.0, 10.0, float("inf")),
        ("negative_amount", *base[:2], 1.0, 10.0, -1.0),
        ("negative_passenger", *base[:2], -1.0, 10.0, 20.0),
        ("nan_passenger", *base[:2], float("nan"), 10.0, 20.0),
    ]
    validated = TaxiDataCleaner().validate(
        taxi_data=spark.createDataFrame(records, TAXI_SCHEMA)
    )
    rows = {row.id: row for row in validated.collect()}
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
    assert rows["bad_pickup"].raw_pickup_datetime == "2024-01-32 10:00:00"
    assert rows["bad_pickup"].tpep_pickup_datetime is None


def test_taxi_boundaries_and_month_crossing_dropoff(spark: SparkSession) -> None:
    records = [
        ("month_start", "2024-01-01T00:00", "2024-01-01T00:30", None, 1.0, 0.0),
        ("month_end", "2024-01-31T23:59:59", "2024-02-01T00:01:00", 1.0, 0.5, 10.0),
        (
            "distance_speed_limit",
            "2024-01-02 00:00:00",
            "2024-01-02 02:00:00",
            1.0,
            200.0,
            100.0,
        ),
        (
            "duration_limit",
            "2024-01-02 00:00:00",
            "2024-01-03 00:00:00",
            1.0,
            100.0,
            100.0,
        ),
    ]
    cleaned = TaxiDataCleaner().clean(
        taxi_data=spark.createDataFrame(records, TAXI_SCHEMA)
    )
    assert {row.id for row in cleaned.collect()} == {row[0] for row in records}
    hours = {
        row.id: row.hour
        for row in cleaned.select(
            "id", F.hour("tpep_pickup_datetime").alias("hour")
        ).collect()
    }
    assert hours["month_start"] == 0
    assert hours["month_end"] == 23


def test_taxi_analysis_rules_are_configurable(spark: SparkSession) -> None:
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
    assert cleaner.clean(taxi_data=data).count() == 1
    assert TaxiDataCleaner().clean(taxi_data=data).count() == 0


@pytest.mark.parametrize("ansi", ["true", "false"])
def test_weather_timestamps_are_consistent_and_invalid_values_are_rejected(
    spark: SparkSession,
    ansi: str,
) -> None:
    previous = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", ansi)
    try:
        values = [
            "2024-01-01T10:00",
            "2024-01-01T10:00:00",
            "2024-01-01 10:00:00.000000",
            "2024-02-30T10:00",
            "not-a-date",
            None,
            "2024-01-01T10:00:00junk",
        ]
        data = spark.createDataFrame(
            [(value, -2.5, 0.0) for value in values],
            "time string, temperature_2m double, precipitation double",
        )
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
    assert [row.id for row in cleaner.clean(weather_data=data).collect()] == [
        "valid_cold"
    ]
    rows = {row.id: row for row in cleaner.validate(weather_data=data).collect()}
    assert rows["negative_precip"].rejection_reasons == ["invalid_precipitation"]
    assert rows["nan_temp"].rejection_reasons == ["invalid_temperature"]
    assert rows["partial_hour"].rejection_reasons == ["weather_not_on_hour"]

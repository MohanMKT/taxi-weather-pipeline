import math
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def _local_timestamp(column: str, *, utc_offset_seconds: Column | None = None) -> Column:
    """Parse local labels using a supplied offset or the session timezone."""
    value = F.col(column).cast("string")
    pattern = r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2}(\.\d{1,6})?)?$"

    # Check the format before parsing so offsets and trailing text are rejected.
    timestamp = F.try_to_timestamp(value)
    if utc_offset_seconds is not None:
        # Parse in UTC first so session DST rules cannot alter the source label.
        timestamp = F.timestamp_seconds(
            F.try_to_timestamp(
                F.concat(F.regexp_replace(value, "T", " "), F.lit("Z")),
                F.lit("yyyy-MM-dd HH:mm[:ss[.SSSSSS]]X"),
            ).cast("double")
            - utc_offset_seconds
        )
    return F.when(value.rlike(pattern), timestamp)


def _finite(column: str) -> Column:
    """Exclude null, NaN, and both infinities from numeric validation."""
    value = F.col(column)

    return value.isNotNull() & ~F.isnan(value) & (F.abs(value) < float("inf"))


def _reasons(rules: list[tuple[Column, str]]) -> Column:
    """Collect every failed rule; a row can have more than one rejection reason."""
    return F.filter(
        F.array(*[F.when(condition, F.lit(reason)) for condition, reason in rules]),
        lambda reason: reason.isNotNull(),
    )


class TaxiDataCleaner:
    """Apply the analysis period and configurable trip plausibility limits."""

    def __init__(
        self,
        *,
        start_date: str = "2024-01-01",
        end_date: str = "2024-02-01",
        max_distance_miles: float = 200.0,
        max_duration_hours: float = 24.0,
        max_speed_mph: float = 100.0,
    ) -> None:
        for name, value in (("start_date", start_date), ("end_date", end_date)):
            try:
                parsed = date.fromisoformat(value)
                if parsed.isoformat() != value:
                    raise ValueError
            except (TypeError, ValueError) as error:
                raise ValueError(
                    f"{name} must be a valid date in YYYY-MM-DD format."
                ) from error
        if start_date >= end_date:
            raise ValueError("start_date must be before end_date.")

        for name, limit in (
            ("max_distance_miles", max_distance_miles),
            ("max_duration_hours", max_duration_hours),
            ("max_speed_mph", max_speed_mph),
        ):
            try:
                valid = math.isfinite(limit) and limit > 0
            except (TypeError, ValueError, OverflowError):
                valid = False
            if not valid:
                raise ValueError(f"{name} must be finite and greater than zero.")

        self.start_date = start_date
        self.end_date = end_date

        self.max_distance_miles = max_distance_miles
        self.max_duration_hours = max_duration_hours
        self.max_speed_mph = max_speed_mph

    def validate(self, *, taxi_data: DataFrame) -> DataFrame:
        """Normalize timestamps and retain every source row with rejection reasons."""
        # Keep the source timestamps so rejected records can be investigated.
        normalized = (
            taxi_data.withColumn("raw_pickup_datetime", F.col("tpep_pickup_datetime"))
            .withColumn("raw_dropoff_datetime", F.col("tpep_dropoff_datetime"))
            .withColumn(
                "tpep_pickup_datetime", _local_timestamp("tpep_pickup_datetime")
            )
            .withColumn(
                "tpep_dropoff_datetime", _local_timestamp("tpep_dropoff_datetime")
            )
            .withColumn(
                "trip_duration_seconds",
                F.col("tpep_dropoff_datetime").cast("double")
                - F.col("tpep_pickup_datetime").cast("double"),
            )
            .withColumn(
                "average_speed_mph",
                # Invalid durations receive their own reason; avoid dividing by zero.
                F.when(
                    F.col("trip_duration_seconds") > 0,
                    F.col("trip_distance") * 3600 / F.col("trip_duration_seconds"),
                ),
            )
        )

        pickup = F.col("tpep_pickup_datetime")
        duration = F.col("trip_duration_seconds")

        rules = [
            # Missing or unparseable timestamps cannot support a temporal join.
            (pickup.isNull(), "invalid_pickup_timestamp"),
            (F.col("tpep_dropoff_datetime").isNull(), "invalid_dropoff_timestamp"),
            # The end date is exclusive; only pickup determines the analysis month.
            (
                (pickup < F.lit(self.start_date).cast("timestamp"))
                | (pickup >= F.lit(self.end_date).cast("timestamp")),
                "pickup_outside_analysis_period",
            ),
            # Duration is in seconds; distance and average speed use miles and mph.
            (duration <= 0, "non_positive_duration"),
            (duration > self.max_duration_hours * 3600, "excessive_duration"),
            (
                ~_finite("trip_distance") | (F.col("trip_distance") <= 0),
                "invalid_trip_distance",
            ),
            (
                _finite("trip_distance")
                & (F.col("trip_distance") > self.max_distance_miles),
                "excessive_distance",
            ),
            (
                _finite("trip_distance")
                & (F.col("average_speed_mph") > self.max_speed_mph),
                "excessive_speed",
            ),
            (
                ~_finite("total_amount") | (F.col("total_amount") < 0),
                "invalid_total_amount",
            ),
            # Passenger count is optional, but any supplied value must be valid.
            (
                F.col("passenger_count").isNotNull()
                & (~_finite("passenger_count") | (F.col("passenger_count") < 0)),
                "invalid_passenger_count",
            ),
        ]

        return normalized.withColumn("rejection_reasons", _reasons(rules))

    def clean(self, *, taxi_data: DataFrame) -> DataFrame:
        """Return only trips that pass every validation rule."""
        return self.validate(taxi_data=taxi_data).filter(
            F.size("rejection_reasons") == 0
        )


class WeatherDataCleaner:
    """Validate hourly weather observations while retaining rejected source data."""

    def validate_calendar(self, *, weather_data: DataFrame) -> None:
        """Require both kinds of coverage for January in the session timezone."""
        # The fixed monthly input is small; collect only its normalized hour labels.
        available = {
            row.time
            for row in weather_data.select(
                F.date_format("time", "yyyy-MM-dd HH:mm:ss").alias("time")
            ).collect()
        }
        if not available:
            raise ValueError("Cleaned weather input is empty; January coverage is required.")

        missing_observations = []
        missing_precipitation = []
        start = datetime(2024, 1, 1, tzinfo=ZoneInfo("America/New_York"))
        for offset in range(31 * 24):
            hour = start + timedelta(hours=offset)
            label = hour.strftime("%Y-%m-%d %H:%M:%S")
            if label not in available:
                missing_observations.append(label)
            if (hour + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S") not in available:
                missing_precipitation.append(label)

        if missing_observations or missing_precipitation:
            raise ValueError(
                "Missing weather coverage for January pickup hours: "
                f"instantaneous={missing_observations}; "
                f"precipitation={missing_precipitation}"
            )

    def validate(self, *, weather_data: DataFrame) -> DataFrame:
        """Keep invalid observations available for quarantine instead of dropping them."""
        # In-memory observations without source metadata retain local-time parsing.
        offset = (
            F.col("utc_offset_seconds")
            if "utc_offset_seconds" in weather_data.columns
            else None
        )
        normalized = weather_data.withColumn("raw_time", F.col("time")).withColumn(
            "time", _local_timestamp("time", utc_offset_seconds=offset)
        )

        rules = [
            (F.col("time").isNull(), "invalid_weather_timestamp"),
            (F.col("time") != F.date_trunc("hour", "time"), "weather_not_on_hour"),
            (~_finite("temperature_2m"), "invalid_temperature"),
            (
                ~_finite("precipitation") | (F.col("precipitation") < 0),
                "invalid_precipitation",
            ),
        ]

        # The reader supplies this field; in-memory observations may omit it.
        if "_corrupt_record" in normalized.columns:
            rules.append(
                (F.col("_corrupt_record").isNotNull(), "malformed_weather_row")
            )

        return normalized.withColumn("rejection_reasons", _reasons(rules))

    def clean(self, *, weather_data: DataFrame) -> DataFrame:
        """Return only observations that can be used in an hourly weather join."""
        return self.validate(weather_data=weather_data).filter(
            F.size("rejection_reasons") == 0
        )

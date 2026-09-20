from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def _local_timestamp(column: str) -> Column:
    # Accept local ISO dates with minute/second precision and optional microseconds.
    value = F.col(column).cast("string")
    pattern = r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2}(\.\d{1,6})?)?$"
    return F.when(value.rlike(pattern), F.try_to_timestamp(value))


def _finite(column: str) -> Column:
    value = F.col(column)
    return value.isNotNull() & ~F.isnan(value) & (F.abs(value) < float("inf"))


def _reasons(rules: list[tuple[Column, str]]) -> Column:
    return F.filter(
        F.array(*[F.when(condition, F.lit(reason)) for condition, reason in rules]),
        lambda reason: reason.isNotNull(),
    )


class TaxiDataCleaner:
    def __init__(
        self,
        *,
        start_date: str = "2024-01-01",
        end_date: str = "2024-02-01",
        max_distance_miles: float = 200.0,
        max_duration_hours: float = 24.0,
        max_speed_mph: float = 100.0,
    ) -> None:
        self.start_date = start_date
        self.end_date = end_date
        self.max_distance_miles = max_distance_miles
        self.max_duration_hours = max_duration_hours
        self.max_speed_mph = max_speed_mph

    def validate(self, *, taxi_data: DataFrame) -> DataFrame:
        """Normalize timestamps and retain every source row with rejection reasons."""
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
                F.when(
                    F.col("trip_duration_seconds") > 0,
                    F.col("trip_distance") * 3600 / F.col("trip_duration_seconds"),
                ),
            )
        )
        pickup = F.col("tpep_pickup_datetime")
        duration = F.col("trip_duration_seconds")
        rules = [
            (pickup.isNull(), "invalid_pickup_timestamp"),
            (F.col("tpep_dropoff_datetime").isNull(), "invalid_dropoff_timestamp"),
            (
                (pickup < F.lit(self.start_date).cast("timestamp"))
                | (pickup >= F.lit(self.end_date).cast("timestamp")),
                "pickup_outside_analysis_period",
            ),
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
            (
                F.col("passenger_count").isNotNull()
                & (~_finite("passenger_count") | (F.col("passenger_count") < 0)),
                "invalid_passenger_count",
            ),
        ]
        return normalized.withColumn("rejection_reasons", _reasons(rules))

    def clean(self, *, taxi_data: DataFrame) -> DataFrame:
        return self.validate(taxi_data=taxi_data).filter(
            F.size("rejection_reasons") == 0
        )


class WeatherDataCleaner:
    def validate(self, *, weather_data: DataFrame) -> DataFrame:
        """Keep invalid observations available for quarantine instead of dropping them."""
        normalized = weather_data.withColumn("raw_time", F.col("time")).withColumn(
            "time", _local_timestamp("time")
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
        if "_corrupt_record" in normalized.columns:
            rules.append(
                (F.col("_corrupt_record").isNotNull(), "malformed_weather_row")
            )
        return normalized.withColumn("rejection_reasons", _reasons(rules))

    def clean(self, *, weather_data: DataFrame) -> DataFrame:
        return self.validate(weather_data=weather_data).filter(
            F.size("rejection_reasons") == 0
        )

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class TaxiWeatherJoiner:
    def join(
        self,
        *,
        taxi_data: DataFrame,
        weather_data: DataFrame,
    ) -> DataFrame:
        """Join instantaneous weather and preceding-hour precipitation separately."""
        # Multiple observations for an hour would multiply trips in the join.
        duplicate_hours = (
            weather_data.groupBy("time").count().filter(F.col("count") > 1)
        )

        if duplicate_hours.limit(1).count():
            raise ValueError("Weather timestamps must be unique before joining.")

        taxi_with_hour = taxi_data.withColumn(
            "pickup_hour",
            F.date_trunc("hour", F.col("tpep_pickup_datetime")),
        )

        observations = weather_data.drop("precipitation")
        precipitation_intervals = weather_data.select(
            (F.col("time") - F.expr("INTERVAL 1 HOUR")).alias(
                "precipitation_interval_start"
            ),
            "precipitation",
        )

        with_observations = taxi_with_hour.join(
            observations,
            taxi_with_hour["pickup_hour"] == observations["time"],
            how="left",
        ).drop("time")

        # Precipitation stamped at 10:00 covers pickups in the 09:00 hour.
        # Keep the next month's midnight observation for the final pickup hour.
        return with_observations.join(
            precipitation_intervals,
            with_observations["pickup_hour"]
            == precipitation_intervals["precipitation_interval_start"],
            how="left",
        )

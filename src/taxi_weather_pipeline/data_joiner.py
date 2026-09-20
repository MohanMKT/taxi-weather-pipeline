from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class TaxiWeatherJoiner:
    def join(
        self,
        *,
        taxi_data: DataFrame,
        weather_data: DataFrame,
    ) -> DataFrame:
        """Attach hourly weather while preserving trips without an observation."""
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

        return taxi_with_hour.join(
            weather_data,
            taxi_with_hour["pickup_hour"] == weather_data["time"],
            how="left",
        ).drop("time")

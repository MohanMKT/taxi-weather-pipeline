from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class TaxiWeatherJoiner:
    def join(
        self,
        *,
        taxi_data: DataFrame,
        weather_data: DataFrame,
    ) -> DataFrame:
        taxi_with_hour = taxi_data.withColumn(
            "pickup_hour",
            F.date_trunc("hour", F.col("tpep_pickup_datetime")),
        )

        return (
            taxi_with_hour
            .join(
                weather_data,
                taxi_with_hour["pickup_hour"] == weather_data["time"],
                how="left",
            )
            .drop("time")
        )
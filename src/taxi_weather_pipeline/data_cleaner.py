from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class TaxiDataCleaner:
    def clean(self, *, taxi_data: DataFrame) -> DataFrame:
        return (
            taxi_data
            # Normalize timestamps so taxi and weather data use a consistent type.
            .withColumn(
                "tpep_pickup_datetime",
                F.to_timestamp("tpep_pickup_datetime"),
            )
            .withColumn(
                "tpep_dropoff_datetime",
                F.to_timestamp("tpep_dropoff_datetime"),
            )
            # Trips without valid timestamps cannot be reliably joined or analyzed.
            .filter(F.col("tpep_pickup_datetime").isNotNull())
            .filter(F.col("tpep_dropoff_datetime").isNotNull())
            .filter(
                F.col("tpep_dropoff_datetime") >= F.col("tpep_pickup_datetime")
            )
            .filter(F.col("trip_distance") > 0)
            .filter(F.col("total_amount") >= 0)
            # Passenger count is optional, but negative values are invalid.
            .filter(
                F.col("passenger_count").isNull()
                | (F.col("passenger_count") >= 0)
            )
        )

class WeatherDataCleaner:
    def clean(self, *, weather_data: DataFrame) -> DataFrame:
        return (
            weather_data
            # Normalize hourly weather timestamps before temporal joining.
            .withColumn(
                "time",
                F.to_timestamp(
                    "time",
                    "yyyy-MM-dd'T'HH:mm",
                ),
            )
            .filter(F.col("time").isNotNull())
            .filter(F.col("temperature_2m").isNotNull())
            .filter(F.col("precipitation").isNotNull())
        )
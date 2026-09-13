from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class TaxiWeatherAnalyzer:
    def summarize_by_weather(
        self,
        *,
        taxi_weather_data: DataFrame,
    ) -> DataFrame:
        data_with_weather_category = taxi_weather_data.withColumn(
            "weather_category",
            F.when(
                F.col("precipitation").isNull(),
                "Unknown",
            )
            .when(
                F.col("precipitation") > 0,
                "Precipitation",
            )
            .otherwise("No precipitation"),
        )

        return (
            data_with_weather_category
            .groupBy("weather_category")
            .agg(
                F.count("*").alias("trip_count"),
                F.round(
                    F.avg("total_amount"),
                    2,
                ).alias("average_total_amount"),
                F.round(
                    F.avg("trip_distance"),
                    2,
                ).alias("average_trip_distance"),
            )
            .orderBy("weather_category")
        )

    def summarize_by_hour(
        self,
        *,
        taxi_weather_data: DataFrame,
    ) -> DataFrame:
        return (
            taxi_weather_data
            .withColumn(
                "pickup_hour_of_day",
                F.hour("tpep_pickup_datetime"),
            )
            .groupBy("pickup_hour_of_day")
            .agg(
                F.count("*").alias("trip_count"),
            )
            .orderBy("pickup_hour_of_day")
        )
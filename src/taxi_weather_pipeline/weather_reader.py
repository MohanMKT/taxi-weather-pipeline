from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


class WeatherReader:
    def __init__(self, *, spark: SparkSession) -> None:
        self._spark = spark

    def read(self, *, path: str) -> DataFrame:
        raw_data = self._spark.read.text(path)

        # Open-Meteo CSV files contain metadata before the hourly observations.
        weather_rows = raw_data.filter(
            F.col("value").rlike(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2},")
        )

        columns = F.split(F.col("value"), ",")

        return weather_rows.select(
            columns[0].alias("time"),
            columns[1].cast("double").alias("temperature_2m"),
            columns[2].cast("integer").alias("weather_code"),
            columns[3].cast("double").alias("precipitation"),
        )
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


class WeatherReader:
    """Read the Open-Meteo CSV format without losing malformed observations."""

    HEADER = "time,temperature_2m (°C),weather_code (wmo code),precipitation (mm)"

    # Spark populates _corrupt_record when the row does not match this schema.
    SCHEMA = (
        "time STRING, temperature_2m DOUBLE, weather_code INT, "
        "precipitation DOUBLE, _corrupt_record STRING"
    )

    def __init__(self, *, spark: SparkSession) -> None:
        self._spark = spark

    def read(self, *, path: str) -> DataFrame:
        """Validate each file's header and parse the observation rows after it."""
        # Each monthly weather file is small; preserve line positions after metadata.
        files = (
            self._spark.read.text(path, wholetext=True)
            .withColumn("lines", F.split("value", r"\r?\n"))
            .withColumn("header_position", F.array_position("lines", self.HEADER))
        )

        invalid_headers = files.filter(
            (F.col("header_position") == 0)
            | (F.size(F.filter("lines", lambda line: line.startswith("time,"))) != 1)
        )

        if invalid_headers.limit(1).count():
            raise ValueError(f"Expected exactly one weather CSV header: {self.HEADER}")

        rows = (
            files.select(
                "header_position", F.posexplode("lines").alias("position", "value")
            )
            # array_position is one-based; posexplode is zero-based.
            .filter(F.col("position") >= F.col("header_position"))
            .filter(F.length(F.trim("value")) > 0)
            .select(F.col("value").alias("raw_weather_row"))
        )

        # PERMISSIVE parsing retains bad rows for the cleaner to quarantine.
        return rows.select(
            "raw_weather_row",
            F.from_csv(
                "raw_weather_row",
                self.SCHEMA,
                {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"},
            ).alias("observation"),
        ).select("raw_weather_row", "observation.*")

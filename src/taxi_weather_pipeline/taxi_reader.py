from pyspark.sql import DataFrame, SparkSession


class TaxiReader:
    def __init__(self, *, spark: SparkSession) -> None:
        self.spark = spark

    def read(self, *, path: str) -> DataFrame:
        """Load taxi records using the schema stored in the Parquet source."""
        return self.spark.read.parquet(path)

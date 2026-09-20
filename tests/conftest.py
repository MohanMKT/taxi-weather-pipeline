from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """Share a small Spark session with deterministic local-hour interpretation."""
    session = (
        SparkSession.builder.master("local[1]")
        .appName("TaxiWeatherPipelineTests")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    yield session

    session.stop()

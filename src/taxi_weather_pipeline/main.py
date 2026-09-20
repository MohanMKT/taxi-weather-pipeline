from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from taxi_weather_pipeline.analyzer import TaxiWeatherAnalyzer
from taxi_weather_pipeline.data_cleaner import TaxiDataCleaner, WeatherDataCleaner
from taxi_weather_pipeline.data_joiner import TaxiWeatherJoiner
from taxi_weather_pipeline.taxi_reader import TaxiReader
from taxi_weather_pipeline.visualizer import TaxiDemandVisualizer
from taxi_weather_pipeline.weather_reader import WeatherReader


def _report_rejections(*, rejected: DataFrame, label: str) -> None:
    """Count each rejection reason, including overlapping failures on one row."""
    print(f"{label} rejection reasons (one record may have multiple reasons):")
    (
        rejected.select(F.explode("rejection_reasons").alias("reason"))
        .groupBy("reason")
        .count()
        .orderBy("reason")
        .show(truncate=False)
    )


def run_pipeline(*, spark: SparkSession) -> None:
    """Read, validate, enrich, and summarize the January case-study datasets."""
    # Cache validation results because both quarantine and analysis reuse them.
    taxi_data = TaxiReader(spark=spark).read(
        path="data/raw/taxi/yellow_tripdata_2024-01.parquet"
    )
    validated_taxi = TaxiDataCleaner().validate(taxi_data=taxi_data).cache()
    cleaned_taxi = validated_taxi.filter(F.size("rejection_reasons") == 0)
    rejected_taxi = validated_taxi.filter(F.size("rejection_reasons") > 0)

    weather_data = WeatherReader(spark=spark).read(
        path="data/raw/weather/weather_january_2024.csv"
    )
    validated_weather = WeatherDataCleaner().validate(weather_data=weather_data).cache()
    cleaned_weather = validated_weather.filter(F.size("rejection_reasons") == 0)
    rejected_weather = validated_weather.filter(F.size("rejection_reasons") > 0)

    # Preserve source fields, raw timestamps/CSV rows, and all reasons for review.
    rejected_taxi.write.mode("overwrite").parquet("data/processed/rejected_taxi")
    rejected_weather.write.mode("overwrite").parquet("data/processed/rejected_weather")

    enriched = (
        TaxiWeatherJoiner()
        .join(
            taxi_data=cleaned_taxi,
            weather_data=cleaned_weather.select(
                "time", "temperature_2m", "precipitation", "weather_code"
            ),
        )
        .select(
            "tpep_pickup_datetime",
            "trip_distance",
            "total_amount",
            "temperature_2m",
            "precipitation",
        )
        .cache()
    )

    analyzer = TaxiWeatherAnalyzer()
    weather_summary = analyzer.summarize_by_weather(taxi_weather_data=enriched)
    hourly_summary = analyzer.summarize_by_hour(taxi_weather_data=enriched)

    TaxiDemandVisualizer().plot_by_hour(
        hourly_summary=hourly_summary,
        output_path="data/output/trips_by_hour.png",
    )

    raw_taxi_count = validated_taxi.count()
    cleaned_taxi_count = cleaned_taxi.count()
    missing_weather_count = enriched.filter(F.col("temperature_2m").isNull()).count()

    print(f"Raw taxi data count: {raw_taxi_count}")
    print(f"Cleaned taxi data count: {cleaned_taxi_count}")
    print(f"Rejected taxi records: {rejected_taxi.count()}")
    out_of_period_count = validated_taxi.filter(
        F.array_contains("rejection_reasons", "pickup_outside_analysis_period")
    ).count()
    print(f"Out-of-period taxi records: {out_of_period_count}")
    print(f"Raw weather data count: {validated_weather.count()}")
    print(f"Cleaned weather data count: {cleaned_weather.count()}")
    print(f"Rejected weather records: {rejected_weather.count()}")
    print(f"Enriched taxi data count: {enriched.count()}")
    print(f"Trips without matching weather: {missing_weather_count}")

    if cleaned_taxi_count:
        coverage = 100 * (1 - missing_weather_count / cleaned_taxi_count)
        print(f"Weather match coverage: {coverage:.4f}%")
    else:
        print("Weather match coverage: N/A (no valid trips)")

    _report_rejections(rejected=rejected_taxi, label="Taxi")
    _report_rejections(rejected=rejected_weather, label="Weather")

    print("Taxi metrics by weather:")
    weather_summary.show(truncate=False)

    print("Taxi trips by hour of day:")
    hourly_summary.show(24, truncate=False)

    # Release cached datasets after the final Spark actions have completed.
    enriched.unpersist()
    validated_weather.unpersist()
    validated_taxi.unpersist()


def main() -> None:
    """Own the local Spark session and stop it even if the pipeline fails."""
    spark = (
        SparkSession.builder.appName("TaxiWeatherPipeline")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )

    try:
        run_pipeline(spark=spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

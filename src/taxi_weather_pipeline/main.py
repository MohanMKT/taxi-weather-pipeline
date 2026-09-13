from pyspark.sql import SparkSession

from taxi_weather_pipeline.analyzer import TaxiWeatherAnalyzer
from taxi_weather_pipeline.data_cleaner import (
    TaxiDataCleaner,
    WeatherDataCleaner,
)
from taxi_weather_pipeline.data_joiner import TaxiWeatherJoiner
from taxi_weather_pipeline.taxi_reader import TaxiReader
from taxi_weather_pipeline.visualizer import TaxiDemandVisualizer
from taxi_weather_pipeline.weather_reader import WeatherReader


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("TaxiWeatherPipeline")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )

    taxi_reader = TaxiReader(spark=spark)
    weather_reader = WeatherReader(spark=spark)
    taxi_cleaner = TaxiDataCleaner()
    weather_cleaner = WeatherDataCleaner()
    taxi_weather_joiner = TaxiWeatherJoiner()
    taxi_weather_analyzer = TaxiWeatherAnalyzer()
    taxi_demand_visualizer = TaxiDemandVisualizer()

    taxi_data = taxi_reader.read(
        path="data/raw/taxi/yellow_tripdata_2024-01.parquet"
    )
    cleaned_taxi_data = taxi_cleaner.clean(
        taxi_data=taxi_data
    )

    weather_data = weather_reader.read(
        path="data/raw/weather/weather_january_2024.csv"
    )
    cleaned_weather_data = weather_cleaner.clean(
        weather_data=weather_data
    )

    enriched_taxi_data = taxi_weather_joiner.join(
        taxi_data=cleaned_taxi_data,
        weather_data=cleaned_weather_data,
    )

    missing_weather_count = enriched_taxi_data.filter(
        enriched_taxi_data["temperature_2m"].isNull()
    ).count()

    weather_summary = taxi_weather_analyzer.summarize_by_weather(
        taxi_weather_data=enriched_taxi_data,
    )

    hourly_summary = taxi_weather_analyzer.summarize_by_hour(
        taxi_weather_data=enriched_taxi_data,
    )

    taxi_demand_visualizer.plot_by_hour(
        hourly_summary=hourly_summary,
        output_path="data/output/trips_by_hour.png",
    )

    print(f"Raw taxi data count: {taxi_data.count()}")
    print(f"Cleaned taxi data count: {cleaned_taxi_data.count()}")
    print(f"Raw weather data count: {weather_data.count()}")
    print(f"Cleaned weather data count: {cleaned_weather_data.count()}")
    print(f"Trips without matching weather: {missing_weather_count}")

    print("Taxi metrics by weather:")
    weather_summary.show(truncate=False)

    print("Taxi trips by hour of day:")
    hourly_summary.show(24, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
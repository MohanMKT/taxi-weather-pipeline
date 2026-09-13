# Taxi Weather Pipeline

Small data engineering case study based on New York City taxi trip data and hourly weather data.

The goal is to demonstrate a simple and scalable data pipeline that:

- ingests taxi and weather data
- applies basic data quality rules
- standardizes timestamps
- joins taxi trips with hourly weather data
- performs a small analysis on taxi demand and weather conditions

## Data Sources

### Taxi Data

New York City Yellow Taxi Trip Records for January 2024.

The dataset is stored locally as:

```text
data/raw/taxi/yellow_tripdata_2024-01.parquet
```

### Weather Data

Hourly weather data for New York City for January 2024.

The dataset contains:

- timestamp
- temperature
- precipitation
- weather code

The raw file is stored as:

```text
data/raw/weather/weather_january_2024.csv
```

## Project Structure

```text
taxi-weather-pipeline/
├── data/
│   ├── raw/
│   │   ├── taxi/
│   │   │   └── yellow_tripdata_2024-01.parquet
│   │   └── weather/
│   │       └── weather_january_2024.csv
│   ├── processed/
│   └── output/
│
├── src/
│   └── taxi_weather_pipeline/
│       ├── __init__.py
│       ├── analyzer.py
│       ├── data_cleaner.py
│       ├── data_joiner.py
│       ├── main.py
│       ├── taxi_reader.py
│       └── weather_reader.py
│
├── tests/
│   ├── test_data_cleaner.py
│   └── test_data_joiner.py
│
├── .gitignore
├── pyproject.toml
└── README.md
```

Each component has a single responsibility:

- `TaxiReader` reads taxi parquet data
- `WeatherReader` parses the weather source
- `TaxiDataCleaner` validates and cleans taxi records
- `WeatherDataCleaner` normalizes weather timestamps
- `TaxiWeatherJoiner` joins taxi trips with hourly weather
- `TaxiWeatherAnalyzer` creates simple analytical summaries
- `main.py` wires the pipeline components together

## Data Processing Flow

```text
Taxi parquet
    |
    v
TaxiReader
    |
    v
TaxiDataCleaner
    |
    +-------------------+
                        |
                        v
                  TaxiWeatherJoiner
                        ^
                        |
    +-------------------+
    |
Weather CSV
    |
    v
WeatherReader
    |
    v
WeatherDataCleaner
    |
    v
Hourly weather data

TaxiWeatherJoiner
    |
    v
Enriched taxi + weather data
    |
    v
TaxiWeatherAnalyzer
    |
    +--> Weather-based metrics
    |
    +--> Hourly taxi demand
```

## Data Quality Rules

### Taxi Data

Taxi records are filtered when:

- pickup timestamp is missing
- dropoff timestamp is missing
- dropoff time is before pickup time
- trip distance is less than or equal to zero
- total amount is negative
- passenger count is negative

Passenger count is treated as optional, therefore null values are allowed.

### Weather Data

Weather records require:

- a valid timestamp
- temperature
- precipitation

Weather timestamps are converted to Spark timestamp values before joining.

## Timezone Handling

The pipeline uses:

```text
America/New_York
```

as the Spark session timezone.

This ensures taxi and weather timestamps are interpreted consistently and are not affected by the timezone of the machine running the pipeline.

## Join Logic

Weather data is available at hourly granularity.

Taxi pickup timestamps are therefore truncated to the hour before joining.

Example:

```text
Taxi pickup:
2024-01-01 10:37:15

Truncated pickup hour:
2024-01-01 10:00:00

Weather observation:
2024-01-01 10:00:00
```

A left join is used so that all taxi trips are preserved even when no matching weather observation exists.

The pipeline also checks how many taxi trips do not have matching weather data.

## Analysis

Two small analyses are included.

### Taxi Metrics by Weather

Trips are grouped into:

- `Precipitation`
- `No precipitation`
- `Unknown`

`Precipitation` is used instead of `Rain` because the source weather field may contain different forms of precipitation, including rain and snow.

The following metrics are calculated:

- trip count
- average total amount
- average trip distance

### Taxi Demand by Hour

Taxi trips are grouped by pickup hour of day.

This provides a simple view of taxi demand patterns across the day and can support operational decisions such as taxi positioning.

## Local Results

For the January 2024 dataset:

```text
Raw taxi data count:      2,964,624
Cleaned taxi data count:  2,872,094

Raw weather data count:   744
Cleaned weather data count: 744
```

The taxi quality rules remove roughly 3% of the source records.

Only a very small number of taxi trips do not find a matching hourly weather observation.

## Setup

Python 3.12 is used.

Create and activate a virtual environment:

```bash
python3.12 -m venv venv
source venv/bin/activate
```

Install the project and development dependencies:

```bash
python -m pip install -e ".[dev]"
```

## Run the Pipeline

Run from the project root:

```bash
python -m taxi_weather_pipeline.main
```

## Code Quality

The project uses:

- Ruff for linting and code-quality checks
- mypy for static type checking
- pytest for automated testing

Run linting:

```bash
ruff check src tests
```

Run type checking:

```bash
mypy src
```

Run tests:

```bash
pytest -v
```

Or run all quality checks:

```bash
ruff check src tests
mypy src
pytest -v
```

## Testing

Tests follow the Arrange-Act-Assert pattern.

Current tests cover:

- invalid taxi records are removed correctly
- taxi trips are matched to the correct hourly weather observation

The current test suite is intentionally small and focused on the core business logic of the assignment.

## Design Principles

The implementation intentionally stays small and avoids unnecessary abstractions.

The following principles are applied:

- Single Responsibility Principle
- Dependency Injection
- Composition over Inheritance
- DRY
- YAGNI
- explicit type hints
- keyword-only arguments
- concise comments only where intent is not obvious

The codebase avoids unnecessary interfaces, factories, or inheritance hierarchies because the current requirements do not justify them.

## Production Considerations

The local implementation focuses on correctness, clarity, and testability.

For a production-scale taxi platform, the same logical flow could evolve into:

```text
Taxi fleet
    |
    v
Streaming ingestion
    |
    v
Stream processing
    |
    +----------------------+
    |                      |
    v                      v
Lakehouse              Real-time processing
Bronze / Silver / Gold Fraud / positioning
    |
    v
Analytics / BI
```

A production solution would additionally consider:

- scalable event ingestion
- streaming and batch processing
- durable lakehouse storage
- schema evolution
- data contracts
- monitoring and observability
- retry and failure handling
- data lineage
- access control
- CI/CD
- infrastructure as code
- sending operational information back to individual taxis

The local case study intentionally does not implement these production components because the assignment focuses on demonstrating the engineering approach rather than reproducing the full production environment.

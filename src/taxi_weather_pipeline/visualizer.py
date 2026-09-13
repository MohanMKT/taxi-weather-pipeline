from pathlib import Path

import matplotlib.pyplot as plt
from pyspark.sql import DataFrame


class TaxiDemandVisualizer:
    def plot_by_hour(
        self,
        *,
        hourly_summary: DataFrame,
        output_path: str,
    ) -> None:
        rows = hourly_summary.collect()

        hours = [row["pickup_hour_of_day"] for row in rows]
        trip_counts = [row["trip_count"] for row in rows]

        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        plt.figure(figsize=(10, 5))
        plt.bar(hours, trip_counts)

        plt.title("Taxi Trips by Hour of Day")
        plt.xlabel("Hour of Day")
        plt.ylabel("Trip Count")
        plt.xticks(range(24))

        plt.tight_layout()
        plt.savefig(output_file, dpi=150)
        plt.close()
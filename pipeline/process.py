"""
Step 3 – Process
Aggregates arrest records by precinct, borough, month, and offence category.
"""

from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def run_process(spark: SparkSession, processed_parquet: str, aggregated_dir: str) -> None:
    df = spark.read.parquet(processed_parquet)
    total = df.count()
    print(f"  Records read: {total:,}")

    Path(aggregated_dir).mkdir(parents=True, exist_ok=True)

    # Precinct-level summary
    precinct_agg = (df
        .groupBy("ARREST_PRECINCT", "ARREST_BORO")
        .agg(
            F.count("*").alias("total_arrests"),
            F.sum(F.when(F.col("LAW_CAT_CD") == "F", 1).otherwise(0)).alias("felonies"),
            F.sum(F.when(F.col("LAW_CAT_CD") == "M", 1).otherwise(0)).alias("misdemeanors"),
            F.sum(F.when(F.col("LAW_CAT_CD") == "V", 1).otherwise(0)).alias("violations"),
            F.avg("Latitude").alias("centroid_lat"),
            F.avg("Longitude").alias("centroid_lon"),
        )
    )
    precinct_agg.write.mode("overwrite").parquet(f"{aggregated_dir}/precinct_summary.parquet")
    print(f"  Precincts with data: {precinct_agg.count()}")

    # Borough × month trend
    monthly_agg = (df
        .filter(F.col("ARREST_YEAR").isNotNull() & F.col("ARREST_MONTH").isNotNull())
        .groupBy("ARREST_BORO", "ARREST_YEAR", "ARREST_MONTH", "LAW_CAT_CD")
        .agg(F.count("*").alias("arrest_count"))
        .orderBy("ARREST_BORO", "ARREST_YEAR", "ARREST_MONTH")
    )
    monthly_agg.write.mode("overwrite").parquet(f"{aggregated_dir}/monthly_trend.parquet")

    print(f"  Aggregated outputs written to: {aggregated_dir}")

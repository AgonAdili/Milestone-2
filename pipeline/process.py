"""
Step 3 – Process
Aggregates arrest records using PySpark + Apache Sedona.

Sedona is used for two spatial operations:
  1. ST_Point + ST_Within — validates every record falls inside the NYC
     bounding box before aggregation.
  2. H3 hexagonal binning via a UDF — produces city-block-level density
     cells for the heat layer in the expose step.
"""

from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

import h3 as h3lib

from pipeline.config import NYC_BBOX_WKT, H3_RESOLUTION


@F.udf(returnType=StringType())
def to_h3_cell(lat, lon):
    if lat is None or lon is None:
        return None
    try:
        return h3lib.latlng_to_cell(float(lat), float(lon), H3_RESOLUTION)
    except Exception:
        return None


def run_process(spark: SparkSession, processed_parquet: str, aggregated_dir: str) -> None:
    df = spark.read.parquet(processed_parquet)
    total = df.count()
    print(f"  Records read: {total:,}")

    Path(aggregated_dir).mkdir(parents=True, exist_ok=True)

    # Spatial validation with Sedona
    df = df.withColumn(
        "geom",
        F.expr("ST_Point(CAST(Longitude AS DOUBLE), CAST(Latitude AS DOUBLE))")
    )
    df = df.withColumn(
        "in_nyc",
        F.expr(f"ST_Within(geom, ST_GeomFromText('{NYC_BBOX_WKT}'))")
    )
    outliers = df.filter(~F.col("in_nyc")).count()
    print(f"  Records outside NYC bounding box (Sedona ST_Within): {outliers:,}")
    df = df.filter(F.col("in_nyc")).drop("geom", "in_nyc")

    # H3 hexagonal binning (resolution 8 ≈ 460 m edge length)
    df = df.withColumn("h3_cell", to_h3_cell(F.col("Latitude"), F.col("Longitude")))

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
        .withColumn(
            "dominant_cat",
            F.when(
                (F.col("felonies") >= F.col("misdemeanors")) & (F.col("felonies") >= F.col("violations")), "F"
            ).when(
                F.col("misdemeanors") >= F.col("violations"), "M"
            ).otherwise("V")
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

    # H3 cell density
    h3_agg = (df
        .filter(F.col("h3_cell").isNotNull())
        .groupBy("h3_cell")
        .agg(
            F.count("*").alias("arrest_count"),
            F.avg("Latitude").alias("cell_lat"),
            F.avg("Longitude").alias("cell_lon"),
        )
    )
    h3_agg.write.mode("overwrite").parquet(f"{aggregated_dir}/h3_density.parquet")
    print(f"  H3 cells populated: {h3_agg.count()}")

    # Top-10 precincts by arrest volume
    top10 = precinct_agg.orderBy(F.col("total_arrests").desc()).limit(10)
    top10.write.mode("overwrite").parquet(f"{aggregated_dir}/top10_precincts.parquet")
    print("  Top 10 precincts:")
    for row in top10.collect():
        print(f"    Precinct {row.ARREST_PRECINCT:>3} ({row.ARREST_BORO}): {row.total_arrests:>5} arrests")

    print(f"  Aggregated outputs written to: {aggregated_dir}")

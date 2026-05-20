"""
Step 1 – Ingest
Reads the raw CSV, validates schema and data quality, writes raw Parquet.
No transformations or aggregations happen here; this step is purely about
getting trustworthy data into the columnar store.
"""

from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)


# Explicit schema avoids a full CSV scan for type inference and catches
# casting failures as null values rather than parse errors.
ARRESTS_SCHEMA = StructType([
    StructField("ARREST_KEY",        StringType(),  True),
    StructField("ARREST_DATE",       StringType(),  True),
    StructField("PD_CD",             StringType(),  True),
    StructField("PD_DESC",           StringType(),  True),
    StructField("KY_CD",             StringType(),  True),
    StructField("OFNS_DESC",         StringType(),  True),
    StructField("LAW_CODE",          StringType(),  True),
    StructField("LAW_CAT_CD",        StringType(),  True),
    StructField("ARREST_BORO",       StringType(),  True),
    StructField("ARREST_PRECINCT",   StringType(),  True),
    StructField("JURISDICTION_CODE", StringType(),  True),
    StructField("AGE_GROUP",         StringType(),  True),
    StructField("PERP_SEX",          StringType(),  True),
    StructField("PERP_RACE",         StringType(),  True),
    StructField("X_COORD_CD",        StringType(),  True),
    StructField("Y_COORD_CD",        StringType(),  True),
    StructField("Latitude",          StringType(),  True),
    StructField("Longitude",         StringType(),  True),
    StructField("Location",          StringType(),  True),
])

# Columns that carry '(null)' as a literal string instead of SQL NULL
PLACEHOLDER_NULL_COLS = ["AGE_GROUP", "PERP_SEX", "PERP_RACE"]


def run_ingest(spark: SparkSession, csv_path: str, output_path: str) -> None:
    print(f"  Reading CSV: {csv_path}")
    df = spark.read.csv(
        csv_path,
        header=True,
        schema=ARRESTS_SCHEMA,
        quote='"',
    )

    total_raw = df.count()
    print(f"  Raw record count: {total_raw:,}")

    # Cast numeric and date columns
    df = (df
        .withColumn("ARREST_DATE",       F.to_date("ARREST_DATE", "MM/dd/yyyy"))
        .withColumn("ARREST_MONTH",      F.month("ARREST_DATE"))
        .withColumn("ARREST_YEAR",       F.year("ARREST_DATE"))
        .withColumn("PD_CD",             F.col("PD_CD").cast(IntegerType()))
        .withColumn("KY_CD",             F.col("KY_CD").cast(IntegerType()))
        .withColumn("ARREST_PRECINCT",   F.col("ARREST_PRECINCT").cast(IntegerType()))
        .withColumn("JURISDICTION_CODE", F.col("JURISDICTION_CODE").cast(IntegerType()))
        .withColumn("X_COORD_CD",        F.col("X_COORD_CD").cast(IntegerType()))
        .withColumn("Y_COORD_CD",        F.col("Y_COORD_CD").cast(IntegerType()))
        .withColumn("Latitude",          F.col("Latitude").cast(DoubleType()))
        .withColumn("Longitude",         F.col("Longitude").cast(DoubleType()))
    )

    # Replace '(null)' placeholder strings with SQL NULL
    for col_name in PLACEHOLDER_NULL_COLS:
        df = df.withColumn(
            col_name,
            F.when(F.col(col_name) == "(null)", None).otherwise(F.col(col_name))
        )

    # --- Data quality report ------------------------------------------------
    print("  Null / missing value counts:")
    quality_cols = ["LAW_CAT_CD", "KY_CD", "AGE_GROUP", "PERP_SEX", "PERP_RACE",
                    "Latitude", "Longitude"]
    for c in quality_cols:
        n = df.filter(F.col(c).isNull()).count()
        if n > 0:
            print(f"    {c}: {n:,} nulls")

    # Flag records with missing or out-of-range coordinates
    df = df.withColumn(
        "coord_valid",
        F.col("Latitude").isNotNull()
        & F.col("Longitude").isNotNull()
        & (F.col("Latitude").between(40.4, 40.95))
        & (F.col("Longitude").between(-74.3, -73.65))
    )
    coord_invalid = df.filter(~F.col("coord_valid")).count()
    print(f"  Records with invalid/missing coordinates: {coord_invalid:,}")

    # Write raw validated Parquet (all records retained; outlier flag included)
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df.write.mode("overwrite").parquet(output_path)
    print(f"  Wrote raw Parquet: {output_path}")
    print(f"  Total records written: {total_raw:,}")

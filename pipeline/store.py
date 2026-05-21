"""
Step 2 – Store
Reads the raw validated Parquet and writes a clean copy in Parquet format.
"""

from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def run_store(spark: SparkSession, raw_parquet: str, output_path: str) -> None:
    df = spark.read.parquet(raw_parquet)
    total = df.count()
    print(f"  Records read from raw Parquet: {total:,}")

    # Drop records flagged as having invalid coordinates
    df_clean = df.filter(F.col("coord_valid") == True).drop("coord_valid")
    dropped = total - df_clean.count()
    print(f"  Records dropped (invalid coordinates): {dropped:,}")
    print(f"  Records retained: {total - dropped:,}")

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df_clean.write.mode("overwrite").parquet(output_path)
    print(f"  Wrote processed Parquet: {output_path}")

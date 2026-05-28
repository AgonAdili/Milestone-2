"""
Step 2 – Store
Converts the raw Parquet to a query-optimised layout:
  • Snappy compression — fast decompression for analytics workloads
  • Partitioned by ARREST_BORO (5 partitions) so borough-scoped queries
    skip 4/5 of the data via partition pruning without reading a byte
"""

import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def _dir_size_mb(path: str) -> float:
    total = 0
    for dirpath, _, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if os.path.isfile(fp):
                total += os.path.getsize(fp)
    return total / (1024 * 1024)


def run_store(spark: SparkSession, raw_parquet: str, output_path: str) -> None:
    df = spark.read.parquet(raw_parquet)
    total = df.count()
    print(f"  Records read from raw Parquet: {total:,}")

    df_clean = df.filter(F.col("coord_valid") == True).drop("coord_valid")
    dropped = total - df_clean.count()
    print(f"  Records dropped (invalid coordinates): {dropped:,}")
    print(f"  Records retained: {total - dropped:,}")

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    (df_clean
        .repartition(F.col("ARREST_BORO"))
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .partitionBy("ARREST_BORO")
        .parquet(output_path)
    )

    raw_mb  = _dir_size_mb(raw_parquet)
    proc_mb = _dir_size_mb(output_path)
    ratio   = raw_mb / proc_mb if proc_mb > 0 else 0
    print(f"  Raw Parquet:       {raw_mb:.1f} MB")
    print(f"  Processed Parquet: {proc_mb:.1f} MB  (ratio {ratio:.1f}x)")
    partitions = [p for p in os.listdir(output_path) if p.startswith("ARREST_BORO=")]
    print(f"  Partitions: {sorted(partitions)}")
    print(f"  Wrote processed Parquet: {output_path}")

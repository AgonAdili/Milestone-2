"""
NYPD Arrests Big Data Pipeline — end-to-end runner
Run:  python run_pipeline.py
"""

import sys
import time
from pathlib import Path

from pipeline.config import (
    RAW_CSV, RAW_PARQUET, PROCESSED_PARQUET,
    DATA_DIR, OUTPUT_DIR,
)


def main():
    for d in [Path(DATA_DIR) / "raw", Path(DATA_DIR) / "processed", Path(OUTPUT_DIR)]:
        d.mkdir(parents=True, exist_ok=True)

    if not Path(RAW_CSV).exists():
        print(f"ERROR: Dataset not found at {RAW_CSV}")
        sys.exit(1)

    from pyspark.sql import SparkSession
    spark = (SparkSession.builder
             .master("local[*]")
             .appName("NYPD-Arrests-Pipeline")
             .config("spark.driver.memory", "4g")
             .config("spark.sql.shuffle.partitions", "8")
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    print("\n[1/2] Ingest: CSV → raw Parquet")
    t = time.time()
    from pipeline.ingest import run_ingest
    run_ingest(spark, str(RAW_CSV), RAW_PARQUET)
    print(f"  Done in {time.time() - t:.1f} s")

    print("\n[2/2] Store: raw Parquet → processed Parquet")
    t = time.time()
    from pipeline.store import run_store
    run_store(spark, RAW_PARQUET, PROCESSED_PARQUET)
    print(f"  Done in {time.time() - t:.1f} s")

    spark.stop()
    print("\nPipeline complete.")


if __name__ == "__main__":
    main()

"""
NYPD Arrests Big Data Pipeline — end-to-end runner
Run:  python run_pipeline.py
"""

import sys
import time
from pathlib import Path

from pipeline.config import (
    RAW_CSV, RAW_PARQUET, PROCESSED_PARQUET,
    AGGREGATED_DIR, DATA_DIR, OUTPUT_DIR,
)


def main():
    for d in [
        Path(DATA_DIR) / "raw",
        Path(DATA_DIR) / "processed",
        Path(DATA_DIR) / "aggregated",
        Path(OUTPUT_DIR),
    ]:
        d.mkdir(parents=True, exist_ok=True)

    if not Path(RAW_CSV).exists():
        print(f"ERROR: Dataset not found at {RAW_CSV}")
        sys.exit(1)

    from pyspark.sql import SparkSession
    from sedona.spark import SedonaContext

    # spark.jars.packages must be set before the JVM starts so Sedona JARs
    # are on the classpath when SedonaContext.create() is called.
    spark = (SparkSession.builder
             .master("local[*]")
             .appName("NYPD-Arrests-Pipeline")
             .config("spark.driver.memory", "4g")
             .config("spark.sql.shuffle.partitions", "8")
             .config("spark.ui.showConsoleProgress", "false")
             .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
             .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator")
             .config(
                 "spark.jars.packages",
                 "org.apache.sedona:sedona-spark-shaded-3.0_2.12:1.6.1,"
                 "org.datasyslab:geotools-wrapper:1.6.1-28.2",
             )
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    sedona = SedonaContext.create(spark)

    print("\n[1/3] Ingest: CSV → raw Parquet")
    t = time.time()
    from pipeline.ingest import run_ingest
    run_ingest(sedona, str(RAW_CSV), RAW_PARQUET)
    print(f"  Done in {time.time() - t:.1f} s")

    print("\n[2/3] Store: raw Parquet → partitioned Parquet")
    t = time.time()
    from pipeline.store import run_store
    run_store(sedona, RAW_PARQUET, PROCESSED_PARQUET)
    print(f"  Done in {time.time() - t:.1f} s")

    print("\n[3/3] Process: Sedona spatial validation + aggregations")
    t = time.time()
    from pipeline.process import run_process
    run_process(sedona, PROCESSED_PARQUET, AGGREGATED_DIR)
    print(f"  Done in {time.time() - t:.1f} s")

    sedona.stop()
    print("\nPipeline complete.")


if __name__ == "__main__":
    main()

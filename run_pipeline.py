"""
NYPD Arrests Big Data Pipeline — end-to-end runner
Run:  python run_pipeline.py
"""

import os
import sys
import time
from pathlib import Path

# Reconfigure stdout/stderr to UTF-8 so Unicode in log messages never crashes
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# Ensure stdout/stderr can handle any Unicode in log messages
os.environ.setdefault("PYTHONIOENCODING", "utf-8")

# Windows-only: Spark/Hadoop on Windows requires winutils.exe and explicit paths
if sys.platform == "win32":
    if "HADOOP_HOME" not in os.environ:
        os.environ["HADOOP_HOME"] = "C:\\hadoop"

    hadoop_home = os.environ["HADOOP_HOME"]
    hadoop_bin = os.path.join(hadoop_home, "bin")

    if hadoop_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] = hadoop_bin + ";" + os.environ.get("PATH", "")

    # Propagates hadoop.home.dir to every JVM spawned by PySpark (including
    # the Ivy subprocess that resolves spark.jars.packages).
    _prop = f"-Dhadoop.home.dir={hadoop_home.replace(os.sep, '/')}"
    _jto = os.environ.get("JAVA_TOOL_OPTIONS", "")
    if _prop not in _jto:
        os.environ["JAVA_TOOL_OPTIONS"] = (_prop + " " + _jto).strip()
else:
    hadoop_home = None
    hadoop_bin = None

from pipeline.config import (
    RAW_CSV, RAW_PARQUET, PROCESSED_PARQUET,
    AGGREGATED_DIR, MAP_OUTPUT, DATA_DIR, OUTPUT_DIR,
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
    builder = (SparkSession.builder
               .master("local[*]")
               .appName("NYPD-Arrests-Pipeline")
               .config("spark.driver.memory", "4g")
               .config("spark.sql.shuffle.partitions", "8")
               .config("spark.ui.showConsoleProgress", "false")
               .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
               .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator")
               .config("spark.sql.warehouse.dir", str(Path(DATA_DIR) / "warehouse"))
               .config("spark.python.worker.reuse", "true")
               .config("spark.network.timeout", "600s")
               .config(
                   "spark.jars.packages",
                   "org.apache.sedona:sedona-spark-shaded-3.0_2.12:1.6.1,"
                   "org.datasyslab:geotools-wrapper:1.6.1-28.2",
               ))

    if sys.platform == "win32":
        hadoop_home_fwd = hadoop_home.replace("\\", "/")
        hadoop_bin_fwd = hadoop_bin.replace("\\", "/")
        builder = (builder
                   .config("spark.driver.extraJavaOptions",
                           f"-Dfile.encoding=UTF-8"
                           f" -Dhadoop.home.dir={hadoop_home_fwd}"
                           f" -Djava.library.path={hadoop_bin_fwd}")
                   .config("spark.executor.extraJavaOptions",
                           f"-Dhadoop.home.dir={hadoop_home_fwd}"
                           f" -Djava.library.path={hadoop_bin_fwd}")
                   .config("spark.hadoop.io.native.lib.available", "false"))

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    sedona = SedonaContext.create(spark)

    print("\n[1/4] Ingest: CSV -> raw Parquet")
    t = time.time()
    from pipeline.ingest import run_ingest
    run_ingest(sedona, str(RAW_CSV), RAW_PARQUET)
    print(f"  Done in {time.time() - t:.1f} s")

    print("\n[2/4] Store: raw Parquet -> partitioned Parquet")
    t = time.time()
    from pipeline.store import run_store
    run_store(sedona, RAW_PARQUET, PROCESSED_PARQUET)
    print(f"  Done in {time.time() - t:.1f} s")

    print("\n[3/4] Process: Sedona spatial validation + aggregations")
    t = time.time()
    from pipeline.process import run_process
    run_process(sedona, PROCESSED_PARQUET, AGGREGATED_DIR)
    print(f"  Done in {time.time() - t:.1f} s")

    print("\n[4/4] Expose: generate interactive map")
    t = time.time()
    from pipeline.expose import run_expose
    run_expose(sedona, AGGREGATED_DIR, MAP_OUTPUT)
    print(f"  Done in {time.time() - t:.1f} s")

    sedona.stop()
    print("\nPipeline complete.")
    print(f"Open the map: open \"{MAP_OUTPUT}\"")


if __name__ == "__main__":
    main()

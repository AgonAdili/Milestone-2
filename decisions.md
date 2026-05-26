# Architectural Decisions — NYPD Arrests Pipeline

This document records three decisions made during the design and implementation of the pipeline. Each entry answers: what forced the decision, what was evaluated, what was chosen, and what was given up.

---

## Decision 1 — Storage format: Parquet over CSV

### Situation

The source dataset is delivered as a single 61 MB flat CSV file with 278,953 rows and 19 columns. The analytical queries the pipeline must answer filter on two specific columns — `ARREST_PRECINCT` and `LAW_CAT_CD` — and aggregate on a third (`ARREST_DATE`). The remaining 16 columns are never read during query execution. Keeping the data in CSV forces every query to deserialise all 19 columns on every row, even those that contribute nothing to the result.

### Alternatives evaluated

**Option A — Keep raw CSV in place.**  
No conversion cost. Any tool that can open a text file can read the data. The cost is query performance: a full-file scan reads all 61 MB for every query regardless of which columns or rows are needed. At 278,953 rows this is still fast enough on a laptop, but the dataset grows by roughly 70,000 rows every quarter; running across multiple years of accumulated data multiplies the volume significantly, and a full CSV scan becomes the dominant bottleneck at that scale.

**Option B — Load into PostgreSQL or SQLite.**  
Row-oriented relational databases are optimised for OLTP workloads (many small reads and writes). Running analytical GROUP BY queries on a row store requires reading every column of every qualifying row from disk. PostgreSQL adds ACID overhead (write-ahead logging, transaction management) that provides no benefit for a read-only analytics workload. Neither database integrates natively with the distributed Spark execution model used in the Process step.

**Option C — Convert to Apache Parquet (chosen).**  
Parquet is a columnar binary format. Two course concepts apply directly here.  
*Column pruning*: Parquet stores each column in its own set of row-group pages. When a query references only `ARREST_PRECINCT` and `LAW_CAT_CD`, the Parquet reader fetches only those two column segments, skipping the other 17 completely. On the 19-column dataset this reduces the bytes read per query from 61 MB to roughly 8 MB — a 7–8× reduction before any filtering begins.  
*Predicate pushdown*: Parquet stores min/max statistics for each row group. When a query filters `ARREST_PRECINCT = 114`, the reader compares 114 against the stored min/max of each row group and skips groups that cannot contain matching rows without decompressing them. This further reduces the effective scan size.  
In addition, Snappy compression reduces the on-disk footprint by roughly 3–4× compared to raw uncompressed Parquet, which matters when reading from distributed storage.

### Cost and acceptability

Parquet is a write-once format; it cannot be appended to in place. A new quarterly delivery requires re-running the Store step to produce a fresh Parquet file. For a dataset that updates four times per year this is an entirely acceptable cost: one re-run per quarter versus continuous query overhead on every analytical access. The conversion also adds a one-time processing step of a few seconds on local hardware.

---

## Decision 2 — Processing engine: PySpark over single-node alternatives

### Situation

The pipeline must perform two categories of computation: (a) spatial operations on lat/lon coordinates — specifically bounding-box containment checks and H3 hexagonal binning — and (b) multi-dimensional GROUP BY aggregations over precinct, borough, month, and offence category. Both classes of work are data-parallel: each row can be processed independently, and partial aggregates from different subsets of rows can be combined at the end.

### Alternatives evaluated

**Option A — pandas + GeoPandas (single-node).**  
pandas is the natural starting point for Python data work. GeoPandas provides spatial operations (Shapely-backed geometry, spatial joins) and produces clean result DataFrames. The fundamental constraint is that pandas loads the entire dataset into a single process's address space. At 278,953 rows the memory pressure is negligible, but the dataset accumulates roughly 70,000 new rows per quarter across multiple years. Loading several years of data into a single pandas DataFrame produces multiple large intermediate copies during aggregation, which easily exceeds typical laptop memory limits. More importantly, there is no migration path: pandas code cannot be distributed across nodes without a rewrite, making a single-node implementation a dead end architecturally.

**Option B — DuckDB.**  
DuckDB is an in-process OLAP database with native Parquet support and a columnar vectorised execution engine. It would handle the aggregation queries efficiently and requires no JVM. The gap is spatial: DuckDB's spatial extension provides basic geometry types but has no native H3 support and no equivalent of Apache Sedona's spatial SQL primitives (ST_Within, ST_Point, spatial joins with polygon boundaries). Adding H3 would require a UDF calling an external library, which defeats the purpose of using DuckDB for performance.

**Option C — PySpark with Apache Sedona (chosen).**  
PySpark implements the Spark programming model: computations are expressed as transformations on Resilient Distributed Datasets (RDDs) or DataFrames, compiled into a Directed Acyclic Graph (DAG) of stages, and executed in parallel across available CPU cores (local mode on a single machine) or across a cluster. The course concept of *data partitioning* is central here: Spark divides the input data into partitions and assigns each partition to an executor thread. Aggregations that span the whole dataset (GROUP BY ARREST_BORO, for example) require a *shuffle* — redistributing rows so that all rows with the same key land on the same executor — but Spark's cost-based optimizer minimises the number of shuffles and pipelines as many transformations as possible within a single stage.  
Apache Sedona extends Spark with spatial SQL functions (ST_Point, ST_Within, ST_GeomFromText) that execute as native Catalyst expressions inside Spark's query engine rather than as Python UDFs, which avoids the Python–JVM serialisation overhead of calling a Shapely function row by row.

### Cost and acceptability

PySpark requires Java (JVM startup time ~3–5 seconds) and more configuration than pandas or DuckDB. For a 278,953-row dataset on a laptop, a pandas implementation would run faster in absolute wall-clock time. This cost is acceptable for two reasons: first, the pipeline is designed to scale to the full historical archive without code changes, which PySpark provides and pandas does not; second, the course objective is to demonstrate distributed-computing tools, and a pandas pipeline would not satisfy that requirement regardless of performance.

---

## Decision 3 — Ingest pattern: batch file load over event streaming

### Situation

The M1 proposal included Apache Kafka in the Ingest layer, justified by "replay capability and a real-time upgrade path." The M1 feedback identified this as a misrepresentation: the NYPD dataset is updated quarterly by a scheduled file drop from the NYPD Office of Management Analysis and Planning. There is no event stream. Adding Kafka to a quarterly batch pipeline introduces real operational complexity — a running broker, producer and consumer processes, offset management — without solving any problem the dataset actually has.

### Alternatives evaluated

**Option A — Apache Kafka as event stream.**  
Kafka is appropriate when data arrives continuously or at high frequency and consumers must process events in near-real time. The canonical use case is a high-throughput message bus: web clickstreams, financial tick data, IoT sensor feeds. Wrapping a quarterly file drop in a Kafka producer and immediately reading it back with a Kafka consumer adds an end-to-end latency of milliseconds to a process that was already completing in minutes, and introduces a broker that must be running and healthy for the pipeline to function at all. The course concept of *stream processing* (as in the Lambda or Kappa architecture) applies to continuously arriving, time-sensitive data; it does not apply here.

**Option B — Cron job reading via REST API.**  
The NYC Open Data platform exposes a Socrata REST API that returns the dataset as JSON or CSV on demand. A cron job could call this API quarterly and pipe the result into the pipeline. This is technically viable but couples the pipeline to an external network dependency and adds JSON parsing overhead. The dataset owner already publishes a direct CSV download; using the API adds complexity without any benefit over a direct file copy.

**Option C — Direct file batch ingest (chosen).**  
PySpark reads the 61 MB CSV in a single pass in local mode, completing the ingest step in under 30 seconds on a modern laptop. The course concept of *batch processing* directly applies: the input is a bounded, finite dataset available in full at the start of the computation. Batch processing is characterised by high throughput on bounded inputs, predictable completion time, and simple fault recovery (re-run the job from the beginning). All three properties match the quarterly file-drop delivery model exactly.

### Cost and acceptability

A direct file read has no replay capability: if the pipeline fails mid-run, it re-reads the CSV from scratch. For a 61 MB file this is a non-issue; a re-run takes seconds. Even with several years of quarterly data accumulated, a full re-read of a few hundred MB completes in under two minutes on local hardware, which remains acceptable for a quarterly batch job. The forward-looking argument — "Kafka gives us a real-time upgrade path" — is not an architectural requirement for the current system. If a future version of this pipeline needed to ingest arrest records in real time (which would require a live feed from NYPD, not a quarterly file), replacing the file-read ingest step with a Kafka consumer would be a single-step change; the Store, Process, and Expose layers are already decoupled from the ingest mechanism via Parquet as the intermediate format.

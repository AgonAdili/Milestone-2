# NYPD Arrests Big Data Pipeline — Milestone 2

**Question:** Which NYPD precincts have the highest arrest concentrations in 2025, and how do offence types vary by borough and month?

**Dataset:** NYPD Arrest Data (Year to Date) — 278,953 records, 19 columns, 61 MB  
**Source:** https://data.cityofnewyork.us/Public-Safety/NYPD-Arrest-Data-Year-to-Date-/uip8-fykc/about_data

---

## How to run

### Prerequisites

**Java 17** (required by PySpark and Sedona):
```bash
brew install openjdk@17
export JAVA_HOME=/opt/homebrew/opt/openjdk@17
export PATH="$JAVA_HOME/bin:$PATH"
```

### Setup

```bash
git clone <repository-url>
cd <repository-folder>

python3 -m venv env
source env/bin/activate
pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
```

The dataset (`data/NYPD_Arrest_Data_(Year_to_Date)_20260410.csv`) is included in the repository — no separate download needed.

### Run

```bash
python3 run_pipeline.py
```

### View output

```bash
open output/hotspot_map.html
```

---

## Pipeline steps

| Step | Script | What it does |
|---|---|---|
| 1 – Ingest | `pipeline/ingest.py` | Reads CSV, validates schema and coordinates, writes raw Parquet |
| 2 – Store | `pipeline/store.py` | Converts to Snappy-compressed Parquet, partitioned by borough |
| 3 – Process | `pipeline/process.py` | Sedona ST_Within for spatial validation, H3 binning, precinct aggregations |
| 4 – Expose | `pipeline/expose.py` | Generates `output/hotspot_map.html` — heat layer + precinct circles + popups |

---

## Troubleshooting

| Error | Fix |
|---|---|
| `Unable to locate a Java Runtime` | Run `brew install openjdk@17` and set `JAVA_HOME` as above |
| `ModuleNotFoundError: No module named 'sedona'` | Activate the venv: `source env/bin/activate` |
| `FileNotFoundError` on the CSV | Check the file is in `data/` with the exact filename |
| `OutOfMemoryError` | Increase `spark.driver.memory` in `run_pipeline.py` (default 4g) |
| Spark log noise | Normal — Spark prints to stderr, pipeline output goes to stdout |

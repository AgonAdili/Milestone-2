# NYPD Arrests Big Data Pipeline — Milestone 2

**Question:** Which NYPD precincts have the highest arrest concentrations in 2025, and how do offence types vary by borough and month?

**Dataset:** NYPD Arrest Data (Year to Date) — 278,953 records, 19 columns, 61 MB  
**Source:** https://data.cityofnewyork.us/Public-Safety/NYPD-Arrest-Data-Year-to-Date-/uip8-fykc/about_data

The dataset is included in the repository — no separate download needed.

---

## Prerequisites — Java 17

PySpark and Sedona require Java 17. Install it once, before anything else.

**macOS**
```bash
brew install openjdk@17
export JAVA_HOME=/opt/homebrew/opt/openjdk@17
export PATH="$JAVA_HOME/bin:$PATH"
```
To make it permanent: add both `export` lines to `~/.zshrc`.

**Windows**
1. Download and install Java 17 from https://adoptium.net (choose *Temurin 17*, JDK, Windows x64)
2. During install, enable the option **"Set JAVA_HOME variable"** — this sets it automatically
3. Verify in a new terminal:
```
java -version
```

---

## Setup

**macOS / Linux**
```bash
git clone <repository-url>
cd <repository-folder>
python3 -m venv env
source env/bin/activate
pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
```

**Windows** (Command Prompt or PowerShell)
```
git clone <repository-url>
cd <repository-folder>
python -m venv env
env\Scripts\activate
python.exe -m pip install --upgrade pip setuptools wheel
python.exe -m pip install --only-binary :all: apache-sedona h3
python.exe -m pip install -r requirements.txt
```

> `apache-sedona` and `h3` require a C compiler if built from source on Windows.
> The `--only-binary :all:` flag forces pip to use pre-built wheels and skips compilation entirely.

---

## Run

**macOS / Linux**
```bash
python3 run_pipeline.py
```

**Windows**
```
python run_pipeline.py
```

---

## View output

**macOS**
```bash
open output/hotspot_map.html
```

**Windows**
```
start output\hotspot_map.html
```

Or open `output/hotspot_map.html` directly in any browser.

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
| `Unable to locate a Java Runtime` | Java 17 is not installed or JAVA_HOME is not set — follow the prerequisites above |
| `UnsupportedClassVersionError` | Wrong Java version is active — verify `java -version` shows 17, then set JAVA_HOME |
| `ModuleNotFoundError: No module named 'sedona'` | Virtual environment is not activated — run `source env/bin/activate` (Mac) or `env\Scripts\activate` (Windows) |
| `FileNotFoundError` on the CSV | The file must be at `data/NYPD_Arrest_Data_(Year_to_Date)_20260410.csv` |
| `OutOfMemoryError` | Increase `spark.driver.memory` in `run_pipeline.py` (default 4g) |
| Spark log noise | Normal — Spark prints INFO to stderr; pipeline output goes to stdout |

# NYPD Arrests Big Data Pipeline — Milestone 2

**Question:** Which NYPD precincts have the highest arrest concentrations in 2025, and how do offence types vary by borough and month?

**Dataset:** NYPD Arrest Data (Year to Date) — 278,953 records, 19 columns, 61 MB  
**Source:** https://data.cityofnewyork.us/Public-Safety/NYPD-Arrest-Data-Year-to-Date-/uip8-fykc/about_data

The dataset is included in the repository — no separate download needed.

---

## Prerequisites

### Java 17

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

### Hadoop winutils (Windows only)

PySpark on Windows needs `winutils.exe` to handle file operations. This is not required on Mac or Linux.

1. Download the pre-built Hadoop 3.3.x binaries:
   - Go to https://github.com/cdarlint/winutils
   - Open the `hadoop-3.3.5/bin/` folder and download every file in it
   - Create the folder `C:\hadoop\bin\` and place all downloaded files there

   The result should be: `C:\hadoop\bin\winutils.exe` (and several other files alongside it).

2. Set `HADOOP_HOME` permanently so it survives reboots:
   - Open **Settings → System → About → Advanced system settings → Environment Variables**
   - Under *User variables*, click **New**:
     - Variable name: `HADOOP_HOME`
     - Variable value: `C:\hadoop`
   - Click OK, then open a **new** terminal to pick up the change

3. Verify:
   ```powershell
   $env:HADOOP_HOME                                    # should print C:\hadoop
   Test-Path "$env:HADOOP_HOME\bin\winutils.exe"       # should print True
   ```

> **Note:** If you extract to a different path than `C:\hadoop`, update `HADOOP_HOME` accordingly. The pipeline reads it automatically — no code changes needed.

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

**Windows** (PowerShell)
```powershell
git clone <repository-url>
cd <repository-folder>
python -m venv env
env\Scripts\activate
python -m pip install --upgrade pip setuptools wheel
python -m pip install -r requirements.txt
```

> **If pip fails to build `apache-sedona` or `h3` from source**, install Microsoft Visual C++ Build Tools:
> 1. Download from https://visualstudio.microsoft.com/visual-cpp-build-tools/
> 2. Run the installer, select **"Desktop development with C++"** and **"CMake tools for Windows"**
> 3. Restart your terminal and re-run the pip install command

---

## Run

Activate your virtual environment first, then:

**macOS / Linux**
```bash
source env/bin/activate
python3 run_pipeline.py
```

**Windows**
```powershell
env\Scripts\activate
python run_pipeline.py
```

The pipeline prints progress for each of the four steps and takes roughly 15–30 seconds end-to-end on a modern laptop.

---

## View output

**macOS**
```bash
open output/hotspot_map.html
```

**Windows**
```powershell
start output\hotspot_map.html
```

Or open `output/hotspot_map.html` directly in any browser.

---

## Pipeline steps

| Step | Script | What it does |
|---|---|---|
| 1 – Ingest | `pipeline/ingest.py` | Reads CSV, validates schema and coordinates, writes raw Parquet |
| 2 – Store | `pipeline/store.py` | Converts to Snappy-compressed Parquet, partitioned by borough |
| 3 – Process | `pipeline/process.py` | Sedona ST_Within spatial validation, H3 hexagonal binning, precinct aggregations |
| 4 – Expose | `pipeline/expose.py` | Generates `output/hotspot_map.html` — heat layer + precinct circles + popups |

---

## Troubleshooting

| Error | Fix |
|---|---|
| `Unable to locate a Java Runtime` | Java 17 is not installed or `JAVA_HOME` is not set — follow the Java prerequisites above |
| `UnsupportedClassVersionError` | Wrong Java version is active — verify `java -version` shows 17 |
| `ModuleNotFoundError: No module named 'pyspark'` | Virtual environment is not activated — run `source env/bin/activate` (Mac) or `env\Scripts\activate` (Windows) |
| `ModuleNotFoundError: No module named 'sedona'` | Same as above, or run `pip install -r requirements.txt` inside the activated env |
| `HADOOP_HOME and hadoop.home.dir are unset` *(Windows)* | `HADOOP_HOME` is not set — follow the Hadoop winutils section above, then open a **new** terminal |
| `Test-Path "$env:HADOOP_HOME\bin\winutils.exe"` returns `False` *(Windows)* | The files were not extracted to the right folder — `winutils.exe` must be at `C:\hadoop\bin\winutils.exe` |
| `FileNotFoundError` on the CSV | The dataset must be at `data/NYPD_Arrest_Data_(Year_to_Date)_20260410.csv` |
| `OutOfMemoryError` | Increase `spark.driver.memory` in `run_pipeline.py` (default `4g`) |
| Spark log noise / temp-file warnings at exit | Normal — harmless cleanup messages from the JVM on Windows |

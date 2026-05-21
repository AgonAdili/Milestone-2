from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"

RAW_CSV = DATA_DIR / "NYPD_Arrest_Data_(Year_to_Date)_20260410.csv"
RAW_PARQUET = str(DATA_DIR / "raw" / "arrests_raw.parquet")
PROCESSED_PARQUET = str(DATA_DIR / "processed" / "arrests.parquet")
AGGREGATED_DIR = str(DATA_DIR / "aggregated")
MAP_OUTPUT = str(OUTPUT_DIR / "hotspot_map.html")

# NYC bounding box (WGS84, lon/lat order to match the Location WKT column)
NYC_BBOX_WKT = "POLYGON((-74.3 40.4, -73.65 40.4, -73.65 40.95, -74.3 40.95, -74.3 40.4))"

BOROUGH_NAMES = {"B": "Bronx", "K": "Brooklyn", "M": "Manhattan", "Q": "Queens", "S": "Staten Island"}

# H3 resolution 8 ≈ 460 m average edge length (city-block granularity)
H3_RESOLUTION = 8

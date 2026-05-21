"""
Step 4 – Expose
Reads the aggregated Parquet files produced by Step 3 (with pandas, since
the data is now small enough) and writes a self-contained HTML map using
Folium.  No server is required: a reviewer opens the file in any browser.

Map layers:
  • HeatMap     – H3 cell centroids weighted by arrest count
  • CircleMarker per precinct – radius ∝ total arrests, colour = dominant
    offence category (red=Felony, orange=Misdemeanour, yellow=Violation)
  • Popup on click – precinct stats (total / F / M / V)
"""

from pathlib import Path
import pandas as pd
import folium
from folium.plugins import HeatMap

from pipeline.config import BOROUGH_NAMES

# Colour scheme matches standard law-severity convention used by NYC Open Data
CATEGORY_COLOUR = {"F": "#d73027", "M": "#fc8d59", "V": "#fee090"}
CATEGORY_LABEL  = {"F": "Felony", "M": "Misdemeanour", "V": "Violation"}


def _scale_radius(arrests: int, max_arrests: int) -> float:
    return 4 + (arrests / max_arrests) * 22  # 4 – 26 px


def run_expose(aggregated_dir: str, output_path: str) -> None:
    precinct_df = pd.read_parquet(f"{aggregated_dir}/precinct_summary.parquet")
    h3_df       = pd.read_parquet(f"{aggregated_dir}/h3_density.parquet")
    top10_df    = pd.read_parquet(f"{aggregated_dir}/top10_precincts.parquet")
    monthly_df  = pd.read_parquet(f"{aggregated_dir}/monthly_trend.parquet")

    print(f"  Precincts to map: {len(precinct_df)}")
    print(f"  H3 cells to map:  {len(h3_df)}")

    # --- build map -----------------------------------------------------------
    m = folium.Map(
        location=[40.73, -73.95],
        zoom_start=11,
        tiles="CartoDB positron",
        control_scale=True,
    )

    # Layer 1: heat map from H3 density
    heat_data = (
        h3_df[h3_df["arrest_count"] >= 3]   # suppress single-record noise
        [["cell_lat", "cell_lon", "arrest_count"]]
        .dropna()
        .values
        .tolist()
    )
    HeatMap(
        heat_data,
        name="Arrest density (H3 heat map)",
        min_opacity=0.25,
        max_zoom=15,
        radius=18,
        blur=12,
    ).add_to(m)

    # Layer 2: precinct circles
    precinct_layer = folium.FeatureGroup(name="Precincts (circle = arrest volume)", show=True)
    max_arrests = int(precinct_df["total_arrests"].max())

    for _, row in precinct_df.dropna(subset=["centroid_lat", "centroid_lon"]).iterrows():
        boro_name = BOROUGH_NAMES.get(str(row["ARREST_BORO"]), str(row["ARREST_BORO"]))
        cat = str(row.get("dominant_cat", "M"))
        colour = CATEGORY_COLOUR.get(cat, "#999999")
        radius = _scale_radius(int(row["total_arrests"]), max_arrests)

        popup_html = (
            f"<b>Precinct {int(row['ARREST_PRECINCT'])}</b> – {boro_name}<br>"
            f"Total arrests: <b>{int(row['total_arrests']):,}</b><br>"
            f"Felonies:       {int(row['felonies']):,}<br>"
            f"Misdemeanours:  {int(row['misdemeanors']):,}<br>"
            f"Violations:     {int(row['violations']):,}<br>"
            f"Dominant: <b>{CATEGORY_LABEL.get(cat, cat)}</b>"
        )
        folium.CircleMarker(
            location=[float(row["centroid_lat"]), float(row["centroid_lon"])],
            radius=radius,
            color=colour,
            fill=True,
            fill_color=colour,
            fill_opacity=0.55,
            weight=1.2,
            popup=folium.Popup(popup_html, max_width=220),
            tooltip=f"Precinct {int(row['ARREST_PRECINCT'])} – {int(row['total_arrests']):,} arrests",
        ).add_to(precinct_layer)

    precinct_layer.add_to(m)

    # Layer 3: top-10 labels
    top10_layer = folium.FeatureGroup(name="Top-10 hotspot labels", show=True)
    for rank, (_, row) in enumerate(
        top10_df.sort_values("total_arrests", ascending=False)
               .dropna(subset=["centroid_lat", "centroid_lon"])
               .iterrows(),
        start=1,
    ):
        folium.Marker(
            location=[float(row["centroid_lat"]), float(row["centroid_lon"])],
            icon=folium.DivIcon(
                html=f'<div style="font-size:10px;font-weight:bold;color:#222;'
                     f'background:rgba(255,255,255,0.75);padding:1px 3px;'
                     f'border-radius:3px;">#{rank} P{int(row["ARREST_PRECINCT"])}</div>',
                icon_size=(60, 18),
                icon_anchor=(30, 9),
            ),
        ).add_to(top10_layer)
    top10_layer.add_to(m)

    # Legend
    legend_html = """
    <div style="position:fixed;bottom:30px;left:30px;z-index:1000;
                background:white;padding:10px 14px;border-radius:6px;
                border:1px solid #ccc;font-size:12px;line-height:1.7;">
      <b>NYPD Arrest Hotspots 2025</b><br>
      Circle size = arrest volume<br>
      <span style="color:#d73027;">&#9679;</span> Dominant: Felony<br>
      <span style="color:#fc8d59;">&#9679;</span> Dominant: Misdemeanour<br>
      <span style="color:#fee090;">&#9679;</span> Dominant: Violation<br>
      Background = H3 arrest density
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))

    # Title
    title_html = """
    <div style="position:fixed;top:10px;left:50%;transform:translateX(-50%);
                z-index:1000;background:rgba(255,255,255,0.9);
                padding:6px 16px;border-radius:6px;border:1px solid #ccc;
                font-size:14px;font-weight:bold;">
      NYPD Arrest Hotspots by Precinct — 2025 (Year to Date)
    </div>
    """
    m.get_root().html.add_child(folium.Element(title_html))

    folium.LayerControl(collapsed=False).add_to(m)

    # --- monthly summary to console ------------------------------------------
    print("\n  Monthly arrest counts by borough (sample):")
    summary = (monthly_df
               .groupby(["ARREST_BORO", "ARREST_MONTH"])["arrest_count"]
               .sum()
               .reset_index()
               .sort_values(["ARREST_BORO", "ARREST_MONTH"]))
    for boro, grp in summary.groupby("ARREST_BORO"):
        name = BOROUGH_NAMES.get(str(boro), boro)
        counts = " | ".join(f"M{int(r.ARREST_MONTH)}:{int(r.arrest_count):,}" for _, r in grp.iterrows())
        print(f"    {name}: {counts}")

    # --- save ----------------------------------------------------------------
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    m.save(output_path)
    print(f"\n  Map saved: {output_path}")
    print(f"  Open with: open \"{output_path}\"")

import json
import os

import geopandas as gpd
import pandas as pd
import pydeck as pdk
import requests
from shiny import App, reactive, render, ui

from gridcast.config import EXTERNAL_DATA_DIR

API_URL = os.getenv("API_URL", "http://localhost:8000")

# sequential blue ramp, light->dark (references/palette.md in the dataviz skill)
HEAT_LOW = (0xCD, 0xE2, 0xFB)
HEAT_HIGH = (0x0D, 0x36, 0x6B)


def heat_color(value: float, vmin: float, vmax: float) -> list[int]:
    t = 0.0 if vmax == vmin else (value - vmin) / (vmax - vmin)
    rgb = [round(lo + t * (hi - lo)) for lo, hi in zip(HEAT_LOW, HEAT_HIGH)]
    return [*rgb, 140]

app_ui = ui.page_fluid(
    ui.h2("gridcast — PJM zonal load forecast"),
    ui.input_select("horizon", "Horizon", choices=["1h", "24h", "72h"]),
    ui.output_text("as_of"),
    ui.output_ui("map"),
    ui.output_data_frame("table"),
)


def server(input, output, session):
    @reactive.calc
    def zone_geometry() -> gpd.GeoDataFrame:
        return gpd.read_file(EXTERNAL_DATA_DIR / "pjm_zones.geojson")

    @reactive.calc
    def predictions() -> pd.DataFrame:
        reactive.invalidate_later(300)
        response = requests.get(f"{API_URL}/predict", timeout=10)
        response.raise_for_status()
        return pd.DataFrame(response.json())

    @reactive.calc
    def merged() -> pd.DataFrame:
        return predictions().rename(columns={f"y_{input.horizon()}": "predicted_mw"})

    @render.text
    def as_of():
        return f"Forecasts as of {predictions()['time'].iloc[0]} (latest available data)"

    @render.ui
    def map():
        data = merged()
        zones = zone_geometry().merge(data, left_on="zone_id", right_on="zone")
        vmin, vmax = data["predicted_mw"].min(), data["predicted_mw"].max()
        zones["fill_color"] = zones["predicted_mw"].apply(lambda v: heat_color(v, vmin, vmax))
        deck = pdk.Deck(
            map_provider="carto",
            map_style="light",
            initial_view_state=pdk.ViewState(latitude=39.5, longitude=-78.5, zoom=5.5),
            layers=[
                pdk.Layer(
                    "GeoJsonLayer",
                    data=json.loads(zones.to_json()),
                    get_fill_color="properties.fill_color",
                    get_line_color=[80, 80, 80],
                    line_width_min_pixels=1,
                    stroked=True,
                    filled=True,
                    pickable=True,
                )
            ],
            tooltip={"text": "{zone}\n{predicted_mw} MW"},
        )
        return ui.tags.iframe(
            srcdoc=deck.to_html(as_string=True, notebook_display=False),
            style="border:none;width:100%;height:600px",
        )

    @render.data_frame
    def table():
        data = merged()[["zone", "predicted_mw"]].sort_values("predicted_mw", ascending=False)
        return render.DataGrid(data)


app = App(app_ui, server)

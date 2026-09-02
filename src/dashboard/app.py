import datetime
import json
import os
from pathlib import Path
import re
from zoneinfo import ZoneInfo

import geopandas as gpd
import pandas as pd
import plotly.graph_objects as go
import pydeck as pdk
from pydeck.io.html import CDN_URL as DECKGL_WIDGET_CDN_URL
import requests
from shiny import App, reactive, render, ui

from gridcast.config import EXTERNAL_DATA_DIR, PROCESSED_DATA_DIR

EASTERN = ZoneInfo("US/Eastern")

API_URL = os.getenv("API_URL", "http://localhost:8000")
# inst_load carries no settlement lag, so this threshold reflects actual pipeline
# health rather than a settled feed's expected lag - matches sources.yml's warn_after
# for the instantaneous_load source.
FRESHNESS_THRESHOLD_HOURS = 1
HORIZONS = (1, 24, 72)
DRILLDOWN_LOOKBACK_HOURS = 48
ACCURACY_WINDOW_HOURS = 72
ZONE_LINES_DEFAULT_VISIBLE = 5
EASTERN_TZ = "America/New_York"
# sentinel "zone" for the system-wide row - not a real zone_id, so it never collides
# or gets picked for map highlighting (no polygon represents it)
PJM_ZONE = "PJM"

# sequential blue ramp, light->mid->high (palette.md steps 100/400/700) - the 400 step
# is also the "actual load" line color below, so the drill-down chart's blue is
# literally what "50% of peak" looks like on the map.
HEAT_LOW = (0xCD, 0xE2, 0xFB)
HEAT_MID = (0x39, 0x87, 0xE5)
HEAT_HIGH = (0x0D, 0x36, 0x6B)
HEAT_LOW_HEX = "#cde2fb"
HEAT_MID_HEX = "#3987e5"
HEAT_HIGH_HEX = "#0d366b"
UNSELECTED_GREY = [190, 190, 190, 140]
HIGHLIGHT_OUTLINE = [255, 255, 255, 220]
HIGHLIGHT_HALO = [0, 0, 0, 255]

# actual/predicted colors, reused across the drill-down chart, CSS, and the map:
# actual = heat ramp's 50%-of-peak stop (HEAT_MID), predicted = page accent color.
LINE_ACTUAL_HEX = "#3987e5"
LINE_PREDICTED_LIGHT_HEX = "#ed7a4c"
LINE_PREDICTED_DARK_HEX = "#de6d40"
# inst_load: a proxy series, not the headline actual/predicted pair - green reads as a
# clearly third hue against the blue/orange above in both themes.
LINE_INSTLOAD_HEX = "#3fa66b"

# chart chrome, light/dark - mirrors the CSS custom properties below so plotly iframes
# (can't see page CSS) track OS theme via their own prefers-color-scheme bridge script.
SURFACE_LIGHT, SURFACE_DARK = "#fcfcfb", "#1a1a19"
INK_LIGHT, INK_DARK = "#0b0b0b", "#ffffff"
INK_SECONDARY_LIGHT, INK_SECONDARY_DARK = "#52514e", "#c3c2b7"
GRIDLINE_LIGHT, GRIDLINE_DARK = "#e1e0d9", "#2c2c2a"

# zone identity -> full name, for the per-zone chart's legend (geojson only has zone_id)
ZONE_NAMES = (
    pd.read_csv(PROCESSED_DATA_DIR / "pjm_eia930_subregions.csv", usecols=["id", "name"])
    .set_index("id")["name"]
    .to_dict()
)
# fixed alphabetical order for color assignment - color follows the zone, not its
# current MW rank, so a line's color doesn't repaint every refresh
ZONE_ORDER = sorted(ZONE_NAMES)
# palette slots 3-8 (slots 1/2 = blue/orange, reserved for actual/predicted above).
# Only 6 hues for 20 zones, so hue repeats every 6th zone in ZONE_ORDER - a dash cycle
# (solid/dash/dot/dashdot) disambiguates repeats; legend text is the real identity source.
ZONE_LINE_HUES = ["#3dbb8e", "#eea914", "#eb8fb2", "#269626", "#6558b4", "#e76463"]
ZONE_LINE_DASHES = ["solid", "dash", "dot", "dashdot"]


def _zone_line_style(zone: str) -> tuple[str, str]:
    rank = ZONE_ORDER.index(zone)
    hue = ZONE_LINE_HUES[rank % len(ZONE_LINE_HUES)]
    dash = ZONE_LINE_DASHES[(rank // len(ZONE_LINE_HUES)) % len(ZONE_LINE_DASHES)]
    return hue, dash


def fmt_mw(value: float) -> str:
    """3 decimal places, comma thousands separator - the general MW display format."""
    return f"{value:,.3f}"


def fmt_mw_conditional(value: float) -> str:
    """Same 3-decimal precision, but the thousands comma only kicks in above 9999 MW
    (used on the actual-vs-predicted plot's hover text, per design spec)."""
    return f"{value:,.3f}" if abs(value) > 9999 else f"{value:.3f}"


def to_eastern(series: pd.Series) -> pd.Series:
    return series.dt.tz_convert(EASTERN_TZ)


def eastern_label(ts: pd.Timestamp) -> str:
    return f"{ts:%m-%d %-I:%M%p}"


def c_to_f(value_c: float) -> float:
    return value_c * 9 / 5 + 32


def kmh_to_mph(value_kmh: float) -> float:
    return value_kmh * 0.621371


# No weather code in this schema (temp/precipitation/wind/cloud only), so condition is
# inferred from precipitation + cloud_cover. 0.1mm floor filters sensor noise on dry readings.
def weather_condition(temp_c: float, precipitation_mm: float, cloud_cover_pct: float) -> str:
    if precipitation_mm > 0.1:
        return "snow" if temp_c <= 0 else "rain"
    if cloud_cover_pct >= 85:
        return "cloudy"
    if cloud_cover_pct >= 40:
        return "partly_cloudy"
    return "clear"


WEATHER_CONDITION_LABELS = {
    "clear": "Clear",
    "partly_cloudy": "Partly cloudy",
    "cloudy": "Cloudy",
    "rain": "Rain",
    "snow": "Snow",
}

# icons built from primitive shapes, not a copied icon-set - cloud drawn last in
# "partly_cloudy" so it covers the small sun behind it
_CLOUD_ICON = (
    '<circle cx="9" cy="13.5" r="3.1"/><circle cx="13.6" cy="10.8" r="4"/>'
    '<rect x="6" y="13.5" width="12.4" height="5" rx="2.5"/>'
)
_SUN_ICON = (
    '<circle cx="12" cy="12" r="4"/>'
    '<line x1="12" y1="2" x2="12" y2="4.5"/><line x1="12" y1="19.5" x2="12" y2="22"/>'
    '<line x1="2" y1="12" x2="4.5" y2="12"/><line x1="19.5" y1="12" x2="22" y2="12"/>'
    '<line x1="4.9" y1="4.9" x2="6.6" y2="6.6"/><line x1="17.4" y1="17.4" x2="19.1" y2="19.1"/>'
    '<line x1="4.9" y1="19.1" x2="6.6" y2="17.4"/><line x1="17.4" y1="6.6" x2="19.1" y2="4.9"/>'
)
_SMALL_SUN_ICON = (
    '<circle cx="8" cy="8" r="2.6"/>'
    '<line x1="8" y1="2.5" x2="8" y2="4.2"/><line x1="2.5" y1="8" x2="4.2" y2="8"/>'
    '<line x1="4.1" y1="4.1" x2="5.3" y2="5.3"/>'
)
_RAIN_DROPS_ICON = (
    '<line x1="8" y1="19.3" x2="7" y2="21.8"/><line x1="12" y1="19.3" x2="11" y2="21.8"/>'
    '<line x1="16" y1="19.3" x2="15" y2="21.8"/>'
)
_SNOW_FLAKES_ICON = (
    '<circle cx="8" cy="20.2" r="0.9" fill="currentColor" stroke="none"/>'
    '<circle cx="12" cy="21.2" r="0.9" fill="currentColor" stroke="none"/>'
    '<circle cx="16" cy="20.2" r="0.9" fill="currentColor" stroke="none"/>'
)
WEATHER_ICON_INNER = {
    "clear": _SUN_ICON,
    "partly_cloudy": _SMALL_SUN_ICON + _CLOUD_ICON,
    "cloudy": _CLOUD_ICON,
    "rain": _CLOUD_ICON + _RAIN_DROPS_ICON,
    "snow": _CLOUD_ICON + _SNOW_FLAKES_ICON,
}


def weather_icon_svg(condition: str) -> ui.HTML:
    return ui.HTML(
        '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" '
        f'stroke-linecap="round" stroke-linejoin="round">{WEATHER_ICON_INNER[condition]}</svg>'
    )

# pydeck's to_html() hardcodes this CDN script src into every map's HTML (not exposed as
# a URL param) - matched here so it can be swapped for the self-hosted copy below
# regardless of which mapbox-gl-js version the installed pydeck's template points at.
MAPBOX_GL_CDN_PATTERN = re.compile(r"https://api\.tiles\.mapbox\.com/mapbox-gl-js/[^\"']+/mapbox-gl\.js")

# Injected into the deck.gl iframe: pydeck's static to_html() has no built-in click
# bridge to Python, but @deck.gl/jupyter-widget's createDeck() accepts a handleEvent()
# callback - reused here to postMessage onClick to the parent page instead.
MAP_CLICK_BRIDGE_JS = """
function gridcastHandleMapEvent(eventName, info) {
  if (eventName === 'deck-click-event' && info && info.object && info.object.properties) {
    var zone = info.object.properties.zone;
    if (zone) {
      window.parent.postMessage({source: 'gridcast-map-click', zone: zone}, '*');
    }
  }
}
"""

CSS = """
:root {
  --bg: #f9f9f7;
  --surface: #fcfcfb;
  --ink: #0b0b0b;
  --ink-secondary: #52514e;
  --ink-muted: #898781;
  --gridline: #e1e0d9;
  --border: rgba(11,11,11,.10);
  --accent: #ed7a4c;
  --accent-yellow: #e8cf6b;
  --status-live: #0ca30c;
  --line-actual: #3987e5;
  --line-predicted: #ed7a4c;
  --line-instload: #3fa66b;
}
@media (prefers-color-scheme: dark) {
  :root {
    --bg: #0d0d0d;
    --surface: #1a1a19;
    --ink: #ffffff;
    --ink-secondary: #c3c2b7;
    --ink-muted: #898781;
    --gridline: #2c2c2a;
    --border: rgba(255,255,255,.10);
    --accent: #de6d40;
    --accent-yellow: #cbb254;
    --line-actual: #3987e5;
    --line-predicted: #de6d40;
    --line-instload: #3fa66b;
  }
}
* { box-sizing: border-box; }
body {
  font-family: Inter, -apple-system, "system-ui", "Segoe UI", Roboto, sans-serif;
  background: var(--bg);
  color: var(--ink);
}
.app-shell { max-width: 1200px; margin: 0 auto; padding: 24px; display: flex; flex-direction: column; gap: 20px; }
.card { background: var(--surface); border: 1px solid var(--border); border-radius: 12px; padding: 16px; }
.topbar {
  display: flex; flex-direction: row; align-items: center; justify-content: space-between;
  flex-wrap: wrap; gap: 12px;
}
.topbar-title { display: flex; flex-direction: column; gap: 2px; }
.app-title { margin: 0; font-size: 1.3rem; font-weight: 700; }
.app-subtitle { color: var(--ink-muted); font-size: .85rem; }
.app-credit { color: inherit; text-decoration: underline; }
.app-footer { text-align: center; color: var(--ink-muted); font-size: .8rem; padding: 4px 0; }
.topbar-controls { display: flex; align-items: center; gap: 16px; }
.topbar-controls .shiny-input-container { margin-bottom: 0; }
.freshness { display: flex; align-items: center; gap: 8px; font-size: .85rem; color: var(--ink-secondary); }
.status-dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; }
.status-live { background: var(--status-live); }
.status-stale { background: var(--ink-muted); }
.accuracy-row-inner { display: flex; flex-direction: row; gap: 16px; }
.zone-chart-row { display: flex; gap: 16px; align-items: stretch; }
.zone-chart-plot { flex: 1; min-width: 0; }
.zone-chart-legend {
  flex: 0 0 220px; max-height: 440px; overflow-y: auto; font-size: .78rem;
  color: var(--ink-secondary); border-left: 1px solid var(--border); padding-left: 12px;
}
.zone-legend-row {
  display: flex; align-items: center; gap: 6px; padding: 3px 4px; border-radius: 4px;
  cursor: pointer;
}
.zone-legend-row:hover { background: var(--bg); }
.zone-legend-swatch { display: inline-block; width: 14px; height: 3px; flex: 0 0 auto; }
.zone-legend-label { overflow-wrap: anywhere; }
.stat-tile {
  flex: 1 1 0; min-width: 0; background: var(--surface); border: 1px solid var(--border);
  border-top: 3px solid var(--accent); border-radius: 10px; padding: 10px 14px;
}
.stat-label { color: var(--ink-muted); font-size: .75rem; text-transform: uppercase; letter-spacing: .04em; }
.stat-value { font-size: 1.4rem; font-weight: 600; margin-top: 2px; }
.stat-sub { color: var(--ink-secondary); font-size: .8rem; margin-top: 2px; }
.main-row { display: flex; gap: 20px; align-items: stretch; }
.main-row .card { margin: 0; }
.map-card { flex: 2; min-width: 0; }
.right-column { display: flex; flex-direction: column; gap: 20px; flex: 1; min-width: 0; }
.table-card { min-width: 0; }
.weather-card { display: flex; flex-direction: column; flex: 1; }
.weather-card h4 { margin: 0 0 10px; flex: 0 0 auto; }
.weather-body {
  flex: 1; display: flex; align-items: center; justify-content: space-between; gap: 16px;
}
.weather-main { display: flex; align-items: center; gap: 14px; }
.weather-icon { color: var(--accent); flex: 0 0 auto; line-height: 0; }
.weather-icon svg { display: block; width: 46px; height: 46px; }
.weather-temp { font-size: 1.7rem; font-weight: 600; }
.weather-condition { color: var(--ink-secondary); font-size: .85rem; margin-top: 2px; }
.weather-stats { display: flex; flex-direction: column; align-items: flex-end; gap: 8px; flex: 0 0 auto; }
.map-header h4 { margin: 0 0 2px; }
.map-toolbar { margin-bottom: 8px; display: flex; align-items: center; gap: 16px; flex-wrap: wrap; }
.map-frame { position: relative; }
.map-label {
  position: absolute; top: 12px; left: 12px; z-index: 5;
  background: var(--surface); border: 1px solid var(--border); border-radius: 8px;
  padding: 6px 12px; font-size: .85rem; font-weight: 600; color: var(--ink);
  box-shadow: 0 1px 4px rgba(0,0,0,.15);
}
.map-legend { display: flex; align-items: center; gap: 8px; margin-top: 10px; font-size: .75rem; color: var(--ink-muted); }
.legend-ramp { flex: 1; height: 8px; border-radius: 999px; border: 1px solid var(--border); }
.drilldown-header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 8px; }
.drilldown-header h4 { margin: 0; }
.hint { color: var(--ink-muted); font-size: .8rem; margin: 0 0 8px; }
.info-tip {
  position: relative; display: inline-flex; align-items: center; justify-content: center;
  width: 15px; height: 15px; margin-left: 6px; border-radius: 50%; vertical-align: middle; top: -2px;
  background: var(--bg); border: 1px solid var(--border); color: var(--ink-muted);
  font-size: .68rem; font-weight: 600; cursor: help;
}
.info-tip .tip-text {
  visibility: hidden; opacity: 0; position: absolute; bottom: 130%; left: 50%;
  transform: translateX(-50%); background: var(--ink); color: var(--surface);
  font-size: .75rem; font-weight: 400; text-align: left; white-space: nowrap;
  padding: 6px 10px; border-radius: 6px; z-index: 10; transition: opacity .15s;
}
.info-tip:hover .tip-text { visibility: visible; opacity: 1; }
.badge {
  background: var(--bg); border: 1px solid var(--border); border-radius: 999px;
  padding: 3px 10px; font-size: .8rem; color: var(--ink-secondary);
}
.info-card { border-top: 3px solid var(--accent-yellow); }
.info-card h4 { margin: 0 0 12px; }
.info-row-list { display: flex; flex-direction: column; gap: 12px; }
.info-row { display: flex; align-items: flex-start; gap: 10px; }
.info-dot {
  flex: 0 0 auto; width: 8px; height: 8px; margin-top: 6px; border-radius: 50%;
  background: var(--accent-yellow);
}
.info-title { font-weight: 600; font-size: .85rem; }
.info-desc { color: var(--ink-secondary); font-size: .8rem; margin-top: 2px; }
"""


def heat_color(ratio: float, vmin: float, vmax: float) -> list[int]:
    """ratio = predicted load / zone's own recent peak, domain stretched to the
    current spread across zones so nearby values stay visually distinguishable.
    Three-stop ramp (low->mid->high) so t=0.5 always lands on HEAT_MID exactly -
    that's the fixed color the drill-down chart's "actual" line matches."""
    t = 0.0 if vmax <= vmin else (ratio - vmin) / (vmax - vmin)
    t = min(max(t, 0.0), 1.0)
    lo, hi = (HEAT_LOW, HEAT_MID) if t <= 0.5 else (HEAT_MID, HEAT_HIGH)
    local_t = t / 0.5 if t <= 0.5 else (t - 0.5) / 0.5
    rgb = [round(a + local_t * (b - a)) for a, b in zip(lo, hi)]
    return [*rgb, 180]


def accuracy_stats(history: pd.DataFrame) -> pd.DataFrame:
    """Per-horizon MAE/MAPE from a /history frame, rows without an actual yet dropped."""
    valid = history.dropna(subset=["actual_mw"])
    if valid.empty:
        return pd.DataFrame(columns=["horizon_h", "mae", "mape"])
    error = valid["actual_mw"] - valid["predicted_mw"]
    valid = valid.assign(abs_error=error.abs(), abs_pct_error=(error / valid["actual_mw"]).abs() * 100)
    return valid.groupby("horizon_h", as_index=False).agg(mae=("abs_error", "mean"), mape=("abs_pct_error", "mean"))


app_ui = ui.page_fluid(
    ui.head_content(
        ui.tags.link(rel="icon", type="image/svg+xml", href="favicon.svg"),
        ui.tags.style(CSS),
        ui.tags.script(
            """
            // Bootstrap ships a full [data-bs-theme="dark"] palette (and sets
            // color-scheme: dark, which is what themes native scrollbars/form
            // chrome) but only applies it once that attribute is set - it doesn't
            // follow prefers-color-scheme on its own. Mirrors the page's own
            // @media (prefers-color-scheme: dark) switch onto <html> so Shiny's
            // native widgets (select, switch, data grid) track the same theme.
            function gridcastApplyBsTheme(dark) {
              document.documentElement.setAttribute('data-bs-theme', dark ? 'dark' : 'light');
            }
            var gridcastBsMql = window.matchMedia('(prefers-color-scheme: dark)');
            gridcastApplyBsTheme(gridcastBsMql.matches);
            gridcastBsMql.addEventListener('change', function(e) { gridcastApplyBsTheme(e.matches); });

            window.addEventListener('message', function(event) {
              if (event.data && event.data.source === 'gridcast-map-click') {
                Shiny.setInputValue('map_click_zone', event.data.zone, {priority: 'event'});
              }
            });
            """
        ),
    ),
    ui.div(
        ui.div(
            ui.div(
                ui.h2("gridcast", class_="app-title"),
                ui.span(
                    "PJM zonal demand forecast · by ",
                    ui.tags.a(
                        "Angie Ohaeri",
                        href="https://github.com/angieohaeri/bikeshare",
                        target="_blank",
                        rel="noopener noreferrer",
                        class_="app-credit",
                    ),
                    class_="app-subtitle",
                ),
                class_="topbar-title",
            ),
            ui.div(
                ui.input_select("horizon", None, choices=["1h", "24h", "72h"], selected="24h"),
                ui.output_ui("freshness"),
                class_="topbar-controls",
            ),
            class_="topbar card",
        ),
        ui.div(
            ui.h5("Project info"),
            ui.div(
                ui.div(
                    ui.div(class_="info-dot"),
                    ui.div(
                        ui.div("Data sources", class_="info-title"),
                        ui.div(
                            "PJM is a regional organization that manages the movement of electricity."
                            " Live energy metrics and weather data from 20 zones in the PJM are ingested through Apache Kafka into TimescaleDB.",
                            class_="info-desc",
                        ),
                    ),
                    class_="info-row",
                ),
                ui.div(
                    ui.div(class_="info-dot"),
                    ui.div(
                        ui.div("Model", class_="info-title"),
                        ui.div(
                            "A single LightGBM model is continously trained across all 20 PJM zones"
                            ", forecasting power demand 1h, 24h, and 72h ahead.",
                            class_="info-desc",
                        ),
                    ),
                    class_="info-row",
                ),
                ui.div(
                    ui.div(class_="info-dot"),
                    ui.div(
                        ui.div("Update cadence", class_="info-title"),
                        ui.div(
                            "This dashboard refreshes every 5 minutes. PJM's settled zonal demand "
                            "feed lags 2-3 days behind by design, so actual/predicted comparisons "
                            "trail that far. The Live/Stale badge above tracks instantaneous load, "
                            "PJM's unverified telemetry, which has no settlement lag. ",
                            class_="info-desc",
                        ),
                    ),
                    class_="info-row",
                ),
                class_="info-row-list",
            ),
            class_="card info-card",
        ),
        ui.output_ui("accuracy_panel"),
        ui.div(
            ui.h4("Predicted demand by zone", style="margin-top:0"),
            ui.p(
                "Predicted demand for each zone at the selected horizon. Click a zone in the "
                "legend to toggle it on or off.",
                class_="hint",
            ),
            ui.div(
                ui.div(ui.output_ui("zone_lines_chart"), class_="zone-chart-plot"),
                ui.output_ui("zone_lines_legend"),
                class_="zone-chart-row",
            ),
            class_="card",
        ),
        ui.div(
            ui.div(
                ui.div(
                    ui.h4("PJM zonal demand map"),
                    ui.p(
                        "Predicted demand per zone, colored by how close each zone is to its "
                        "own 30-day peak.",
                        class_="hint",
                    ),
                    class_="map-header",
                ),
                ui.div(
                    ui.input_switch("mode_3d", "3D view", value=False),
                    ui.panel_conditional(
                        "input.mode_3d",
                        ui.input_slider("bearing", "Rotate", min=0, max=360, value=0, step=5),
                        ui.span(
                            "Or drag with right-click / two fingers to rotate & tilt.",
                            class_="hint",
                            style="margin:0",
                        ),
                    ),
                    class_="map-toolbar",
                ),
                ui.output_ui("map"),
                class_="card map-card",
            ),
            ui.div(
                ui.div(
                    ui.h4("Zones", style="margin-top:0"),
                    ui.p("Select a zone to see a breakdown view.", class_="hint"),
                    ui.output_data_frame("table"),
                    class_="card table-card",
                ),
                ui.output_ui("weather_card"),
                class_="right-column",
            ),
            class_="main-row",
        ),
        ui.div(ui.output_ui("drilldown"), class_="card drilldown-card"),
        ui.div(
            "Created by Angie Ohaeri • ",
            ui.tags.a(
                "GitHub",
                href="https://github.com/angieohaeri/bikeshare",
                target="_blank",
                rel="noopener noreferrer",
                class_="app-credit",
            ),
            " • ",
            ui.tags.a(
                "LinkedIn",
                href='www.linkedin.com/in/angie-ohaeri-ba076b276',
                target='_blank',
                rel='noopener noreferrer',
                class_='app-credit',
            ), 
            f" • Published: {datetime.datetime(year=2026, month=8, day=14, tzinfo=EASTERN).date()}",
            f" • Republished: {datetime.datetime.now(EASTERN)}",
            class_="app-footer",
        ),
        class_="app-shell",
    ),
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
        df = pd.DataFrame(response.json())
        df["time"] = pd.to_datetime(df["time"], utc=True)
        return df

    @reactive.calc
    def merged() -> pd.DataFrame:
        return predictions().rename(columns={f"y_{input.horizon()}": "predicted_mw"})

    @reactive.calc
    def peaks() -> pd.DataFrame:
        reactive.invalidate_later(3600)
        response = requests.get(f"{API_URL}/peak", timeout=10)
        response.raise_for_status()
        return pd.DataFrame(response.json())

    @reactive.calc
    def system_history() -> pd.DataFrame:
        reactive.invalidate_later(300)
        # window has to cover the accuracy window PLUS the longest horizon, since a
        # 72h-ahead prediction made near the start of the window only lands an
        # actual to compare against near the window's end
        hours = ACCURACY_WINDOW_HOURS + max(HORIZONS)
        response = requests.get(f"{API_URL}/history", params={"hours": hours}, timeout=10)
        response.raise_for_status()
        df = pd.DataFrame(response.json())
        df["time"] = pd.to_datetime(df["time"], utc=True)
        return df

    @reactive.calc
    def inst_load_history() -> pd.DataFrame:
        reactive.invalidate_later(300)
        # same window as system_history(), for a consistent cache/refresh cadence
        hours = ACCURACY_WINDOW_HOURS + max(HORIZONS)
        response = requests.get(f"{API_URL}/inst_load_history", params={"hours": hours}, timeout=10)
        response.raise_for_status()
        df = pd.DataFrame(response.json())
        if df.empty:
            return pd.DataFrame({
                "time": pd.Series(dtype="datetime64[ns, UTC]"),
                "zone": pd.Series(dtype="object"),
                "inst_load_mw": pd.Series(dtype="float64"),
            })
        df["time"] = pd.to_datetime(df["time"], utc=True)
        return df

    @reactive.calc
    def table_data() -> pd.DataFrame:
        # real zones only - PJM_ZONE (the system-wide total) is a synthetic default
        # for drilldown_zone() below, not a selectable row (mixing a summed total in
        # among individually-ranked zones reads as a misleading 21st zone)
        return (
            merged()[["zone", "predicted_mw"]]
            .sort_values("predicted_mw", ascending=False)
            .reset_index(drop=True)
        )

    # None = "follow the live top-N by predicted MW" (the default); once the user
    # clicks any legend row it freezes into an explicit set they control from then on
    zone_lines_toggled = reactive.Value(None)

    @reactive.calc
    def zone_lines_visible() -> set[str]:
        manual = zone_lines_toggled.get()
        if manual is not None:
            return manual
        return set(table_data().head(ZONE_LINES_DEFAULT_VISIBLE)["zone"])

    @reactive.effect
    @reactive.event(input.zone_toggle_click)
    def _toggle_zone_line():
        zone = input.zone_toggle_click()
        current = set(zone_lines_visible())
        current.symmetric_difference_update({zone})
        zone_lines_toggled.set(current)

    @reactive.calc
    def selected_zone() -> str | None:
        data = table_data()
        rows = table.cell_selection()["rows"]
        if rows:
            return data.iloc[rows[0]]["zone"]
        return None

    @reactive.calc
    def drilldown_zone() -> str | None:
        zone = selected_zone()
        # nothing picked (or the selection was just cleared) - PJM_ZONE
        # (system-wide total) is the default
        return zone if zone is not None else PJM_ZONE

    @reactive.calc
    def weather_for_zone() -> dict | None:
        zone = drilldown_zone()
        cols = ["temperature", "precipitation", "wind_speed", "cloud_cover"]
        df = predictions()
        if zone == PJM_ZONE:
            # no single-zone reading for the system-wide default - average across
            # the 20 zones (unlike demand, which sums; weather isn't additive)
            row = df[cols].mean()
        else:
            match = df[df["zone"] == zone]
            if match.empty:
                return None
            row = match.iloc[0][cols]
        if row.isna().any():
            return None
        return row.to_dict()

    @reactive.calc
    def zone_history() -> pd.DataFrame:
        zone = drilldown_zone()
        empty = pd.DataFrame(columns=["time", "zone", "horizon_h", "actual_mw", "predicted_mw"])
        if zone is None:
            return empty
        h = int(input.horizon().removesuffix("h"))
        # same reasoning as system_history: enough lookback that the 48h window has an
        # actual to compare at every point. Always <= system_history()'s own 144h window,
        # so filtering its already-fetched frame avoids a second /history round trip.
        hours = DRILLDOWN_LOOKBACK_HOURS + h
        cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=hours)
        df = system_history()
        df = df[df["time"] >= cutoff]
        if zone == PJM_ZONE:
            # no zone filter -> all 20 real zones; summed into a system-wide total.
            # sum(min_count=1): an hour where every zone's actual is still missing
            # (the forecast tail) stays null rather than summing to a fake 0 MW
            df = df.groupby(["time", "horizon_h"], as_index=False).agg(
                actual_mw=("actual_mw", lambda s: s.sum(min_count=1)),
                predicted_mw=("predicted_mw", "sum"),
            )
            df["zone"] = PJM_ZONE
        else:
            df = df[df["zone"] == zone]
        return df

    @reactive.calc
    def zone_inst_load() -> pd.DataFrame:
        # separate from zone_history(): inst_load_history() is raw 5-min telemetry, not
        # on the same hourly grid as actual/predicted, so it isn't merged into that frame
        zone = drilldown_zone()
        df = inst_load_history()
        if zone is None:
            return df.iloc[0:0]
        h = int(input.horizon().removesuffix("h"))
        hours = DRILLDOWN_LOOKBACK_HOURS + h
        cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=hours)
        df = df[df["time"] >= cutoff]
        if zone == PJM_ZONE:
            # Kafka messages land per-zone independently, so the newest timestamp(s)
            # often haven't heard from every zone yet - sum only once all 20 have
            # reported, else a still-landing reading reads as a fake system-wide dip
            out = df.groupby("time", as_index=False).agg(
                inst_load_mw=("inst_load_mw", "sum"),
                n_zones=("zone", "nunique"),
            )
            out.loc[out["n_zones"] < len(ZONE_ORDER), "inst_load_mw"] = float("nan")
            out["zone"] = PJM_ZONE
            return out.drop(columns="n_zones")
        return df[df["zone"] == zone]

    @reactive.calc
    def freshness_data() -> dict:
        reactive.invalidate_later(300)
        response = requests.get(f"{API_URL}/freshness", timeout=10)
        response.raise_for_status()
        return response.json()

    @render.ui
    def freshness():
        raw = freshness_data()["latest_inst_load_time"]
        if raw is None:
            return ui.div("No inst_load data yet", class_="freshness")
        latest = pd.to_datetime(raw, utc=True)
        age_hours = (pd.Timestamp.now(tz="UTC") - latest).total_seconds() / 3600
        is_live = age_hours <= FRESHNESS_THRESHOLD_HOURS
        return ui.div(
            ui.span(class_=f"status-dot {'status-live' if is_live else 'status-stale'}"),
            ui.span(f"{'Live' if is_live else 'Stale'} — data through {latest.strftime('%b %d, %H:%M UTC')}"),
            class_="freshness",
        )

    @render.ui
    def accuracy_panel():
        stats = accuracy_stats(system_history())
        tiles = []
        for h in HORIZONS:
            row = stats[stats["horizon_h"] == h]
            mae_txt = f"{fmt_mw(row['mae'].iloc[0])} MW" if not row.empty else "—"
            mape_txt = (
                f"Mean Absolute Percentage Error (MAPE): {row['mape'].iloc[0]:.1f}%"
                if not row.empty
                else "Mean Absolute Percentage Error (MAPE): —"
            )
            tiles.append(
                ui.div(
                    ui.div(
                        ui.tags.b(f"{h}h"),
                        " Horizon Rolling Mean Absolute Error (MAE)",
                        class_="stat-label",
                    ),
                    ui.div(mae_txt, class_="stat-value"),
                    ui.div(mape_txt, class_="stat-sub"),
                    class_="stat-tile",
                )
            )
        return ui.div(*tiles, class_="accuracy-row-inner card")

    def _theme_bridge_js(div_id: str, light: dict, dark: dict) -> str:
        # plotly's static to_html() can't see the page's CSS custom properties from
        # inside its own iframe, so this mirrors prefers-color-scheme against the
        # figure directly via Plotly.restyle/relayout - same workaround as
        # MAP_CLICK_BRIDGE_JS above, for theme instead of clicks.
        return f"""
        function gridcastApplyTheme(dark) {{
          var vals = dark ? {json.dumps(dark)} : {json.dumps(light)};
          var gd = document.getElementById('{div_id}');
          if (!gd) return;
          Plotly.relayout(gd, vals.layout);
          if (vals.traces) Plotly.restyle(gd, vals.traces.props, vals.traces.indices);
        }}
        var gridcastMql = window.matchMedia('(prefers-color-scheme: dark)');
        gridcastApplyTheme(gridcastMql.matches);
        gridcastMql.addEventListener('change', function(e) {{ gridcastApplyTheme(e.matches); }});
        """

    def _plotly_iframe(fig: go.Figure, div_id: str, height_px: int, theme_js: str) -> ui.Tag:
        fig.update_layout(
            autosize=True,
            paper_bgcolor=SURFACE_LIGHT,
            plot_bgcolor=SURFACE_LIGHT,
            font={"color": INK_SECONDARY_LIGHT, "family": "Inter, -apple-system, sans-serif"},
        )
        html = fig.to_html(
            full_html=True,
            # self-hosted (see Dockerfile), not "cdn" - srcdoc iframes resolve relative
            # URLs against the parent page's origin, so both chart iframes (and every
            # later session) share one cached fetch instead of each hitting cdn.plot.ly
            include_plotlyjs="plotly.min.js",
            div_id=div_id,
            config={"displayModeBar": False, "responsive": True},
        )
        # to_html()'s wrapper is height:100% of a parent <html>/<body> with no explicit
        # height (auto), so the plot falls back to a taller default and the iframe
        # scrolls. Force the chain down to the iframe's fixed pixel height instead.
        fixed_sizing = "<style>html,body{margin:0;height:100%;overflow:hidden}</style>"
        html = html.replace("<head>", f"<head>{fixed_sizing}")
        html = html.replace("</body>", f"<script>{theme_js}</script></body>")
        return ui.tags.iframe(
            srcdoc=html,
            style=f"border:none;width:100%;height:{height_px}px;overflow:hidden;display:block",
        )

    @render.ui
    def zone_lines_chart():
        h = int(input.horizon().removesuffix("h"))
        sub = system_history()
        sub = sub[sub["horizon_h"] == h].copy()
        if sub.empty:
            return ui.p("No data yet.", class_="hint")
        sub["time_et"] = to_eastern(sub["time"])

        visible = zone_lines_visible()

        fig = go.Figure()
        for zone in ZONE_ORDER:
            if zone not in visible:
                continue
            zsub = sub[sub["zone"] == zone].sort_values("time_et")
            if zsub.empty:
                continue
            hue, dash = _zone_line_style(zone)
            hover_text = [
                f"{zone}<br>{eastern_label(t)}<br>{fmt_mw_conditional(v)} MW"
                for t, v in zip(zsub["time_et"], zsub["predicted_mw"])
            ]
            fig.add_trace(
                go.Scatter(
                    x=zsub["time_et"],
                    y=zsub["predicted_mw"],
                    mode="lines",
                    line={"color": hue, "width": 2, "dash": dash},
                    hovertext=hover_text,
                    hoverinfo="text",
                )
            )
        fig.update_layout(
            showlegend=False,
            margin={"l": 50, "r": 15, "t": 10, "b": 40},
            xaxis={
                "tickformat": "%m-%d %I:%M%p",
                "tickangle": -30,
                "gridcolor": GRIDLINE_LIGHT,
                "linecolor": GRIDLINE_LIGHT,
            },
            yaxis={"title": "MW", "gridcolor": GRIDLINE_LIGHT, "linecolor": GRIDLINE_LIGHT},
            hovermode="closest",
        )
        theme_js = _theme_bridge_js(
            "zone-lines-chart",
            light={
                "layout": {
                    "paper_bgcolor": SURFACE_LIGHT,
                    "plot_bgcolor": SURFACE_LIGHT,
                    "font.color": INK_SECONDARY_LIGHT,
                    "xaxis.gridcolor": GRIDLINE_LIGHT,
                    "xaxis.linecolor": GRIDLINE_LIGHT,
                    "yaxis.gridcolor": GRIDLINE_LIGHT,
                    "yaxis.linecolor": GRIDLINE_LIGHT,
                }
            },
            dark={
                "layout": {
                    "paper_bgcolor": SURFACE_DARK,
                    "plot_bgcolor": SURFACE_DARK,
                    "font.color": INK_SECONDARY_DARK,
                    "xaxis.gridcolor": GRIDLINE_DARK,
                    "xaxis.linecolor": GRIDLINE_DARK,
                    "yaxis.gridcolor": GRIDLINE_DARK,
                    "yaxis.linecolor": GRIDLINE_DARK,
                }
            },
        )
        return _plotly_iframe(fig, "zone-lines-chart", height_px=460, theme_js=theme_js)

    @render.ui
    def zone_lines_legend():
        visible = zone_lines_visible()
        rows = []
        for zone in ZONE_ORDER:
            hue, _dash = _zone_line_style(zone)
            is_on = zone in visible
            rows.append(
                ui.tags.div(
                    ui.tags.span(
                        style=f"background:{hue};opacity:{1 if is_on else .35}",
                        class_="zone-legend-swatch",
                    ),
                    ui.tags.span(
                        f"{zone} — {ZONE_NAMES.get(zone, zone)}",
                        style=f"opacity:{1 if is_on else .5}",
                    ),
                    class_="zone-legend-row",
                    onclick=(
                        f"Shiny.setInputValue('zone_toggle_click', '{zone}', "
                        "{priority: 'event'})"
                    ),
                )
            )
        return ui.div(*rows, class_="zone-chart-legend")

    @render.ui
    def map():
        data = merged()
        zones = zone_geometry().merge(data, left_on="zone_id", right_on="zone")
        zones = zones.merge(peaks().rename(columns={"zone": "zone_id"}), on="zone_id")
        zones["load_ratio"] = zones["predicted_mw"] / zones["peak_mw"]
        vmin, vmax = zones["load_ratio"].min(), zones["load_ratio"].max()
        zones["fill_color"] = zones["load_ratio"].apply(lambda v: heat_color(v, vmin, vmax))
        zones["predicted_mw_label"] = zones["predicted_mw"].apply(fmt_mw)
        # to_json() can't serialize the tz-aware "time" column pulled in from predictions()
        zones = zones[
            ["zone", "predicted_mw", "predicted_mw_label", "load_ratio", "fill_color", "geometry"]
        ]

        selected = selected_zone()
        # PJM_ZONE (the system-wide total) has no polygon to highlight - treat it
        # like "nothing selected" for map purposes
        highlight = selected if selected != PJM_ZONE else None
        extruded = input.mode_3d()
        if highlight is not None:
            zones["fill_color"] = zones.apply(
                lambda r: r["fill_color"] if r["zone"] == highlight else UNSELECTED_GREY, axis=1
            )

        layers = [
            pdk.Layer(
                "GeoJsonLayer",
                data=json.loads(zones.to_json()),
                get_fill_color="properties.fill_color",
                get_line_color=[80, 80, 80],
                line_width_min_pixels=1,
                stroked=True,
                filled=True,
                extruded=extruded,
                get_elevation="properties.predicted_mw",
                elevation_scale=20,
                pickable=True,
            ),
        ]
        label = None
        if highlight is not None:
            selected_zone_gdf = zones[zones["zone"] == highlight]
            # dark halo behind the white outline - a pale fill_color (light end of the
            # ramp) would otherwise make the outline invisible. line_joint_rounded=True
            # caps the spikes deck.gl's default miter joins produce on this wide a
            # stroke over dense, jagged real-world polygons.
            layers.append(
                pdk.Layer(
                    "GeoJsonLayer",
                    data=json.loads(selected_zone_gdf.to_json()),
                    get_line_color=HIGHLIGHT_HALO,
                    get_line_width=4,
                    line_width_units="'pixels'",
                    line_joint_rounded=True,
                    line_cap_rounded=True,
                    stroked=True,
                    filled=False,
                )
            )
            layers.append(
                pdk.Layer(
                    "GeoJsonLayer",
                    data=json.loads(selected_zone_gdf.to_json()),
                    get_line_color=HIGHLIGHT_OUTLINE,
                    get_line_width=2,
                    line_width_units="'pixels'",
                    line_joint_rounded=True,
                    line_cap_rounded=True,
                    stroked=True,
                    filled=False,
                )
            )
            selected_mw_label = selected_zone_gdf["predicted_mw_label"].iloc[0]
            # clicking the label mirrors clicking the highlighted polygon again -
            # both route through map_click_zone, whose handler already treats a
            # repeat click on the current selection as "deselect"
            label = ui.div(
                f"{highlight} — {selected_mw_label} MW",
                class_="map-label",
                style="cursor:pointer",
                onclick=(
                    f"Shiny.setInputValue('map_click_zone', '{highlight}', "
                    "{priority: 'event'})"
                ),
            )

        deck = pdk.Deck(
            map_provider="carto",
            map_style="light",
            initial_view_state=pdk.ViewState(
                latitude=39.5,
                longitude=-78.5,
                zoom=5.5,
                pitch=45 if extruded else 0,
                bearing=input.bearing() if extruded else 0,
            ),
            layers=layers,
            tooltip={"text": "{zone}\n{predicted_mw_label} MW"},
        )
        deck_html = deck.to_html(as_string=True, notebook_display=False)
        # self-hosted (see Dockerfile) - these two are the largest of pydeck's CDN
        # pulls (~5MB combined), so swap them for the local copies instead of every
        # map render/session hitting jsdelivr + mapbox's CDN fresh.
        deck_html = deck_html.replace(f"src='{DECKGL_WIDGET_CDN_URL}'", "src='deckgl-widget.js'")
        deck_html = MAPBOX_GL_CDN_PATTERN.sub("mapbox-gl.js", deck_html)
        deck_html = deck_html.replace(
            "const deckInstance = createDeck({",
            MAP_CLICK_BRIDGE_JS + "\n    const deckInstance = createDeck({\n      handleEvent: gridcastHandleMapEvent,",
        )
        return ui.div(
            ui.div(
                ui.tags.iframe(
                    srcdoc=deck_html,
                    style="border:none;width:100%;height:600px",
                ),
                *([label] if label is not None else []),
                class_="map-frame",
            ),
            ui.div(
                ui.span(f"{vmin:.0%} of peak"),
                ui.div(
                    class_="legend-ramp",
                    style=(
                        "background: linear-gradient(to right, "
                        f"{HEAT_LOW_HEX}, {HEAT_MID_HEX}, {HEAT_HIGH_HEX});"
                    ),
                ),
                ui.span(f"{vmax:.0%} of peak"),
                class_="map-legend",
            ),
        )

    @render.data_frame
    def table():
        display = table_data().assign(predicted_mw=lambda d: d["predicted_mw"].apply(fmt_mw))
        display = display.rename(columns={"zone": "Zone", "predicted_mw": "Predicted MW"})
        return render.DataGrid(display, selection_mode="row")

    @render.ui
    def weather_card():
        zone = drilldown_zone()
        data = weather_for_zone()
        if data is None:
            return ui.div(
                ui.h4("Weather", style="margin-top:0"),
                ui.p("No weather data yet.", class_="hint"),
                class_="card weather-card",
            )

        condition = weather_condition(data["temperature"], data["precipitation"], data["cloud_cover"])
        temp_f = c_to_f(data["temperature"])
        wind_mph = kmh_to_mph(data["wind_speed"])

        return ui.div(
            ui.h4(
                f"Weather — {zone}",
                ui.span(
                    "?",
                    ui.span(
                        "Averaged across representative cities within the zone.",
                        class_="tip-text",
                    ),
                    class_="info-tip",
                ),

            ),
            ui.p("Weather is a model feature used to predict demand.", class_="hint"),
            ui.div(
                ui.div(
                    ui.div(weather_icon_svg(condition), class_="weather-icon"),
                    ui.div(
                        ui.div(f"{temp_f:.0f}°F", class_="weather-temp"),
                        ui.div(WEATHER_CONDITION_LABELS[condition], class_="weather-condition"),
                    ),
                    class_="weather-main",
                ),
                ui.div(
                    ui.span(f"Wind {wind_mph:.0f} mph", class_="badge"),
                    ui.span(f"Precip {data['precipitation']:.1f} mm", class_="badge"),
                    ui.span(f"Cloud cover {data['cloud_cover']:.0f}%", class_="badge"),
                    class_="weather-stats",
                ),
                class_="weather-body",
            ),

            class_="card weather-card",
        )

    @reactive.effect
    @reactive.event(input.map_click_zone)
    async def _sync_table_selection_from_map_click():
        clicked = input.map_click_zone()
        if clicked == selected_zone():
            # clicking the already-selected zone again clears the selection
            await table.update_cell_selection(None)
            return
        data = table_data()
        rows = data.index[data["zone"] == clicked].tolist()
        if rows:
            await table.update_cell_selection({"type": "row", "rows": (rows[0],)})

    @render.ui
    def drilldown():
        zone = drilldown_zone()
        if zone is None:
            return ui.p("Select a zone to see a breakdown view.", class_="hint")
        h = int(input.horizon().removesuffix("h"))
        stats = accuracy_stats(zone_history())
        row = stats[stats["horizon_h"] == h]
        mae_txt = f"MAE {fmt_mw(row['mae'].iloc[0])} MW" if not row.empty else "MAE —"
        return ui.div(
            ui.div(
                ui.h4(f"{zone} — actual vs. predicted ({input.horizon()})"),
                ui.span(mae_txt, class_="badge"),
                class_="drilldown-header",
            ),
            ui.p(
                "Actual demand compared to what the model predicted at the selected horizon. "
                "Instantaneous load (dotted) is near-real-time PJM telemetry, shown because "
                "actual demand settles 2-3 days behind. Hover over any line for exact values.",
                class_="hint",
            ),
            ui.output_ui("drilldown_chart"),
        )

    @render.ui
    def drilldown_chart():
        h = int(input.horizon().removesuffix("h"))
        sub = zone_history()
        sub = sub[sub["horizon_h"] == h].sort_values("time")
        if sub.empty:
            return ui.p("No data yet.", class_="hint")
        sub = sub.assign(time_et=to_eastern(sub["time"]))
        inst_load = zone_inst_load().sort_values("time")
        inst_load = inst_load.assign(time_et=to_eastern(inst_load["time"]))

        def hover(times: pd.Series, values: pd.Series) -> list[str]:
            return [
                f"{eastern_label(t)}<br>{fmt_mw_conditional(v)} MW"
                for t, v in zip(times, values)
                if pd.notna(v)
            ]

        actual = sub.dropna(subset=["actual_mw"])
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=actual["time_et"],
                y=actual["actual_mw"],
                mode="lines",
                name="Actual",
                line={"color": LINE_ACTUAL_HEX, "width": 2},
                hovertext=hover(sub["time_et"], sub["actual_mw"]),
                hoverinfo="text",
            )
        )
        fig.add_trace(
            go.Scatter(
                x=sub["time_et"],
                y=sub["predicted_mw"],
                mode="lines",
                name="Predicted",
                line={"color": LINE_PREDICTED_LIGHT_HEX, "width": 2, "dash": "dash"},
                hovertext=hover(sub["time_et"], sub["predicted_mw"]),
                hoverinfo="text",
            )
        )
        fig.add_trace(
            go.Scatter(
                x=inst_load["time_et"],
                y=inst_load["inst_load_mw"],
                mode="lines",
                name="Instantaneous load",
                line={"color": LINE_INSTLOAD_HEX, "width": 1.5, "dash": "dot"},
                hovertext=hover(inst_load["time_et"], inst_load["inst_load_mw"]),
                hoverinfo="text",
            )
        )
        fig.update_layout(
            margin={"l": 50, "r": 10, "t": 35, "b": 40},
            legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "x": 0},
            xaxis={
                "tickformat": "%m-%d %I:%M%p", "gridcolor": GRIDLINE_LIGHT, "linecolor": GRIDLINE_LIGHT
            },
            yaxis={"title": "MW", "gridcolor": GRIDLINE_LIGHT, "linecolor": GRIDLINE_LIGHT},
            hovermode="closest",
        )
        theme_js = _theme_bridge_js(
            "drilldown-chart",
            light={
                "layout": {
                    "paper_bgcolor": SURFACE_LIGHT,
                    "plot_bgcolor": SURFACE_LIGHT,
                    "font.color": INK_SECONDARY_LIGHT,
                    "legend.font.color": INK_SECONDARY_LIGHT,
                    "xaxis.gridcolor": GRIDLINE_LIGHT,
                    "xaxis.linecolor": GRIDLINE_LIGHT,
                    "yaxis.gridcolor": GRIDLINE_LIGHT,
                    "yaxis.linecolor": GRIDLINE_LIGHT,
                },
                "traces": {
                    "props": {"line.color": [LINE_ACTUAL_HEX, LINE_PREDICTED_LIGHT_HEX, LINE_INSTLOAD_HEX]},
                    "indices": [0, 1, 2],
                },
            },
            dark={
                "layout": {
                    "paper_bgcolor": SURFACE_DARK,
                    "plot_bgcolor": SURFACE_DARK,
                    "font.color": INK_SECONDARY_DARK,
                    "legend.font.color": INK_SECONDARY_DARK,
                    "xaxis.gridcolor": GRIDLINE_DARK,
                    "xaxis.linecolor": GRIDLINE_DARK,
                    "yaxis.gridcolor": GRIDLINE_DARK,
                    "yaxis.linecolor": GRIDLINE_DARK,
                },
                "traces": {
                    "props": {"line.color": [LINE_ACTUAL_HEX, LINE_PREDICTED_DARK_HEX, LINE_INSTLOAD_HEX]},
                    "indices": [0, 1, 2],
                },
            },
        )
        return _plotly_iframe(fig, "drilldown-chart", height_px=340, theme_js=theme_js)


# classic App() (unlike Shiny Express) doesn't auto-mount a "www" dir next to the app
# file - without this, favicon.svg and the self-hosted plotly.min.js both 404.
app = App(app_ui, server, static_assets=Path(__file__).parent / "www")

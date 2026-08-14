# Dashboard Design

**Title: Initial dashboard design — layout, map encoding, drill-down, accuracy panel, Author: Angie Ohaeri, Date: August 14th Time: (session)**

Design for `src/dashboard/app.py` (Shiny for Python + Pydeck), built on top of the existing
stub. Not yet implemented — captured here before building so the decisions behind it don't
get lost. See `references/decisions.md` (Dashboard section) for the Streamlit → Shiny
framework note.

---

## Layout

- **Top bar:** title, horizon selector (1h / 24h / 72h), "as of" timestamp, data-freshness
  indicator
- **Main map:** all 20 PJM zones, fixed marker size per zone, color gradient by load
  relative to that zone's own recent peak (see Map Encoding below)
- **Zone drill-down:** clicking a zone opens a panel with its actual-vs-predicted time
  series and current MAE (see Drill-Down below)
- **Accuracy panel:** small system-wide rolling MAE/MAPE by horizon, always visible
  alongside or below the map — not tucked behind a click
- **Table:** zone, predicted MW, rank (kept from the existing stub)

## Map Encoding

Zones vary hugely in absolute load (AEP is huge, RECO is tiny). Sizing markers by raw
predicted MW — the current stub's approach — makes small zones nearly invisible next to
large ones.

Decided: **normalize per zone.** Fixed marker size; color by
`current_load / zone's recent peak`, single-hue blue light → dark (see Visual Design below
— revised from an earlier cool→hot idea, which misrepresents magnitude-only data as if it
had two opposite poles). Keeps every zone visibly comparable regardless of absolute scale.
Recent peak needs to come from somewhere (rolling 30-day max per zone) — not yet decided
whether that's computed in dbt as a features column or derived in the API/dashboard from
`/history` data.

## Map Mode: 3D Option

**Title: Added 3D extruded-polygon map mode, Author: Angie Ohaeri, Date: August 14th Time: (session)**

Toggle to switch the map between the flat mode above and a 3D view: PJM zone polygons
extruded, with polygon height mapped to demand (MW). Zone boundaries already exist —
`data/external/pjm_zones.geojson` — so this uses Pydeck's `PolygonLayer` (or `GeoJsonLayer`
with `extruded=True`), `get_elevation` driven by predicted MW, not the `ScatterplotLayer`
points the current stub and flat-mode design above use.

Open questions, not yet decided:
- Elevation scale: raw MW (AEP towers over RECO) vs. the same per-zone-normalized value used
  for flat-mode color (consistent story, but "height = % of own peak" reads oddly since
  viewers instinctively compare bar heights across zones as absolute)
- Whether color still encodes normalized load in 3D (height = magnitude, color = redundant
  encoding of the same thing) or color switches to something else now that height carries
  the load signal
- Default view: 3D on load, or flat by default with 3D as an opt-in toggle

## Drill-Down (predicted vs. actual)

Clicking a zone shows actual load (solid line) vs. predicted load (dashed) for roughly the
last 48 hours plus the current forecast horizon. This is the part of the dashboard that
demonstrates the model actually works — important for a portfolio piece, not just an
operational nicety.

## Accuracy Panel

Rolling MAE/MAPE per horizon (1h/24h/72h), system-wide. Builds credibility for a portfolio
viewer without requiring them to dig into MLflow.

## API Surface

- `GET /predict` (existing, unchanged) — current snapshot per zone
- `GET /history?zone=X&hours=48` (new) — actual + predicted MW over a window, per horizon.
  Powers both the drill-down and the accuracy panel.

**Implementation note on `/history`:** recommended approach is running the existing
`predict()` function over a range of historical rows from `analytics.features` (same
function, wider input) rather than logging served predictions to a new table. No schema
change needed, and results stay correct after model retrains. Tradeoff: this shows what the
*current* registry model would have predicted at each past point, not necessarily what was
actually served live at that time. If a true served-prediction audit trail is ever needed,
that requires a logging table instead — not built now.

## Refresh Cadence

Keep the existing stub's `reactive.invalidate_later(300)` (5 min) — matches the EIA-930 /
weather polling cadence already in place elsewhere in the pipeline.

## Visual Design

**Title: Style direction from two reference dashboards, Author: Angie Ohaeri, Date: August 14th Time: (session)**

Two references, combined leaning toward the first:

- [drillhole-visualizer.plotly.app](https://drillhole-visualizer.plotly.app/) — card-based
  layout, rounded corners, restrained white/gray chrome with one warm accent color, small
  stat tiles with a colored top border, green "Active" status dot, Inter typeface
- [Shiny respiratory disease app](https://gallery.shinyapps.io/respiratory_disease_pyshiny/)
  — the simplicity/restraint: soft single accent, generous whitespace, no competing colors

Palette pulled from the design skill's pre-validated default (colorblind-safe, contrast-checked
— see `references/dashboard-design.md` git history or the `dataviz` skill's
`references/palette.md` for the full set and validation method), not hand-picked:

| Role | Light | Dark | Used for |
|---|---|---|---|
| Page background | `#f9f9f7` | `#0d0d0d` | body |
| Card / chart surface | `#fcfcfb` | `#1a1a19` | top bar, cards, panels |
| Primary ink | `#0b0b0b` | `#ffffff` | headings, values |
| Secondary ink | `#52514e` | `#c3c2b7` | body text |
| Muted ink | `#898781` | `#898781` | axis labels, captions |
| Gridline | `#e1e0d9` | `#2c2c2a` | chart gridlines |
| Border | `rgba(11,11,11,.10)` | `rgba(255,255,255,.10)` | card hairlines |
| Accent (orange) | `#eb6834` | `#d95926` | active-tab underline, zone badge, stat-card top border |
| Status "Live/Active" | `#0ca30c` | `#0ca30c` | the green dot in the top bar |

**Charts** — actual load = blue (`#2a78d6` light / `#3987e5` dark), predicted load = orange
(same hex as the chrome accent above) — reuses the two colors already anchoring the page, so
the drill-down chart doesn't feel like a bolted-on chart library. Accuracy panel's 3 lines
(1h/24h/72h horizons) add the palette's third categorical slot, aqua (`#1baf7a` /
`#199e70`).

**Map** — single-hue blue sequential ramp, light → dark, by `load / zone's recent peak` (see
Map Encoding above): `#cde2fb` (near 0%) → `#3987e5` (mid) → `#0d366b` (near/at peak). Fixed
marker size per zone.

**Typography** — Inter (`Inter, -apple-system, "system-ui", "Segoe UI", Roboto, sans-serif`),
matching the Plotly reference.

**Shape/spacing** — ~10–12px rounded corners on cards and panels, pill-shaped badges/status
indicators, hairline borders instead of heavy shadows, generous padding.

**Dark mode** — both light and dark values are specified above (not an afterthought); Shiny
for Python doesn't auto-switch on OS theme the way a browser can, so this needs an explicit
toggle or a `prefers-color-scheme` check wired into the app's CSS — not yet decided which.

## Deferred / Not Yet Decided

- Where the per-zone "recent peak" baseline for map normalization is computed (dbt vs.
  API/dashboard-side)
- Whether `/history` needs pagination or a hard `hours` cap
- Weather overlay on the map (mentioned as a stretch goal in `architecture.md`, not
  scoped here)
- Dark mode toggle mechanism (OS-detected vs. explicit switch) in Shiny for Python
- 3D mode elevation scale, redundant color encoding, and default-view questions (see Map
  Mode: 3D Option above)

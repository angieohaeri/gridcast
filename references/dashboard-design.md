# Dashboard Design

**Title: Initial dashboard design — layout, map encoding, drill-down, accuracy panel, Author: Angie Ohaeri, Date: August 14th Time: (session)**

Design for `src/dashboard/app.py` (Shiny for Python + Pydeck), built on the existing stub. Captured here before building so the reasoning isn't lost. See `references/decisions.md` (Dashboard section) for the Streamlit → Shiny note.

---

## Layout

- **Top bar:** title, horizon selector (1h/24h/72h), "as of" timestamp, freshness indicator
- **Main map:** all 20 PJM zones, fixed marker size, color gradient by load relative to that zone's own recent peak (see Map Encoding)
- **Zone drill-down:** clicking a zone opens actual-vs-predicted time series + current MAE
- **Accuracy panel:** system-wide rolling MAE/MAPE by horizon, always visible, not behind a click
- **Table:** zone, predicted MW, rank (from the existing stub)

## Map Encoding

Zones vary hugely in absolute load (AEP huge, RECO tiny), so sizing markers by raw predicted MW makes small zones invisible. Decided: fixed marker size, color by `current_load / zone's recent peak`, single-hue blue light→dark (revised from an earlier cool→hot idea, which misrepresents magnitude-only data as if it had two poles). Recent peak source (dbt column vs. API-derived) not yet decided.

## Map Mode: 3D Option

**Title: Added 3D extruded-polygon map mode, Author: Angie Ohaeri, Date: August 14th Time: (session)**

Toggle between the flat map and a 3D view: zone polygons (`data/external/pjm_zones.geojson`) extruded via Pydeck's `PolygonLayer`/`GeoJsonLayer(extruded=True)`, height driven by predicted MW.

Open questions: raw MW vs. normalized elevation scale; whether color stays a redundant encoding of the same signal in 3D; default view (3D vs. flat-with-opt-in).

## Drill-Down (predicted vs. actual)

Clicking a zone shows actual (solid) vs. predicted (dashed) load for ~48h plus the current forecast horizon — the part of the dashboard that proves the model works.

## Accuracy Panel

Rolling MAE/MAPE per horizon (1h/24h/72h), system-wide — builds credibility without requiring a viewer to dig into MLflow.

## API Surface

- `GET /predict` (existing) — current snapshot per zone
- `GET /history?zone=X&hours=48` (new) — actual + predicted MW over a window, per horizon; powers drill-down and accuracy panel

**`/history` implementation:** runs `predict()` over a range of historical rows from `analytics.features` rather than logging served predictions to a new table — no schema change, stays correct after retrains. Tradeoff: shows what the *current* registry model would have predicted, not necessarily what was actually served live. A true audit trail would need a logging table — not built now.

## Refresh Cadence

Keep the stub's `reactive.invalidate_later(300)` (5 min) — matches the EIA-930/weather polling cadence elsewhere in the pipeline.

## Visual Design

**Title: Style direction from two reference dashboards, Author: Angie Ohaeri, Date: August 14th Time: (session)**

Two references, leaning toward the first: [drillhole-visualizer.plotly.app](https://drillhole-visualizer.plotly.app/) (card-based, rounded corners, restrained chrome + one warm accent, Inter typeface) and the [Shiny respiratory disease app](https://gallery.shinyapps.io/respiratory_disease_pyshiny/) (restraint — soft single accent, no competing colors).

Palette pulled from the `dataviz` skill's pre-validated default (colorblind-safe, contrast-checked), not hand-picked:

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

**Charts** — actual = blue (`#2a78d6` light / `#3987e5` dark), predicted = orange (same as chrome accent), reusing colors already anchoring the page. Accuracy panel's 3 horizon lines add the palette's third categorical slot, aqua (`#1baf7a` / `#199e70`).

**Map** — single-hue blue sequential ramp by `load / zone's recent peak`: `#cde2fb` (near 0%) → `#3987e5` (mid) → `#0d366b` (near/at peak). Fixed marker size per zone.

**Typography** — Inter (`Inter, -apple-system, "system-ui", "Segoe UI", Roboto, sans-serif`).

**Shape/spacing** — ~10-12px rounded corners, pill-shaped badges, hairline borders instead of shadows, generous padding.

**Dark mode** — both light/dark values specified above; Shiny doesn't auto-switch on OS theme, so needs an explicit toggle or `prefers-color-scheme` check — not yet decided which.

## Deferred / Not Yet Decided

- Whether `/history` needs pagination or a hard `hours` cap
- Weather overlay on the map (stretch goal in `architecture.md`, not scoped here)

## Implementation Notes

**Title: Implemented in `src/dashboard/app.py` / `src/api/main.py`, decisions resolved along the way, Author: Angie Ohaeri, Date: August 14th Time: (session)**

Built out against the design above. The map itself was already `GeoJsonLayer` over real polygons (`data/external/pjm_zones.geojson`), not the stub's `ScatterplotLayer` points this doc's Map Encoding section was written against — the "fixed marker size" concern no longer applies since polygon area comes from real geography.

Open questions resolved:
- **Recent peak:** computed dashboard-side via `GET /peak?zone=&days=30`, backed by `gridcast.dataset.recent_peak()` (a plain 30-day trailing `max(demand_mw)` group-by) — not a dbt model, too simple to warrant one
- **"current_load" in the color formula:** the selected horizon's predicted MW (`/predict` has no raw current-load field) → map color = `predicted_mw / peak_mw`, clipped to `[0, 1]`
- **3D elevation:** raw predicted MW (`elevation_scale=20`, a visual scale factor) — AEP towers over RECO on purpose. Color stays the same normalized-peak ramp in both modes
- **Default view:** flat; 3D is an opt-in switch (`ui.input_switch`)
- **Dark mode:** `prefers-color-scheme` media query, no explicit toggle
- **Drill-down trigger:** clicking a row in the zone table, not a map click — the map is a pydeck deck embedded via `srcdoc` iframe (`deck.to_html()`) with no clean Python-side click callback; `render.DataGrid` row selection gives the same interaction without a JS bridge
- **Accuracy panel metric:** both MAE (MW) and MAPE (%) per horizon as three stat tiles — MAPE is nearly free once the actual/predicted join exists

`GET /history` aligns predicted vs. actual by shifting each horizon's prediction forward by its horizon length (`y_24h` at time `t` targets `t+24h`) and left-joining actual `demand_mw` at that target time — rows past "now" get `actual_mw = null`, which is the forecast tail the drill-down chart wants.

**Title: First round of design feedback applied, Author: Angie Ohaeri, Date: August 14th Time: (session)**

- **Map ramp domain fixed at `[0,1]` was too flat to read** — most zones' ratios cluster in a narrow band. Switched to stretching the domain to the current min/max ratio across displayed zones each refresh, plus a scale legend showing the two endpoint percentages. Checked against the `dataviz` skill first: a distinct pastel hue per zone fails colorblind safety past ~3 adjacent map categories, and a multi-hue ramp makes magnitude harder to compare — zone identity is already unambiguous via shape + tooltip + table
- **`/history` window sizing bug:** drill-down/accuracy tiles requested exactly the display window (e.g. 48h), but a horizon-`h` prediction near the window's start targets a time `h` hours past it, so for `h` ≥ window size every target fell in the future and no actual line rendered. Fixed by fetching `display_window + h` hours
- **Freshness indicator reworded:** "As of {forecast time}" read like forecast validity, not pipeline health. Changed to `GET /freshness` (`max(time)` from raw `load` for `source='pjm'`) labeled "Data through {time}" — a proxy, since there's no `ingested_at` column, and PJM settles ~2-3 days late by design, so the live/stale threshold is 96h. A true "did the consumer run recently" signal would need an `ingested_at` column — not done here
- **Actual-load baseline, investigated:** `load.demand_forecast_mw` (EIA's day-ahead forecast) only exists for EIA's system-wide `zone='RTO'` rows, and `load_features.sql` filters to `source='pjm'` zonal rows only, so it never reaches `analytics.features`. No per-zone forecast baseline available without wiring EIA's RTO-level forecast through, or sourcing PJM's own zonal forecast product and building a new producer for it. Neither done here

**Title: Second round of design feedback applied, Author: Angie Ohaeri, Date: August 14th Time: (session)**

- **MW formatting standardized to 3 decimals everywhere** (`fmt_mw()`) — raw model output carries ~13 decimal digits. Hover text uses `fmt_mw_conditional()`, which only adds the thousands comma above 9999 MW
- **Table headers renamed** "Zone"/"Predicted MW" (display-only; internals still key off `zone`/`predicted_mw`)
- **Drill-down and a new per-zone comparison chart moved to Plotly** for hover tooltips — matplotlib dropped entirely. Both charts are self-contained `to_html()` documents embedded via `srcdoc` iframes, same workaround as the map's click bridge (Shiny's `ui.HTML` won't execute injected `<script>`). A small bridge script tracks `prefers-color-scheme` and calls `Plotly.relayout`/`restyle`, since the iframe can't see the parent's CSS variables
- **New "Predicted load by zone" chart** — one line per zone at the selected horizon, Plotly's legend doubling as a show/hide toggle, "code — full name" key from `pjm_eia930_subregions.csv`. Top 5 zones by current predicted MW visible by default, rest start `legendonly`. Color assigned by fixed zone identity (alphabetical), not MW rank, so a line's color is stable across refreshes
  - Ran line colors through the `dataviz` skill's validator: the 8-hue categorical theme only clears accessibility gates at full saturation, and only the first 3 slots clear all-pairs comparison. Slots 1-2 (blue/orange) are reserved for actual/predicted elsewhere, leaving 6 hues for 20 zones. A true pastel tint (~45% white) fails the validator outright; settled on a lighter ~15% tint (yellow still sits slightly outside the lightness band — an accepted gap). Added a secondary dash-style encoding (solid/dash/dot/dashdot, one step per 6-zone cycle) since 6 hues repeat across 20 zones; legend text remains the ultimate identity source. Dark-mode chart chrome adapts via the bridge script; the 6 line hues themselves stay fixed, same limitation as the map's light-only style
- **Actual/predicted colors now literally match the map and stat tiles.** The map's blue ramp switched from a 2-stop to a 3-stop lerp (`HEAT_LOW→HEAT_MID→HEAT_HIGH`) so a ratio of 0.5 always renders `HEAT_MID` (`#3987e5`) — now also the fixed "actual load" line color (`--line-actual` unified to `#3987e5` in both themes, was `#2a78d6` in light). "Predicted" already matched the page accent (`#ed7a4c`/`#de6d40`)
- **3D map gained an explicit rotate slider** (`bearing`, 0-360°, 3D-only) alongside a hint that right-click/drag also rotates+tilts — pydeck already enabled the drag gesture by default, so the gap was discoverability, not capability
- **Map header + one-line description added**, matching the table/zone-chart card pattern

**Title: Third round of design feedback applied, Author: Angie Ohaeri, Date: August 14th Time: (session)**

- **Map legend gradient bug fixed** — the "% of peak" bar was a 2-stop CSS `linear-gradient`, but `heat_color()` is a 3-stop ramp with a `HEAT_MID` breakpoint at t=0.5, so they diverged everywhere except the endpoints. Fixed by adding the same `HEAT_MID` stop to the CSS gradient
- **Accuracy tiles moved back to a standalone full-width row**, zone chart got its own full-width card below. Found and fixed a latent Bootstrap conflict along the way: Bootstrap's `.card` sets `flex-direction:column`, silently overriding custom flex-row classes (`.topbar`, `.accuracy-row-inner`) that relied on the flex default of `row` rather than setting it explicitly. Fixed by adding explicit `flex-direction: row` to both rules
- **Stat tiles now stretch to fill the row** (`flex: 1 1 0`) with unabbreviated labels ("Mean Absolute Error" instead of "MAE") to use the extra width. The compact MAE badge on the drill-down chart header is untouched (space-constrained by design)
- **Zone-chart legend no longer horizontally scrolls** — removed `white-space: nowrap`, which had pushed long names ("AEP — American Electric Power") wider than the column; vertical scroll for the 20-name list remains
- **Plotly iframes no longer show their own scrollbar.** `to_html()`'s wrapper div is `height:100%` of a parent `<html>`/`<body>` that never got an explicit height, so Plotly fell back to a taller default size than the iframe. Fixed generically in `_plotly_iframe()`: inject `html,body{height:100%;margin:0;overflow:hidden}` plus `autosize=True`, applied to both charts
- **Topbar horizon selector and freshness indicator now align** — Bootstrap's default `.shiny-input-container` margin-bottom shifted the select upward inside a centered flex row. Zeroed with `.topbar-controls .shiny-input-container { margin-bottom: 0; }`

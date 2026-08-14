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

- Whether `/history` needs pagination or a hard `hours` cap
- Weather overlay on the map (mentioned as a stretch goal in `architecture.md`, not
  scoped here)

## Implementation Notes

**Title: Implemented in `src/dashboard/app.py` / `src/api/main.py`, decisions resolved along the
way, Author: Angie Ohaeri, Date: August 14th Time: (session)**

Built out against the design above. The map itself was not touched (already `GeoJsonLayer` over
real zone polygons from `data/external/pjm_zones.geojson`, not the stub's `ScatterplotLayer`
points this doc's Map Encoding section was written against — the "fixed marker size" problem it
worried about no longer applies, since polygon area already comes from real geography rather
than a data-driven marker size).

Decisions made on previously-open questions:
- **Recent peak baseline:** computed dashboard-side, via a new `GET /peak?zone=&days=30` endpoint
  backed by `gridcast.dataset.recent_peak()` (a plain `max(demand_mw)` group-by over
  `analytics.features`, trailing 30 days). Not a dbt model — simple enough not to warrant one.
- **"current_load" in the color formula:** read as the already-selected horizon's predicted MW
  (there's no raw current-load field in `/predict`), so map color = `predicted_mw / peak_mw`,
  clipped to `[0, 1]`.
- **3D elevation:** raw predicted MW (`elevation_scale=20`, a visual scale factor, not a unit
  conversion) — AEP towers over RECO on purpose. Color stays the same normalized-peak ramp in
  both modes (redundant encoding, but consistent legend beats a second scheme).
- **Default view:** flat; 3D is an opt-in switch (`ui.input_switch`).
- **Dark mode:** `prefers-color-scheme` media query, no explicit toggle — simplest option, no OS
  detection reflected in the doc's palette table needed additional wiring.
- **Drill-down trigger:** clicking a row in the zone table, not a literal map click. The map is
  a `pydeck` deck embedded via `srcdoc` on an iframe (`deck.to_html()`); there's no clean
  Python-side click callback through that boundary. `render.DataGrid`'s row selection
  (`selection_mode="row"`, read via `table.cell_selection()`) gives the same "click a zone, see
  its drill-down" interaction without a JS bridge.
- **Accuracy panel metric:** both MAE (MW) and MAPE (%) per horizon, as three stat tiles — the
  doc named both metrics, and computing MAPE alongside MAE is nearly free once the actual/predicted
  join exists.

`GET /history` aligns predicted vs. actual by shifting each horizon's prediction forward by its
horizon length (a `y_24h` predicted at time `t` targets `t + 24h`) and left-joining against
actual `demand_mw` at that target time — rows past "now" simply have `actual_mw = null`, which is
exactly the "current forecast" tail the drill-down chart wants.

**Title: First round of design feedback applied, Author: Angie Ohaeri, Date: August 14th Time:
(session)**

- **Map ramp domain fixed at `[0, 1]` was too flat to read.** Most zones' `predicted/peak` ratios
  cluster in a narrow band, so a fixed 0–1 domain barely used the ramp. Switched to stretching the
  domain to the current min/max ratio *across the displayed zones* each refresh, plus a scale
  legend under the map showing the two endpoint percentages. Ran this by the `dataviz` skill first
  — the alternative floated (a distinct pastel hue per zone, shade = magnitude) fails colorblind
  safety past ~3 mutually-adjacent categories on a map, and a multi-hue ramp also makes magnitude
  harder to compare zone-to-zone than a single stretched hue. Zone identity is already unambiguous
  via shape + tooltip + the zone table, so color wasn't asked to also carry it.
- **`/history` window sizing bug:** the drill-down chart and accuracy tiles were requesting exactly
  the display window's worth of history (e.g. 48h), but a horizon-`h` prediction near the start of
  that window targets a time `h` hours past it — so for `h` ≥ the window size, *every* prediction's
  target fell in the future and `actual_mw` was null across the board (no actual line rendered).
  Fixed by fetching `display_window + h` hours instead, so the request has enough lookback for the
  full display window to have a real actual to compare against.
- **Freshness indicator reworded.** "As of {forecast time}" read like the forecast's validity time,
  not pipeline health. Changed to `GET /freshness` (`gridcast.dataset.latest_load_time()`, `max(time)`
  from the raw `load` table for `source='pjm'`) with the label "Data through {time}". This is a
  proxy, not literal consumer wall-clock run time — there's no `ingested_at` column recording when a
  consumer write happened, only the business `time` (Hour Ending) column, and PJM's zonal feed
  settles ~2-3 days behind by design (see `references/pjm_eia_data_gotchas` memory), so the
  live/stale threshold is set to 96h to mean "keeping pace with normal settlement lag," not
  real-time. A true "did the consumer run recently" signal would need an `ingested_at` column added
  to `load` and threaded through `src/consumers/load_consumer.py` — not done here since it's a
  schema/consumer change, not a dashboard change.
- **Actual-load baseline for testing model performance, investigated:** `load.demand_forecast_mw`
  (EIA-930's own day-ahead forecast) exists in the raw table but only for EIA's `zone='RTO'`
  system-wide rows, not per-zone, and `load_features.sql` filters to `source = 'pjm'` zonal rows
  only — so it never reaches `analytics.features` today. No per-zone PJM/EIA forecast baseline is
  available without either (a) wiring EIA's RTO-level forecast through as a system-wide-only
  baseline, or (b) sourcing PJM's own zonal load forecast product (a separate Data Miner 2 report,
  not the metered feed this pipeline currently pulls) and building a new producer/dbt model for it.
  Neither done here.

**Title: Second round of design feedback applied, Author: Angie Ohaeri, Date: August 14th Time:
(session)**

- **MW formatting standardized to 3 decimal places everywhere** (`fmt_mw()`) — raw `predicted_mw`
  values from the model carry ~13 decimal digits, so every display surface (stat tiles, table, map
  label/tooltip, drill-down badge) now goes through one helper. The actual-vs-predicted plot's hover
  text uses a variant (`fmt_mw_conditional()`) that only adds the thousands comma above 9999 MW, per
  spec — everywhere else always shows the comma (matches the pre-existing style).
- **Table headers renamed** "Zone" / "Predicted MW" (display-only rename in the `table()` renderer;
  `table_data()` internals still key off `zone`/`predicted_mw` for selection lookups).
- **Drill-down chart and a new per-zone comparison chart moved to Plotly**, for hover tooltips —
  matplotlib is no longer used anywhere in the app (dropped from `pyproject.toml`). Both charts are
  full self-contained `to_html()` documents embedded via `srcdoc` iframes, the same workaround the
  map already used for its Python↔JS click bridge (Shiny's `ui.HTML` won't execute injected
  `<script>` tags, but an iframe's own document will). A small bridge script per chart listens for
  `prefers-color-scheme` changes and calls `Plotly.relayout`/`Plotly.restyle` to track the page's
  light/dark toggle, since the iframe can't see the parent page's CSS custom properties.
- **New "Predicted load by zone" chart**, next to the accuracy tiles: one line per zone at the
  selected horizon, Plotly's native legend doubling as the zone toggle (click a legend entry to
  show/hide) and the "code — full name" key (full names from
  `data/processed/pjm_eia930_subregions.csv`, the canonical id→name mapping already documented in
  `references/data-dictionary.md`). Top 5 zones by current predicted MW are visible by default (user
  choice — the alternatives were "all 20 on" or "none, user picks"), rest start `legendonly`. Color
  is assigned by fixed zone identity (alphabetical), never by current MW rank, so a zone's line
  color is stable across refreshes.
  - Ran the pastel line colors through the `dataviz` skill's palette validator first: the 8-hue
    default categorical theme (`references/palette.md`) only clears its accessibility gates at full
    saturation, and even then only the first 3 slots clear *all-pairs* comparison (relevant here —
    this chart can show many series at once). Slots 1-2 (blue/orange) are reserved for the
    actual/predicted lines elsewhere in the app, leaving 6 usable hues for 20 zones. A true pastel
    tint (~45% white) fails the validator's lightness/chroma floors outright; settled on a much
    lighter touch (~15% tint) that mostly passes (one hue, yellow, still sits slightly outside the
    lightness band even at that reduced tint — a known, accepted gap, not a validator pass). With
    only 6 unique hues cycling across 20 zones, added a secondary dash-style encoding
    (solid/dash/dot/dashdot, one step per 6-zone cycle) so repeated hues stay distinguishable;
    legend text is the ultimate identity source regardless. Dark-mode chart chrome (background,
    gridlines, font, legend text) adapts via the bridge script above; the 6 line hues themselves do
    not have separate validated dark-mode steps and stay fixed — same limitation the map's light-only
    `map_style="light"` already carries.
- **Actual/predicted color now literally matches the map and stat tiles, not just visually similar
  hexes.** The map's blue ramp was a 2-stop lerp (`HEAT_LOW`→`HEAT_HIGH`); switched to 3-stop
  (`HEAT_LOW`→`HEAT_MID`→`HEAT_HIGH`, using the sequential ramp's step 100/400/700 from
  `palette.md`) so a ratio of exactly 0.5 always renders as `HEAT_MID` (`#3987e5`) — that hex is now
  also the fixed "actual load" line color (`--line-actual` CSS var unified to `#3987e5` in both
  light and dark, previously `#2a78d6` in light mode). "Predicted" already matched the page accent
  exactly in both themes (`#ed7a4c` / `#de6d40`) — no change needed there, just carried the same
  hexes into the new Plotly traces.
- **3D map gained an explicit rotate slider** (`bearing`, 0-360°, shown only in 3D mode via
  `panel_conditional`) alongside a hint that right-click/two-finger drag also rotates+tilts — pydeck
  enables the drag gesture by default (`controller: true` is baked into `Deck`'s default `views`,
  not a constructor kwarg), so the actual gap was discoverability, not capability.
- **Map header + one-line description added** above the 3D toggle, matching the pattern the
  "Zones"/table card and the new zone-chart card already use.

**Title: Third round of design feedback applied, Author: Angie Ohaeri, Date: August 14th Time:
(session)**

- **Map legend gradient bug fixed.** The "% of peak" bar under the map was a plain 2-stop CSS
  `linear-gradient(LOW, HIGH)`, which lerps straight across — but `heat_color()` (see above) is a
  3-stop ramp with a fixed `HEAT_MID` breakpoint at t=0.5. The two didn't match at any point above
  0%/below 100%, most visibly at the middle. Fixed by adding the same `HEAT_MID` stop to the CSS
  gradient (`linear-gradient(LOW, MID, HIGH)`), which lands it at 50% by default — same breakpoint
  `heat_color()` uses.
- **Accuracy tiles moved back to their original standalone full-width row** (not squeezed next to
  the zone chart) and the zone chart now gets its own full-width card below, giving it far more
  room. Along the way, found and fixed a **latent Bootstrap conflict**: Bootstrap 5.3's own
  `.card` rule sets `display:flex; flex-direction:column`, and any element carrying both `.card`
  and a custom flex-row class (`.topbar`, `.accuracy-row-inner`) inherited that `column` direction
  silently, since our custom classes never explicitly set `flex-direction` themselves (relying on
  the flex default of `row`, which only applies when nothing else sets it — Bootstrap's explicit
  `column` wins over "unset" regardless of stylesheet order). This was already present before this
  round of changes, just not load-bearing enough to notice until the accuracy tiles were combined
  with `.card` directly and rendered fully stacked. Fixed by adding an explicit
  `flex-direction: row` to both rules rather than relying on the default.
- **Stat tiles now stretch to fill the row** (`flex: 1 1 0` instead of `flex: 0 0 auto; width:
  fit-content`) and their labels are unabbreviated ("Mean Absolute Error" / "Mean Absolute
  Percentage Error" instead of "MAE" / "MAPE") to use the extra width instead of leaving it empty.
  The compact MAE badge on the per-zone drill-down chart header is untouched (different context,
  space-constrained by design).
- **Zone-chart legend no longer horizontally scrolls.** It's `flex: 0 0 220px` with `overflow-y:
  auto` (vertical scroll is expected — 20 zone names don't fit in the visible height) but previously
  also had `white-space: nowrap`, which pushed long names (e.g. "AEP — American Electric Power")
  wider than the column and forced a horizontal scrollbar too. Removed `nowrap` so labels wrap
  instead.
- **Plotly iframes no longer show their own scrollbar.** Root cause: `to_html()`'s wrapper div is
  `height:100%` of its parent, but the parent `<html>`/`<body>` never got an explicit height of
  their own (default `auto`) — so `100%` resolved against nothing, Plotly fell back to a taller
  default plot size than the iframe's fixed pixel height, and the iframe scrolled to show the
  overflow. Fixed generically in `_plotly_iframe()`: inject `html,body{height:100%;margin:0;
  overflow:hidden}` into every generated chart's `<head>`, plus `autosize=True` on the figure, so
  the plot always exactly fills the iframe instead of overflowing it. Applies to both the
  drill-down chart and the per-zone chart.
- **Topbar horizon selector and freshness indicator now align.** Bootstrap's default
  `.shiny-input-container` carries a `margin-bottom: 1rem`; inside a `display:flex; align-items:
  center` row, that asymmetric margin (bottom-only) shifted the select's centered position upward
  relative to its sibling. Zeroed out with `.topbar-controls .shiny-input-container {
  margin-bottom: 0; }`.

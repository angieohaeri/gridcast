# Decisions

Why I made certain decisions, for future reference.

## Project Pivot

Title: Pivoted from bike-share availability to PJM electricity demand forecasting, Author: Angie Ohaeri, Date: August 4th Time: (session)

Bike-share demand forecasting is a saturated portfolio project category. Switched to
short-term electricity load forecasting instead, using the same architecture
(Kafka → TimescaleDB → dbt → LightGBM → FastAPI → Streamlit).

Picked **PJM** over NYISO and MISO: NYISO would have kept geographic continuity with the
old Citi Bike framing and required no ISO account, but PJM's larger, more heterogeneous
market (more zones, more topology) makes for a more interesting engineering story. The
tradeoff is a free PJM Data Miner account and per-zone weather features instead of one
metro's weather feed. Explicit fallback if that scope proves too costly: cut down to a
handful of zones rather than modeling the full PJM footprint — the interesting
engineering is in the prediction-log and scoring layer, not breadth of coverage.

## Data Exploration
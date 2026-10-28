# Prompt for the modeling session

**Title: Handoff prompt for the pnode-level spread GNN, Author: Angie Ohaeri (assisted), Date: October 26, 7:34PM UTC**

Paste everything below the line into a new Claude Code session opened in the repo root.

---

You are continuing the modeling work on the LMP pricing model in this repo (branch `lmp-pricing-model`).
The data pipeline is finished; nothing about the model itself is built yet. Read `CLAUDE.md` first
(working style: examine a table's schema before changing code, no try/except for roadblocks that don't
exist, no CLI unless asked, no unrequested extras, small notebook edits, loguru not print, every md
entry needs author/date/time). Then `references/lmp-pricing-model/{lmp-pricing-model,decisions,schema}.md`.

## Goal
One forecast a day, issued 14:30 ET on day D, covering every hour of D+1 at every one of 1,505 pnodes.
**Target**: per node, `spread = rt_lmp - da_lmp` for the same hour (RT forecast = DA + predicted
spread), train on verified RT only, robust loss (Huber) because spikes are real. Model: a GNN over the
pnode graph. Metrics are MAE in $/MWh, overall and on spike hours (top 1% of |spread|).

## Already built (do not rebuild)
- **Loader** `lmp_model/dataset.py`: `load_nodes()` (nodes sorted by pnode_id; that order IS the node
  index everywhere) and `load_window(start, end, pnode_ids) -> Window` for a half-open hour range.
  `Window`: `x [time,node,9]` (FEATURES: da_lmp, da_congestion_price, da_marginal_loss_price,
  spread_last_verified_day, congestion_spread_last_verified_day, spread_mean_7d, spread_std_7d,
  spike_share_7d, congestion_spread_mean_7d), `y [time,node]` (NaN where RT is not verified yet),
  `exists [time,node]` (False for the 60 late-starting nodes before they trade), `system [time,6]` (hour
  and month sin/cos, is_weekend, is_holiday; same for all nodes), `areas`+`area_forecast [time,11]`+
  `node_area [node]` (PJM load forecast MW by area, indexed per node), `zones`+`zone_features
  [time,20,10]`+`node_zone [node]` (weather and operator-initiated commitments, `ZONE_FEATURES`; -1 for
  the 22 zoneless nodes: hubs, interfaces, PJM-RTO, OVEC). `system` also carries the
  `system_features` columns (shadow prices, fuel mix, losses, outages, gas cost; `SYSTEM_FEATURES`). Tables are in the `analytics_lmp`
  schema (baseline_features, spread, load_forecast_features, zone_features,
  system_features, nodes). All features respect the 14:30 ET cutoff; the 7-day
  stats end at the same hour as spread_last_verified_day (72h before the target hour).
- **Splits** `lmp_model/splits.py`: 4 expanding-window walk-forward folds, windows half-open, each with a
  2-month validation block before its 3-month test block. Train starts 2024-10-23; fold test blocks start
  2025-12-01, 2026-03-01, 2026-06-01, 2026-09-01 (the last is partial, data ends ~2026-10-25). Why
  walk-forward: retraining always uses the newest data.
- **Graph** `lmp_model/graph.py`: `build_edges(train_start, train_end)` writes
  `data/processed/pnode_edges_<train_end>.parquet` (undirected, src<dst, kind = geographic|correlation).
  2,921 geographic substation edges plus top-3 correlated same-zone neighbours for every node without one
  (the 426 fill-ins have none from topology; ComEd has no attached nodes at all). `edge_index(edges,
  pnode_ids)` gives the [2, 2E] array. **Correlation edges are learned from data, so build them per fold
  from the fold's training window only: `build_edges(fold.train_start, fold.val_start)`.** The existing
  file (train end 2026-04-01) is exactly right for fold 3 and leaks the test period into folds 1 and 2.
- **Baselines** (test window 2026-04-01 on, 7.38M node-hours): spread = 0 gives MAE 25.0 (median AE 7.8, spike
  MAE 686); last verified day's spread gives MAE 40.1 (spike 672). Last-day spread loses to zero every month,
  so the spread has no day-to-day persistence. Recompute both on each fold's own test block.

## Not built yet (your work)
1. **Normalization** fit on each fold's train window only. Prices are heavy-tailed (LMP -2,328 to 4,673;
   99.8% inside -59 to 1,275); the graph step already clips spread to +-300 for correlations. Decide clip vs
   robust scale. Handle NaN: ~0.9% in the history features (window start, late nodes), 47 hours (0.27%) with
   no load forecast (PJM's history is missing the DST-spring-forward days), weather -1 index.
2. **Loss mask** = `exists & ~isnan(y)`; late nodes must not contribute before they start.
3. **Tensor cache** per fold on disk (parquet/npz under data/processed, DVC if wanted). One training-window
   load takes ~160s when the DB is idle (511s under contention), so load once.
4. A plain per-node reference model (LightGBM is installed) before the GNN, then the GNN, then an ablation
   ladder: GNN on the 4 core price features, then + history/calendar, then + load/weather forecast. If the
   GNN does not beat a per-node model with identical inputs, say so before more graph work.
5. Training loop, early stopping on the validation block, evaluation vs the two baselines, MLflow logging
   (run naming `{model}_{feature_set}_{YYYYMMDD}`), walk-forward over all folds.
6. **torch and torch_geometric** are in the optional `gnn` group in pyproject, not the main dependencies (the
   Docker image installs with `--no-dev`). The Mac Mini has the CPU build of torch and no GPU.
7. Deferred features, ranked high in the docs: lagged binding-constraint count and shadow prices (availability
   at the cutoff is unverified: the D-1 limit seen earlier was our sync window, fixed in `7b66260`; re-check the DB
   before building on them, see `decisions.md`), fuel mix, forecasted outages, gas price, and linking shadow prices to
   graph edges (name matching covers only ~43-47% of RT shadow-price weight). Add them as ablation steps,
   each as a new dbt feature model.

## Known train/serve differences (documented in decisions.md)
Load forecast: training uses the 11:45 ET evaluation (last historical one before the cutoff), serving a ~14:20
ET snapshot. Weather: training uses Open-Meteo's previous_day1 forecast (~24h lead), serving the live
forecast (real lead 10-34h), so evening hours are a little fresher in training.

## Operations
- Environment: `.venv` (python), `uv run`/`uvx ruff` for lint (`ruff` is not in the venv), `.venv/bin/python -m pytest
  tests/lmp_model`. dbt: `make features` (load stack) and `make features-lmp` (LMP model: selectors
  `lmp_history` then `lmp_serving` in `src/dbt/selectors.yml`). The `analytics_lmp.lmp_*` tables are large
  (26M rows); avoid heavy queries around 13:30 ET when the OpenMetadata ingest runs.
- Daily schedule (ET): `lmp_morning` 12:15 (verified RT sync, then dbt lmp_history); `lmp_afternoon` 13:50 (DA sync,
  load forecast hist and live, weather forecast, then dbt lmp_serving); forecast cutoff 14:30.
- PJM Data Miner allows 6 requests/min on one shared key; do not hammer it.
- Nothing is pushed; commit only when asked.

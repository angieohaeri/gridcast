# LMP pricing model: experiment log

**Title: Experiment log for the next-day RT-DA spread model, Author: Angie Ohaeri (assisted), Date: October 29th Time: 1:47am CT**

One entry per experiment, newest last. Reasoning and design choices live in `decisions.md`;
this file is the results record. Add new experiments at the bottom with the same layout.

## Shared setup

- **Target**: per node-hour `spread = rt_lmp - da_lmp` (verified RT), forecast at 14:30 ET on day D for every hour of D+1.
- **Metrics** ($/MWh): MAE; spike MAE = MAE on the top 1% of |spread| within the scored cells; skill = 1 - MAE / MAE of predicting zero.
- **Scored nodes**: the original 1,505 nodes unless marked **1,580** (adds the 75 `constraint` nodes from Oct 27).
- **Folds** (`lmp_model/splits.py`, expanding train from 2024-10-23):

| fold | val | test |
|---|---|---|
| 0 | 2025-10-01 to 2025-12-01 | 2025-12-01 to 2026-03-01 |
| 1 | 2026-01-01 to 2026-03-01 | 2026-03-01 to 2026-06-01 |
| 2 | 2026-04-01 to 2026-06-01 | 2026-06-01 to 2026-09-01 |
| 3 | 2026-07-01 to 2026-09-01 | 2026-09-01 to 2026-12-01 (data ends late Oct) |

- **GNN defaults** (`models/model.ipynb`): SAGEConv (mean aggregation), 2 layers, hidden 64, 16-dim node embedding, Huber loss, Adam lr 1e-3, 16 hours per batch, early stopping on val MAE (patience 5), seed 0. Best epoch is usually 0-3.
- **Input sets**: `core4` = `da_lmp`, `da_congestion_price`, `da_marginal_loss_price`, `spread_last_verified_day`. 15 inputs = the 9 node `FEATURES` + 6 calendar. 31 / 37 = 15 + PJM-wide `system_features` copied into every node.
- **Runs**: MLflow experiment `gridcast-lmp-spread` on the Mac Mini, except where noted.

## Leaderboard (fold 0 test, 1,505 nodes)

| model | test MAE | spike MAE | skill | entry |
|---|---|---|---|---|
| GNN, 15 inputs (Oct 27 graph) | **29.15** | **610.0** | 12.9% | E03 |
| GNN, 15 inputs (Oct 28-29 graph, 1,580-node model) | 29.27 | 618.0 | 12.6% | E06 |
| GNN, 15 inputs, GPU, mean of seeds 0-2 (Oct 28-29 graph without `hifld_osm`) | 29.05 | 622.1 | 13.2% | E10 |
| GNN, `core4` | 29.66-29.70 | 635-637 | 11.3% | E03 |
| hour model only (same prediction at every node) | 31.38 | 672.8 | 6.3% | E08 |
| LightGBM, node as a category | 32.09 | 669.9 | 4.1% | E02 |
| predict zero | 33.48 | 689.3 | 0% | |

---

## E01: Naive baselines (Oct 25)

- **Question**: is yesterday's spread a usable forecast?
- **Setup**: no fitting; test window 2026-04-01 onward, 1,505 nodes. Spike threshold $342/MWh.

| | spread = 0 | last verified day's spread |
|---|---|---|
| MAE | 25.0 | 40.1 |
| spike MAE | 686 | 672 |

- **Takeaway**: no day-to-day persistence; zero is the bar to beat. Spikes are 1% of rows but 28% of absolute error.

## E02: LightGBM reference, all folds (Oct 26)

- **Question**: how well does a global tree model with the node as a categorical feature do?
- **Setup**: `notebooks/3.06-lmp-lightgbm-reference.ipynb`, all inputs available on Oct 26.

| fold | test MAE | zero | skill | spike MAE |
|---|---|---|---|---|
| 0 | 32.09 | 33.48 | 4.1% | 669.9 |
| 1 | 18.31 | 19.04 | 3.8% | 426.7 |
| 2 | 23.17 | 24.05 | 3.7% | 623.2 |
| 3 | 32.53 | 33.94 | 4.2% | 1,138.3 |

- **Takeaway**: about 4% skill in every fold. MLflow names are off by one (`fold0`, `fold1`, `fold3`, `fold4` = folds 0-3).

## E03: First GNN runs (Oct 26-27, fold 0)

| run | inputs | graph | best val | test MAE | spike MAE | skill |
|---|---|---|---|---|---|---|
| `gnn_core4` | 4 | full | 13.33 | 29.70 | 635.2 | 11.3% |
| `gnn_core4_none` | 4 | none | 13.32 | 29.66 | 637.1 | 11.4% |
| `lgbm_core4` | 4 | n/a | 13.78 | 34.94 | 701.6 | -4.4% |
| `gnn_history_full` | 15 | full | 13.17 | **29.15** | **610.0** | 12.9% |

- **Takeaway**: the GNN beats LightGBM by ~3 MAE; the graph adds nothing on 4 inputs; 15 inputs helps mostly on spikes.

## E04: Adding PJM-wide system features (Oct 28, fold 0)

- **Question**: do the new `system_features` (shadow totals, fuel mix, outages, gas cost, interfaces, interchange) help when copied into every node?

| inputs | test MAE (1,580) | test MAE (1,505) | spike MAE (1,580) |
|---|---|---|---|
| 15 | 29.49 | 29.28 | 626.1 |
| 31 (pruned) | 30.09 | 29.87 | 635.6 |
| 37 (all) | 30.70-30.84 | 30.49 | 637-638 |
| zero | 33.67 | 33.48 | 693.3 |

- **Takeaway**: each step of system features makes it worse; validation moves the same way. See E07 for why.

## E05: Edge ablation (Oct 28, fold 0, 37 inputs, 1,580 nodes)

| edge set | edges | test MAE | spike MAE |
|---|---|---|---|
| map_only | 4,217 | 30.82 | 637.5 |
| direct_lines | 4,121 | 30.84 | 638.3 |
| all_lines | 4,865 | 30.70 | 637.1 |
| no graph | 0 | 30.74 | 647.7 |

- **Takeaway**: the edge set does not move overall MAE; any graph lowers spike MAE by ~10. One fold and seed.

## E06: Per-node shadow-price exposure (Oct 29, fold 0)

- **Question**: do location-specific constraint signals beat the PJM-wide shadow totals?
- **Setup**: `lmp_model/exposure.py`, `notebooks/3.07-lmp-exposure-features.ipynb`. Ridge weights (top 100 RT facilities -> node RT congestion price), fit per fold. Features `da_exposure` (target-day DA shadow prices) and `rt_exposure_prev_day` (RT, 48h back).
- **Weights out of sample** (same hour, R²): 0.38 fold 0, 0.61 fold 3. They cover 89% of RT shadow-price weight but only 43-56% of DA.

| inputs | best val | test MAE | spike MAE | skill |
|---|---|---|---|---|
| 15 | 13.17 | 29.27 | 618.0 | 12.6% |
| 15 + exposure | 13.14 | 29.28 | 623.8 | 12.5% |
| zero | 15.04 | 33.48 | 689.3 | |

- **Takeaway**: no lift. Folds 1-3 not run (by choice). Code kept, unused.

## E07: Spread decomposition (Oct 29, analysis)

- **Setup**: Oct 2025-Feb 2026, 1,505 nodes, SQL on `raw_lmp.lmp_rt` / `lmp_da`.

| | value |
|---|---|
| spread variance shared by every node in the hour (hour mean) | 59% |
| node MAE predicting zero | 26.0 |
| node MAE if each hour's mean were known exactly | 14.4 |
| DA LMP variance that is between hours | 79% |
| mean DA system energy price / mean \|congestion\| / mean \|loss\| | $73 / $14 / $3 |

- **Takeaway**: over half the reachable error is one number per hour (mostly the RT-DA energy price difference). System-wide features belong in an hour-level model, not copied into nodes.

## E08: Hour-level model (Oct 29, fold 0)

- **Setup**: LightGBM (L1 loss), one row per hour, target = the hour's mean spread over the 1,505 nodes. 53 inputs: calendar, `system_features`, DA energy price, mean DA LMP, area load forecasts, lagged (72h) hour-mean spread. Scratch script, not in the repo.

| | hour val | hour test | node test MAE | node spike MAE |
|---|---|---|---|---|
| zero | 12.20 | 27.95 | 33.48 | 689.3 |
| same hour, last verified day | 17.06 | 42.51 | | |
| hour model | 10.42 | 25.92 | 31.38 | 672.8 |
| hour mean known exactly | | | 18.42 | 343.1 |

- **Takeaway**: closes 14% of the gap to the exact-hour-mean bound. Top inputs: DA energy price, 7-day lagged hour mean, mean DA LMP, solar share, 7-day RT shadow total.

## E09: Fresh real-time features in the hour model (Oct 29, fold 0)

- **Setup**: unverified RT LMPs are in the DB through HE14 of the cutoff day; verified values stand in for them in training. Five features: same hour previous day (48h), cutoff day HE1-14 mean (spread and energy-only), cutoff day HE14, previous day mean.

| model | inputs | hour test MAE | node spike MAE |
|---|---|---|---|
| cutoff day HE14 alone, as the guess | 1 | 26.13 | |
| LightGBM L1, current | 53 | 25.92 | 672.8 |
| LightGBM L1, current + fresh | 58 | 26.03 | 672.7 |
| LightGBM Huber, current + fresh | 58 | 26.45 | 669.4 |
| linear fit on 4 fresh features | 4 | 25.82 | 636.5 |

- **Takeaway**: fresh features carry signal (HE14 correlation 0.54) but overlap with the system features; no gain combined. Fold 0's test months swing twice as hard as training (hour-mean std 75 vs 35), so one fold is a weak judge.

## E10: Run A / Run B across seeds, and permutation importance (Oct 29, fold 0, GPU)

**Title: Seed check of runs A and B and permutation importance on Colab, Author: Angie Ohaeri (assisted), Date: October 29th Time: 12:07pm CT**

- **Question**: do the system-feature variants beat the 15-input baseline once seed noise is counted, and which inputs does the model use?
- **Setup**: Colab Tesla T4 (`device=cuda` tag; decisions.md, Colab GPU training), the A/B cell in `models/model.ipynb`, graph without `hifld_osm` edges (4,865 edges), 1,580-node model. Seeds 0, 1, 2. Run A = 15 inputs + `da_shadow_total`, `da_constraint_count` copied into every node. Run B = `ContextGNN`, 9 node inputs + 16 hour-level inputs added after message passing. About 0.7 min per run.

Test MAE / spike MAE, original 1,505 nodes (all 1,580 in MLflow `test_mae`):

| variant | seed 0 | seed 1 | seed 2 | mean |
|---|---|---|---|---|
| baseline, 15 inputs | 29.28 / 618.9 | 29.01 / 625.0 | 28.86 / 622.3 | **29.05 / 622.1** |
| A: 15 + 2 target-day | 29.00 / 615.4 | 29.21 / 627.7 | 29.15 / 605.5 | 29.12 / **616.2** |
| B: 9 node + 16 context | 29.16 / 622.4 | 29.57 / 643.9 | 29.32 / 631.7 | 29.35 / 632.7 |

- **Seed noise**: the baseline alone ranges 28.86-29.28 across seeds, wider than any gap between variants. Seed 0's A-over-baseline lead (0.28) reversed at seeds 1 and 2. Compare variants on the mean of 3+ seeds from now on.
- **Permutation importance** (Run A, seed 0, test, all 1,580 nodes; base 29.21 / 623.2). Each input's values are shuffled across hours (the same shuffle for every node), 3 shuffles averaged. Increase in MAE / spike MAE:

| input | MAE | spike MAE |
|---|---|---|
| `da_lmp` | +11.15 | +56.4 |
| `da_congestion_price` | +2.45 | +0.3 |
| `congestion_spread_mean_7d` | +1.13 | +2.4 |
| `spread_mean_7d` | +0.61 | +3.5 |
| `da_marginal_loss_price` | +0.48 | +2.3 |
| `da_shadow_total` | +0.38 | +9.8 |
| `spike_share_7d` | +0.33 | +1.6 |
| `da_constraint_count` | +0.15 | -0.1 |
| calendar (6), `spread_std_7d`, `congestion_spread_last_verified_day` | +0.08 or less | within ±0.6 |
| `spread_last_verified_day` | -0.07 | +2.8 |
| `is_weekend` | -0.20 | -2.6 |

- **Takeaway**: neither variant beats the baseline on MAE; A's spike MAE is ~6 lower on average but swings 22 between seeds; B is worse on both. The model leans on the day-ahead price itself (`da_lmp`), then DA congestion and the 7-day averages; `da_shadow_total` matters most for spikes after `da_lmp`. Yesterday's spread (`spread_last_verified_day`) is unused, matching E01 (no day-to-day persistence). Shuffling `is_weekend` slightly helps, a hint of overfitting to it. Inputs that move together share credit, so a low score means "not needed given the others", not "no signal". One model, one fold.
- **Artifacts**: `permutation_importance_test.csv` on Run A's MLflow run (`e3892389a8f6484ebf10d18f3a846de0`).

## Open

- GNN on the node part (spread minus hour mean), node and zone inputs only, plus the hour model; compare to 29.27.
- Hour model on all 4 folds.
- From here on, score on all 1,580 nodes, with the 1,505 number alongside for one round.

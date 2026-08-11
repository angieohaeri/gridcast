# Decisions

Why I made certain decisions, for future reference.

## Project Pivot

**Title: Pivoted from bike-share availability to PJM electricity demand forecasting, Author: Angie Ohaeri, Date: August 4th Time: (session)**

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


## Database Design

**Title: `time` is Interval End (Hour Ending), in UTC, for `load` and `lmp`, Author: Angie Ohaeri, Date: August 10th Time: (session)**

Switched from Interval Start to Interval End (PJM's "HE" convention) for industry
consistency; both producers must agree since the `load`/`lmp` dbt join depends on it.
`lmp_producer.py` also now converts from US/Eastern to UTC (`load` was already UTC).

Migrated existing rows (+1h shift, 189,164 `load` / 725,305 `lmp` rows) so old and new
rows don't silently mix conventions. Gotcha: a plain `UPDATE ... SET time = time +
INTERVAL '1 hour'` fails partway through on TimescaleDB hypertables - chunks enforce
their range via CHECK constraint, and UPDATE doesn't re-route rows across chunks like
INSERT does. Fixed by copying to a staging table, truncating, and re-inserting.

**Title: `lmp` retains all 23 PJM zones historically; `load` only ever has the 4 in-scope zones, Author: Angie Ohaeri, Date: August 10th Time: (session)**

`load`'s scope (4 zones + `RTO`) was fixed at the query level from the start
(`load_producer.py:80`), so nothing else was ever stored. `lmp`'s 4-zone scope was
decided later (`lmp_producer.py:40`) - the historical bulk import predates that decision
and pulled all 23 zones, which were never pruned after. Confirmed via `SELECT zone,
count(*) FROM lmp GROUP BY zone` (same for `load`), not assumed.

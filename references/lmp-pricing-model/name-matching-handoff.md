# Facility to substation name matching: session handoff

**Title: Handoff for continuing facility/substation name matching, Author: Angie Ohaeri (assisted), Date: October 27th Time: 8:45pm CT**

## Purpose
`public.substations`, `public.facilities`, `public.pnodes` (plus `contingency_equipment`, `plants`, `generators`) are
**temporary working tables** for matching PJM shadow-price facility names to substations as accurately as possible.
Goal: fill `facilities.end1_substation_id` / `end2_substation_id` so shadow prices can attach to GNN pnodes. Accuracy
matters more than coverage. Do not change anything live (DB ALTER/DROP, commits, deploys) without asking.

## Where things are
- Code: `network_reference/{schema.sql,build.py,eia860.py}`. Run `uv run python -m network_reference.build`.
  The build is transactional and **deletes and reloads every table**, so manual edits in the DB are lost on rebuild.
  Anything learned must live in a file the build reads.
- Data (untracked): `data/external/{lmp-bus-model-data,pjm-pnodes,m2m-flowgates,eia860}/`.
- Review files in `references/lmp-pricing-model/`: `unmatched-facilities-review.csv` (217 rows, Angie's answers in
  `angie_end1`, `angie_end2`, `comments`), `facility-crosswalk-review.csv`, `location-disagreements-review.csv` (no
  comments added). Nothing from these has been applied yet.

## State of facilities (parse_status)
line both_ends 344, line one_end 18, line no_match 184, substation_equipment end_matched 111 / no_match 15,
unclassified end_matched 76 / no_match 601 (underscore and model-name styles not parsed yet), external 800 (MISO/NYISO,
not expected to match), interface 3. The 217-row review file = line/one_end/no_match + substation_equipment no_match.

## Findings
- `substations.source` is `bus_model` (6,390, has zone) or `contingency_list` (1,346, no zone, 0 pnodes).
- The contingency list truncates codes to 8 characters, creating duplicate substations: BRANCHBU, BURCHESH, CONASTON,
  JEFFERSO, NEWFREED, PRUNTYTO, WYLIERID (vs the longer bus-model code) and STJOHNS (vs ST_JOHNS). Safe to merge by rule.
- Angie says the pairs GOOSECRE/GOOSECRK and MIDLOTH/MIDLOTHIAN are probably the same station. Plan: add a
  `substation_group_id`, keep original ids.
- Angie's label typos (her words: 88SKOKI, 110 DEVO, 945 CRET are typos; PLANO needs `167 PLANO` or `527PLANO`).
  Others to correct: MIDLOTHAIN->MIDLOTHIAN, RODGERSRD->ROGERSRD, SHENAGO->SHENANGO, EPALMRT->EPALMERT,
  LEXINGTO->LEXINGTN/LEXINGTON, CAYUGA CT->CAYUGACT, CASEY 3->`CASEY  3` (two spaces), GOOSE CRE/CRK->GOOSECRE/GOOSECRK.
  Names not in `substations` (BURNHAM, MISO/ComEd area) stay external/unmatched. `L345.Wayne-SilverLake` end 2 is
  probably SILVERLK (AEP), not SILVERLA (AECO). 27 rows are unlabeled.
- Contingency-style names (`L230.Dooms-Charlottesville.233`) should be resolved by an exact join to the contingency list
  (element codes), not by parsing the `L230.` prefix.
- Lines with 3+ stations: endpoints are first and last; keep middle stations in a `middle_codes` column. Split `+`
  names into separate elements first; digits like `8803` are line ids, not stations.
- Rule: exact matching only, no substring matching.

## Agreed plan (nothing started)
1. Merge the 8 truncation duplicates. 2. Apply Angie's labels via an alias CSV in `network_reference/` that the build
   reads (name token -> substation code). 3. Resolve contingency-style names by exact join. 4. Rank remaining candidates
   by voltage (`voltages_kv`), zone of the resolved other end, contingency/M2M/EIA-860 evidence; link only when exactly
   one survives, otherwise leave `n_matches > 1` for Angie. 5. Hold out her labels to measure accuracy before
   auto-applying.

## Findings from checking labels against the contingency list
**Title: Contingency-join check of Angie's labels, Author: Angie Ohaeri (assisted), Date: October 27th Time: 9:00pm CT**

- The review CSVs now live in `~/Downloads/`, not in this folder.
- 117 of the 217 review rows are contingency names, so their stations can be read straight from the contingency list
  (the substations that have `CB` breaker elements). On the 100 rows with two such stations, both ends agree with
  Angie's labels on 42 and one end agrees on 39. Most disagreements are a same-named station in another utility:
  MARS (ATSI) vs MARS2 (DOM), HAYES (DOM) vs HAYES_FE (ATSI), BUTLER (AECO) vs BUTL APS, NORTHWES (BGE) vs NWEST (DOM),
  HARRISBU (PPL Harrisburg) vs HARRSNBG (DOM Harrisonburg), NELSON (DPL) vs 155 NELS (ComEd), 502JCT (AEP) vs 111 ELEC.
  The contingency list is the better source for these rows.
- Corrections to the plan above: GOOSECRE (DOM, 500 kV) and GOOSECRK (Illinois, `[ILL_EQ]`, 345 kV) are **different**
  stations, so they are not grouped. STJOHNS is NIPSCO St. Johns (MISO), not PPL ST_JOHNS, so it is not merged.
  `L345.Wayne-SilverLake` is ComEd: 144 WAYN to 138 SILV, not SILVERLK. Corbin is CRBN (Illinois/MISO), not N_/S_CORBIN.
- Truncation duplicates with exactly one longer bus-model code: 10, not 8 (adds 112 WILT, 167 PLAN, 23 COLLI).
- Contingency codes name a whole station, but bus-model codes are often one bus/voltage. Dominion splits stations as
  `X` (500 kV) and `X4` (230 kV): CARSON/CARSON4, DOOMS/DOOMS4, ELMONT/ELMONT4, VALLEY/VALLEY4, MIDLOTHIAN/MIDLOTH. AEP
  and PENELEC do similar things (KAMMER/KAMMER2, CONEMAUGH/CONEMAUG). `L500.Carson-Midlothian` lists CARSON4 and
  MIDLOTH in the contingency list, but the 500 kV pnodes are at CARSON and MIDLOTHIAN. Linking facilities to the right
  pnodes needs a station grouping.
- Sectionalized lines (`(Sctnlz)`) can list a breaker station that is not in the name (Carson-ClubHouse lists SAPONY),
  so take endpoints from the name order, not only from the breaker list.

## Station grouping and facility linking (built, not yet loaded)
**Title: Station groups in network_reference/build.py, Author: Angie Ohaeri (assisted), Date: October 27th Time: 9:10pm CT**

- `substations.station_id` points at the contingency-list station code a bus-model code belongs to (`station_rule`):
  `self` (default), `truncated_code` (bus code's first 8 characters are a station code, 24 rows), or `name_token`
  (EHV codes only, 40 rows: CARSON -> CARSON4, MIDLOTHIAN -> MIDLOTH). `name_token` means the code, read as a word,
  appears in at least 3 contingency names in the same zone and one breaker station is in at least 80% of them. A
  station must also carry the bus code's top voltage in the contingency list. It is limited to EHV codes because a
  115 kV tap (VERNON) was grouped into the wrong station (FRANKLIN).
- Facility ends, in order of trust (`end1_source`/`end2_source`): `contingency_list` (facility name is a contingency
  name, ends are its breaker stations ordered by the name, `middle_codes` keeps the rest), then `label` (from
  `network_reference/facility_labels.csv`, Angie's labels with the typo fixes above, used only when the station has
  the facility's voltage), then `parsed`. Multi-station names whose first and last stations cannot be read are left as
  `contingency_list_unresolved` (12).
- Dry run: lines with both ends 344 -> 791, contingency-linked ends 515, labels applied 61 (2 rejected).
- Holdout check of Angie's labels against the contingency list, by station: 181 of 216 agree. Of the 35 that
  disagree, most are a middle station picked as an end (Bethlehem, 76 Blue Island) or a sectionalized line where the
  breaker is elsewhere. About 9 are a different station with the same name. The voltage check only rejects 12 of the
  35, so `label` ends are roughly 90-95% right; filter on `end*_source` when that matters.

## Also pending (separate from matching)
Live DB renames of the raw tables, deleting the old Prefect deployment, commit/push (ask first), 601 unclassified
facilities, adding ~42-84 missing pnodes as GNN nodes, `agg_definitions.csv` (useful for aggregate membership; 0 of 401
unmatched facility codes resolve through it; not loaded).

# Line edges: data quirks and a structural alternative to name parsing

**Title: Quirks behind missed line edges, Author: Angie Ohaeri (assisted), Date: October 28th Time: 4:51pm CT**

Applied the same day: `network_reference.build` writes these lines to `data/interim/contingency_lines.csv`, and
`network_reference.line_edges` adds them to `raw_lmp.pnode_topology_edges` by source (`line_single_outage`,
`line_only_candidate`, `line_id_letters`, `line_same_circuit`, `facility_line`, `line_via_1`, `line_via_2`):
1,280 new edges, isolated GNN substations 528 -> 218.

## The structural source
Every line outage in `pjm-contingency-list.xml` has exactly one `LN` row per line: the station it is listed
from, the voltage, and the line's EMS ID (`b1="CONASTON" b2="500 KV" b3="CNS-PEA"`). The same outage opens
breakers (`CB`) at both ends. So a line's two stations can be read without text matching:
- near end = the `LN` row's station
- far end = the other station with breakers at the same voltage in that outage

8,386 unique `LN` rows (line segments). Far end found for 6,026:

| method | segments | station pairs | agrees with map edge* | confidence |
|---|---|---|---|---|
| single-line outage, one other breaker station | 3,574 | 3,167 | 287 / 336 (85%) | high |
| several lines in the outage, only one other station | 444 | 213 | not measured | high |
| several lines, far end picked by the ID's letters (`CNS-PEA` -> `PEACHBOT`) among the outage's stations | 2,008 | 1,716 | 61 / 91 (67%) | medium |
| unresolved | 2,360 | | | |

\*Only for pairs where both stations are GNN nodes with a location. Map edges are not complete, so a
disagreement does not mean the pair is wrong: the median distance of disagreeing single-outage pairs is
12 km. A few medium pairs are clearly wrong (Tazewell-Buckhorn, 374 km).

Totals: 5,096 station pairs (647 today from facilities), 640 between two GNN stations. Isolated GNN
substations would drop from 482 to 334.

Check of today's facility edges against these pairs: `contingency_list` ends 348 / 400 confirmed,
`label` 31 / 48, `parsed` 267 / 345.

## `_LN` facility names
`CONASTON_500KVCNS-PEA_1_LN` = station `CONASTON` (spaces written as `_`), `500` kV, line ID `CNS-PEA`,
circuit `1`. Joined on (station, kV, line ID with spaces removed): 595 / 630 match an `LN` row exactly, 451 get
a far end, carrying 4.24M of the 5.27M day-ahead weight. 21 names have no `KV` field
(`0704_07013807043_1_LN`) and need their own rule.

## Quirks in facility names (why text parsing misses)
1. `_LN` EMS names are filed as `external` by the `_LN\b` regex (630).
2. ComEd codes are a number plus a truncated name (`6 BYRON`, `12 DRESD`), but names write `Byron` or
   `12Dresden`. ComEd line IDs are numbers (`13806 1`), so the ID has no letters to check against.
3. Codes are cut to 8 characters, with odd spacing (`MAHANS LANE` -> `MAHANSLA`, `CASEY  3` has two spaces).
4. Contingency-style names use CamelCase (`SugarCreek`, `KarnsCity`) while codes are upper case with no spaces.
5. Short forms: `Hopat`, `Lacka`, `Shick`, `PB` (Peach Bottom), `JF` (Jacksons Ferry), `EPalm`.
6. PJM's own misspellings: `Dreesser`, `Metchuen`, `Lacakawa`, `DICERSO2`, `SELAMPAR`, `HAMSHIR`.
7. Same name in different utilities: MARS (ATSI) / MARS2 (DOM), HAYES / HAYES_FE, BUTLER / BUTL APS,
   NELSON (DPL) / 155 NELS (ComEd), NORTHWES (BGE) / NWEST (DOM).
8. Codes that cover only one voltage: Dominion `CARSON` (500 kV) / `CARSON4` (230 kV), same for DOOMS, ELMONT,
   VALLEY. AEP and PENELEC do the same (KAMMER/KAMMER2).
9. Codes that appear in more than one zone: GORDONSV, MILLVILL, SAMMISFE, SOMERSET.
10. Compound names: `+`, `&`, ` and `, with `DBL`/`TEMP` prefixes. The parser keeps only the first element.
11. Lines through 3+ stations (tapped lines). Name order does not always match the physical order, and line
    numbers sit in the middle (`L345.Gdngs-11614-BlIsl-Wltn.11614`).
12. Suffixes mixed into station text: circuit letters, `TIE`, `(Sctnlz)`, `[ILL_EQ]`, `(NIPS)`, `(IP/MISO)`,
    `(DUKE)`, line numbers (`.2027`, `5012`, `R`).
13. Sectionalized lines: the outage opens breakers at a switching station that is not in the name
    (Carson-ClubHouse lists SAPONY). The `LN` method gives the real A-to-switching-station segment.
14. Names that are not lines: transformers with ratio prefixes (`230/115.Carolina.TX4`), grouped outages
    (`AEP-DOM contingency 11`, `BED-BLA`).
15. M2M flowgates (`A - B kV l/o C - D kV`): the monitored line is mostly in MISO.

## Quirks in the contingency list itself (why the structural method misses)
1. Far-end breakers not listed (261 segments): lines tied to other RTOs (`(Ameren)`, `[CIN]`, `(Tie)`) and some
   sectionalized lines.
2. Outages that take out several lines (2,099 unresolved segments): tapped lines where the ID has no letters
   (ComEd `13806 1`) or matches more than one station.
3. ComEd: 212 of 268 isolated ComEd GNN substations stay isolated, because of item 2 and because no ComEd node
   has a location for the map edges.

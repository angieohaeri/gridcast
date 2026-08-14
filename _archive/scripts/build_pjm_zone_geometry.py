"""Build PJM zone boundaries from real utility service-territory polygons.

An earlier version of this script inferred zone shapes from substation *points* (Voronoi
tessellation over matched HIFLD substations) since no direct PJM zone shapefile exists. That
approach never looked right: Voronoi cells fill 100% of whatever envelope they're given, so
sparse or bbox-bound areas either ballooned into the ocean or filled entire host states well
past PJM's actual footprint.

The fix: HIFLD also publishes "Electric Retail Service Territories" - actual drawn utility
service-area polygons, sourced from EIA/ORNL, with a `CNTRL_AREA` field that already tags each
one's balancing authority/RTO. Filtering to `CNTRL_AREA = 'PJM'` gives real boundaries for
every PJM member utility directly, no inference needed:

    https://services3.arcgis.com/OYP7N6mAJJCyH6hd/arcgis/rest/services/
        Electric_Retail_Service_Territories_HIFLD/FeatureServer/0

That query returns 311 polygons, saved to
`data/external/hifld_electric_retail_service_territories_pjm.geojson`. Only ~25 of those are
the major Transmission Owners gridcast tracks as zones (`MAJOR_UTILITY_TO_ZONE`); the rest are
small municipal utilities and rural co-ops (e.g. "CITY OF ANDERSON - (IN)", "BARC ELECTRIC COOP
INC") that sit inside or alongside a major utility's territory without being one of PJM's
settlement zones themselves. Those get folded into whichever major zone polygon they overlap
most - if a small enclave doesn't overlap any (a sliver gap between two territories), it falls
back to whichever zone polygon is geographically nearest.

`OVEC` (Ohio Valley Electric Corp) is dropped - a legacy PJM member, not one of gridcast's 20
tracked zones.

HIFLD's polygons follow jurisdictional/county lines, which sometimes run out into a bay or
lake rather than hugging the actual shoreline (e.g. a sliver of AEP's Indiana Michigan Power
corner overhangs Lake Michigan; a DOM inlet along the NC coast overshoots past a barrier
island). Zones are clipped to Natural Earth's 10m land polygon
(`data/external/ne_10m_land/`) as a final step to trim these back to real coastline.
"""

import geopandas as gpd
from loguru import logger
import pandas as pd

from gridcast.config import EXTERNAL_DATA_DIR, setup_logging

setup_logging()

# the ~25 PJM Transmission Owners gridcast tracks as zones, by their HIFLD service-territory NAME
MAJOR_UTILITY_TO_ZONE = {
    "ATLANTIC CITY ELECTRIC CO": "AE",
    "APPALACHIAN POWER CO": "AEP",
    "OHIO POWER CO": "AEP",
    "KENTUCKY POWER CO": "AEP",
    "INDIANA MICHIGAN POWER CO": "AEP",
    "WHEELING POWER CO": "AEP",
    "MONONGAHELA POWER CO": "AP",
    "THE POTOMAC EDISON COMPANY": "AP",
    "WEST PENN POWER COMPANY": "AP",
    "OHIO EDISON CO": "ATSI",
    "CLEVELAND ELECTRIC ILLUM CO": "ATSI",
    "THE TOLEDO EDISON CO": "ATSI",
    "PENNSYLVANIA POWER CO": "ATSI",
    "BALTIMORE GAS & ELECTRIC CO": "BC",
    "COMMONWEALTH EDISON CO": "CE",
    "DAYTON POWER & LIGHT CO": "DAY",
    "DUKE ENERGY OHIO INC": "DEOK",
    "DUKE ENERGY KENTUCKY": "DEOK",
    "VIRGINIA ELECTRIC & POWER CO": "DOM",
    # rural VA co-ops - no PJM zone of their own; on PJM's own zone map their territory reads
    # as part of Dominion's, and excluding them (same size-threshold logic as everything else
    # under MINOR_AREA_THRESHOLD_KM2 below) left a large real hole in central/western VA
    # between DOM, AEP, and AP
    "MECKLENBURG ELECTRIC COOPERATIVE": "DOM",
    "SOUTHSIDE ELECTRIC COOP, INC": "DOM",
    "RAPPAHANNOCK ELECTRIC COOP": "DOM",
    "SHENANDOAH VALLEY ELEC COOP": "DOM",
    "CRAIG-BOTETOURT ELECTRIC COOP": "DOM",
    "CENTRAL VIRGINIA ELECTRIC COOP": "DOM",
    "COMMUNITY ELECTRIC COOP": "DOM",
    "BARC ELECTRIC COOP INC": "DOM",
    "PRINCE GEORGE ELECTRIC COOP": "DOM",
    "NORTHERN NECK ELEC COOP, INC": "DOM",
    "A & N ELECTRIC COOP": "DOM",
    "NORTHERN VIRGINIA ELEC COOP": "DOM",
    # same pattern across the Potomac in southern Maryland/the Eastern Shore - large co-ops
    # with no PJM zone of their own, empirically overlapping DOM/DPL respectively
    "SOUTHERN MARYLAND ELEC COOP INC": "DOM",
    "CHOPTANK ELECTRIC COOPERATIVE, INC": "DPL",
    "DELMARVA POWER": "DPL",
    "DUQUESNE LIGHT CO": "DUQ",
    "JERSEY CENTRAL POWER & LT CO": "JC",
    "METROPOLITAN EDISON CO": "ME",
    "PECO ENERGY CO": "PE",
    "POTOMAC ELECTRIC POWER CO": "PEP",
    "PPL ELECTRIC UTILITIES CORP": "PL",
    "PENNSYLVANIA ELECTRIC CO": "PN",
    "PUBLIC SERVICE ELEC & GAS CO": "PS",
    "ROCKLAND ELECTRIC CO": "RECO",
    # EKPC (East Kentucky Power Coop) is a generation & transmission co-op - it doesn't retail
    # directly to end customers, so it has no HIFLD entity of its own. Its 16 owner-member
    # distribution co-ops do, and match exactly to HIFLD's KY co-op entries:
    "BIG SANDY RURAL ELEC COOP CORP": "EKPC",
    "BLUE GRASS ENERGY COOP CORP": "EKPC",
    "CLARK ENERGY COOP INC - (KY)": "EKPC",
    "CUMBERLAND VALLEY ELECTRIC, INC.": "EKPC",
    "FARMERS RURAL ELECTRIC COOP CORP - (KY)": "EKPC",
    "FLEMING-MASON ENERGY COOP INC": "EKPC",
    "GRAYSON RURAL ELECTRIC COOP CORP": "EKPC",
    "INTER COUNTY ENERGY COOP CORP": "EKPC",
    "JACKSON ENERGY COOP CORP - (KY)": "EKPC",
    "LICKING VALLEY RURAL E C C": "EKPC",
    "NOLIN RURAL ELECTRIC COOP CORP": "EKPC",
    "OWEN ELECTRIC COOP INC": "EKPC",
    "SALT RIVER ELECTRIC COOP CORP": "EKPC",
    "SHELBY ENERGY CO-OP, INC": "EKPC",
    "SOUTH KENTUCKY RURAL E C C": "EKPC",
    "TAYLOR COUNTY RURAL E C C": "EKPC",
}

TERRITORIES_PATH = EXTERNAL_DATA_DIR / "hifld_electric_retail_service_territories_pjm.geojson"
LAND_PATH = EXTERNAL_DATA_DIR / "ne_10m_land" / "ne_10m_land.shp"
OUTPUT_PATH = EXTERNAL_DATA_DIR / "pjm_zones.geojson"
CONUS_ALBERS = 5070
# generous box around PJM's footprint - keeps the land clip from processing unrelated geometry
# (Natural Earth's land layer is global)
PJM_AREA_BBOX = (-92, 33, -72, 47)

territories = gpd.read_file(TERRITORIES_PATH)
territories["geometry"] = territories["geometry"].make_valid()
territories = territories.to_crs(CONUS_ALBERS)

territories["zone_id"] = territories["NAME"].map(MAJOR_UTILITY_TO_ZONE)
major = territories[territories["zone_id"].notna()]

# "minor" was meant to catch small municipal/co-op enclaves that sit inside a major zone's
# footprint and would otherwise leave a gap - but plenty of PJM-tagged entries are themselves
# large, independent rural co-ops (Midwest Energy Cooperative in MI is 17,835 km2, bigger than
# some tracked zones) that just happen to border a major zone without belonging to it. Folding
# those in wholesale distorted the neighboring zone's shape (a rural co-op's odd, elongated
# footprint reading as a "sliver" on the map). Only fold in entries small enough to plausibly be
# gap-filling enclaves; anything bigger is dropped, same as OVEC.
MINOR_AREA_THRESHOLD_KM2 = 1_000
territories["area_km2"] = territories.geometry.area / 1e6
minor = territories[
    territories["zone_id"].isna()
    & (territories["NAME"] != "OHIO VALLEY ELECTRIC CORP")
    & (territories["area_km2"] < MINOR_AREA_THRESHOLD_KM2)
]
logger.info(f"{len(major)} major utility territories, {len(minor)} minor (co-op/municipal) territories to fold in")

major_dissolved = major.dissolve(by="zone_id").reset_index()[["zone_id", "geometry"]]

# fold each minor territory into whichever major zone it overlaps most; a small enclave with no
# overlap at all (a sliver gap between two territories) falls back to whichever is nearest
overlap = gpd.overlay(
    minor[["NAME", "geometry"]].reset_index(names="minor_id"),
    major_dissolved,
    how="intersection",
    keep_geom_type=False,
)
overlap["area"] = overlap.geometry.area
best_overlap = overlap.loc[overlap.groupby("minor_id")["area"].idxmax(), ["minor_id", "zone_id"]]

minor = minor.reset_index(names="minor_id").merge(best_overlap, on="minor_id", how="left", suffixes=("", "_overlap"))
no_overlap = minor["zone_id_overlap"].isna()
if no_overlap.any():
    # a small gap-filling enclave can sit just across a sliver from its zone with no direct
    # overlap - but an unbounded "nearest zone" fallback also happily reattaches an orphan
    # (e.g. a Michigan town that used to border the excluded Midwest Energy Cooperative
    # territory) to whichever tracked zone is merely closest, even 30+ miles away with no real
    # adjacency. Cap the fallback to a tight distance so only genuine near-misses get folded in;
    # true orphans are dropped, same as the large independent co-ops above.
    NEAREST_FALLBACK_MAX_M = 3_000
    nearest = gpd.sjoin_nearest(
        minor[no_overlap][["minor_id", "geometry"]], major_dissolved, how="left", max_distance=NEAREST_FALLBACK_MAX_M
    )
    nearest = nearest.drop_duplicates(subset="minor_id")
    minor.loc[no_overlap, "zone_id_overlap"] = minor.loc[no_overlap, "minor_id"].map(
        nearest.set_index("minor_id")["zone_id"]
    )
minor = minor[minor["zone_id_overlap"].notna()]
minor["zone_id"] = minor["zone_id_overlap"]

all_territories = pd.concat([major[["zone_id", "geometry"]], minor[["zone_id", "geometry"]]], ignore_index=True)
all_territories = gpd.GeoDataFrame(all_territories, crs=CONUS_ALBERS)

# HIFLD's individual utility polygons aren't mutually exclusive - a big investor-owned
# utility's recorded territory sometimes isn't clipped around a co-op/muni enclave nested
# inside it (e.g. the VA co-ops folded into DOM above sit mostly inside VIRGINIA ELECTRIC &
# POWER CO's own polygon, but also spill into APPALACHIAN POWER CO's (AEP) territory by
# several hundred km2). Two zones claiming the same ground renders as a visibly darker
# "double-filled" patch where their translucent fills stack. Resolve smallest-first: a co-op's
# specific, granular boundary is more likely correct for a given spot than a big utility's
# broad-brush outer polygon, so each entity carves its footprint out of whatever's already
# claimed by a smaller one, and larger entities only get what's left over.
all_territories = all_territories.assign(area=all_territories.geometry.area).sort_values("area")
claimed = None
carved = []
for geom in all_territories.geometry:
    remaining = geom if claimed is None else geom.difference(claimed)
    carved.append(remaining)
    claimed = remaining if claimed is None else claimed.union(remaining)
all_territories["geometry"] = carved
all_territories = all_territories[~all_territories.geometry.is_empty]

zones = all_territories.dissolve(by="zone_id").reset_index()

land = gpd.read_file(LAND_PATH, bbox=PJM_AREA_BBOX).to_crs(CONUS_ALBERS)
land = land.geometry.make_valid().union_all()
zones["geometry"] = zones["geometry"].intersection(land)
zones["geometry"] = zones["geometry"].simplify(100, preserve_topology=True)
zones = zones.to_crs(4326)

zones.to_file(OUTPUT_PATH, driver="GeoJSON")
logger.success(f"{OUTPUT_PATH.name}: {len(zones)} zone polygons for {sorted(zones['zone_id'])}")

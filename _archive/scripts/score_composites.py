"""Score every station subset per zone, and report best composite vs best single."""

from itertools import combinations

import numpy as np
import pandas as pd

from gridcast.config import INTERIM_DATA_DIR


def r_squared(y, design):
    coeffs, *_ = np.linalg.lstsq(design, y, rcond=None)
    resid = y - design @ coeffs
    return 1 - (resid @ resid) / ((y - y.mean()) @ (y - y.mean()))


def partial_r2(df):
    y = df["demand_mw"].to_numpy(float)
    hour = pd.get_dummies(df["time"].dt.hour, prefix="h", drop_first=True).to_numpy(float)
    dow = pd.get_dummies(df["time"].dt.dayofweek, prefix="d", drop_first=True).to_numpy(float)
    baseline = np.hstack([np.ones((len(df), 1)), hour, dow])
    r2_base = r_squared(y, baseline)
    r2_full = r_squared(y, np.hstack([baseline, df[["cdh", "hdh"]].to_numpy(float)]))
    return (r2_full - r2_base) / (1 - r2_base)


load = pd.read_csv(INTERIM_DATA_DIR / "pjm_load_hourly_2023_present.csv")
load["time"] = pd.to_datetime(load["Interval End"], utc=True, format="mixed")
load = load.groupby(["time", "Zone"], as_index=False)["MW"].sum().rename(columns={"MW": "demand_mw"})

temps = pd.read_parquet(INTERIM_DATA_DIR / "candidate_temps.parquet")
temps["time"] = temps["time"] + pd.Timedelta(hours=1)

rows = []
for zone_id, zone_temps in temps.groupby("zone_id"):
    zone_load = load[load["Zone"] == zone_id][["time", "demand_mw"]]
    wide = zone_temps.pivot_table(index="time", columns="city", values="temperature")
    cities = list(wide.columns)

    for size in range(1, len(cities) + 1):
        for combo in combinations(cities, size):
            temp_c = wide[list(combo)].mean(axis=1)
            df = pd.DataFrame({"time": temp_c.index, "temp_f": temp_c.to_numpy() * 9 / 5 + 32})
            df["cdh"] = (df["temp_f"] - 65).clip(lower=0)
            df["hdh"] = (65 - df["temp_f"]).clip(lower=0)
            df = df.merge(zone_load, on="time", how="inner").dropna()
            rows.append(
                {"zone_id": zone_id, "n": size, "stations": " + ".join(combo), "partial_r2": partial_r2(df)}
            )

scores = pd.DataFrame(rows)
scores.to_csv(INTERIM_DATA_DIR / "zone_city_scoring.csv", index=False)

summary = []
for zone_id, chunk in scores.groupby("zone_id"):
    best_single = chunk[chunk["n"] == 1].nlargest(1, "partial_r2").iloc[0]
    best_multi = chunk[chunk["n"] > 1].nlargest(1, "partial_r2").iloc[0]
    summary.append(
        {
            "zone": zone_id,
            "best_single": best_single["stations"],
            "single_r2": best_single["partial_r2"],
            "best_composite": best_multi["stations"],
            "composite_r2": best_multi["partial_r2"],
            "gain": best_multi["partial_r2"] - best_single["partial_r2"],
        }
    )

out = pd.DataFrame(summary).sort_values("gain", ascending=False)
pd.set_option("display.width", 250)
print(out.to_string(index=False, float_format=lambda v: f"{v:.4f}"))

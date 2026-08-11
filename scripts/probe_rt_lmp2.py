import os
import gridstatus as gs
import pandas as pd

pjm = gs.PJM(api_key=os.environ["PJM_API_KEY"], retries=3)

raw = pjm._get_pjm_json(
    "rt_hrl_lmps",
    start=pd.Timestamp("2026-08-08", tz="US/Eastern"),
    end=pd.Timestamp("2026-08-10", tz="US/Eastern"),
    params={
        "type": "*ZONE*",
        "fields": "congestion_price_rt,datetime_beginning_ept,datetime_beginning_utc,equipment,marginal_loss_price_rt,pnode_id,pnode_name,row_is_current,system_energy_price_rt,total_lmp_rt,type,version_nbr,voltage,zone",
    },
    filter_timestamp_name="datetime_beginning",
    interval_duration_min=60,
    verbose=True,
)
print("\nRaw rt_hrl_lmps, no row_is_current filter:")
print("  rows:", len(raw))
if len(raw):
    print("  max datetime_beginning_utc:", raw["datetime_beginning_utc"].max())
    print("  row_is_current value counts:")
    print(raw["row_is_current"].value_counts(dropna=False))

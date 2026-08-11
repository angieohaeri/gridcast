# Data Flow

**Title:** Data flow diagram — sources through storage
**Author:** Angie Ohaeri
**Date:** August 10, 2026 **Time:** 9:10pm

Traces every byte from external API to TimescaleDB, and sketches the not-yet-built modeling path. Solid lines/boxes are implemented today; dashed boxes are planned (`references/architecture.md`). Colors group nodes by role — data, pipeline infra, modeling, and serving/deployment.

---

## End-to-end flow

```mermaid
flowchart TB
    subgraph LEGEND[" "]
        direction LR
        L1["Data"]
        L2["Pipeline infra"]
        L3["Modeling"]
        L4["Serving / deployment"]
    end

    %% ---------- External sources (data) ----------
    subgraph SOURCES["External sources"]
        direction TB
        PJM["<b>PJM Data Miner 2</b><br/><i>gridstatus.PJM</i>"]
        EIA["<b>EIA Open Data v2</b><br/><i>gridstatus.EIA</i>"]
        OMF["<b>Open-Meteo Forecast</b><br/><i>no key required</i>"]
        OMH["<b>Open-Meteo Archive</b><br/><i>no key required</i>"]
    end

    ZONES["<b>Zone reference file</b><br/>pjm_weather_zones.csv<br/><i>4 zones — CE, DOM, AEP, BC — read-only mount</i>"]

    %% ---------- Producers (infra) ----------
    subgraph PRODUCERS["Producers"]
        direction TB
        LOADP["<b>load_producer.py</b><br/>hourly, 7-day trailing window"]
        LMPP["<b>lmp_producer.py</b><br/>hourly, 7-day trailing window"]
        WXP["<b>weather_producer.py</b><br/>every 20 min, current obs"]
    end

    BACKFILL["<b>backfill_weather.py</b><br/><i>one-off, bypasses Kafka</i>"]

    PJM -->|"load: zone MW + verified flag"| LOADP
    EIA -->|"load: RTO region data"| LOADP
    PJM -->|"real-time hourly LMP"| LMPP
    OMF -->|"current conditions"| WXP
    OMH -->|"hourly history"| BACKFILL

    ZONES -.-> LOADP
    ZONES -.-> LMPP
    ZONES -.-> WXP
    ZONES -.-> BACKFILL

    %% ---------- Kafka (infra) ----------
    subgraph KAFKA["Kafka — single broker, key = zone"]
        direction TB
        TLOAD(["<b>load</b>"])
        TLMP(["<b>lmp</b>"])
        TWX(["<b>weather</b>"])
        DLQL(["load_dlq"])
        DLQM(["lmp_dlq"])
        DLQW(["weather_dlq"])
    end

    LOADP --> TLOAD
    LMPP -->|"JSON, idempotent"| TLMP
    WXP --> TWX

    %% ---------- Consumers (infra) ----------
    subgraph CONSUMERS["Consumers — manual offset commits"]
        direction TB
        LOADC["<b>load_consumer.py</b>"]
        LMPC["<b>lmp_consumer.py</b>"]
        WXC["<b>weather_consumer.py</b>"]
    end

    TLOAD --> LOADC
    TLMP --> LMPC
    TWX --> WXC

    LOADC -.->|"rejected rows"| DLQL


    %% ---------- Storage (data) ----------
    subgraph TSDB["TimescaleDB — Postgres 16, db: gridcast"]
        direction TB
        HLOAD[("<b>load</b> hypertable<br/>upsert on time, zone, source")]
        HLMP[("<b>lmp</b> hypertable<br/>upsert on time, pnode_id")]
        HWX[("<b>weather</b> hypertable<br/>insert only")]
    end

    LOADC -->|"upsert"| HLOAD
    LMPC -->|"upsert"| HLMP
    WXC -->|"insert"| HWX
    BACKFILL -->|"insert, skips existing hours"| HWX

    %% ---------- Modeling (planned) ----------
    subgraph MODEL["Modeling — planned"]
        direction TB
        DBT["<b>dbt models</b><br/><i>joins load + weather + lmp</i>"]
        FEAT["<b>feature table</b><br/><i>lags, rolling means, zone</i>"]
        TRAIN["<b>LightGBM training</b><br/><i>logged to MLflow</i>"]
        REG["<b>MLflow registry</b>"]
    end

    %% ---------- Serving & deployment (planned) ----------
    subgraph SERVE["Serving & deployment — planned"]
        direction TB
        API["<b>FastAPI</b><br/>/predict"]
        DASH["<b>Streamlit + Pydeck</b><br/><i>via Cloudflare Tunnel</i>"]
    end

    HLOAD -.-> DBT
    HLMP -.-> DBT
    HWX -.-> DBT
    DBT -.-> FEAT
    FEAT -.-> TRAIN
    TRAIN -.-> REG
    REG -.-> API
    FEAT -.-> API
    API -.-> DASH
    HLOAD -.->|"actuals for the map"| DASH

    classDef dataNode fill:#dbeafe,stroke:#2563eb,color:#1e3a8a;
    classDef infraNode fill:#ede9fe,stroke:#7c3aed,color:#4c1d95;
    classDef modelNode fill:#fef3c7,stroke:#d97706,color:#78350f;
    classDef serveNode fill:#dcfce7,stroke:#16a34a,color:#14532d;
    classDef planned stroke-dasharray: 5 5;
    classDef legend fill:none,stroke:none;

    class PJM,EIA,OMF,OMH,ZONES,HLOAD,HLMP,HWX dataNode;
    class LOADP,LMPP,WXP,BACKFILL,TLOAD,TLMP,TWX,DLQL,DLQM,DLQW,LOADC,LMPC,WXC infraNode;
    class DBT,FEAT,TRAIN,REG modelNode;
    class API,DASH serveNode;
    class DBT,FEAT,TRAIN,REG,API,DASH planned;
    class L1 dataNode;
    class L2 infraNode;
    class L3 modelNode;
    class L4 serveNode;
    class LEGEND legend;
```

---

## Orchestration and control plane

Prefect drives every producer and consumer as a scheduled flow. It carries no grid data itself — only run state — and it stores that in a **separate `prefect` database on the same Postgres instance**, never in the hypertables.

```mermaid
flowchart LR
    DEP["<b>deployments.py</b><br/><i>serves all 6 flows</i>"] --> SRV["<b>prefect-server</b><br/>:4200"]

    SRV -->|"hourly at :10"| LP["<b>load_producer</b>"]
    SRV -->|"hourly at :10"| MP["<b>lmp_producer</b>"]
    SRV -->|"every 20 min"| WP["<b>weather_producer</b>"]
    SRV -->|"hourly at :15"| LC["<b>load_consumer</b>"]
    SRV -->|"hourly at :15"| MC["<b>lmp_consumer</b>"]
    SRV -->|"every 20 min"| WC["<b>weather_consumer</b>"]

    SRV <-->|"run metadata"| PDB[("<b>prefect</b> db")]
    PDB -.->|"same server, different db"| GDB[("<b>gridcast</b> db")]

    LP & MP & WP -->|"produce"| K(["Kafka"])
    K -->|"consume"| LC & MC & WC
    LC & MC & WC -->|"write"| GDB

    classDef infraNode fill:#ede9fe,stroke:#7c3aed,color:#4c1d95;
    classDef dataNode fill:#dbeafe,stroke:#2563eb,color:#1e3a8a;

    class DEP,SRV,LP,MP,WP,LC,MC,WC,K infraNode;
    class PDB,GDB dataNode;
```

The 5-minute producer→consumer offset is deliberate: producers publish at :10, so
consumers at :15 find a drained-and-settled topic rather than racing the write.

---

## Per-source detail

| Source | Endpoint | Cadence | Topic → Table | Write semantics |
|---|---|---|---|---|
| PJM Data Miner 2 | `hrl_load_metered` | hourly, 7-day window | `load` → `load` | upsert on `(time, zone, source='pjm')` |
| EIA Open Data v2 | `electricity/rto/region-data` | hourly, 7-day window | `load` → `load` | upsert on `(time, zone='RTO', source='eia')` |
| PJM Data Miner 2 | `rt_hrl_lmps` | hourly, 7-day window | `lmp` → `lmp` | upsert on `(time, pnode_id)` |
| Open-Meteo Forecast | `/v1/forecast`, `current=` | every 20 min | `weather` → `weather` | append-only |
| Open-Meteo Archive | `/v1/archive`, `hourly=` | manual, one-off | *(none)* → `weather` | direct insert, de-duped against existing hours |

**Why the trailing 7-day window matters:** PJM revises a given hour from unverified to verified over ~3 days, and EIA revises over ~1 day. Re-polling the past week and upserting means those revisions land automatically — the same `(time, zone, source)` row gets overwritten with the corrected number instead of accumulating duplicates. Weather has no revision concept, so it is append-only.

**Frequency alignment:** load and LMP are both hourly here and stamped on **interval end** ("hour ending"), which is what makes them joinable. Weather is sampled on a 20-minute poll clock and is *not* pre-aggregated at ingestion — the resample to hourly happens explicitly in dbt, never silently in a producer.

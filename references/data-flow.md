# Data Flow

**Title:** Data flow diagram — sources through storage
**Author:** Angie Ohaeri
**Date:** August 10, 2026 **Time:** 9:10pm
**Revised:** August 11, 2026 8:36am — split the single diagram into ingestion / downstream, dropped the inline legend

Traces every byte from external API to TimescaleDB, then the not-yet-built modeling path. Colors group nodes by role: <b>blue = data</b>, <b>purple = pipeline infra</b>, <b>amber = modeling</b>, <b>green = serving</b>. Endpoint and cadence detail lives in the table at the bottom rather than on the edges.

---

## Ingestion — sources to storage

Everything here is running today.

```mermaid
flowchart TB
    subgraph SOURCES["External sources"]
        direction LR
        PJM["<b>PJM Data Miner 2</b>"]
        EIA["<b>EIA Open Data v2</b>"]
        OMF["<b>Open-Meteo Forecast</b>"]
        OMH["<b>Open-Meteo Archive</b>"]
    end

    subgraph PRODUCERS["Producers"]
        direction LR
        LOADP["<b>load_producer</b>"]
        LMPP["<b>lmp_producer</b>"]
        WXP["<b>weather_producer</b>"]
    end

    subgraph KAFKA["Kafka — key = zone"]
        direction LR
        TLOAD(["<b>load</b>"])
        TLMP(["<b>lmp</b>"])
        TWX(["<b>weather</b>"])
    end

    subgraph CONSUMERS["Consumers — manual offset commits"]
        direction LR
        LOADC["<b>load_consumer</b>"]
        LMPC["<b>lmp_consumer</b>"]
        WXC["<b>weather_consumer</b>"]
    end

    subgraph TSDB["TimescaleDB hypertables"]
        direction LR
        HLOAD[("<b>load</b>")]
        HLMP[("<b>lmp</b>")]
        HWX[("<b>weather</b>")]
    end

    ZONES["<b>pjm_weather_zones.csv</b><br/><i>20 zones · 30 stations</i>"]
    BACKFILL["<b>backfill_weather</b><br/><i>one-off, bypasses Kafka</i>"]
    DLQ(["<b>DLQ topics</b><br/><i>load_dlq · lmp_dlq · weather_dlq</i>"])

    PJM --> LOADP
    EIA --> LOADP
    PJM --> LMPP
    OMF --> WXP
    OMH --> BACKFILL

    ZONES -.->|"zones + coords"| PRODUCERS
    ZONES -.-> BACKFILL

    LOADP --> TLOAD --> LOADC -->|"upsert"| HLOAD
    LMPP --> TLMP --> LMPC -->|"upsert"| HLMP
    WXP --> TWX --> WXC -->|"insert"| HWX
    BACKFILL -->|"insert"| HWX

    LOADC -.-> DLQ
    LMPC -.-> DLQ
    WXC -.-> DLQ

    classDef dataNode fill:#dbeafe,stroke:#2563eb,color:#1e3a8a;
    classDef infraNode fill:#ede9fe,stroke:#7c3aed,color:#4c1d95;

    class PJM,EIA,OMF,OMH,ZONES,HLOAD,HLMP,HWX dataNode;
    class LOADP,LMPP,WXP,BACKFILL,TLOAD,TLMP,TWX,DLQ,LOADC,LMPC,WXC infraNode;
```

Producers publish JSON keyed by zone with `acks=all` and idempotence on. Consumers commit offsets one message at a time; anything unparseable or rejected by the database goes to that source's DLQ topic and is committed past.

---

## Downstream — storage to serving

None of this is built yet.

```mermaid
flowchart LR
    subgraph TSDB["TimescaleDB"]
        direction TB
        HLOAD[("<b>load</b>")]
        HLMP[("<b>lmp</b>")]
        HWX[("<b>weather</b>")]
    end

    DBT["<b>dbt models</b><br/><i>joins load + weather + lmp</i>"]
    FEAT["<b>feature table</b><br/><i>lags, rolling means, zone</i>"]
    TRAIN["<b>LightGBM training</b>"]
    REG["<b>MLflow registry</b>"]
    API["<b>FastAPI</b> /predict"]
    DASH["<b>Streamlit + Pydeck</b><br/><i>via Cloudflare Tunnel</i>"]

    HLOAD --> DBT
    HLMP --> DBT
    HWX --> DBT
    DBT --> FEAT --> TRAIN --> REG --> API --> DASH
    FEAT -->|"inference features"| API
    HLOAD -->|"actuals for the map"| DASH

    classDef dataNode fill:#dbeafe,stroke:#2563eb,color:#1e3a8a;
    classDef modelNode fill:#fef3c7,stroke:#d97706,color:#78350f;
    classDef serveNode fill:#dcfce7,stroke:#16a34a,color:#14532d;

    class HLOAD,HLMP,HWX dataNode;
    class DBT,FEAT,TRAIN,REG modelNode;
    class API,DASH serveNode;
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

The 5-minute producer→consumer offset is deliberate: producers publish at :10, so consumers at :15 find a drained-and-settled topic rather than racing the write.

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

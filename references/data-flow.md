# Data Flow

**Title:** Data flow diagram — sources through storage, **Author:** Angie Ohaeri, **Date:** August 10, 2026 **Time:** 9:10pm (split into ingestion / downstream Aug 11 8:36am; tightened by Angie Ohaeri (assisted) Oct 29 2:25am CT)

Load stack only (the LMP model's batch syncs are in `lmp-pricing-model/`). Colors: <b>blue = data</b>, <b>purple = pipeline infra</b>, <b>amber = modeling</b>, <b>green = serving</b>. Endpoints and cadence: table at the bottom.

---

## Ingestion — sources to storage

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
    BACKFILL["<b>fill_weather_gaps</b><br/><i>maintenance/, after outages, bypasses Kafka</i>"]
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

Producers publish JSON keyed by zone with `acks=all` and idempotence on. Consumers commit offsets one message at a time; anything unparseable or DB-rejected goes to that source's DLQ topic and is committed past.

---

## Downstream — storage to serving

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
    DASH["<b>Shiny + Pydeck</b><br/><i>via Cloudflare Tunnel</i>"]

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

Prefect runs every producer/consumer as a scheduled flow. It stores only run state, in a **separate `prefect` database on the same Postgres instance**.

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


---

## Per-source detail

| Source | Endpoint | Cadence | Topic → Table | Write semantics |
|---|---|---|---|---|
| PJM Data Miner 2 | `hrl_load_metered` | hourly, 7-day window | `load` → `load` | upsert on `(time, zone, source='pjm')` |
| EIA Open Data v2 | `electricity/rto/region-data` | hourly, 7-day window | `load` → `load` | upsert on `(time, zone='RTO', source='eia')` |
| PJM Data Miner 2 | `rt_hrl_lmps` | hourly, 7-day window | `lmp` → `lmp` | upsert on `(time, pnode_id)` |
| Open-Meteo Forecast | `/v1/forecast`, `current=` | every 20 min | `weather` → `weather` | append-only |
| Open-Meteo Archive | `/v1/archive`, `hourly=` | manual, after outages (`maintenance/fill_weather_gaps.py`) | *(none)* → `weather` | direct insert of missing zone-hours only |

**Trailing 7-day window:** PJM revises an hour unverified→verified over ~3 days, EIA over ~1 day; re-polling the week and upserting overwrites the same `(time, zone, source)` row. Weather has no revisions, so it is append-only.

**Frequency alignment:** load and LMP are hourly, stamped **hour ending**, so they join directly. Weather is polled every 20 minutes and resampled to hourly in dbt, not in the producer.

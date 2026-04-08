# Designing a Reliable Data Platform for Subscription Lifecycle Events

Subscription systems are inherently event-driven. The current state of a subscription is not stored directly, but reconstructed from a sequence of events such as creations, renewals, upgrades, and cancellations. The core engineering question is: given a continuous stream of events that may arrive out of order, how do you build a pipeline that always produces a correct and consistent view of the current state?

This project builds a data platform that models subscription lifecycle events, stores them as immutable data in a medallion architecture, and reconstructs state through incremental processing. It is built with Python, Airflow, and AWS S3, deployed on EC2 via Ansible and GitHub Actions, and structured to evolve from batch processing to real-time streaming with Kafka and Spark.

## System Design
### Architecture

![Data Flow](docs/architecture/data-flow-phase2.png)

The system separates orchestration, storage, and transformation concerns:

* **Orchestration (Airflow)**

  Handles scheduling, dependency management, retries, and execution state.

* **Storage (S3-backed data lake)**

  Implements a medallion architecture:

  * Bronze: append-only raw events
  * Silver: reconstructed history and current state
  * Gold: analytical datasets

* **Transformation (Compute layer)**

  Stateless jobs decoupled from orchestration, enabling portability and reuse.


### Key Decisions

**1. Transition-based event generation instead of predefined scenarios**
  
The event generator defines all valid state transitions explicitly in `get_allowed_next_events()` and randomly selects from them at each step. Rather than writing test cases for known scenarios, the system continuously produces structurally valid but unpredictable sequences. 

This was a deliberate choice to surface edge cases that predefined fixtures would never reach. In practice, running the generator continuously produced events with `event_time` values that fell into already-processed partitions, which directly exercised the partition-aware recomputation logic and confirmed that late-arriving events were being correctly handled without full rewrites.

**2. Separating `ingested_at` from `event_time` as the watermark**
  
The watermark tracks `ingested_at`, not `event_time`. This means the pipeline reliably picks up every new event regardless of when it actually occurred. Once ingested, `event_time` determines which partitions need to be recomputed. Without this separation, a late-arriving event with an `event_time` in the past would be invisible to the pipeline, already past the watermark, or force a full history rewrite to guarantee correctness.

**3. Partition-aware recomputation for late-arriving events**
  
When new events are detected via the `ingested_at` watermark, the pipeline identifies the earliest affected `event_time` date and recomputes only the partitions from that point forward. This avoids full rewrites while still guaranteeing correctness when events arrive out of order. The same pattern carries forward naturally into a streaming design.

**4. State reconstruction from immutable event history**
  
Subscription state is never updated in place. Events are appended to Bronze as JSONL, and Silver reconstructs the full history and current snapshot on each run. This makes the pipeline deterministic and replayable, and keeps object storage viable as the backend without needing ACID guarantees.

**5. Idempotent processing via `event_id` deduplication**
  
Every event carries a unique event_id. Deduplication happens at the Silver layer before any state reconstruction, so retries and backfills always produce the same result regardless of how many times an event was written to Bronze.

**6. Storage abstraction via abstract base class**

All data access goes through a storage abstraction layer, allowing the same pipeline logic to run on both local filesystem (`LocalStorage`) and S3 (`S3Storage`) without environment-specific branching. Switching backends requires no changes to transformation code.

**7. Cross-layer consistency validation**

At the Gold layer, `active_subscriptions` and `mrr` from the latest KPI partition are compared against the Silver current snapshot. If the numbers don't match, the pipeline fails. This catches inconsistencies that layer-level schema checks alone would miss.

**8. Batch-first, streaming-ready design**
The current system runs on Airflow with file-based storage. The separation between ingestion, state reconstruction, and serving, combined with watermark-driven incremental processing, mirrors how a Kafka-based streaming pipeline would be structured. Moving to Kafka and Spark would change the transport layer, not the data model or processing logic.


## Data Models

#### Bronze — Immutable event log

Raw lifecycle events stored as append-only JSONL, partitioned by ingestion date (`dt=YYYY-MM-DD` based on `ingested_at`).

* `subscription_created`
* `subscription_renewed`
* `subscription_upgraded`
* `subscription_downgraded`
* `subscription_cancelled`
* `payment_failed`

#### Silver — Reconstructed state

* `subscription_state_history`
  : Full event history stored as Parquet, partitioned by event date (`dt=YYYY-MM-DD` based on `event_time`). Deduplicated by `event_id` and ordered by `event_time`, `ingested_at`. Only affected partitions are rewritten when new events arrive.

* `subscription_state_current`
  : Latest state per subscription derived from the full history. Stored as a single unpartitioned Parquet file. One row per `subscription_id`.

Separates **historical truth** from **current-state convenience**.

#### Gold — Analytical models

* `kpi_daily`
  : Daily KPIs stored as Parquet, partitioned by date. Derived from `subscription_state_history` and recomputed from the earliest affected date forward.

  * Flow metrics: `new subscriptions`, `cancellations`
  * Stock metrics: `active subscriptions`, `MRR`


## Infrastructure & Deployment

![Development & Deployment](docs/architecture/development-deployment-architecture.png)

The platform is deployed on AWS with a containerized Airflow setup and automated infrastructure provisioning.

### Infrastructure

- **Compute (EC2)** - Hosts the Airflow environment and pipeline execution, IAM role attached for secure S3 access  
- **Storage (S3)** - Data lake for Bronze, Silver, and Gold layers  
- **Networking** - Elastic IP for stable access to Airflow UI  

### Orchestration & Runtime

- **Airflow (Dockerized)** - Orchestrates end-to-end pipeline execution  
- **Configuration (Ansible)** - Automates provisioning, dependency setup, and deployment  

### CI/CD

- **GitHub** - Source control  
- **GitHub Actions**  - Automated deployment to EC2  


## Limitations & Future Work

### Limitations

* **Recomputation and storage cost**
  
  Silver history rewrites only affected partitions, but `subscription_state_current` is rebuilt from the full history on every run. This is intentional, deriving current state from the complete event log guarantees correctness without relying on incremental state updates. However, as the history grows, this full rebuild becomes a bottleneck. Addressing this would require either incremental current-state updates or a table format that supports efficient upserts, such as Apache Iceberg.

  Append-only Bronze storage also grows continuously over time with no compaction or retention policy in place.

* **Batch execution**
  
  The system currently runs in batch mode. Latency is bounded by the Airflow schedule interval, not by event arrival time.

### Future Work

* **Streaming ingestion (Kafka)**
  : Replace batch ingestion with real-time event streaming

* **Distributed processing (Spark)**
  : Scale transformations for larger datasets

* **Table format upgrade (Iceberg)**
  : Enable efficient updates and ACID-like guarantees

* **Observability (Grafana)**
  : Add monitoring, dashboards, and alerting


## How to Navigate This Repo

```text
├── .github/
│   └── workflows/
│       └── deploy-airflow.yml              # CI/CD pipeline to EC2
├── dags/
│   ├── subscription_events_bronze_ingestion.py
│   ├── subscription_events_gold_kpi_daily.py
│   └── subscription_events_silver_transform.py
├── data/                                   # Local mirror of S3 storage layout
│   ├── bronze/
│   │   └── subscription_events/
│   │       └── dt=YYYY-MM-DD/              # Partitioned by ingested_at
│   ├── gold/
│   │   └── kpi_daily/
│   │       └── dt=YYYY-MM-DD/              # Partitioned by event_time
│   ├── silver/
│   │   ├── subscription_state_current/
│   │   │   └── current.parquet
│   │   └── subscription_state_history/
│   │       └── dt=YYYY-MM-DD/              # Partitioned by event_time
│   └── state/
│       ├── generator/                      # Generator state: active subscriptions and sequence      
│       └── pipeline/
│           └── watermark.json              # Watermark per pipeline
├── docs
│   └── architecture
├── infra
│   └── ansible
│       ├── playbooks
│       │   ├── bootstrap.yml         # Install Docker and dependencies
│       │   ├── deploy-airflow.yml    # Deploy Airflow environment
│       │   └── init-airflow.yml      # Initialize Airflow (connections, variables)
│       ├── ansible.cfg
│       └── inventory.ini
├── src
│   ├── common
│   │   ├── config.py
│   │   ├── constants.py
│   │   ├── schema.py                 # Schema contracts for all layers
│   │   ├── storage.py                # Storage ABC, LocalStorage, S3Storage
│   │   ├── storage_factory.py        # Backend switcher (local / S3)
│   │   └── validation.py             # Shared validation primitives
│   ├── gold
│   │   ├── kpi_daily.py              # KPI computation
│   │   └── validation.py             # Cross-layer consistency checks
│   ├── ingestion
│   │   ├── bronze_writer.py
│   │   ├── generator.py              # Stateful event generator
│   │   └── validation.py
│   ├── silver
│   │   ├── transform.py              # Incremental state reconstruction                      
│   │   ├── validation.py
│   │   └── watermark.py              # Watermark load / save
│   └── __init__.py
├── tests
│   ├── test_generator.py
│   ├── test_gold_kpi_daily.py
│   ├── test_s3_storage.py
│   └── test_silver_pipeline.py
├── docker-compose.airflow.yml
└── requirements.txt
```


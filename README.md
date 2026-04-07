# Designing a Reliable Data Platform for Subscription Lifecycle Events

How do you build a reliable data platform for systems where state is not stored, but derived from events?

Subscription systems are inherently event-driven. The current state of a subscription is not stored directly, but reconstructed from a sequence of events such as creations, renewals, upgrades, and cancellations. In practice, this introduces challenges such as late-arriving data and the need for deterministic state reconstruction.

This project builds a data platform that models subscription lifecycle events, stores them as immutable data in a medallion architecture, and reconstructs state through incremental processing. The system is designed for correctness, idempotency, and reproducibility, and is structured to evolve from batch processing with Airflow to real-time streaming with Kafka and Spark.

## System Design

### Design Principles

* **Event-based immutable design**

  Events are treated as immutable facts and serve as the source of truth, with state derived from event history rather than stored directly. This enables replayability, traceability, and simpler reasoning about state.

* **Correctness through recomputation**

  State is reconstructed from event history rather than incrementally updated, ensuring deterministic results even with late-arriving data.

* **Idempotent processing**

  Transformations are idempotent, guaranteeing consistent outputs across retries and backfills.

* **Separation of storage and compute**

  Storage is abstracted from compute, allowing the same pipeline to run across environments.

* **Designed for batch-to-stream evolution**

  The system starts with batch processing but is structured to evolve naturally into streaming.

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

* **Storage abstraction**

  A unified interface allows the same pipeline logic to run on local filesystem and S3.


### Key Decisions

* **Append-only event storage**
  
  Events are written as append-only JSONL files in the Bronze layer, with no in-place updates or deletions.
  This preserves the full event history and simplifies replay, debugging, and downstream recomputation.

* **State reconstruction from event history**
  
  Instead of maintaining mutable state, subscription state is fully reconstructed from the event log in the Silver layer.
  A complete history (`subscription_state_history`) is first built, and the latest state (`subscription_state_current`) is derived from it.
  This ensures deterministic results even with late-arriving data, and allows the system to operate reliably on object storage without relying on ACID guarantees.

* **Partition-aware recomputation**
  
  When new events are identified via the `ingested_at` watermark, affected partitions are determined based on `event_time`, and only those partitions are recomputed.
  This balances correctness with efficiency while avoiding complex in-place update logic.

* **Idempotent processing via event deduplication**
  
  All transformations are designed to be idempotent by deduplicating events using a unique `event_id`.
  This guarantees consistent outputs across retries, backfills, and partial failures.

* **Storage-agnostic pipeline design**
  
  All data access is abstracted through a storage interface, allowing the same pipeline logic to run on both local filesystem and S3.
  This removes environment-specific branching and simplifies deployment.


## Data Flow & Modeling

### Data Flow

The pipeline follows a medallion architecture (Bronze → Silver → Gold):

1. **Event ingestion (Bronze)**  

   A stateful generator produces lifecycle events, stored as append-only data.

2. **State reconstruction (Silver)**
   
   - Full history (`subscription_state_history`)  
   - Current snapshot (`subscription_state_current`)  

3. **Analytical modeling (Gold)**
   
   Daily KPIs (new subscriptions, cancellations, active subscriptions, MRR) are derived from reconstructed state.

The system maintains correctness through incremental processing and selective recomputation, rather than in-place updates.

### Data Models

#### Bronze — Immutable event log

Stores raw lifecycle events as append-only JSONL records:

* `subscription_created`
* `subscription_renewed`
* `subscription_upgraded`
* `subscription_downgraded`
* `subscription_cancelled`
* `payment_failed`

#### Silver — Reconstructed state

* `subscription_state_history`
  : Full event history ordered by `event_time` and `ingested_at`

* `subscription_state_current`
  : Latest state per subscription derived from history

Separates **historical truth** from **current-state convenience**.

#### Gold — Analytical models

* **kpi_daily**

Captures:

* Flow metrics: new subscriptions, cancellations
* Stock metrics: active subscriptions, MRR

Derived from reconstructed state to ensure analytical consistency.

## Implementation Details

* **Stateful event simulation**
  
  A stateful event generator simulates subscription lifecycles by producing event sequences based on valid state transitions.
  Instead of relying on predefined test cases, it generates diverse and unpredictable scenarios, allowing the system to encounter edge cases and "unknown unknowns" during development.
  This approach improves robustness by validating the pipeline against realistic and evolving data patterns.

* **Incremental processing with late-arriving data handling**
  
  Processing is driven by an `ingested_at` watermark, which defines the set of newly arrived events.
  Affected partitions are determined using `event_time`, and only those partitions are recomputed, ensuring correctness even with late-arriving data.

* **Layered validation and cross-layer consistency enforcement**
  
  Validation is performed at each layer (Bronze, Silver, Gold) to ensure data integrity at different stages of the pipeline.
  At the Gold layer, derived KPIs are cross-checked against the reconstructed state in the Silver layer, ensuring consistency between aggregated metrics and underlying state.

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
  
  Recomputing partitions ensures correctness but increases compute cost as data grows.
  Append-only design leads to continuous data growth over time.

* **Batch execution**
  
  The system currently runs in batch mode, with streaming planned as a future extension.

### Future Work

* **Streaming ingestion (Kafka)**
  : Replace batch ingestion with real-time event streaming

* **Distributed processing (Spark)**
  : Scale transformations for larger datasets

* **Table format upgrade (Iceberg)**
  : Enable efficient updates and ACID-like guarantees

* **Observability (Grafana)**
  : Add monitoring, dashboards, and alerting


## Project Structure

```text
.
├── .github/workflows/
├── dags/
├── docs/
│   ├── architecture/
│   └── reflections/
├── infra/ansible/playbooks/
├── src/
│   ├── common/
│   ├── ingestion/
│   ├── silver/
│   └── gold/
├── tests/
├── docker-compose.airflow.yml
├── README.md
└── requirements.txt
```


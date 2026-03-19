# SaaS Subscription Lifecycle Data Platform

This project builds an event-driven data platform that simulates subscription lifecycle events, 
ingests them into a medallion architecture, and incrementally transforms them into reliable analytical datasets.

It starts as a batch-based pipeline orchestrated with Airflow 
and is designed to evolve into a real-time streaming system using Kafka and Spark,
with minimal changes to the core data model and storage layout.


## Key Features

- Event-driven lifecycle modeling (append-only, no overwrites)
- Incremental & idempotent processing (watermark + deduplication)
- Medallion architecture (Bronze → Silver → Gold)
- Deterministic state reconstruction (history → current snapshot)
- Designed for batch-to-streaming evolution

## Architecture

![Data Flow](docs/architecture/data-flow.png)

### Data Flow (Phase 1)

1. **Source (Event Generation)**
   - Mock event generator simulates subscription lifecycle events
2. **Ingestion (Airflow)**
   - Events are written to Bronze layer as JSONL files
   - Partitioned by ingestion date (`dt=YYYY-MM-DD`)
3. **Storage (Local FS)**
   - Local filesystem used as S3-compatible structure for seamless migration to object storage 
4. **Processing (Airflow)**
   - Incremental Bronze → Silver transformation
     - Builds:
       - Silver History: append-only event log
       - Silver Current: latest subscription snapshot
5. **Serving (Gold Layer)**
   - Aggregated business metrics (e.g., MRR, churn, active users)
   - Designed for downstream analytics / dashboards

![Development & Deployment](docs/architecture/development-deployment-architecture.png)

### Development & Deployment
- **Code Versioning (GitHub)**
- **CI/CD Pipeline (GitHub Acrionts)**
- **Infrastructure (AWS EC2)**
  - Compute layer hosting the data platform
- **Configuration (Ansible)**
  - Provision EC2 instance
  - Install Docker and dependencies 
  - Deploy Airflow environment
- **Runtime (Dockerized Airflow)**
  - Orchestrates data pipelines


## Key Decisions

- Designed a **stateful event generator**
    
    → simulates realistic lifecycle transitions and uncovers edge cases beyond predefined tests

- Used **`ingested_at` as watermark**

    → aligns with ingestion semantics and prevents missing late-arriving data

- Built **idempotent pipeline with event-level deduplication**

    → ensures safe reprocessing and consistent outputs

- Modeled **current state as snapshot table**

    → optimized for analytical queries and downstream consumption

- Partitioned Bronze data by ingestion time

    → enables efficient incremental processing and seamless transition to S3

- Structured pipeline for **batch-to-streaming extensibility**

    → minimal redesign required when introducing Kafka/Spark


## Future Work

### Phase 2 - Cloud & Data Quality
- Migrate storage from local filesystem to AWS S3
- Introduce schema validation layer 
- Add data quality checks and monitoring

### Phase 3 - Streaming & Real-time Processing
- Introduce Kafka-based event ingestion
- Build streaming pipeline with Spark Structured Streaming
- Transition to table formats like Apache Iceberg
- Support real-time dashboards and near real-time analytics

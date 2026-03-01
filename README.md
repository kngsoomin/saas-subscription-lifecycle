```mermaid
flowchart LR
  %% =========================
  %% Phase 1: MVP (Mock events)
  %% =========================
  subgraph P1[Phase 1 - MVP (Mock events)]
    A[Event Generator<br/>(mock request/event)] -->|JSON| B[Airflow on EC2<br/>(Docker Compose)]
    B -->|write raw| C[S3 Bronze<br/>subscription_events/dt=...]
    B -->|transform| D[S3 Silver<br/>current + history (parquet)]
    B -->|scheduled refresh| E[S3 Gold<br/>KPI marts (parquet)]
    E --> F[Streamlit / Email Report]
  end

  %% =========================
  %% Phase 2: Extension (Kafka)
  %% =========================
  subgraph P2[Phase 2 - Extension (Kafka ingestion)]
    U[Slack / API] --> K[Kafka Topic<br/>subscription_events]
    K --> W[Kafka Consumer / Writer<br/>(ingest service)]
    W -->|write raw| C
  end

  %% =========================
  %% Ops / Delivery (side rail)
  %% =========================
  subgraph OPS[Ops / Delivery]
    G[GitHub] --> H[GitHub Actions<br/>(lint/test/DAG import)]
    H --> I[AWS CodeDeploy<br/>(deploy dags/app)]
    J[Ansible<br/>(bootstrap EC2)] --> B
  end
```

## EC2 Specification (Phase 1 - Airflow Orchestration)

| Category | Configuration |
|-----------|---------------|
| Instance Type | t3.large (2 vCPU, 8 GiB RAM) |
| AMI | Ubuntu 24.04 LTS |
| Region | us-east-1 |
| Availability Zone | us-east-1a |
| Network | Default VPC (Public Subnet) |
| Security Group | airflow-ec2-sg (newly created) |
| Inbound Rules | SSH (22) - My IP only <br> Airflow UI (8080) - My IP only |
| Outbound Rules | Allow all outbound traffic |
| Storage | gp3 - 50GB |
| IAM Role | EC2 Instance Profile with scoped S3 read/write access (bronze/silver/gold prefixes) |
| Runtime | Docker + Docker Compose (Airflow Webserver, Scheduler, Postgres) |
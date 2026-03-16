### Project: SaaS Subscription Lifecycle Data Platform

**Goal**
- infrastructure automation with Ansible
- workflow orchestration with Airflow
- CI/CD with GitHub Actions
- medallion data modeling
- extension with Kafka-based streaming ingestion

#### Phase 1: MVP with batch processing

- provision EC2 for Airflow
- event generator generates mock event
- airflow orchestrates batch ingestion and medallion-layer transformation
- store data in bronze/silver/gold layers on local filesystem using S3-like folder structure

Checklist:

- [x] define repository structure
- AWS
  - [x] set up EC2 & S3
  - [x] allocate EIP and configure security group
  - [x] configure IAM role/policy for ec2 to access s3
- Infra
  - [x] docker compose for airflow installation
  - [x] prepare playbook that install docker & git and create project directory; clone git repo; create `.env`
  - [x] prepare playbook that initializes airflow with docker-compose
  - [x] prepare playbook that deploys airflow, and trigger it by GitHub actions (needs to set up GitHub secrets for SSH key)
- Scripts
  - [x] event schema design, event types definition, partition/folder convention
  - [x] mock events random generator
  - [ ] dag for bronze: raw mock events as ingested; output JSONL partitioned by date
  - [ ] dag for silver: cleaned and standardized subscription state/history tables; output `subscription_state_history` & `subscription_state_current`
  - [ ] dag for gold: business-ready kpi marts - like daily active subscriptions, cancellations, etc.; output `kpi_daily`

#### Phase 2: Upgrade to Cloud data lake

- [ ] replace local filesystem with s3-based storage
- [ ] store sivler/gold in Parquet
- [ ] add testing / validation (tbc)

#### Phase 3: streaming extension for real-time ingestion

tbu
- provision separate EC2 for kafka broker
- publish mock events to Kafka
- consume events into bronze storage
- continue downstream transformation with Airflow for silver/gold

#### EC2 Specification (Phase 1 - Airflow Orchestration)

| Category          | Configuration                                                                       |
| ----------------- | ----------------------------------------------------------------------------------- |
| Instance Type     | t3.large (2 vCPU, 8 GiB RAM)                                                        |
| AMI               | Ubuntu 24.04 LTS                                                                    |
| Region            | us-east-1                                                                           |
| Availability Zone | us-east-1a                                                                          |
| Network           | Default VPC (Public Subnet)                                                         |
| Security Group    | airflow-ec2-sg (newly created)                                                      |
| Inbound Rules     | SSH (22) - My IP only <br> Airflow UI (8080) - My IP only                           |
| Outbound Rules    | Allow all outbound traffic                                                          |
| Storage           | gp3 - 50GB                                                                          |
| IAM Role          | EC2 Instance Profile with scoped S3 read/write access (bronze/silver/gold prefixes) |
| Runtime           | Docker + Docker Compose (Airflow Webserver, Scheduler, Postgres)                    |

- `bootstrap.yml`: one-time host provisioning → .env 생성
- `init-airflow.yml`: one-time Airflow initialization (DB migration + admin user setup) → db migrate, admin 생성
- `deploy-airflow.yml`: repeatable Airflow deployment triggered by GitHub Actions → repo update, airflow 서비스 up

#### Schema design
event_name: subscription_lifecycle_event
schema_version: "1.0"
```YAML
event_name: subscription_lifecycle_event
schema_version: "1.0"

fields:
  - name: event_id
    type: string
    required: true
    description: Unique identifier for each event

  - name: event_time
    type: timestamp
    required: true
    description: UTC timestamp when the event occurred

  - name: event_type
    type: string
    required: true
    allowed_values:
      - subscription_created
      - subscription_renewed
      - subscription_upgraded
      - subscription_downgraded
      - subscription_cancelled
      - payment_failed

  - name: schema_version
    type: string
    required: true
    description: Version of the event schema

  - name: user_id
    type: string
    required: true

  - name: subscription_id
    type: string
    required: true

  - name: plan_id
    type: string
    required: true

  - name: billing_cycle
    type: string
    required: true
    allowed_values:
      - monthly

  - name: price
    type: float
    required: true

  - name: currency
    type: string
    required: true
    allowed_values:
      - USD

  - name: status
    type: string
    required: true
    allowed_values:
      - active
      - cancelled
      - past_due

  - name: source
    type: string
    required: true
    allowed_values:
      - web
      - mobile
      - system

  - name: ingested_at
    type: timestamp
    required: true
    description: UTC timestamp when the event was written to bronze
```

```JSON
{
  "event_id": "evt_000001",
  "event_time": "2026-03-15T14:05:23Z",
  "event_type": "subscription_created",
  "schema_version": "1.0",

  "user_id": "user_123",
  "subscription_id": "sub_456",

  "plan_id": "pro_monthly",
  "billing_cycle": "monthly",
  "price": 29.99,
  "currency": "USD",
  "status": "active",

  "source": "web",
  "ingested_at": "2026-03-15T14:05:25Z"
}
```
plan catalog
```
basic_monthly
pro_monthly
enterprise_monthly
```

| event_type              | status      |
| ---------------------   | --------    |
| subscription_created    | active      |
| subscription_renewed    | active      |
| subscription_upgraded   | active      |
| subscription_downgraded | active      |
| subscription_cancelled  | cancelled   |
| payment_failed          | past_due    |

#### partition/folder convention
```
data/
  bronze/
    subscription_events/
      dt=YYYY-MM-DD/
        subscription_events_<run_ts>.jsonl

  silver/
    subscription_state_history/
      dt=YYYY-MM-DD/
        part-000.parquet

    subscription_state_current/
      snapshot_date=YYYY-MM-DD/
        part-000.parquet

  gold/
    kpi_daily/
      dt=YYYY-MM-DD/
        part-000.parquet

  state/
    generator/
      subscription_state_current.json
      last_subscription_seq.txt

```
Silver - subscription_state_history
```
subscription_id
user_id
event_id
event_time
event_type
plan_id
billing_cycle
price
currency
status
source
```
Silver - subscription_state_current
```
subscription_id
user_id
current_plan_id
current_billing_cycle
current_price
currency
current_status
last_event_type
last_event_time
```
Gold - kpi_daily
```
date
new_subscriptions
cancellations
active_subscriptions
mrr
```
#### mock event generator
generate_mock_events(n_subscriptions=10)

for each subscription
    create subscription_created event
    generate 0~4 follow-up events
    update subscription state each time


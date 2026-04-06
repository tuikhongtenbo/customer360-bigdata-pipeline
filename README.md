# Customer 360 — End-to-End Data Engineering

> **Muc tieu:** Build portfolio-ready Customer 360 platform, dung tool hien dai de manh CV Data Engineer / Data Analyst.

---

## 1. Data Sources

| # | Nguon | Loai data | Dac diem | Cong cu | Tan suat |
|---|---|---|---|---|---|
| 1 | **Salesforce / HubSpot** | Contact, Account, Opportunity, Lead | Core identity | ADF + Airbyte | CDC / 15 min |
| 2 | **Zendesk / Intercom** | Ticket, CSAT, response time | Interaction history | ADF REST | Every 15 min |
| 3 | **Google Analytics 4** | Web events, session, conversion | High volume, behavioral | Airbyte GA4 | Streaming |
| 4 | **Stripe / Payment** | Transaction, subscription, invoice | Revenue + LTV signals | Airbyte Stripe | Near-real-time |
| 5 | **Email Marketing** | Sends, opens, clicks, unsubs | Engagement signals | Airbyte | Daily batch |
| 6 | **IoT / Telemetry** *(opt)* | Feature usage, in-app events | Deep behavior | Kafka Connect | Streaming |
| 7 | **CSV/JSON mock** | Synthetic data | Demo / portfolio | Python script | On-demand |

> **Priority:** CRM + Support + Analytics + Stripe = 4 nguon co ban.

---

## 2. Tool Stack — Azure-Centric

```
 Cloud             Microsoft Azure
 Ingestion          ADF + Airbyte OSS + Debezium CDC
 Streaming          Azure Event Hubs + Apache Flink
 Processing         Azure Synapse Spark + Databricks Runtime
 Lake               ADLS Gen2 + Delta Lake (Bronze/Silver/Gold)
 Warehouse          Azure Synapse SQL Pool (serverless)
 Transform          dbt Core + Spark SQL
 Orchestration      Airflow (MWAA) + ADF Pipelines
 Identity           Synapse ML (fuzzy matching)
 Quality            Great Expectations + dbt tests
 Governance         Purview + OpenLineage (Marquez)
 BI                 Power BI (Pro / Premium)
 IaC                Terraform (Azure provider)
 CI/CD              Azure DevOps Pipelines
 Feature Store      Azure Cache for Redis (optional)
 Monitoring         Azure Monitor + Log Analytics
```

---

## 3. Architecture

```
                    ╔══════════════════════════════════════════════════╗
                    ║              CUSTOMER 360 PIPELINE               ║
                    ╚══════════════════════════════════════════════════╝

  SOURCES                    INGESTION LAYER
  ─────────                   ──────────────
  ┌──────────┐               ┌────────────────────────────┐
  │Salesforce│──────────────▶│ ADF pipelines              │
  │Zendesk   │──────────────▶│ Airbyte OSS (350+ conn)   │
  │GA4       │──────────────▶│ Debezium CDC ──▶ Event    │
  │Stripe    │──────────────▶│   Hubs (Kafka API)        │
  │Mailchimp │──────────────▶│ REST API polling          │
  │IoT events│──────────────▶└────────────────────────────┘
  └──────────┘                               │
                                               ▼
                    ┌──────────────────────────────────────────────┐
                    │              PROCESSING LAYER                 │
                    │  ┌─────────────────┐  ┌────────────────────┐  │
                    │  │ STREAMING PATH  │  │ BATCH PATH         │  │
                    │  │ Event Hubs ──▶  │  │ ADF ──▶ ADLS Gen2  │  │
                    │  │ Apache Flink    │  │ Synapse Spark      │  │
                    │  │ ──▶ Redis Cache │  │ Delta Live Tables  │  │
                    │  └─────────────────┘  └────────────────────┘  │
                    │                                              │
                    │  IDENTITY RESOLUTION (ML):                   │
                    │  Deterministic + Fuzzy ──▶ Golden Record    │
                    └──────────────────────────┬───────────────────┘
                                               │
           ┌──────────────────────────────────┼──────────────────────────┐
           │            BRONZE LAYER          │  Raw, immutable           │
           │  Delta Lake on ADLS Gen2         │  Schema-on-read           │
           │  _ingestion_ts, _source, _seq   │  Append-only              │
           └──────────────────────────────────┼──────────────────────────┘
                                               │ Bronze → Silver
                                               │ dedupe, type-enforce
                                               │ PII flag, quality gate
                                               ▼
           ┌──────────────────────────────────┼──────────────────────────┐
           │            SILVER LAYER          │  Curated, entity-resolved │
           │  Delta Lake + Synapse Serverless │                          │
           │  ─────────────────────────────    │  DATA VAULT 2.0:          │
           │  H_CUST (SHA-256 HK)            │                          │
           │  L_CUST_ORDER                   │  Hubs / Links / Sats     │
           │  L_CUST_TICKET                  │                          │
           │  L_CUST_CAMPAIGN                │  S_CUST_PROFILE          │
           │  S_CUST_PROFILE                 │  S_CUST_ADDRESS          │
           │  S_CUST_ADDRESS                 │  S_CONSENT               │
           │  S_CONSENT                      │                          │
           └──────────────────────────────────┼──────────────────────────┘
                                               │ Silver → Gold
                                               │ business logic, metrics
                                               ▼
           ┌──────────────────────────────────┬──────────────────────────┐
           │              GOLD LAYER          │  Analytics-ready          │
           │  Synapse Dedicated SQL Pool      │                          │
           │  ──────────────────────────      │  STAR SCHEMA MARTS:      │
           │  DIM_CUSTOMER (SCD2)            │  • Customer Overview     │
           │  DIM_DATE, DIM_PRODUCT          │  • Churn Risk             │
           │  FCT_CUSTOMER_ACTIVITY          │  • LTV / Revenue          │
           │  FCT_MARKETING_CAMPAIGN         │  • Support Operations    │
           │  FCT_REVENUE                    │                          │
           │  FCT_SUPPORT_TICKETS            │                          │
           └──────────────────────────────────┴──────────────────────────┘
                                               │
                                               ▼
           ┌──────────────────────────────────┬──────────────────────────┐
           │         SEMANTIC LAYER           │  dbt metrics +            │
           │                                  │  Power BI Tabular Model   │
           └──────────────────────────────────┴──────────────────────────┘
                                               │
                                               ▼
           ┌──────────────────────────────────┬──────────────────────────┐
           │         CONSUMPTION LAYER        │  Power BI Dashboards     │
           │  • Executive Customer Overview   │  • Customer Health Score │
           │  • Acquisition Funnel            │  • LTV & Revenue         │
           │  • Support Operations            │  + Semantic Model + RLS  │
           └──────────────────────────────────┴──────────────────────────┘

  ═══════════════════════════════════════════════════════════════════
  CROSS-CUTTING LAYERS
  ─────────────────────
  ORCHESTRATION    Airflow MWAA (6 DAGs) + ADF pipeline triggers
  DATA QUALITY     Great Expectations + dbt tests + quarantine tables
  GOVERNANCE       Microsoft Purview + OpenLineage (Marquez)
  PRIVACY/GDPR     Consent ledger + RTBF (Right-to-be-forgotten) pipeline
  IaC              Terraform (all infrastructure as code)
  CI/CD            Azure DevOps (lint dbt + tests on PR → deploy)
  ```

---

## 4. Modules

### Module 1 — Data Sources & Ingestion
- Mock generators (Python): `customers.csv`, `support_tickets.csv`, `web_events.csv`, `transactions.csv`
- ADF: linked services + datasets + copy pipelines
- Airbyte OSS: alternative ingestion, 350+ connectors
- Debezium CDC: PostgreSQL → Event Hubs (real-time, zero-ETL)
- Azure Event Hubs: Kafka-compatible endpoint

### Module 2 — Bronze Layer
- Synapse Spark: mount ADLS Gen2 → write Delta Lake Bronze
- Schema enforcement (prevent drift)
- Metadata columns: `_ingestion_ts`, `_source_system`, `_seq_no`, `_load_id`
- Immutable, append-only, partition by `date` + `source_system`

### Module 3 — Silver Layer (Data Vault 2.0 + Cleansing)
- **Hub:** `H_CUSTOMER` (HK = SHA-256 of natural keys)
- **Links:** `L_CUST_ORDER`, `L_CUST_TICKET`, `L_CUST_CAMPAIGN`
- **Satellites:** `S_CUST_PROFILE`, `S_CUST_ADDRESS`, `S_CONSENT`
- Deduplication (CDC sequence comparison), PII flagging, type casting
- Great Expectations: not null, email format, range checks
- Quarantine table cho bad records

### Module 4 — Identity Resolution (Golden Record)
- **Tier 1:** Deterministic — exact email hash (SHA-256) match
- **Tier 2:** Probabilistic — fuzzy match (first_name + postal_code + phone)
- Graph-based confidence scoring (Synapse ML)
- Survivorship rules: CRM → Billing → Support → Analytics (priority)
- Output: `GOLDEN_CUSTOMER` table + identity graph table

### Module 5 — Gold Layer (dbt Star Schema Marts)
| Mart | Bang | Key Metrics |
|---|---|---|
| **Customer Overview** | `dim_customer`, `fct_activity` | MAU, DAU, cohort retention |
| **Churn Risk** | `fct_churn_signals` | Churn score, engagement, tenure |
| **LTV / Revenue** | `fct_revenue`, `dim_subscription` | ARPU, LTV by cohort, MRR, ARR |
| **Marketing** | `fct_campaign`, `dim_channel` | Attribution, channel perf, CAC |
| **Support Ops** | `fct_support_tickets` | Ticket vol, CSAT, resolution time |

- dbt tests: unique, not null, referential integrity, accepted range
- RLS in Synapse: filter by region / customer tier

### Module 6 — Orchestration (Airflow MWAA)
| DAG | Trigger | Action |
|---|---|---|
| `ingest_sources` | Daily | Trigger ADF pipelines |
| `bronze_to_silver` | After ingest | Synapse Spark job |
| `silver_to_gold` | Daily 6 AM | dbt run |
| `quality_gate` | After silver | Great Expectations + quarantine |
| `rtbf_pipeline` | On-request | Right-to-be-forgotten |
| `lineage_capture` | After each job | Emit OpenLineage → Marquez |

### Module 7 — Governance & Lineage
- **Purview:** auto-scan ADLS + Synapse, PII classification, business glossary
- **OpenLineage:** tu ADF → Event Hubs → Synapse → Power BI
- **Marquez:** open-source lineage catalog (host on Azure VM)
- **PII masking views:** hashed email, generalized city, no raw PII for analysts
- **Consent ledger:** opt-in/opt-out per customer per channel (DV satellite)
- **Business glossary:** "Customer Segment" = Gold/Silver/Bronze, "Churn Risk" = 0–100

### Module 8 — Power BI Dashboards
| # | Dashboard | Key Metrics |
|---|---|---|
| 1 | **Executive Overview** | Total customers, MAU, DAU, churn trend, cohort retention |
| 2 | **Acquisition Funnel** | Lead → MQL → SQL → Opportunity → Closed Won, conversion rates |
| 3 | **Health Score** | Engagement (40%) + Support (30%) + NPS (30%) |
| 4 | **LTV & Revenue** | ARPU, LTV by cohort, MRR/ARR trend, revenue by product |
| 5 | **Support Operations** | Ticket volume, CSAT trend, resolution time, top issues |

- Semantic model: KPI nhat quyen (Customer Health Score = DAX)
- Row-level security (RLS): filter by region / customer tier / department

### Module 9 — IaC & Deployment
- **Terraform:** Synapse workspace, SQL pools, ADLS Gen2, Event Hubs, Purview, Redis, IAM
- **Azure DevOps CI:** lint dbt models + run dbt tests on every PR
- **Azure DevOps CD:** deploy ADF ARM templates + `terraform plan` → apply
- **Power BI Deployment Pipeline:** DEV → TEST → PROD

---

## 5. Modules bo sung (thieu so voi plan cu)

| Module | Ly do | Cong cu |
|---|---|---|
| **Debezium CDC** | Zero-ETL, real-time capture, khong API polling | Debezium VM + Event Hubs |
| **Apache Flink** | Stateful stream processing manh hon Spark streaming | Flink on Azure HDInsight |
| **Identity Resolution ML** | Graph-based golden record, professional hon SQL join | Azure Synapse ML |
| **Feature Store (Redis)** | Cache features cho real-time ML serving | Azure Cache for Redis |
| **OpenLineage** | End-to-end lineage tracking giua cac tool | Marquez (open-source) |
| **Microsoft Purview** | Auto-scan, PII classification, business glossary | Purview / Fabric Catalog |
| **Data Contracts** | SLA giua producer/consumer — modern governance | dbt contracts |
| **Quarantine Tables** | Bad data khong mat, con debug duoc | Delta Lake error schema |
| **CI/CD cho dbt** | Schema check + test on PR | Azure DevOps Pipelines |
| **Consent Ledger** | Track opt-in/opt-out (GDPR compliance) | DV Satellite |
| **RTBF Pipeline** | Right-to-be-forgotten — GDPR bat buoc | Airflow DAG triggered |
| **SCD Type 2** | Track customer attribute changes over time | dbt SCD2 macro |

---

## 6. Implementation Phases

| Phase | Noi dung | Output |
|---|---|---|
| **1. Foundations** | Mock data, ADF setup, ADLS, Delta Lake Bronze | Raw data landing |
| **2. Processing** | Synapse Silver, dbt staging, quality checks | Cleansed data |
| **3. Identity & Core** | Identity resolution, Golden Record, Data Vault model | Unified customer profile |
| **4. Marts & BI** | dbt marts, Power BI dashboards, semantic layer | BI-ready data |
| **5. Orchestration** | Airflow DAGs, scheduling, alerting | Automated pipeline |
| **6. Governance** | Lineage, PII masking, consent ledger, RTBF | Compliance-ready |
| **7. IaC & Deploy** | Terraform, Azure DevOps, Power BI deployment | Production-ready |

---

## 7. Verification Checklist

- [ ] Full pipeline: Airflow DAG chay khong failed
- [ ] Data quality: `dbt test` pass 100% + Great Expectations green
- [ ] Golden Record: sample 10 records verify identity resolution dung
- [ ] Power BI: live query Synapse, dashboards render chinh xac
- [ ] Lineage: Purview trace tu source → dashboard, day du
- [ ] RTBF: fake deletion request → PII removed trong 30 phut
- [ ] Terraform plan: infrastructure changes chinh xac
- [ ] CI/CD: PR voi schema change → dbt test pass/fail dung

---

## 8. Tools tren CV

```
Cloud & Infra       Azure  •  Terraform  •  Azure DevOps
Ingestion            ADF  •  Airbyte OSS  •  Debezium CDC  •  Event Hubs
Processing           Synapse Analytics  •  Spark  •  Flink  •  Delta Lake
Transformation       dbt Core  •  Data Vault 2.0  •  Star Schema  •  SCD Type 2
Identity            Identity Resolution (ML)  •  Golden Record
Quality              Great Expectations  •  dbt tests  •  Quarantine tables
Orchestration        Airflow MWAA  •  ADF Pipelines
Governance           Purview  •  OpenLineage  •  Marquez  •  PII Masking  •  Consent Ledger
BI                   Power BI  •  DAX  •  Semantic Model  •  RLS  •  Deployment Pipeline
ML                   Synapse ML  •  Redis Feature Store
```

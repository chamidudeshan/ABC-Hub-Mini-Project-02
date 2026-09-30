# ABC Hub – Mini Project 02: Evolving the Data Platform

## Overview
This project evolves the ABC Hub data platform developed in Mini Project 01. Apache NiFi is retained for source ingestion and data movement, while Apache Airflow is introduced for workflow orchestration, dependencies, retries, runtime parameterisation, data-quality processing and SCD processing. PostgreSQL provides the Bronze, Silver, Gold/analytics and audit storage layers.

## Technology Stack
- PostgreSQL
- Apache NiFi 2.10.0
- Apache Airflow 3.3.0
- DataGrip
- WSL

## Databases
- `abc_hub_operational` – operational source database
- `abc_hub_analytical` – Bronze, Silver, Gold/analytics and audit data
- `airflow_metadata` – Airflow metadata database

## Architecture
Operational systems → Apache NiFi → Bronze → Apache Airflow → Silver → Data Quality → SCD Processing → Gold/Analytics → Fact/Dimension models → Reporting and BI.

NiFi is responsible for ingestion and movement. Airflow is responsible for orchestration. SQL is used for transformations, validation and SCD processing.

## MP1 Assets Reused
The existing analytical layer is preserved and reused:
- `analytics.dim_customer`
- `analytics.dim_subscription`
- `analytics.dim_content`
- `analytics.dim_inventory`
- `analytics.fact_customer_daily_activity`
- `analytics.fact_content_monthly_performance`
- `analytics.fact_inventory_utilisation`

Mini Project 02 adds an SCD Type 2 implementation for `gold.dim_customer`.

## Setup
1. Create the required PostgreSQL schemas and tables using `sql/01_create_tables.sql`.
2. Configure NiFi operational and analytical DBCP controller services.
3. Configure the Avro record reader and the reusable Bronze ingestion process group.
4. Configure the NiFi parameter context containing `source_table`.
5. Configure the Airflow PostgreSQL connection with connection ID `abc_hub_analytical`.
6. Copy `airflow/abc_hub_medallion_pipeline.py` into the Airflow DAG directory.
7. Start NiFi and Airflow.
8. Load Bronze through NiFi and trigger the Airflow DAG.

## Airflow Execution
```bash
airflow dags list-import-errors
airflow dags trigger abc_hub_medallion_pipeline
airflow dags list-runs abc_hub_medallion_pipeline
```

Historical logical dates can be supplied for backfill-style execution:
```bash
airflow dags trigger abc_hub_medallion_pipeline --logical-date 2026-09-15T00:00:00+00:00 --run-id backfill_2026_09_15
```

The DAG uses Jinja runtime values including `{{ run_id }}` and `{{ ds }}`.

## Idempotency
Bronze loading uses UPSERT operations. Silver transformations use PostgreSQL `ON CONFLICT` processing. The customer SCD Type 2 process only creates a new version when tracked attributes change. Reprocessing unchanged data therefore does not create unnecessary duplicate records or SCD versions.

Validated counts included:
- Bronze customers: 1,008
- Silver customers: 1,008
- Current Gold customers: 1,008
- Bronze payments: 16,892
- Silver payments: 16,892
- Existing customer daily activity facts: 58,020
- Existing content monthly performance facts: 7,099
- Existing inventory utilisation facts: 11,951

## Data Quality and Rejected Records
Data-quality checks cover duplicate business keys, mandatory customer fields, SCD current-version uniqueness and Silver/Gold count consistency. Negative payment amounts are retained for traceability in `audit.rejected_records` instead of being silently discarded. The implemented validation identified 170 negative payment records.

## SCD Type 2
`gold.dim_customer` uses:
- surrogate key `customer_key`
- business key `customer_id`
- `effective_from`
- `effective_to`
- `is_current`

Tracked attributes include email, phone and status. A change expires the previous current row and creates a new current version.

## Reliability
The DAG defines two retries with a one-minute retry delay. Task dependencies prevent downstream processing after an upstream failure. During testing, retry and `upstream_failed` propagation were observed.

## Audit
The `audit.pipeline_run` table supports batch/run tracking fields including DAG ID, run ID, start/end time, status, source/processed/inserted/updated/rejected counts. A manual project-validation audit entry was recorded during implementation. This entry should not be interpreted as an automatically generated successful Airflow audit record.

## Assumptions and Limitations
- NiFi owns operational-source ingestion and Bronze loading.
- Airflow owns workflow orchestration and SQL-based processing.
- SCD Type 2 was implemented and demonstrated for the customer dimension.
- Existing MP1 analytical fact/dimension models are reused rather than rebuilt.
- Historical logical-date triggering demonstrates the backfill design; a complete production-scale historical backfill was not executed.
- The final Airflow environment showed task-execution instability during testing even though the transformation SQL was validated successfully in DataGrip.

## Submission Evidence
Recommended screenshots include:
- NiFi Bronze ingestion and reusable process group
- Bronze source/target counts and rerun evidence
- Silver counts
- SCD Type 2 before/after customer versions
- Gold/analytics fact and dimension counts
- Airflow DAG graph
- Airflow retry/failure propagation
- Historical logical-date DAG run
- Rejected-record count
- Audit table result
- Updated architecture diagram
- Updated dimensional/star-schema diagram

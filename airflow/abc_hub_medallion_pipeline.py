from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG(
    dag_id="abc_hub_medallion_pipeline",
    description="ABC Hub Medallion Architecture Pipeline",
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["abc_hub", "mini_project_02", "medallion"],
) as dag:
    start = EmptyOperator(task_id="start")

    bronze_validation = SQLExecuteQueryOperator(
        task_id="bronze_validation",
        conn_id="abc_hub_analytical",
        sql="""
        DO $$
        BEGIN
            IF (SELECT COUNT(*) FROM bronze.customer) = 0 THEN RAISE EXCEPTION 'Bronze customer table is empty'; END IF;
            IF (SELECT COUNT(*) FROM bronze.payment) = 0 THEN RAISE EXCEPTION 'Bronze payment table is empty'; END IF;
            IF (SELECT COUNT(*) FROM bronze.rental) = 0 THEN RAISE EXCEPTION 'Bronze rental table is empty'; END IF;
            IF (SELECT COUNT(*) FROM bronze.streaming_session) = 0 THEN RAISE EXCEPTION 'Bronze streaming_session table is empty'; END IF;
        END $$;
        """,
    )

    silver_transformation = SQLExecuteQueryOperator(
        task_id="silver_transformation",
        conn_id="abc_hub_analytical",
        sql="""
        INSERT INTO silver.customer (customer_id, customer_no, first_name, last_name, email, phone, date_of_birth, gender, registration_date, status)
        SELECT customer_id, TRIM(customer_no), TRIM(first_name), TRIM(last_name), LOWER(TRIM(email)), TRIM(phone), date_of_birth, TRIM(gender), registration_date, TRIM(status)
        FROM bronze.customer
        ON CONFLICT (customer_id) DO UPDATE SET customer_no=EXCLUDED.customer_no, first_name=EXCLUDED.first_name, last_name=EXCLUDED.last_name, email=EXCLUDED.email, phone=EXCLUDED.phone, date_of_birth=EXCLUDED.date_of_birth, gender=EXCLUDED.gender, registration_date=EXCLUDED.registration_date, status=EXCLUDED.status, processed_at=CURRENT_TIMESTAMP;

        INSERT INTO silver.payment (payment_id, customer_id, rental_id, subscription_id, payment_method_id, amount, payment_date, payment_type, status, processed_at)
        SELECT payment_id, customer_id, rental_id, subscription_id, payment_method_id, amount, payment_date, TRIM(payment_type), TRIM(status), CURRENT_TIMESTAMP
        FROM bronze.payment
        ON CONFLICT (payment_id) DO UPDATE SET customer_id=EXCLUDED.customer_id, rental_id=EXCLUDED.rental_id, subscription_id=EXCLUDED.subscription_id, payment_method_id=EXCLUDED.payment_method_id, amount=EXCLUDED.amount, payment_date=EXCLUDED.payment_date, payment_type=EXCLUDED.payment_type, status=EXCLUDED.status, processed_at=CURRENT_TIMESTAMP;
        """,
    )

    silver_data_quality = SQLExecuteQueryOperator(
        task_id="silver_data_quality",
        conn_id="abc_hub_analytical",
        sql="""
        DO $$
        BEGIN
            IF EXISTS (SELECT customer_id FROM silver.customer GROUP BY customer_id HAVING COUNT(*) > 1) THEN RAISE EXCEPTION 'Duplicate customers detected'; END IF;
            IF EXISTS (SELECT 1 FROM silver.customer WHERE customer_id IS NULL OR customer_no IS NULL OR email IS NULL OR status IS NULL) THEN RAISE EXCEPTION 'Mandatory customer data is missing'; END IF;
            IF EXISTS (SELECT payment_id FROM silver.payment GROUP BY payment_id HAVING COUNT(*) > 1) THEN RAISE EXCEPTION 'Duplicate payments detected'; END IF;
        END $$;
        """,
    )

    scd_processing = SQLExecuteQueryOperator(
        task_id="scd_processing",
        conn_id="abc_hub_analytical",
        sql="""
        CREATE TEMP TABLE changed_customers ON COMMIT DROP AS
        SELECT s.customer_id FROM silver.customer s JOIN gold.dim_customer d ON d.customer_id=s.customer_id AND d.is_current=TRUE
        WHERE d.email IS DISTINCT FROM s.email OR d.phone IS DISTINCT FROM s.phone OR d.status IS DISTINCT FROM s.status;

        UPDATE gold.dim_customer d SET effective_to=CURRENT_TIMESTAMP, is_current=FALSE FROM changed_customers c
        WHERE d.customer_id=c.customer_id AND d.is_current=TRUE;

        INSERT INTO gold.dim_customer (customer_id, customer_no, first_name, last_name, email, phone, date_of_birth, gender, registration_date, status, effective_from, effective_to, is_current)
        SELECT s.customer_id, s.customer_no, s.first_name, s.last_name, s.email, s.phone, s.date_of_birth, s.gender, s.registration_date, s.status, CURRENT_TIMESTAMP, NULL, TRUE
        FROM silver.customer s JOIN changed_customers c ON c.customer_id=s.customer_id;

        INSERT INTO gold.dim_customer (customer_id, customer_no, first_name, last_name, email, phone, date_of_birth, gender, registration_date, status, effective_from, effective_to, is_current)
        SELECT s.customer_id, s.customer_no, s.first_name, s.last_name, s.email, s.phone, s.date_of_birth, s.gender, s.registration_date, s.status, CURRENT_TIMESTAMP, NULL, TRUE
        FROM silver.customer s WHERE NOT EXISTS (SELECT 1 FROM gold.dim_customer d WHERE d.customer_id=s.customer_id);
        """,
    )

    gold_data_quality = SQLExecuteQueryOperator(
        task_id="gold_data_quality",
        conn_id="abc_hub_analytical",
        sql="""
        DO $$
        BEGIN
            IF EXISTS (SELECT customer_id FROM gold.dim_customer WHERE is_current=TRUE GROUP BY customer_id HAVING COUNT(*) > 1) THEN RAISE EXCEPTION 'Multiple current SCD versions detected'; END IF;
            IF (SELECT COUNT(*) FROM gold.dim_customer WHERE is_current=TRUE) <> (SELECT COUNT(*) FROM silver.customer) THEN RAISE EXCEPTION 'Silver and Gold customer counts do not match'; END IF;
        END $$;
        """,
    )

    pipeline_summary = SQLExecuteQueryOperator(
        task_id="pipeline_summary",
        conn_id="abc_hub_analytical",
        sql="""
        SELECT '{{ run_id }}' AS airflow_run_id, '{{ ds }}' AS processing_date,
        (SELECT COUNT(*) FROM bronze.customer) AS bronze_customers,
        (SELECT COUNT(*) FROM silver.customer) AS silver_customers,
        (SELECT COUNT(*) FROM gold.dim_customer WHERE is_current=TRUE) AS current_gold_customers,
        (SELECT COUNT(*) FROM gold.dim_customer) AS total_customer_versions,
        (SELECT COUNT(*) FROM silver.payment) AS silver_payments,
        (SELECT COUNT(*) FROM silver.payment WHERE amount < 0) AS negative_payment_records;
        """,
    )

    finish = EmptyOperator(task_id="finish")
    start >> bronze_validation >> silver_transformation >> silver_data_quality >> scd_processing >> gold_data_quality >> pipeline_summary >> finish

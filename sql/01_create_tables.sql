CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE IF NOT EXISTS silver.customer (
    customer_id BIGINT PRIMARY KEY, customer_no VARCHAR(20), first_name VARCHAR(100), last_name VARCHAR(100), email VARCHAR(150), phone VARCHAR(30), date_of_birth DATE, gender VARCHAR(20), registration_date DATE, status VARCHAR(20), processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS silver.payment (
    payment_id BIGINT PRIMARY KEY, customer_id BIGINT, rental_id BIGINT, subscription_id BIGINT, payment_method_id BIGINT, amount NUMERIC(10,2), payment_date TIMESTAMP, payment_type VARCHAR(20), status VARCHAR(20), processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS gold.dim_customer (
    customer_key BIGSERIAL PRIMARY KEY, customer_id BIGINT NOT NULL, customer_no VARCHAR(20), first_name VARCHAR(100), last_name VARCHAR(100), email VARCHAR(150), phone VARCHAR(30), date_of_birth DATE, gender VARCHAR(20), registration_date DATE, status VARCHAR(20), effective_from TIMESTAMP NOT NULL, effective_to TIMESTAMP, is_current BOOLEAN NOT NULL DEFAULT TRUE
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_customer_current ON gold.dim_customer(customer_id) WHERE is_current=TRUE;
CREATE TABLE IF NOT EXISTS audit.rejected_records (
    rejection_id BIGSERIAL PRIMARY KEY, source_table VARCHAR(100) NOT NULL, record_id BIGINT, rejection_reason VARCHAR(255) NOT NULL, rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS audit.pipeline_run (
    batch_id BIGSERIAL PRIMARY KEY, dag_id VARCHAR(200), run_id VARCHAR(250), start_time TIMESTAMP, end_time TIMESTAMP, status VARCHAR(30), source_record_count BIGINT, processed_record_count BIGINT, inserted_record_count BIGINT, updated_record_count BIGINT, rejected_record_count BIGINT
);

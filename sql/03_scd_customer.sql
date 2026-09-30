BEGIN;
CREATE TEMP TABLE changed_customers ON COMMIT DROP AS
SELECT s.customer_id FROM silver.customer s JOIN gold.dim_customer d ON d.customer_id=s.customer_id AND d.is_current=TRUE
WHERE d.email IS DISTINCT FROM s.email OR d.phone IS DISTINCT FROM s.phone OR d.status IS DISTINCT FROM s.status;
UPDATE gold.dim_customer d SET effective_to=CURRENT_TIMESTAMP, is_current=FALSE FROM changed_customers c WHERE d.customer_id=c.customer_id AND d.is_current=TRUE;
INSERT INTO gold.dim_customer (customer_id, customer_no, first_name, last_name, email, phone, date_of_birth, gender, registration_date, status, effective_from, effective_to, is_current)
SELECT s.customer_id, s.customer_no, s.first_name, s.last_name, s.email, s.phone, s.date_of_birth, s.gender, s.registration_date, s.status, CURRENT_TIMESTAMP, NULL, TRUE FROM silver.customer s JOIN changed_customers c ON c.customer_id=s.customer_id;
INSERT INTO gold.dim_customer (customer_id, customer_no, first_name, last_name, email, phone, date_of_birth, gender, registration_date, status, effective_from, effective_to, is_current)
SELECT s.customer_id, s.customer_no, s.first_name, s.last_name, s.email, s.phone, s.date_of_birth, s.gender, s.registration_date, s.status, CURRENT_TIMESTAMP, NULL, TRUE FROM silver.customer s WHERE NOT EXISTS (SELECT 1 FROM gold.dim_customer d WHERE d.customer_id=s.customer_id);
COMMIT;

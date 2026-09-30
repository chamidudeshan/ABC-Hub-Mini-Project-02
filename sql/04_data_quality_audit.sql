DO $$
BEGIN
    IF EXISTS (SELECT customer_id FROM silver.customer GROUP BY customer_id HAVING COUNT(*) > 1) THEN RAISE EXCEPTION 'Duplicate customers detected'; END IF;
    IF EXISTS (SELECT 1 FROM silver.customer WHERE customer_id IS NULL OR customer_no IS NULL OR email IS NULL OR status IS NULL) THEN RAISE EXCEPTION 'Mandatory customer data is missing'; END IF;
    IF EXISTS (SELECT customer_id FROM gold.dim_customer WHERE is_current=TRUE GROUP BY customer_id HAVING COUNT(*) > 1) THEN RAISE EXCEPTION 'Multiple current SCD versions detected'; END IF;
END $$;

TRUNCATE TABLE audit.rejected_records RESTART IDENTITY;
INSERT INTO audit.rejected_records (source_table, record_id, rejection_reason)
SELECT 'payment', payment_id, 'Negative payment amount' FROM silver.payment WHERE amount < 0;

SELECT 'bronze.customer' AS object, COUNT(*) AS row_count FROM bronze.customer
UNION ALL SELECT 'silver.customer', COUNT(*) FROM silver.customer
UNION ALL SELECT 'gold.dim_customer.current', COUNT(*) FROM gold.dim_customer WHERE is_current=TRUE
UNION ALL SELECT 'bronze.payment', COUNT(*) FROM bronze.payment
UNION ALL SELECT 'silver.payment', COUNT(*) FROM silver.payment
UNION ALL SELECT 'audit.rejected_records', COUNT(*) FROM audit.rejected_records;

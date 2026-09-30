INSERT INTO silver.customer (customer_id, customer_no, first_name, last_name, email, phone, date_of_birth, gender, registration_date, status)
SELECT customer_id, TRIM(customer_no), TRIM(first_name), TRIM(last_name), LOWER(TRIM(email)), TRIM(phone), date_of_birth, TRIM(gender), registration_date, TRIM(status)
FROM bronze.customer
ON CONFLICT (customer_id) DO UPDATE SET customer_no=EXCLUDED.customer_no, first_name=EXCLUDED.first_name, last_name=EXCLUDED.last_name, email=EXCLUDED.email, phone=EXCLUDED.phone, date_of_birth=EXCLUDED.date_of_birth, gender=EXCLUDED.gender, registration_date=EXCLUDED.registration_date, status=EXCLUDED.status, processed_at=CURRENT_TIMESTAMP;

INSERT INTO silver.payment (payment_id, customer_id, rental_id, subscription_id, payment_method_id, amount, payment_date, payment_type, status, processed_at)
SELECT payment_id, customer_id, rental_id, subscription_id, payment_method_id, amount, payment_date, TRIM(payment_type), TRIM(status), CURRENT_TIMESTAMP
FROM bronze.payment
ON CONFLICT (payment_id) DO UPDATE SET customer_id=EXCLUDED.customer_id, rental_id=EXCLUDED.rental_id, subscription_id=EXCLUDED.subscription_id, payment_method_id=EXCLUDED.payment_method_id, amount=EXCLUDED.amount, payment_date=EXCLUDED.payment_date, payment_type=EXCLUDED.payment_type, status=EXCLUDED.status, processed_at=CURRENT_TIMESTAMP;

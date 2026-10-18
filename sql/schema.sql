-- ===========================================================================
-- schema.sql
-- Table definitions for the ETL pipeline (PostgreSQL 12+).
-- The Airflow task `database_setup` runs this file on every pipeline run.
-- "IF NOT EXISTS" makes it safe to run repeatedly (idempotent).
-- ===========================================================================

-- Customers: loaded first, because orders reference them.
CREATE TABLE IF NOT EXISTS customers (
    customer_id        INTEGER PRIMARY KEY,
    first_name         VARCHAR(50),
    last_name          VARCHAR(50),
    email              VARCHAR(100) UNIQUE,
    phone              VARCHAR(20),
    city               VARCHAR(50),
    registration_date  DATE,
    customer_type      VARCHAR(20),
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Products: no dependencies on other tables.
CREATE TABLE IF NOT EXISTS products (
    product_id      INTEGER PRIMARY KEY,
    product_name    VARCHAR(200),
    category        VARCHAR(50),
    brand           VARCHAR(50),
    unit_price      DECIMAL(10,2),
    stock_quantity  INTEGER,
    is_active       BOOLEAN,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Orders: customer_id is a foreign key, so every order must point
-- to an existing customer.
CREATE TABLE IF NOT EXISTS orders (
    order_id         INTEGER PRIMARY KEY,
    customer_id      INTEGER REFERENCES customers(customer_id),
    order_date       TIMESTAMP,
    order_status     VARCHAR(20),
    payment_method   VARCHAR(30),
    total_amount     DECIMAL(10,2),
    discount_amount  DECIMAL(10,2),
    shipping_cost    DECIMAL(10,2),
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Audit log: one row per pipeline run (success or failure).
CREATE TABLE IF NOT EXISTS etl_audit (
    run_id             SERIAL PRIMARY KEY,
    dag_name           VARCHAR(100),
    start_time         TIMESTAMP,
    end_time           TIMESTAMP,
    status             VARCHAR(20),
    records_processed  INTEGER
);


-- ===========================================================================
-- Sample queries (for testing and the demo; not run by the pipeline)
-- ===========================================================================

-- 1. Row counts per table (each should be 1000)
-- SELECT 'customers' AS table_name, COUNT(*) FROM customers
-- UNION ALL SELECT 'products', COUNT(*) FROM products
-- UNION ALL SELECT 'orders',   COUNT(*) FROM orders;

-- 2. Referential integrity check (should return 0)
-- SELECT COUNT(*) AS orphan_orders
-- FROM orders o LEFT JOIN customers c ON o.customer_id = c.customer_id
-- WHERE c.customer_id IS NULL;

-- 3. Top 5 customers by total spend
-- SELECT c.first_name, c.last_name, SUM(o.total_amount) AS total_spent
-- FROM orders o JOIN customers c ON o.customer_id = c.customer_id
-- GROUP BY c.customer_id, c.first_name, c.last_name
-- ORDER BY total_spent DESC LIMIT 5;

-- 4. Orders by status
-- SELECT order_status, COUNT(*) FROM orders GROUP BY order_status;

-- 5. Recent pipeline runs
-- SELECT * FROM etl_audit ORDER BY run_id DESC LIMIT 5;

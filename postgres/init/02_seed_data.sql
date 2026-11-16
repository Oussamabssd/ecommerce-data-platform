
-- =====================================================
-- E-commerce sample operational data
-- =====================================================

-- 1. CUSTOMERS
INSERT INTO customers (full_name, email, city, country)
VALUES
    ('John Smith', 'john@example.com', 'New York', 'USA'),
    ('Sarah Johnson', 'sarah@example.com', 'London', 'UK'),
    ('Ahmed Benali', 'ahmed@example.com', 'Algiers', 'Algeria'),
    ('Emma Wilson', 'emma@example.com', 'Paris', 'France'),
    ('David Brown', 'david@example.com', 'Berlin', 'Germany')
ON CONFLICT (email) DO NOTHING;

-- 2. SUPPLIERS
INSERT INTO suppliers (supplier_name, contact_email, country)
VALUES
    ('TechSource Ltd', 'contact@techsource.example', 'China'),
    ('Global Electronics', 'sales@globalelectronics.example', 'Germany'),
    ('Fashion Wholesale', 'info@fashionwholesale.example', 'Turkey')
ON CONFLICT (contact_email) DO NOTHING;

-- 3. WAREHOUSES
INSERT INTO warehouses (warehouse_code, warehouse_name, city, country)
VALUES
    ('WH-NY', 'New York Warehouse', 'New York', 'USA'),
    ('WH-LON', 'London Warehouse', 'London', 'UK')
ON CONFLICT (warehouse_code) DO NOTHING;

-- 4. PRODUCTS
INSERT INTO products (
    sku, product_name, category, supplier_id, unit_price
)
SELECT
    p.sku,
    p.product_name,
    p.category,
    s.supplier_id,
    p.unit_price
FROM (
    VALUES
        ('LAP-001', 'Laptop Pro 15', 'Electronics',
         'contact@techsource.example', 1299.99::NUMERIC),
        ('PHN-001', 'Smartphone X', 'Electronics',
         'sales@globalelectronics.example', 799.99::NUMERIC),
        ('HDP-001', 'Wireless Headphones', 'Accessories',
         'contact@techsource.example', 149.50::NUMERIC),
        ('TSH-001', 'Classic T-Shirt', 'Clothing',
         'info@fashionwholesale.example', 24.99::NUMERIC)
) AS p(sku, product_name, category, supplier_email, unit_price)
JOIN suppliers s ON s.contact_email = p.supplier_email
ON CONFLICT (sku) DO NOTHING;

-- 5. ORDERS
INSERT INTO orders (order_number, customer_id, order_status, order_date)
SELECT
    o.order_number,
    c.customer_id,
    o.order_status,
    o.order_date::TIMESTAMPTZ
FROM (
    VALUES
        ('ORD-1001', 'john@example.com', 'delivered',
         '2026-11-10 10:00:00+00'),
        ('ORD-1002', 'sarah@example.com', 'shipped',
         '2026-11-11 12:30:00+00'),
        ('ORD-1003', 'ahmed@example.com', 'confirmed',
         '2026-11-12 15:00:00+00'),
        ('ORD-1004', 'emma@example.com', 'pending',
         '2026-11-13 09:15:00+00'),
        ('ORD-1005', 'david@example.com', 'delivered',
         '2026-11-14 17:45:00+00')
) AS o(order_number, customer_email, order_status, order_date)
JOIN customers c ON c.email = o.customer_email
ON CONFLICT (order_number) DO NOTHING;

-- 6. ORDER ITEMS
INSERT INTO order_items (order_id, product_id, quantity, unit_price)
SELECT
    o.order_id,
    p.product_id,
    x.quantity,
    x.unit_price
FROM (
    VALUES
        ('ORD-1001', 'LAP-001', 1, 1299.99::NUMERIC),
        ('ORD-1001', 'HDP-001', 2, 149.50::NUMERIC),
        ('ORD-1002', 'PHN-001', 1, 799.99::NUMERIC),
        ('ORD-1003', 'TSH-001', 3, 24.99::NUMERIC),
        ('ORD-1004', 'HDP-001', 1, 149.50::NUMERIC),
        ('ORD-1005', 'LAP-001', 1, 1299.99::NUMERIC)
) AS x(order_number, sku, quantity, unit_price)
JOIN orders o ON o.order_number = x.order_number
JOIN products p ON p.sku = x.sku
ON CONFLICT (order_id, product_id) DO NOTHING;

-- 7. INVENTORY
INSERT INTO inventory (product_id, warehouse_id, quantity_available)
SELECT
    p.product_id,
    w.warehouse_id,
    x.quantity_available
FROM (
    VALUES
        ('LAP-001', 'WH-NY', 25),
        ('PHN-001', 'WH-NY', 50),
        ('HDP-001', 'WH-NY', 100),
        ('TSH-001', 'WH-NY', 200),
        ('LAP-001', 'WH-LON', 15),
        ('PHN-001', 'WH-LON', 30),
        ('HDP-001', 'WH-LON', 75),
        ('TSH-001', 'WH-LON', 150)
) AS x(sku, warehouse_code, quantity_available)
JOIN products p ON p.sku = x.sku
JOIN warehouses w ON w.warehouse_code = x.warehouse_code
ON CONFLICT (product_id, warehouse_id) DO NOTHING;

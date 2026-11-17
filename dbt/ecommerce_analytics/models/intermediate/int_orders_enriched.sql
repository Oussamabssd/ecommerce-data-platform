
WITH orders AS (

    SELECT *
    FROM {{ ref('stg_orders') }}

),

customers AS (

    SELECT *
    FROM {{ ref('stg_customers') }}

),

order_metrics AS (

    SELECT
        order_id,

        COUNT(*) AS order_item_count,

        SUM(quantity) AS total_quantity,

        COUNT(DISTINCT product_id) AS distinct_product_count,

        SUM(line_total) AS order_total

    FROM {{ ref('int_order_items_enriched') }}

    GROUP BY order_id

),

enriched AS (

    SELECT
        o.order_id,
        o.order_number,

        o.customer_id,
        c.full_name AS customer_name,
        c.email AS customer_email,
        c.city AS customer_city,
        c.country AS customer_country,

        o.order_date,
        o.order_status,

        COALESCE(m.order_item_count, 0) AS order_item_count,

        COALESCE(m.total_quantity, 0) AS total_quantity,

        COALESCE(m.distinct_product_count, 0)
            AS distinct_product_count,

        COALESCE(m.order_total, 0)::NUMBER(18, 2)
            AS order_total

    FROM orders o

    LEFT JOIN customers c
        ON o.customer_id = c.customer_id

    LEFT JOIN order_metrics m
        ON o.order_id = m.order_id

)

SELECT *
FROM enriched

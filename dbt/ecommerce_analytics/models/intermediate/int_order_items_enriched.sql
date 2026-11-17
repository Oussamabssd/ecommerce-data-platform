
WITH order_items AS (

    SELECT *
    FROM {{ ref('stg_order_items') }}

),

orders AS (

    SELECT *
    FROM {{ ref('stg_orders') }}

),

products AS (

    SELECT *
    FROM {{ ref('stg_products') }}

),

enriched AS (

    SELECT
        oi.order_item_id,
        oi.order_id,

        o.order_number,
        o.customer_id,
        o.order_date,
        o.order_status,

        oi.product_id,
        p.sku,
        p.product_name,
        p.category,

        oi.quantity,
        oi.unit_price,

        CAST(
            oi.quantity * oi.unit_price
            AS NUMBER(18, 2)
        ) AS line_total

    FROM order_items oi

    INNER JOIN orders o
        ON oi.order_id = o.order_id

    INNER JOIN products p
        ON oi.product_id = p.product_id

)

SELECT *
FROM enriched

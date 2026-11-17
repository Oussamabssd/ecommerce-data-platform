
WITH orders AS (

    SELECT *
    FROM {{ ref('int_orders_enriched') }}

)

SELECT
    order_id,
    order_number,

    MD5(TO_VARCHAR(customer_id)) AS customer_key,
    customer_id,

    order_date,
    order_status,

    order_item_count,
    total_quantity,
    distinct_product_count,

    order_total

FROM orders

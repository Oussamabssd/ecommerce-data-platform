
WITH source AS (

    SELECT *
    FROM {{ source('ecommerce_raw', 'order_items') }}

)

SELECT
    CAST("order_item_id" AS NUMBER(38, 0)) AS order_item_id,

    CAST("order_id" AS NUMBER(38, 0)) AS order_id,

    CAST("product_id" AS NUMBER(38, 0)) AS product_id,

    CAST("quantity" AS NUMBER(38, 0)) AS quantity,

    CAST("unit_price" AS NUMBER(12, 2)) AS unit_price

FROM source

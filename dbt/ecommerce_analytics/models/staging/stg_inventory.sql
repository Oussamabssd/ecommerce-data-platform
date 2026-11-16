
WITH source AS (

    SELECT *
    FROM {{ source('ecommerce_raw', 'inventory') }}

)

SELECT
    CAST("inventory_id" AS NUMBER(38, 0)) AS inventory_id,

    CAST("product_id" AS NUMBER(38, 0)) AS product_id,

    CAST("warehouse_id" AS NUMBER(38, 0)) AS warehouse_id,

    CAST("quantity_available" AS NUMBER(38, 0)) AS quantity_available,

    TO_TIMESTAMP_NTZ("updated_at", 9) AS updated_at

FROM source
